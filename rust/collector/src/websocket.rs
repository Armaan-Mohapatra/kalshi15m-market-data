use crate::book::{BookState, OrderBook};
use crate::event::{hash_bytes, utc_now, Event};
use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use ed25519_dalek::pkcs8::DecodePrivateKey as EdDecodePrivateKey;
use ed25519_dalek::Signer as EdSigner;
use futures_util::{SinkExt, StreamExt};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async_with_config, WebSocketStream};
use uuid::Uuid;

pub const KALSHI_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const KALSHI_WS_PATH: &str = "/trade-api/ws/v2";

/// Produces Kalshi handshake headers from a private key supplied explicitly by
/// the staging operator. The caller owns key discovery; this module never
/// searches environment variables or Phoenix authority paths.
pub fn authentication_headers(
    key_id: &str,
    pem: &str,
    timestamp_ms: u64,
) -> Result<[(&'static str, String); 3]> {
    if key_id.trim().is_empty()
        || key_id.len() > 256
        || key_id.contains('\r')
        || key_id.contains('\n')
    {
        bail!("invalid Kalshi API key identifier")
    }
    let pre_sign = format!("{timestamp_ms}GET{KALSHI_WS_PATH}");
    let signature = if pem.contains("BEGIN PRIVATE KEY") && !pem.contains("BEGIN RSA PRIVATE KEY") {
        if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            BASE64.encode(key.sign(pre_sign.as_bytes()).to_bytes())
        } else if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
            sign_rsa(&key, pre_sign.as_bytes())?
        } else {
            bail!("private key PEM is neither supported Ed25519 nor RSA PKCS#8")
        }
    } else {
        let key = RsaPrivateKey::from_pkcs1_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(pem))
            .context("parse RSA private key PEM")?;
        sign_rsa(&key, pre_sign.as_bytes())?
    };
    Ok([
        ("KALSHI-ACCESS-KEY", key_id.to_owned()),
        ("KALSHI-ACCESS-SIGNATURE", signature),
        ("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string()),
    ])
}

fn authenticated_request(
    url: &str,
    key_id: &str,
    pem: &str,
    timestamp_ms: u64,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let auth = authentication_headers(key_id, pem, timestamp_ms)?;
    let mut request = url.into_client_request()?;
    for (name, value) in auth {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    Ok(request)
}

fn sign_rsa(key: &RsaPrivateKey, message: &[u8]) -> Result<String> {
    let signing_key = rsa::pss::SigningKey::<Sha256>::new_with_salt_len(key.clone(), 32);
    let signature = signing_key
        .sign_with_rng(&mut rand::thread_rng(), message)
        .to_vec();
    Ok(BASE64.encode(signature))
}

fn malformed_frame_event(
    engine: &WebsocketEngine,
    raw: &[u8],
    encoding: &str,
    error: &str,
) -> Event {
    Event::new(
        "websocket_raw",
        "malformed_frame",
        None,
        json!({
            "session_id":engine.session(),
            "received_utc":utc_now(),
            "encoding":encoding,
            "raw_hex":hex::encode(raw),
            "parse_error":error,
        }),
        Some(format!(
            "ws-malformed|{}|{}",
            engine.session(),
            hash_bytes(raw)
        )),
    )
}

fn subscription_command(market_tickers: &[String]) -> Value {
    json!({
        "id":1,
        "cmd":"subscribe",
        "params":{"channels":["orderbook_delta","trade"],"market_tickers":market_tickers}
    })
}

fn snapshot_recovery_command(id: u64, sid: i64, market_tickers: &[String]) -> Value {
    json!({
        "id":id,
        "cmd":"update_subscription",
        "params":{
            "sids":[sid],
            "market_tickers":market_tickers,
            "action":"get_snapshot"
        }
    })
}

fn register_subscription_ack(
    engine: &mut WebsocketEngine,
    frame: &Value,
    market_tickers: &[String],
) {
    if frame["type"] == "subscribed" && frame["msg"]["channel"] == "orderbook_delta" {
        if let Some(sid) = frame["msg"]["sid"].as_i64() {
            engine.register_orderbook_subscription(sid, market_tickers);
        }
    }
}

async fn process_text_frame(
    engine: &mut WebsocketEngine,
    raw: &str,
    events: &mpsc::Sender<Event>,
) -> Result<Vec<i64>> {
    let mut parsed: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(error) => {
            let event = malformed_frame_event(engine, raw.as_bytes(), "utf8", &error.to_string());
            events
                .send(event)
                .await
                .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
            return Err(error).context("invalid WebSocket JSON frame");
        }
    };
    if let Some(object) = parsed.as_object_mut() {
        object.insert("_collector_received_utc".into(), json!(utc_now()));
    }
    let handled = engine.handle(parsed);
    let recovery_sids = handled
        .iter()
        .filter(|event| event.kind == "sequence_gap")
        .filter_map(|event| event.payload.get("sid").and_then(Value::as_i64))
        .filter(|sid| engine.needs_recovery_request(*sid))
        .collect::<Vec<_>>();
    for event in handled {
        events
            .send(event)
            .await
            .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
    }
    Ok(recovery_sids)
}

async fn process_binary_frame(
    engine: &mut WebsocketEngine,
    raw: &[u8],
    events: &mpsc::Sender<Event>,
) -> Result<Vec<i64>> {
    let mut parsed: Value = match serde_json::from_slice(raw) {
        Ok(value) => value,
        Err(error) => {
            let event = malformed_frame_event(engine, raw, "binary", &error.to_string());
            events
                .send(event)
                .await
                .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
            return Err(error).context("invalid binary WebSocket JSON frame");
        }
    };
    if let Some(object) = parsed.as_object_mut() {
        object.insert("_collector_received_utc".into(), json!(utc_now()));
    }
    let handled = engine.handle(parsed);
    let recovery_sids = handled
        .iter()
        .filter(|event| event.kind == "sequence_gap")
        .filter_map(|event| event.payload.get("sid").and_then(Value::as_i64))
        .filter(|sid| engine.needs_recovery_request(*sid))
        .collect::<Vec<_>>();
    for event in handled {
        events
            .send(event)
            .await
            .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
    }
    Ok(recovery_sids)
}

async fn recover_gapped_subscriptions<S>(
    socket: &mut WebSocketStream<S>,
    engine: &mut WebsocketEngine,
    events: &mpsc::Sender<Event>,
    recovery_sids: Vec<i64>,
    next_command_id: &mut u64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for sid in recovery_sids {
        let markets = engine.subscription_markets(sid);
        if markets.is_empty() {
            engine.mark_recovery_requested(sid);
            events
                .send(Event::new(
                    "websocket_health",
                    "sequence_gap_unrecoverable",
                    None,
                    json!({"sid":sid,"session_id":engine.session(),
                        "reason":"no subscribed orderbook markets are known",
                        "intermediate_deltas":"UNRECOVERED"}),
                    Some(format!("ws-gap-unrecoverable|{}|{}", engine.session(), sid)),
                ))
                .await
                .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
            continue;
        }

        let command_id = *next_command_id;
        *next_command_id = command_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("WebSocket command ID exhausted"))?;
        let request = snapshot_recovery_command(command_id, sid, &markets);
        socket
            .send(Message::Text(request.to_string().into()))
            .await
            .context("request fresh orderbook snapshots after sequence gap")?;
        engine.mark_recovery_requested(sid);
        events
            .send(Event::new(
                "websocket_health",
                "snapshot_recovery_requested",
                None,
                json!({"sid":sid,"market_tickers":markets,
                    "session_id":engine.session(),"request_id":command_id,
                    "action":"get_snapshot"}),
                Some(format!(
                    "ws-snapshot-request|{}|{}|{}",
                    engine.session(),
                    sid,
                    command_id
                )),
            ))
            .await
            .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct WebSocketConfigStage {
    pub url: String,
    pub key_id: String,
    pub private_key_path: PathBuf,
    pub market_tickers: Vec<String>,
    pub allow_network: bool,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub output_capacity: usize,
}

impl WebSocketConfigStage {
    pub fn validate(&self) -> Result<()> {
        self.validate_with_loopback(false)
    }

    fn validate_with_loopback(&self, allow_loopback: bool) -> Result<()> {
        if !self.allow_network {
            bail!("WebSocket access is disabled without the explicit operational gate")
        }
        let is_loopback_mock = allow_loopback
            && self.url.starts_with("ws://127.0.0.1:")
            && self.url.ends_with(KALSHI_WS_PATH);
        if self.url != KALSHI_WS_URL && !is_loopback_mock {
            bail!("WebSocket URL must be the documented Kalshi production market-data endpoint")
        }
        if self.key_id.trim().is_empty() || self.private_key_path.as_os_str().is_empty() {
            bail!("explicit API key ID and private-key path are required")
        }
        if self.market_tickers.is_empty() || self.market_tickers.len() > 1000 {
            bail!("WebSocket requires 1..=1000 explicit market tickers")
        }
        if self.output_capacity == 0 || self.output_capacity > 100_000 {
            bail!("WebSocket event queue capacity outside 1..=100000")
        }
        if self.reconnect_initial.is_zero() || self.reconnect_max < self.reconnect_initial {
            bail!("invalid WebSocket reconnect backoff bounds")
        }
        for ticker in &self.market_tickers {
            if ticker.is_empty()
                || ticker.len() > 128
                || ticker.contains('\r')
                || ticker.contains('\n')
            {
                bail!("invalid WebSocket market ticker")
            }
        }
        Ok(())
    }
}

/// Authenticated transport. Construction is intentionally separate from the
/// replay engine, and this run loop is never invoked by ordinary CLI startup.
pub struct AuthenticatedWebSocket {
    config: WebSocketConfigStage,
    #[cfg(test)]
    allow_loopback_mock: bool,
}

impl AuthenticatedWebSocket {
    pub fn new(config: WebSocketConfigStage) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            #[cfg(test)]
            allow_loopback_mock: false,
        })
    }

    #[cfg(test)]
    fn new_loopback_mock(config: WebSocketConfigStage) -> Result<Self> {
        config.validate_with_loopback(true)?;
        Ok(Self {
            config,
            allow_loopback_mock: true,
        })
    }

    /// Reconnects with bounded exponential backoff, resubscribes on each new
    /// socket, and emits every raw frame and derived health event through a
    /// bounded queue. Queue pressure blocks reads and therefore propagates
    /// backpressure instead of silently dropping exchange frames.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
        events: mpsc::Sender<Event>,
    ) -> Result<()> {
        #[cfg(test)]
        self.config
            .validate_with_loopback(self.allow_loopback_mock)?;
        #[cfg(not(test))]
        self.config.validate()?;
        let pem = tokio::fs::read_to_string(&self.config.private_key_path)
            .await
            .context("read explicitly configured WebSocket private key")?;
        let mut engine = WebsocketEngine::new();
        let mut delay = self.config.reconnect_initial;
        loop {
            if *shutdown.borrow() || events.is_closed() {
                return Ok(());
            }
            match self
                .run_session(&pem, &mut engine, &mut shutdown, &events)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let mut failure = Event::new(
                        "websocket_health",
                        "connection_interrupted",
                        None,
                        json!({"error":error.to_string(),"next_action":"RECONNECT_AND_RESUBSCRIBE_WITH_FRESH_SNAPSHOT"}),
                        Some(format!("ws-transport-error|{}", Uuid::new_v4().simple())),
                    );
                    failure.exchange_ts = None;
                    events
                        .send(failure)
                        .await
                        .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
                    engine.reconnect();
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                        }
                    }
                    delay = delay.saturating_mul(2).min(self.config.reconnect_max);
                }
            }
        }
    }

    async fn run_session(
        &self,
        pem: &str,
        engine: &mut WebsocketEngine,
        shutdown: &mut watch::Receiver<bool>,
        events: &mpsc::Sender<Event>,
    ) -> Result<()> {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        let request =
            authenticated_request(&self.config.url, &self.config.key_id, pem, timestamp_ms)?;
        let ws_config = WebSocketConfig::default();
        let (mut socket, _) = tokio::time::timeout(
            Duration::from_secs(20),
            connect_async_with_config(request, Some(ws_config), false),
        )
        .await
        .context("WebSocket handshake timeout")?
        .context("authenticated Kalshi WebSocket handshake failed")?;
        let subscription = subscription_command(&self.config.market_tickers);
        socket
            .send(Message::Text(subscription.to_string().into()))
            .await?;
        let mut next_command_id = 2_u64;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = socket.close(None).await;
                        return Ok(());
                    }
                }
                next = socket.next() => match next {
                    Some(Ok(Message::Text(raw))) => {
                        if let Ok(frame) = serde_json::from_str::<Value>(&raw) {
                            register_subscription_ack(
                                engine,
                                &frame,
                                &self.config.market_tickers,
                            );
                        }
                        let recovery_sids = process_text_frame(engine, &raw, events).await?;
                        recover_gapped_subscriptions(
                            &mut socket,
                            engine,
                            events,
                            recovery_sids,
                            &mut next_command_id,
                        )
                        .await?;
                    }
                    Some(Ok(Message::Binary(raw))) => {
                        if let Ok(frame) = serde_json::from_slice::<Value>(&raw) {
                            register_subscription_ack(
                                engine,
                                &frame,
                                &self.config.market_tickers,
                            );
                        }
                        let recovery_sids = process_binary_frame(engine, &raw, events).await?;
                        recover_gapped_subscriptions(
                            &mut socket,
                            engine,
                            events,
                            recovery_sids,
                            &mut next_command_id,
                        )
                        .await?;
                    }
                    Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Ok(Message::Close(frame))) => return Err(anyhow!("WebSocket peer closed: {frame:?}")),
                    Some(Ok(_)) => {},
                    Some(Err(error)) => return Err(error.into()),
                    None => return Err(anyhow!("WebSocket stream ended")),
                }
            }
        }
    }
}

/// Sequence state belongs to the subscription, NOT the market. A first
/// snapshot may carry seq=2; interleaved markets share their subscription ID.
#[derive(Debug, Default)]
pub struct SubscriptionState {
    last_seq: Option<i64>,
    book_markets: Vec<String>,
    gap: Option<(i64, i64)>,
    resync_pending: bool,
    recovery_requested: bool,
}

#[derive(Debug)]
pub struct WebsocketEngine {
    session: String,
    subscriptions: BTreeMap<i64, SubscriptionState>,
    books: HashMap<String, OrderBook>,
}

impl WebsocketEngine {
    pub fn new() -> Self {
        Self {
            session: Uuid::new_v4().simple().to_string(),
            subscriptions: BTreeMap::new(),
            books: HashMap::new(),
        }
    }
    pub fn with_session(session: &str) -> Self {
        Self {
            session: session.to_owned(),
            subscriptions: BTreeMap::new(),
            books: HashMap::new(),
        }
    }
    pub fn session(&self) -> &str {
        &self.session
    }
    pub fn reconnect(&mut self) {
        self.session = Uuid::new_v4().simple().to_string();
        self.subscriptions.clear();
        for book in self.books.values_mut() {
            book.invalidate("connection replaced");
        }
    }
    pub fn register_orderbook_subscription(&mut self, sid: i64, markets: &[String]) {
        let state = self.subscriptions.entry(sid).or_default();
        for market in markets {
            if !state.book_markets.contains(market) {
                state.book_markets.push(market.clone());
            }
        }
    }
    pub fn subscription_markets(&self, sid: i64) -> Vec<String> {
        self.subscriptions
            .get(&sid)
            .map(|state| state.book_markets.clone())
            .unwrap_or_default()
    }
    #[cfg(test)]
    fn recovery_pending(&self, sid: i64) -> bool {
        self.subscriptions
            .get(&sid)
            .is_some_and(|state| state.resync_pending)
    }
    fn needs_recovery_request(&self, sid: i64) -> bool {
        self.subscriptions
            .get(&sid)
            .is_some_and(|state| state.resync_pending && !state.recovery_requested)
    }
    fn mark_recovery_requested(&mut self, sid: i64) {
        if let Some(state) = self.subscriptions.get_mut(&sid) {
            state.recovery_requested = true;
        }
    }
    pub fn book(&self, ticker: &str) -> Option<&OrderBook> {
        self.books.get(ticker)
    }

    pub fn handle(&mut self, message: Value) -> Vec<Event> {
        // Replay timestamps are source evidence, not the time the fixture
        // happened to be reprocessed. Unknown historic times stay unknown.
        let observed = message
            .get("_collector_received_utc")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN_REPLAY_CAPTURE_TIME")
            .to_string();
        let kind = message
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let sid = message.get("sid").and_then(Value::as_i64);
        let seq = message.get("seq").and_then(Value::as_i64);
        let msg = message.get("msg").cloned().unwrap_or(Value::Null);
        let ticker = msg
            .get("market_ticker")
            .and_then(Value::as_str)
            .map(str::to_string);
        let raw_hash = hash_bytes(
            serde_json::to_string(&message)
                .unwrap_or_default()
                .as_bytes(),
        );
        let key = match (sid, seq) {
            (Some(sid), Some(seq)) => {
                format!("ws|{}|{}|{}|{}|{}", self.session, sid, seq, kind, raw_hash)
            }
            _ => format!("ws|{}|unsequenced|{}|{}", self.session, kind, raw_hash),
        };
        let mut original = Event::new(
            "websocket_raw",
            &kind,
            ticker.as_deref(),
            json!({"raw":message,"session_id":self.session,"received_utc":observed}),
            Some(key),
        );
        original.exchange_ts = msg.get("ts").and_then(Value::as_str).map(str::to_string);
        let mut out = vec![original];
        if let (Some(sid), Some(seq)) = (sid, seq) {
            let state = self.subscriptions.entry(sid).or_default();
            if let Some(last) = state.last_seq {
                if seq != last + 1 {
                    state.gap = Some((last, seq));
                    if !state.resync_pending {
                        state.resync_pending = true;
                        state.recovery_requested = false;
                    }
                    for market in &state.book_markets {
                        if let Some(book) = self.books.get_mut(market) {
                            book.invalidate("subscription sequence gap");
                        }
                    }
                    out.push(Event::new(
                        "websocket_health",
                        "sequence_gap",
                        ticker.as_deref(),
                        json!({"sid":sid,"prior_seq":last,"new_seq":seq,
                            "session_id":self.session,"intermediate_deltas":"UNRECOVERED"}),
                        Some(format!("ws-gap|{}|{}|{}|{}", self.session, sid, last, seq)),
                    ));
                }
            }
            state.last_seq = Some(seq);
            if let Some(ticker_name) = &ticker {
                if !state.book_markets.contains(ticker_name)
                    && (kind == "orderbook_snapshot" || kind == "orderbook_delta")
                {
                    state.book_markets.push(ticker_name.clone());
                }
            }
        }
        if let Some(ticker_name) = ticker {
            if kind == "orderbook_snapshot" {
                let snapshot_result = self
                    .books
                    .entry(ticker_name.clone())
                    .or_default()
                    .snapshot(&msg);
                match snapshot_result {
                    Ok(()) => {
                        out.push(Event::new("websocket_health","book_synchronized",
                            Some(&ticker_name),
                            json!({"session_id":self.session,"sid":sid,"seq":seq,
                                "state":"SYNCHRONIZED_AFTER_FRESH_SNAPSHOT","observed_utc":observed}),
                            Some(format!("ws-sync|{}|{}|{}",self.session,sid.unwrap_or(-1),seq.unwrap_or(-1)))));
                        if let Some(sid) = sid {
                            let all_synced = self.subscriptions.get(&sid).is_some_and(|state| {
                                state.resync_pending
                                    && !state.book_markets.is_empty()
                                    && state.book_markets.iter().all(|market| {
                                        self.books
                                            .get(market)
                                            .is_some_and(|book| book.synchronized())
                                    })
                            });
                            if all_synced {
                                if let Some(state) = self.subscriptions.get_mut(&sid) {
                                    state.resync_pending = false;
                                    state.recovery_requested = false;
                                    state.gap = None;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        out.push(Event::new(
                            "websocket_health",
                            "invalid_snapshot",
                            Some(&ticker_name),
                            json!({"session_id":self.session,"error_type":"INVALID_SNAPSHOT",
                                "detail":error.to_string(),"observed_utc":observed}),
                            Some(format!("ws-invalid-snapshot|{}|{}", self.session, raw_hash)),
                        ));
                    }
                }
            } else if kind == "orderbook_delta" {
                let book = self.books.entry(ticker_name.clone()).or_default();
                if let Err(error) = book.delta(&msg) {
                    book.invalidate(format!("delta rejected: {error}"));
                    out.push(Event::new(
                        "websocket_health",
                        "delta_not_applied",
                        Some(&ticker_name),
                        json!({"session_id":self.session,"reason":error.to_string(),
                            "state":"UNSYNCHRONIZED","observed_utc":observed}),
                        Some(format!("ws-delta-rejected|{}|{}", self.session, raw_hash)),
                    ));
                }
            }
        }
        out
    }

    pub fn state_summary(&self) -> Value {
        let healthy = self
            .books
            .values()
            .filter(|b| b.state() == &BookState::Synchronized)
            .count();
        json!({"session":self.session,"subscriptions":self.subscriptions.len(),
            "books":self.books.len(),"synchronized_books":healthy})
    }
}
impl Default for WebsocketEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::pkcs8::EncodePrivateKey as EdEncodePrivateKey;
    use rand::rngs::OsRng;
    use rsa::signature::Verifier;
    use tokio::net::TcpListener;

    #[test]
    fn first_sequence_two_is_not_a_gap_and_interleaving_valid() {
        let mut ws = WebsocketEngine::with_session("a");
        let a = ws.handle(json!({"type":"orderbook_snapshot","sid":7,"seq":2,
            "msg":{"market_ticker":"A","yes_dollars_fp":[["0.5","2"]]}}));
        assert!(!a.iter().any(|x| x.kind == "sequence_gap"));
        let b = ws.handle(json!({"type":"orderbook_snapshot","sid":7,"seq":3,
            "msg":{"market_ticker":"B","yes_dollars_fp":[]}}));
        assert!(!b.iter().any(|x| x.kind == "sequence_gap"));
        let c = ws.handle(json!({"type":"orderbook_delta","sid":7,"seq":4,
            "msg":{"market_ticker":"A","side":"yes","price_dollars":"0.5","delta_fp":"1"}}));
        assert!(!c.iter().any(|x| x.kind == "sequence_gap"));
        assert!(ws.book("A").unwrap().synchronized());
    }
    #[test]
    fn gap_invalidates_books_until_new_snapshot() {
        let mut ws = WebsocketEngine::with_session("a");
        ws.handle(json!({"type":"orderbook_snapshot","sid":5,"seq":2,
            "msg":{"market_ticker":"A","yes_dollars_fp":[["0.5","2"]]}}));
        ws.handle(json!({"type":"orderbook_snapshot","sid":5,"seq":3,
            "msg":{"market_ticker":"B","yes_dollars_fp":[["0.5","2"]]}}));
        let gap = ws.handle(json!({"type":"orderbook_delta","sid":5,"seq":5,
            "msg":{"market_ticker":"A","side":"yes","price_dollars":"0.5","delta_fp":"1"}}));
        assert!(gap.iter().any(|e| e.kind == "sequence_gap"));
        assert!(!ws.book("A").unwrap().synchronized());
        assert!(!ws.book("B").unwrap().synchronized());
        ws.handle(json!({"type":"orderbook_snapshot","sid":5,"seq":6,
            "msg":{"market_ticker":"A","yes_dollars_fp":[]}}));
        assert!(ws.book("A").unwrap().synchronized());
        assert!(!ws.book("B").unwrap().synchronized());
    }
    #[test]
    fn session_reset_changes_raw_dedupe_identity() {
        let input = json!({"type":"trade","sid":2,"seq":2,"msg":{"market_ticker":"T"}});
        let mut ws = WebsocketEngine::with_session("a");
        let a = ws.handle(input.clone())[0].key().unwrap();
        ws.reconnect();
        let b = ws.handle(input)[0].key().unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn kalshi_ed25519_auth_header_signs_exact_handshake_payload() {
        let key = ed25519_dalek::SigningKey::generate(&mut OsRng);
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let headers = authentication_headers("test-key", pem.as_str(), 1_700_000_000_123).unwrap();
        let payload = b"1700000000123GET/trade-api/ws/v2";
        let signature = BASE64.decode(&headers[1].1).unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&signature).unwrap();
        key.verifying_key().verify(payload, &signature).unwrap();
        assert_eq!(headers[0], ("KALSHI-ACCESS-KEY", "test-key".to_owned()));
        assert_eq!(headers[2].1, "1700000000123");
    }

    #[test]
    fn kalshi_rsa_auth_header_uses_pss_sha256_digest_salt() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let headers = authentication_headers("test-key", pem.as_str(), 42).unwrap();
        let signature = BASE64.decode(&headers[1].1).unwrap();
        let verifying_key =
            rsa::pss::VerifyingKey::<Sha256>::new_with_salt_len(key.to_public_key(), 32);
        let signature = rsa::pss::Signature::try_from(signature.as_slice()).unwrap();
        verifying_key
            .verify(b"42GET/trade-api/ws/v2", &signature)
            .unwrap();
    }

    #[test]
    fn authenticated_transport_requires_explicit_gate_and_official_endpoint() {
        let base = WebSocketConfigStage {
            url: KALSHI_WS_URL.to_owned(),
            key_id: "staging-key".to_owned(),
            private_key_path: PathBuf::from("staging-only.pem"),
            market_tickers: vec!["KXBTC15M-TEST".to_owned()],
            allow_network: false,
            reconnect_initial: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            output_capacity: 512,
        };
        assert!(AuthenticatedWebSocket::new(base.clone()).is_err());
        let mut enabled = base;
        enabled.allow_network = true;
        enabled.url = "wss://attacker.invalid/trade-api/ws/v2".to_owned();
        assert!(AuthenticatedWebSocket::new(enabled).is_err());
    }

    #[test]
    fn malformed_frames_retain_bytes_and_connection_identity() {
        let engine = WebsocketEngine::with_session("fixture-connection");
        let event = malformed_frame_event(&engine, b"{bad", "utf8", "test parse failure");
        assert_eq!(event.kind, "malformed_frame");
        assert_eq!(event.payload["raw_hex"], "7b626164");
        assert_eq!(event.payload["session_id"], "fixture-connection");
        assert!(event.dedupe_key.unwrap().contains("fixture-connection"));
    }

    #[test]
    fn subscription_command_matches_documented_market_data_shape() {
        let tickers = vec!["KXBTC15M-A".to_owned(), "KXBTC15M-B".to_owned()];
        let command = subscription_command(&tickers);
        assert_eq!(command["cmd"], "subscribe");
        assert_eq!(
            command["params"]["channels"],
            json!(["orderbook_delta", "trade"])
        );
        assert_eq!(command["params"]["market_tickers"], json!(tickers));
        let recovery = snapshot_recovery_command(2, 7, &tickers);
        assert_eq!(recovery["cmd"], "update_subscription");
        assert_eq!(recovery["params"]["action"], "get_snapshot");
        assert_eq!(recovery["params"]["sids"], json!([7]));
        assert_eq!(recovery["params"]["market_tickers"], json!(tickers));
    }

    #[tokio::test]
    async fn fixture_frames_detect_gap_and_resynchronize_only_on_snapshot() {
        let mut engine = WebsocketEngine::with_session("recorded-connection");
        let (tx, mut rx) = mpsc::channel(32);
        assert!(process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_snapshot","sid":3,"seq":17,"msg":{"market_ticker":"KXBTC15M-A","yes_dollars_fp":[]}}"#,
            &tx,
        ).await.unwrap().is_empty());
        assert!(engine.book("KXBTC15M-A").unwrap().synchronized());
        let raw = rx.recv().await.unwrap();
        assert!(
            raw.payload["raw"]["_collector_received_utc"]
                .as_str()
                .unwrap()
                .len()
                > 10
        );

        let gap_sids = process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_delta","sid":3,"seq":19,"msg":{"market_ticker":"KXBTC15M-A","side":"yes","price_dollars":"0.5","delta_fp":"1"}}"#,
            &tx,
        ).await.unwrap();
        assert_eq!(gap_sids, vec![3]);
        assert_eq!(engine.subscription_markets(3), vec!["KXBTC15M-A"]);
        assert!(!engine.book("KXBTC15M-A").unwrap().synchronized());
        let mut saw_gap = false;
        while let Ok(event) = rx.try_recv() {
            saw_gap |= event.kind == "sequence_gap";
        }
        assert!(saw_gap);

        assert!(process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_snapshot","sid":3,"seq":20,"msg":{"market_ticker":"KXBTC15M-A","yes_dollars_fp":[]}}"#,
            &tx,
        ).await.unwrap().is_empty());
        assert!(engine.book("KXBTC15M-A").unwrap().synchronized());
        assert!(!engine.recovery_pending(3));
    }

    #[tokio::test]
    async fn authenticated_loopback_requests_snapshot_after_gap_and_recovers_after_reconnect(
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = format!("ws://{address}{KALSHI_WS_PATH}");
        let server = tokio::spawn(async move {
            for (connection, sequence) in [(0, 17), (1, 4)] {
                let (stream, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
                let mut socket = tokio_tungstenite::accept_async(stream).await?;

                let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
                    .await?
                    .ok_or_else(|| anyhow!("client closed before subscribing"))??;
                let Message::Text(subscription) = message else {
                    bail!("client did not send a text subscription")
                };
                let subscription: Value = serde_json::from_str(&subscription)?;
                if subscription["cmd"] != "subscribe"
                    || subscription["params"]["market_tickers"]
                        != json!(["KXBTC15M-MOCK", "KXBTC15M-MOCK-B"])
                {
                    bail!("unexpected subscription after connection {connection}")
                }
                let subscribed = json!({
                    "type":"subscribed",
                    "msg":{"channel":"orderbook_delta","sid":7}
                });
                socket
                    .send(Message::Text(subscribed.to_string().into()))
                    .await?;
                let snapshot = json!({
                    "type":"orderbook_snapshot",
                    "sid":7,
                    "seq":sequence,
                    "msg":{"market_ticker":"KXBTC15M-MOCK","yes_dollars_fp":[]}
                });
                socket
                    .send(Message::Text(snapshot.to_string().into()))
                    .await?;
                let second_snapshot = json!({
                    "type":"orderbook_snapshot",
                    "sid":7,
                    "seq":sequence+1,
                    "msg":{"market_ticker":"KXBTC15M-MOCK-B","yes_dollars_fp":[]}
                });
                socket
                    .send(Message::Text(second_snapshot.to_string().into()))
                    .await?;
                if connection == 0 {
                    let gap_delta = json!({
                        "type":"orderbook_delta",
                        "sid":7,
                        "seq":sequence+3,
                        "msg":{"market_ticker":"KXBTC15M-MOCK","side":"yes",
                            "price_dollars":"0.5","delta_fp":"1"}
                    });
                    socket
                        .send(Message::Text(gap_delta.to_string().into()))
                        .await?;
                    let recovery = tokio::time::timeout(Duration::from_secs(3), socket.next())
                        .await?
                        .ok_or_else(|| anyhow!("client did not request a fresh snapshot"))??;
                    let Message::Text(recovery) = recovery else {
                        bail!("client recovery request was not a text command")
                    };
                    let recovery: Value = serde_json::from_str(&recovery)?;
                    if recovery["cmd"] != "update_subscription"
                        || recovery["params"]["action"] != "get_snapshot"
                        || recovery["params"]["sids"] != json!([7])
                        || recovery["params"]["market_tickers"]
                            != json!(["KXBTC15M-MOCK", "KXBTC15M-MOCK-B"])
                    {
                        bail!("unexpected snapshot recovery request: {recovery}")
                    }
                    let fresh_snapshot = json!({
                        "type":"orderbook_snapshot",
                        "sid":7,
                        "seq":sequence+4,
                        "msg":{"market_ticker":"KXBTC15M-MOCK","yes_dollars_fp":[]}
                    });
                    socket
                        .send(Message::Text(fresh_snapshot.to_string().into()))
                        .await?;
                    let fresh_second_snapshot = json!({
                        "type":"orderbook_snapshot",
                        "sid":7,
                        "seq":sequence+5,
                        "msg":{"market_ticker":"KXBTC15M-MOCK-B","yes_dollars_fp":[]}
                    });
                    socket
                        .send(Message::Text(fresh_second_snapshot.to_string().into()))
                        .await?;
                    socket.close(None).await?;
                } else {
                    while let Some(message) = socket.next().await {
                        if matches!(message?, Message::Close(_)) {
                            break;
                        }
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let temp = tempfile::tempdir().unwrap();
        let key = ed25519_dalek::SigningKey::generate(&mut OsRng);
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let signed_request =
            authenticated_request(&url, "offline-test-key", pem.as_str(), 1_700_000_000_123)
                .unwrap();
        assert_eq!(
            signed_request.headers()["KALSHI-ACCESS-KEY"],
            "offline-test-key"
        );
        let signature = BASE64
            .decode(signed_request.headers()["KALSHI-ACCESS-SIGNATURE"].as_bytes())
            .unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&signature).unwrap();
        key.verifying_key()
            .verify(
                format!("1700000000123GET{KALSHI_WS_PATH}").as_bytes(),
                &signature,
            )
            .unwrap();
        let key_path = temp.path().join("loopback-test-key.pem");
        tokio::fs::write(&key_path, pem.as_bytes()).await.unwrap();
        let transport = AuthenticatedWebSocket::new_loopback_mock(WebSocketConfigStage {
            url,
            key_id: "offline-test-key".into(),
            private_key_path: key_path,
            market_tickers: vec!["KXBTC15M-MOCK".into(), "KXBTC15M-MOCK-B".into()],
            allow_network: true,
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(20),
            output_capacity: 32,
        })
        .unwrap();
        let (stop_tx, stop_rx) = watch::channel(false);
        let (events_tx, mut events_rx) = mpsc::channel(32);
        let client = tokio::spawn(async move { transport.run(stop_rx, events_tx).await });

        let mut events = Vec::new();
        let mut synchronized = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events_rx.recv().await {
                if event.kind == "book_synchronized" {
                    synchronized += 1;
                }
                events.push(event);
                if synchronized == 6 {
                    let _ = stop_tx.send(true);
                    break;
                }
            }
        })
        .await
        .expect("gap recovery and reconnect should obtain fresh snapshots for both markets");
        tokio::time::timeout(Duration::from_secs(3), client)
            .await
            .expect("client should stop promptly")??;
        server.await??;

        let snapshots: Vec<_> = events
            .iter()
            .filter(|event| event.channel == "websocket_raw" && event.kind == "orderbook_snapshot")
            .collect();
        assert_eq!(snapshots.len(), 6);
        let sessions: std::collections::BTreeSet<_> = snapshots
            .iter()
            .map(|event| event.payload["session_id"].as_str().unwrap())
            .collect();
        assert_eq!(
            sessions.len(),
            2,
            "reconnect must use a new connection identity"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "connection_interrupted")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "sequence_gap")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "snapshot_recovery_requested")
                .count(),
            1
        );
        Ok(())
    }
}
