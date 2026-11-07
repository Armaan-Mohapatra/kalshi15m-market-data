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
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
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

async fn process_text_frame(
    engine: &mut WebsocketEngine,
    raw: &str,
    events: &mpsc::Sender<Event>,
) -> Result<()> {
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
    for event in engine.handle(parsed) {
        events
            .send(event)
            .await
            .map_err(|_| anyhow!("WebSocket event receiver closed"))?;
    }
    Ok(())
}

async fn process_binary_frame(
    engine: &mut WebsocketEngine,
    raw: &[u8],
    events: &mpsc::Sender<Event>,
) -> Result<()> {
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
    for event in engine.handle(parsed) {
        events
            .send(event)
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
        if !self.allow_network {
            bail!("WebSocket access is disabled without the explicit operational gate")
        }
        if self.url != KALSHI_WS_URL {
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
}

impl AuthenticatedWebSocket {
    pub fn new(config: WebSocketConfigStage) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
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
        let auth = authentication_headers(&self.config.key_id, pem, timestamp_ms)?;
        let mut request = self.config.url.as_str().into_client_request()?;
        for (name, value) in auth {
            request.headers_mut().insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(&value)?,
            );
        }
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
                        process_text_frame(engine, &raw, events).await?;
                    }
                    Some(Ok(Message::Binary(raw))) => {
                        process_binary_frame(engine, &raw, events).await?;
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
                let book = self.books.entry(ticker_name.clone()).or_default();
                match book.snapshot(&msg) {
                    Ok(()) => {
                        out.push(Event::new("websocket_health","book_synchronized",
                            Some(&ticker_name),
                            json!({"session_id":self.session,"sid":sid,"seq":seq,
                                "state":"SYNCHRONIZED_AFTER_FRESH_SNAPSHOT","observed_utc":observed}),
                            Some(format!("ws-sync|{}|{}|{}",self.session,sid.unwrap_or(-1),seq.unwrap_or(-1)))));
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
    }

    #[tokio::test]
    async fn recorded_frames_detect_gap_and_resynchronize_only_on_snapshot() {
        let mut engine = WebsocketEngine::with_session("recorded-connection");
        let (tx, mut rx) = mpsc::channel(32);
        process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_snapshot","sid":3,"seq":17,"msg":{"market_ticker":"KXBTC15M-A","yes_dollars_fp":[]}}"#,
            &tx,
        ).await.unwrap();
        assert!(engine.book("KXBTC15M-A").unwrap().synchronized());
        let raw = rx.recv().await.unwrap();
        assert!(
            raw.payload["raw"]["_collector_received_utc"]
                .as_str()
                .unwrap()
                .len()
                > 10
        );

        process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_delta","sid":3,"seq":19,"msg":{"market_ticker":"KXBTC15M-A","side":"yes","price_dollars":"0.5","delta_fp":"1"}}"#,
            &tx,
        ).await.unwrap();
        assert!(!engine.book("KXBTC15M-A").unwrap().synchronized());
        let mut saw_gap = false;
        while let Ok(event) = rx.try_recv() {
            saw_gap |= event.kind == "sequence_gap";
        }
        assert!(saw_gap);

        process_text_frame(
            &mut engine,
            r#"{"type":"orderbook_snapshot","sid":3,"seq":20,"msg":{"market_ticker":"KXBTC15M-A","yes_dollars_fp":[]}}"#,
            &tx,
        ).await.unwrap();
        assert!(engine.book("KXBTC15M-A").unwrap().synchronized());
    }
}
