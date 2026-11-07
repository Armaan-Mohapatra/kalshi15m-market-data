use crate::book::{BookState, OrderBook};
use crate::event::{hash_bytes, utc_now, Event};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

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
        Self { session: Uuid::new_v4().simple().to_string(), subscriptions: BTreeMap::new(), books: HashMap::new() }
    }
    pub fn with_session(session: &str) -> Self {
        Self { session: session.to_owned(), subscriptions: BTreeMap::new(), books: HashMap::new() }
    }
    pub fn session(&self) -> &str { &self.session }
    pub fn reconnect(&mut self) {
        self.session = Uuid::new_v4().simple().to_string();
        self.subscriptions.clear();
        for book in self.books.values_mut() { book.invalidate("connection replaced"); }
    }
    pub fn book(&self, ticker: &str) -> Option<&OrderBook> { self.books.get(ticker) }

    pub fn handle(&mut self, message: Value) -> Vec<Event> {
        let observed = utc_now();
        let kind = message.get("type").and_then(Value::as_str).unwrap_or("unknown");
        let sid = message.get("sid").and_then(Value::as_i64);
        let seq = message.get("seq").and_then(Value::as_i64);
        let msg = message.get("msg").cloned().unwrap_or(Value::Null);
        let ticker = msg.get("market_ticker").and_then(Value::as_str).map(str::to_string);
        let raw_hash = hash_bytes(serde_json::to_string(&message).unwrap_or_default().as_bytes());
        let key = match (sid,seq) {
            (Some(sid),Some(seq)) => format!("ws|{}|{}|{}|{}|{}",self.session,sid,seq,kind,raw_hash),
            _ => format!("ws|{}|unsequenced|{}|{}",self.session,kind,raw_hash),
        };
        let mut original = Event::new(
            "websocket_raw", kind, ticker.as_deref(),
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
                        "websocket_health", "sequence_gap", ticker.as_deref(),
                        json!({"sid":sid,"prior_seq":last,"new_seq":seq,
                            "session_id":self.session,"intermediate_deltas":"UNRECOVERED"}),
                        Some(format!("ws-gap|{}|{}|{}|{}",self.session,sid,last,seq))
                    ));
                }
            }
            state.last_seq = Some(seq);
            if let Some(ticker_name) = &ticker {
                if !state.book_markets.contains(ticker_name)
                    && (kind == "orderbook_snapshot" || kind == "orderbook_delta") {
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
                        out.push(Event::new("websocket_health","invalid_snapshot",
                            Some(&ticker_name),
                            json!({"session_id":self.session,"error_type":"INVALID_SNAPSHOT",
                                "detail":error.to_string(),"observed_utc":observed}),
                            Some(format!("ws-invalid-snapshot|{}|{}",self.session,raw_hash))));
                    }
                }
            } else if kind == "orderbook_delta" {
                let book = self.books.entry(ticker_name.clone()).or_default();
                if let Err(error) = book.delta(&msg) {
                    book.invalidate(format!("delta rejected: {error}"));
                    out.push(Event::new("websocket_health","delta_not_applied",
                        Some(&ticker_name),
                        json!({"session_id":self.session,"reason":error.to_string(),
                            "state":"UNSYNCHRONIZED","observed_utc":observed}),
                        Some(format!("ws-delta-rejected|{}|{}",self.session,raw_hash))));
                }
            }
        }
        out
    }

    pub fn state_summary(&self) -> Value {
        let healthy = self.books.values()
            .filter(|b| b.state() == &BookState::Synchronized).count();
        json!({"session":self.session,"subscriptions":self.subscriptions.len(),
            "books":self.books.len(),"synchronized_books":healthy})
    }
}
impl Default for WebsocketEngine {fn default() -> Self {Self::new()}}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_ne!(a,b);
    }
}
