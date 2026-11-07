use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Compatible with Python's Event payload contract; raw JSON is canonicalized
/// separately from the transport envelope to avoid timestamp-based identity drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub channel: String,
    pub kind: String,
    #[serde(default)]
    pub market_ticker: Option<String>,
    #[serde(default)]
    pub exchange_ts: Option<String>,
    #[serde(default = "utc_now")]
    pub received_utc: String,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

pub fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl Event {
    pub fn new(
        channel: &str,
        kind: &str,
        ticker: Option<&str>,
        payload: Value,
        dedupe_key: Option<String>,
    ) -> Self {
        Self {
            channel: channel.to_string(),
            kind: kind.to_string(),
            market_ticker: ticker.map(str::to_string),
            exchange_ts: None,
            received_utc: utc_now(),
            dedupe_key,
            payload,
        }
    }

    pub fn canonical_payload(&self) -> Result<String> {
        serde_json::to_string(&self.payload).context("canonical payload serialization")
    }

    pub fn payload_hash(&self) -> Result<String> {
        Ok(hash_bytes(self.canonical_payload()?.as_bytes()))
    }

    pub fn key(&self) -> Result<String> {
        if let Some(key) = &self.dedupe_key {
            if !key.is_empty() {
                return Ok(key.clone());
            }
        }
        let bytes = format!(
            "{}|{}|{}",
            self.channel,
            self.kind,
            self.canonical_payload()?
        );
        Ok(hash_bytes(bytes.as_bytes()))
    }

    pub fn from_line(line: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(line).context("invalid NDJSON")?;
        Self::from_value(value)
    }

    pub fn from_value(value: Value) -> Result<Self> {
        if value.get("channel").is_some() && value.get("kind").is_some() {
            let event: Event = serde_json::from_value(value).context("invalid event envelope")?;
            event.validate()?;
            Ok(event)
        } else if value.is_object() {
            // For deliberately supplied raw WS fixtures, retain the exact message
            // under payload, namespace with raw type and session. Real exchange
            // subscription semantics are parsed separately in websocket.rs.
            let kind = value.get("type").and_then(Value::as_str).unwrap_or("unknown");
            let ticker = value.get("msg")
                .and_then(|m| m.get("market_ticker"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let session = value.get("_fixture_session").and_then(Value::as_str).unwrap_or("unknown");
            let sid = value.get("sid").and_then(Value::as_i64).unwrap_or(-1);
            let seq = value.get("seq").and_then(Value::as_i64).unwrap_or(-1);
            let identity = format!("ws-fixture|{session}|{sid}|{seq}|{kind}");
            let mut event = Event::new("websocket_raw_fixture", kind, ticker.as_deref(), value, Some(identity));
            event.exchange_ts = None;
            event.validate()?;
            Ok(event)
        } else {
            bail!("expected JSON object or a source event envelope")
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.channel.is_empty() || self.kind.is_empty() {
            bail!("event channel and kind must not be empty")
        }
        if let Some(ticker) = &self.market_ticker {
            if ticker.len() > 256 {
                bail!("implausibly long market ticker")
            }
        }
        if self.canonical_payload()?.len() > 64 * 1024 * 1024 {
            bail!("event payload exceeds 64 MiB budget")
        }
        Ok(())
    }

    /// A source envelope with explicit evidence of payload provenance.
    pub fn poll_observation(path: &str, request_id: &str, metadata: Value) -> Self {
        Event::new(
            "rest_diagnostics",
            "rest_poll_result",
            None,
            json!({"path":path, "request_id":request_id, "metadata":metadata}),
            Some(format!("rest-poll|{request_id}")),
        )
    }
}

/// Parse RFC3339 timestamps, normalizing to UTC for opening-window statistics.
pub fn parse_utc(s: &str) -> Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("invalid timestamp {s}"))?;
    Ok(parsed.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_hash_is_key_order_independent() {
        let a = Event::new("rest", "market_metadata", Some("X"),
            serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap(), Some("k".into()));
        let b = Event::new("rest", "market_metadata", Some("X"),
            serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap(), Some("k".into()));
        assert_eq!(a.payload_hash().unwrap(), b.payload_hash().unwrap());
        assert_eq!(a.key().unwrap(), b.key().unwrap());
    }
    #[test]
    fn raw_ws_fixture_namespaces_session() {
        let a = Event::from_line(r#"{"type":"orderbook_delta","sid":2,"seq":3,"_fixture_session":"one"}"#).unwrap();
        let b = Event::from_line(r#"{"type":"orderbook_delta","sid":2,"seq":3,"_fixture_session":"two"}"#).unwrap();
        assert_ne!(a.key().unwrap(), b.key().unwrap());
    }
    #[test]
    fn rejects_nonobject_fixture() {
        assert!(Event::from_line("[1,2,3]").is_err());
    }
}
