use crate::event::{parse_utc, utc_now, Event};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct Market {
    pub ticker: String,
    pub event_ticker: Option<String>,
    pub series_ticker: Option<String>,
    pub open: DateTime<Utc>,
    pub close: DateTime<Utc>,
    pub status: String,
    pub raw: Value,
}

impl Market {
    pub fn from_api(raw: &Value) -> Result<Self> {
        let ticker = raw
            .get("ticker")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("market ticker missing"))?
            .to_owned();
        let open = parse_utc(
            raw.get("open_time")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("open_time missing"))?,
        )?;
        let close = parse_utc(
            raw.get("close_time")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("close_time missing"))?,
        )?;
        if close <= open {
            return Err(anyhow!("market close before opening"));
        }
        Ok(Market {
            ticker,
            open,
            close,
            event_ticker: raw
                .get("event_ticker")
                .and_then(Value::as_str)
                .map(str::to_string),
            series_ticker: raw
                .get("series_ticker")
                .and_then(Value::as_str)
                .map(str::to_string),
            status: raw
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            raw: raw.clone(),
        })
    }

    pub fn is_exact_15m(&self) -> bool {
        (self.close - self.open).num_seconds() == 900
    }

    pub fn seconds_to_open(&self, now: DateTime<Utc>) -> i64 {
        (self.open - now).num_seconds()
    }

    pub fn metadata_event(&self) -> Event {
        let digest = Event::new(
            "rest_market",
            "market_metadata",
            Some(&self.ticker),
            self.raw.clone(),
            None,
        )
        .payload_hash()
        .expect("valid API JSON");
        Event::new(
            "rest_market",
            "market_metadata",
            Some(&self.ticker),
            self.raw.clone(),
            Some(format!("market-meta|{}|{}", self.ticker, digest)),
        )
    }
}

pub fn series_is_15m_candidate(value: &Value) -> bool {
    let as_string = serde_json::to_string(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    // Do not use a ticker prefix to infer contract duration.
    [
        "15 minutes",
        "15 minute",
        "15-min",
        "15min",
        "15m",
        "900 seconds",
        "fifteen minutes",
    ]
    .iter()
    .any(|phrase| as_string.contains(phrase))
}

#[derive(Default)]
pub struct Discovery {
    markets: BTreeMap<String, Market>,
    first_seen: BTreeSet<String>,
}

impl Discovery {
    pub fn add_rows(&mut self, rows: &[Value], now: DateTime<Utc>) -> Vec<Event> {
        let mut out = Vec::new();
        for row in rows {
            match Market::from_api(row) {
                Ok(market) => {
                    // Preserve candidate status but never assert confirmed 15m
                    // just because the ticker resembles a known crypto series.
                    if !market.is_exact_15m() {
                        continue;
                    }
                    let ticker = market.ticker.clone();
                    let first = self.first_seen.insert(ticker.clone());
                    if first {
                        out.push(Event::new(
                            "coverage",
                            "market_first_seen",
                            Some(&ticker),
                            json!({
                                "ticker": ticker, "scheduled_open_utc":market.open.to_rfc3339(),
                                "observed_utc": utc_now(),
                                "first_seen_is_api_response_time": false,
                                "source": "rust_discovery"
                            }),
                            Some(format!(
                                "rust-market-first-seen|{ticker}|{}",
                                now.timestamp_nanos_opt().unwrap_or(0)
                            )),
                        ));
                    }
                    let has_changed = self
                        .markets
                        .get(&ticker)
                        .map(|old| old.raw != market.raw)
                        .unwrap_or(true);
                    if has_changed {
                        out.push(market.metadata_event());
                    }
                    self.markets.insert(ticker, market);
                }
                Err(_) => {
                    // Incomplete exchange rows may still be stored by the
                    // calling REST ingestion layer as raw observations.
                }
            }
        }
        out
    }

    pub fn active(&self, now: DateTime<Utc>) -> Vec<&Market> {
        self.markets
            .values()
            .filter(|m| {
                m.open <= now
                    && now < m.close
                    && ["open", "active", "unopened", "initialized"].contains(&m.status.as_str())
            })
            .collect()
    }

    pub fn near_open(&self, now: DateTime<Utc>, pre: i64, post: i64) -> Vec<&Market> {
        self.markets
            .values()
            .filter(|m| {
                let delta = m.seconds_to_open(now);
                delta <= pre && delta >= -post
            })
            .collect()
    }

    pub fn all(&self) -> Vec<&Market> {
        self.markets.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    #[test]
    fn discovers_open_and_unopened_but_rejects_wrong_duration() {
        let now = Utc.with_ymd_and_hms(2026, 11, 7, 0, 0, 0).unwrap();
        let a = json!({"ticker":"OPEN","status":"open","open_time":"2026-11-06T23:59:00Z","close_time":"2026-11-07T00:14:00Z"});
        let b = json!({"ticker":"UNOPENED","status":"unopened","open_time":"2026-11-07T00:05:00Z","close_time":"2026-11-07T00:20:00Z","floor_strike":70000});
        let c = json!({"ticker":"BAD","status":"open","open_time":"2026-11-06T23:59:00Z","close_time":"2026-11-07T01:00:00Z"});
        let mut d = Discovery::default();
        let events = d.add_rows(&[a, b, c], now);
        assert_eq!(d.all().len(), 2);
        assert_eq!(d.active(now).len(), 1);
        assert_eq!(d.near_open(now, 600, 120).len(), 2);
        assert!(events
            .iter()
            .any(|x| x.payload.get("floor_strike").is_some()));
        assert!(d.add_rows(&[], now).is_empty());
    }
    #[test]
    fn contract_metadata_revisions_produce_new_events() {
        let now = Utc::now();
        let a = json!({"ticker":"X","status":"open","open_time":"2026-11-07T00:00:00Z",
            "close_time":"2026-11-07T00:15:00Z","floor_strike":100});
        let mut d = Discovery::default();
        d.add_rows(&[a.clone()], now);
        let mut changed = a;
        changed["floor_strike"] = json!(101);
        let events = d.add_rows(&[changed], now);
        assert_eq!(
            events
                .iter()
                .filter(|x| x.kind == "market_metadata")
                .count(),
            1
        );
    }
}
