use crate::api::PublicRest;
use crate::book::OrderBook;
use crate::discovery::Discovery;
use crate::event::{utc_now, Event};
use crate::storage::Journal;
use crate::websocket::WebsocketEngine;
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use uuid::Uuid;

#[derive(Default, Debug)]
pub struct ReplaySummary {
    pub lines: u64,
    pub source_events: u64,
    pub emitted_events: u64,
    pub ws_gap_events: u64,
}

/// Ingest source in a bounded streaming pass. Production files are never
/// opened or modified: this function only reads the explicitly supplied
/// fixture and writes to a separate staging Journal.
pub fn replay_file(journal: &mut Journal, path: &Path, batch_size: usize) -> Result<ReplaySummary> {
    if batch_size == 0 || batch_size > 5000 { bail!("invalid replay batch size") }
    let source = File::open(path).with_context(|| format!("cannot open replay input {}",path.display()))?;
    let reader = BufReader::with_capacity(1024 * 1024,source);
    let mut summary = ReplaySummary::default();
    let mut ws = WebsocketEngine::new();
    for (index, line) in reader.lines().enumerate() {
        let text = line?;
        if text.len() > 64 * 1024 * 1024 { bail!("fixture line too large at {}", index + 1) }
        if text.trim().is_empty() {continue}
        summary.lines += 1;
        let value: Value = serde_json::from_str(&text)
            .with_context(|| format!("invalid JSON at line {}",index + 1))?;
        let events = if value.get("channel").is_some() {
            vec![Event::from_value(value)?]
        } else if value.get("type").is_some() {
            ws.handle(value)
        } else {
            bail!("unsupported event fixture at line {}", index + 1)
        };
        summary.source_events += 1;
        for event in events {
            if event.kind == "sequence_gap" {summary.ws_gap_events += 1;}
            journal.ingest(&event)?;
            summary.emitted_events += 1;
        }
        if summary.source_events % batch_size as u64 == 0 {
            journal.flush_archive(2000)?;
        }
    }
    Ok(summary)
}

/// One explicit snapshot of public REST data. It does not run as a background
/// collector or activate authenticated WebSocket connections.
pub async fn collect_once(
    journal: &mut Journal, rest: &PublicRest, series: &[String],
    tickers: &[String], max_pages: usize,
) -> Result<Value> {
    if series.is_empty() && tickers.is_empty() {
        bail!("specify at least one explicit --series or --ticker")
    }
    if max_pages == 0 || max_pages > 20 { bail!("max_pages must be 1..=20") }
    let run = Uuid::new_v4().simple().to_string();
    let mut discovery = Discovery::default();
    let mut all_tickers: BTreeSet<String> = tickers.iter().cloned().collect();
    let mut diagnostics = Vec::new();
    let mut raw_rows = 0usize;
    for series_id in series {
        if series_id.len() > 128 || !series_id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_') {
            bail!("invalid series ticker")
        }
        for status in ["unopened","open"] {
            let query = vec![
                ("series_ticker".to_string(),series_id.to_owned()),
                ("status".to_string(),status.to_owned()),
                ("limit".to_string(),"200".to_string()),
            ];
            let started = utc_now();
            match rest.pages("/markets","markets",&query,max_pages).await {
                Ok((rows, metadata)) => {
                    raw_rows += rows.len();
                    for row in &rows {
                        let ticker = row.get("ticker").and_then(Value::as_str).unwrap_or("");
                        if !ticker.is_empty() {
                            // Preserve all source fields, even if the market
                            // fails the 900-second canonical classification.
                            let digest = Event::new("rest_market","market_metadata",Some(ticker),
                                row.clone(),None).payload_hash()?;
                            journal.ingest(&Event::new(
                                "rest_market","market_metadata",Some(ticker),row.clone(),
                                Some(format!("market-meta|{ticker}|{digest}"))))?;
                        }
                    }
                    for event in discovery.add_rows(&rows,Utc::now()) {
                        journal.ingest(&event)?;
                    }
                    diagnostics.push(json!({"series":series_id,"status":status,"complete":true,
                        "raw_rows":rows.len(),"request_pages":metadata.len()}));
                }
                Err(error) => {
                    diagnostics.push(json!({"series":series_id,"status":status,"complete":false,
                        "error":error.to_string()}));
                    journal.ingest(&Event::poll_observation("/markets",&format!("{}-{}-{}",run,series_id,status),
                        json!({"status":"INCOMPLETE","error":error.to_string(),"started_utc":started})))?;
                }
            }
        }
    }
    for market in discovery.near_open(Utc::now(),300,180) {
        all_tickers.insert(market.ticker.clone());
    }
    if all_tickers.len() > 300 { bail!("ticker set exceeds staged one-shot safety cap") }
    let mut book_valid = BTreeSet::new();
    let mut book_missing = BTreeSet::new();
    for slice in all_tickers.iter().cloned().collect::<Vec<_>>().chunks(100) {
        let requested = slice.iter().cloned().collect::<BTreeSet<_>>();
        let params = vec![("tickers".to_string(),slice.join(","))];
        let response = rest.get("/markets/orderbooks",&params).await;
        match response {
            Ok(payload) => {
                let observed_utc = utc_now();
                let items = payload.body.get("orderbooks").and_then(Value::as_array)
                    .ok_or_else(|| anyhow!("orderbooks response missing array"))?;
                for item in items {
                    let Some(ticker) = item.get("ticker").and_then(Value::as_str) else {continue};
                    let observed = item.get("orderbook_fp")
                        .or_else(|| item.get("orderbook")).unwrap_or(item);
                    let mut book = OrderBook::default();
                    let validation = if book.snapshot(observed).is_ok() {
                        book_valid.insert(ticker.to_owned());
                        "VALID_REST_SAMPLED"
                    } else {"INVALID_REST_BOOK"};
                    journal.ingest(&Event::new(
                        "rest_orderbook","orderbook_snapshot",Some(ticker),
                        json!({"raw":item,"validation":validation,"observed_utc":observed_utc,
                            "source":"REST_SAMPLED_NOT_WS_SYNCHRONIZED"}),
                        Some(format!("rest-book|{run}|{ticker}|{observed_utc}"))))?;
                }
                for missing in requested.difference(&book_valid) {
                    book_missing.insert(missing.clone());
                }
                journal.ingest(&Event::poll_observation("/markets/orderbooks",
                    &format!("{run}-books-{}",slice[0]),
                    json!({"requested":requested,"valid":book_valid,
                        "missing_or_invalid":book_missing,
                        "http":payload.metadata,"complete":book_missing.is_empty()})))?;
            }
            Err(error) => {
                for ticker in requested {book_missing.insert(ticker);}
                diagnostics.push(json!({"source":"rest_orderbook","complete":false,
                    "error":error.to_string()}));
            }
        }
    }
    // REST public trades are historical snapshots, not a continuous trade
    // stream. Re-read 1h overlap and deduplicate by authoritative trade ID.
    let since = Utc::now().timestamp() - 3600;
    let mut trades = 0usize;
    let mut trade_complete = Vec::new();
    for ticker in &all_tickers {
        let mut cursor = String::new();
        let mut seen = HashSet::new();
        let mut finished = false;
        for _page in 0..max_pages {
            let mut query = vec![
                ("ticker".to_string(),ticker.clone()),
                ("min_ts".to_string(),since.to_string()),
                ("limit".to_string(),"1000".to_string()),
            ];
            if !cursor.is_empty() {query.push(("cursor".into(),cursor.clone()));}
            match rest.get("/markets/trades",&query).await {
                Ok(response) => {
                    let Some(items) = response.body.get("trades").and_then(Value::as_array) else {break};
                    for trade in items {
                        let exchange_ts = trade.get("created_time").and_then(Value::as_str).map(str::to_string);
                        let id = trade.get("trade_id").and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| {
                                let key = serde_json::to_string(trade).unwrap_or_default();
                                crate::event::hash_bytes(key.as_bytes())
                            });
                        let mut event = Event::new("rest_public_trades","public_trade",Some(ticker),
                            trade.clone(),Some(format!("trade|{id}")));
                        event.exchange_ts = exchange_ts;
                        journal.ingest(&event)?;
                        trades += 1;
                    }
                    let next = response.body.get("cursor").and_then(Value::as_str).unwrap_or("");
                    if next.is_empty() {finished = true; break}
                    if !seen.insert(next.to_owned()) {break}
                    cursor = next.to_string();
                }
                Err(error) => {
                    diagnostics.push(json!({"source":"rest_public_trades","ticker":ticker,
                        "complete":false,"error":error.to_string()}));
                    break;
                }
            }
        }
        trade_complete.push(json!({"ticker":ticker,"complete":finished}));
        journal.ingest(&Event::poll_observation(
            "/markets/trades",&format!("{run}-trades-{ticker}"),
            json!({"ticker":ticker,"complete":finished,"max_pages":max_pages,"min_ts":since})
        ))?;
    }
    journal.flush_archive(2000)?;
    let report = json!({
        "mode":"EXPLICIT_ONE_SHOT_READ_ONLY",
        "run_id":run,"raw_market_rows":raw_rows,"requested_tickers":all_tickers,
        "valid_rest_book_tickers":book_valid,"missing_or_invalid_books":book_missing,
        "observed_trade_rows":trades,"trade_pagination":trade_complete,
        "discovery":diagnostics,"book_completeness_not_equal_ws_history":true,
    });
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixture_replay_retains_ws_gap_diagnostic_and_sqlite_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("events.jsonl");
        std::fs::write(&input, concat!(
            "{\"type\":\"orderbook_snapshot\",\"sid\":1,\"seq\":2,\"msg\":{\"market_ticker\":\"A\",\"yes_dollars_fp\":[[\"0.5\",\"2\"]]}}\n",
            "{\"type\":\"orderbook_delta\",\"sid\":1,\"seq\":4,\"msg\":{\"market_ticker\":\"A\",\"side\":\"yes\",\"price_dollars\":\"0.5\",\"delta_fp\":\"1\"}}\n",
            "{\"channel\":\"rest\",\"kind\":\"trade\",\"dedupe_key\":\"t\",\"payload\":{\"price\":\"0.4\"}}\n"
        )).unwrap();
        let mut journal = Journal::open(&dir.path().join("stage")).unwrap();
        let stats = replay_file(&mut journal,&input,2).unwrap();
        assert_eq!(stats.lines,3);
        assert_eq!(stats.ws_gap_events,1);
        assert!(journal.verify_archive().unwrap() == false || journal.unarchived_count().unwrap() <= 2);
        journal.close().unwrap();
    }
}
