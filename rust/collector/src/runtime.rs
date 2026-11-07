//! Sequential, bounded continuous REST collection for isolated staging.
//!
//! The runtime accepts an exchange source so its scheduling and persistence
//! behavior can be exercised with recorded or mocked responses. It never
//! enables a network source itself; callers must construct `RestSource` with
//! the explicit network gate.

use crate::api::{PublicRest, Response};
use crate::book::OrderBook;
use crate::discovery::{Discovery, Market};
use crate::event::{utc_now, Event};
use crate::storage::{ArchiveFlushSummary, IngestResult, Journal};
use crate::websocket::AuthenticatedWebSocket;
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const MAX_WEBSOCKET_SUBSCRIPTION_HEALTH_ROWS: usize = 2048;

fn websocket_subscription_identity(event: &Event) -> Option<(String, i64, String)> {
    let session_id = event.payload.get("session_id")?.as_str()?.to_owned();
    let sid = if event.channel == "websocket_raw" {
        let raw = event.payload.get("raw")?;
        raw.get("sid")
            .and_then(serde_json::Value::as_i64)
            .or_else(|| raw.get("msg")?.get("sid")?.as_i64())?
    } else {
        event.payload.get("sid")?.as_i64()?
    };
    let key = format!("{session_id}:{sid}");
    Some((session_id, sid, key))
}

fn subscription_health_mut(
    health: &mut RuntimeHealth,
    session_id: &str,
    sid: i64,
    key: String,
) -> &mut WebSocketSubscriptionHealth {
    if !health.websocket_subscriptions.contains_key(&key)
        && health.websocket_subscriptions.len() >= MAX_WEBSOCKET_SUBSCRIPTION_HEALTH_ROWS
    {
        if let Some(oldest) = health
            .websocket_subscriptions
            .iter()
            .min_by_key(|(_, state)| state.raw_frames.last_frame_received_utc.as_deref())
            .map(|(key, _)| key.clone())
        {
            health.websocket_subscriptions.remove(&oldest);
        }
    }
    health
        .websocket_subscriptions
        .entry(key)
        .or_insert_with(|| WebSocketSubscriptionHealth {
            session_id: session_id.to_owned(),
            sid,
            ..WebSocketSubscriptionHealth::default()
        })
}

pub fn observe_websocket_event(health: &mut RuntimeHealth, event: &Event, queue_depth: usize) {
    if event.channel.starts_with("websocket_") {
        health.websocket_events_observed += 1;
    }
    health.websocket_last_frame_utc = Some(event.received_utc.clone());
    health.queue_depth = queue_depth;
    if queue_depth.saturating_add(1) >= health.queue_capacity {
        health.queue_saturation_count += 1;
    }
    if event.channel == "websocket_raw"
        && matches!(
            health.websocket_state.as_str(),
            "CONNECTING" | "RECONNECTING"
        )
    {
        health.websocket_state = "CONNECTED".into();
    }
    let raw_frame = event.channel == "websocket_raw";
    if raw_frame {
        health.websocket_raw_frames.observe(event);
    }
    match event.kind.as_str() {
        "connection_interrupted" => {
            health.websocket_reconnect_count += 1;
            health.websocket_state = "RECONNECTING".into();
        }
        "sequence_gap" => {
            health.websocket_sequence_gap_count += 1;
            health.websocket_state = "DEGRADED_SEQUENCE_GAP".into();
        }
        "malformed_frame" | "invalid_snapshot" | "delta_not_applied" => {
            if event.kind == "malformed_frame" {
                health.websocket_malformed_frame_count += 1;
            }
            health.websocket_state = "DEGRADED_INVALID_FRAME_OR_BOOK".into();
        }
        "book_synchronized" => health.websocket_state = "HEALTHY".into(),
        "subscribed" => health.websocket_state = "CONNECTED_AWAITING_FRESH_SNAPSHOT".into(),
        _ => {}
    }
    if let Some(ticker) = event.market_ticker.as_deref() {
        let market = health
            .markets
            .entry(ticker.to_owned())
            .or_insert_with(|| MarketHealth::unknown(ticker, "UNKNOWN_FROM_WEBSOCKET"));
        market.last_websocket_frame_utc = Some(event.received_utc.clone());
        if event.channel.starts_with("websocket_") {
            market.websocket_events_observed += 1;
        }
        if raw_frame {
            market.websocket_raw_frames.observe(event);
        }
        match event.kind.as_str() {
            "orderbook_snapshot" | "orderbook_delta" => {
                market.websocket_book_state = "FRAME_OBSERVED".into();
            }
            "book_synchronized" => {
                market.websocket_book_state = "SYNCHRONIZED".into();
                market.last_error = None;
            }
            "sequence_gap" => {
                market.websocket_book_state = "INVALIDATED_SEQUENCE_GAP".into();
                market.last_error =
                    Some("subscription sequence gap; intermediate deltas unrecovered".into());
            }
            "invalid_snapshot" => {
                market.websocket_book_state = "INVALID_SNAPSHOT".into();
                market.last_error = Some("invalid WebSocket snapshot".into());
            }
            "delta_not_applied" => market.websocket_book_state = "UNSYNCHRONIZED".into(),
            "trade" => market.last_websocket_trade_utc = Some(event.received_utc.clone()),
            _ => {}
        }
    }
    if let Some((session_id, sid, key)) = websocket_subscription_identity(event) {
        let subscription = subscription_health_mut(health, &session_id, sid, key);
        if raw_frame {
            subscription.raw_frames.observe(event);
        }
        if event.kind == "sequence_gap" {
            subscription.sequence_gaps_observed += 1;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebSocketPersistenceOutcome {
    Journaled,
    Duplicate,
    Spooled,
    Failed,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RawFrameHealth {
    pub frames_observed: u64,
    pub orderbook_delta_frames_observed: u64,
    pub orderbook_delta_frames_applied: u64,
    pub orderbook_delta_frames_rejected: u64,
    pub frames_journaled: u64,
    pub orderbook_delta_frames_journaled: u64,
    pub frames_duplicate: u64,
    pub orderbook_delta_frames_duplicate: u64,
    pub frames_spooled: u64,
    pub orderbook_delta_frames_spooled: u64,
    pub frames_unpersisted: u64,
    pub orderbook_delta_frames_unpersisted: u64,
    pub frames_archived: u64,
    pub orderbook_delta_frames_archived: u64,
    pub last_frame_received_utc: Option<String>,
    pub last_delta_received_utc: Option<String>,
    pub last_delta_journal_ack_utc: Option<String>,
    pub last_archive_ack_utc: Option<String>,
    pub last_delta_archive_ack_utc: Option<String>,
}

impl RawFrameHealth {
    fn observe(&mut self, event: &Event) {
        self.frames_observed += 1;
        self.last_frame_received_utc = Some(event.received_utc.clone());
        if event.kind == "orderbook_delta" {
            self.orderbook_delta_frames_observed += 1;
            self.last_delta_received_utc = Some(event.received_utc.clone());
            match event.payload["collector"]["book_processing"].as_str() {
                Some("DELTA_APPLIED") => self.orderbook_delta_frames_applied += 1,
                Some("DELTA_REJECTED") => self.orderbook_delta_frames_rejected += 1,
                _ => {}
            }
        }
    }

    fn observe_persistence(&mut self, event: &Event, outcome: WebSocketPersistenceOutcome) {
        let is_delta = event.kind == "orderbook_delta";
        match outcome {
            WebSocketPersistenceOutcome::Journaled => {
                self.frames_journaled += 1;
                if is_delta {
                    self.orderbook_delta_frames_journaled += 1;
                    self.last_delta_journal_ack_utc = Some(utc_now());
                }
            }
            WebSocketPersistenceOutcome::Duplicate => {
                self.frames_duplicate += 1;
                if is_delta {
                    self.orderbook_delta_frames_duplicate += 1;
                    self.last_delta_journal_ack_utc = Some(utc_now());
                }
            }
            WebSocketPersistenceOutcome::Spooled => {
                self.frames_spooled += 1;
                if is_delta {
                    self.orderbook_delta_frames_spooled += 1;
                }
            }
            WebSocketPersistenceOutcome::Failed => {
                self.frames_unpersisted += 1;
                if is_delta {
                    self.orderbook_delta_frames_unpersisted += 1;
                }
            }
        }
    }

    fn observe_archive(
        &mut self,
        raw_frames: u64,
        delta_frames: u64,
        latest_delta_received_utc: Option<&str>,
        acknowledged_utc: &str,
    ) {
        self.frames_archived += raw_frames;
        self.orderbook_delta_frames_archived += delta_frames;
        if raw_frames > 0 {
            self.last_archive_ack_utc = Some(acknowledged_utc.to_owned());
        }
        if delta_frames > 0 {
            self.last_delta_archive_ack_utc = Some(acknowledged_utc.to_owned());
            self.last_delta_received_utc = latest_delta_received_utc.map(str::to_owned);
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WebSocketSubscriptionHealth {
    pub session_id: String,
    pub sid: i64,
    pub raw_frames: RawFrameHealth,
    pub sequence_gaps_observed: u64,
    pub sequence_gaps_journaled: u64,
    pub sequence_gaps_duplicate: u64,
    pub sequence_gaps_archived: u64,
}

/// Record persistence for a WebSocket event. Journaled means SQLite insert/version;
/// Parquet archive completeness remains represented by the separate archive state.
pub fn observe_websocket_persistence(
    health: &mut RuntimeHealth,
    event: &Event,
    outcome: WebSocketPersistenceOutcome,
) {
    if !event.channel.starts_with("websocket_") {
        return;
    }
    let market = event
        .market_ticker
        .as_deref()
        .and_then(|ticker| health.markets.get_mut(ticker));
    match outcome {
        WebSocketPersistenceOutcome::Journaled => {
            health.websocket_events_journaled += 1;
            if let Some(market) = market {
                market.websocket_events_journaled += 1;
                market.last_websocket_journaled_event_utc = Some(event.received_utc.clone());
                market.last_websocket_journal_ack_utc = Some(utc_now());
            }
        }
        WebSocketPersistenceOutcome::Duplicate => {
            health.websocket_events_duplicate += 1;
            if let Some(market) = market {
                market.websocket_events_duplicate += 1;
                // An idempotent duplicate confirms the same event is already durable.
                market.last_websocket_journaled_event_utc = Some(event.received_utc.clone());
                market.last_websocket_journal_ack_utc = Some(utc_now());
            }
        }
        WebSocketPersistenceOutcome::Spooled => {
            health.websocket_events_spooled += 1;
            if let Some(market) = market {
                market.websocket_events_spooled += 1;
                market.last_websocket_spooled_event_utc = Some(event.received_utc.clone());
            }
        }
        WebSocketPersistenceOutcome::Failed => {
            health.websocket_events_unpersisted += 1;
            if let Some(market) = market {
                market.websocket_events_unpersisted += 1;
            }
        }
    }
    if event.channel == "websocket_raw" {
        health
            .websocket_raw_frames
            .observe_persistence(event, outcome);
        if let Some(ticker) = event.market_ticker.as_deref() {
            if let Some(market) = health.markets.get_mut(ticker) {
                market
                    .websocket_raw_frames
                    .observe_persistence(event, outcome);
            }
        }
    }
    if let Some((session_id, sid, key)) = websocket_subscription_identity(event) {
        let subscription = subscription_health_mut(health, &session_id, sid, key);
        if event.channel == "websocket_raw" {
            subscription.raw_frames.observe_persistence(event, outcome);
        }
        if event.kind == "sequence_gap" {
            match outcome {
                WebSocketPersistenceOutcome::Journaled => subscription.sequence_gaps_journaled += 1,
                WebSocketPersistenceOutcome::Duplicate => subscription.sequence_gaps_duplicate += 1,
                WebSocketPersistenceOutcome::Spooled | WebSocketPersistenceOutcome::Failed => {}
            }
        }
    }
}

/// Record Parquet/archive acknowledgments separately from SQLite journaling.
/// Counters are scoped to flush commits observed by this process; archive
/// manifests and `archive_state` remain the authority across restarts.
pub fn observe_archive_flush(health: &mut RuntimeHealth, summary: &ArchiveFlushSummary) {
    health.websocket_events_archived += summary.websocket_events;
    health.websocket_orderbook_deltas_archived += summary.orderbook_deltas;
    health.websocket_sequence_gaps_archived += summary.sequence_gaps;
    health.websocket_raw_frames.observe_archive(
        summary.websocket_raw_frames,
        summary.orderbook_delta_frames,
        summary.latest_delta_received_utc.as_deref(),
        &utc_now(),
    );
    if summary.websocket_events == 0 {
        return;
    }

    let acknowledged_utc = utc_now();
    health.last_websocket_archive_ack_utc = Some(acknowledged_utc.clone());
    for (ticker, archived) in &summary.by_market {
        let market = health
            .markets
            .entry(ticker.clone())
            .or_insert_with(|| MarketHealth::unknown(ticker, "UNKNOWN_FROM_ARCHIVE"));
        market.websocket_events_archived += archived.websocket_events;
        market.websocket_orderbook_deltas_archived += archived.orderbook_deltas;
        market.websocket_sequence_gaps_archived += archived.sequence_gaps;
        market.websocket_raw_frames.observe_archive(
            archived.raw_frames,
            archived.orderbook_delta_frames,
            archived.latest_delta_received_utc.as_deref(),
            &acknowledged_utc,
        );
        if let Some(received_utc) = &archived.latest_event_received_utc {
            if market
                .last_websocket_archived_event_utc
                .as_ref()
                .is_none_or(|latest| received_utc > latest)
            {
                market.last_websocket_archived_event_utc = Some(received_utc.clone());
            }
        }
        market.last_websocket_archive_ack_utc = Some(acknowledged_utc.clone());
    }
    for (key, archived) in &summary.by_subscription {
        let subscription = health
            .websocket_subscriptions
            .entry(key.clone())
            .or_insert_with(|| WebSocketSubscriptionHealth {
                session_id: archived.session_id.clone(),
                sid: archived.sid,
                ..WebSocketSubscriptionHealth::default()
            });
        subscription.raw_frames.observe_archive(
            archived.raw_frames,
            archived.orderbook_delta_frames,
            archived.latest_delta_received_utc.as_deref(),
            &acknowledged_utc,
        );
        subscription.sequence_gaps_archived += archived.sequence_gaps;
    }
}

/// Flush the bounded remaining SQLite backlog, update per-market archive
/// acknowledgments, and verify the final archive before a runtime reports a
/// clean stop. The loop bound matches Journal::close recovery limits.
pub fn finalize_runtime_archive(journal: &mut Journal, health: &mut RuntimeHealth) -> Result<()> {
    for _ in 0..200 {
        if journal.unarchived_count()? == 0 {
            if journal.verify_archive()? {
                health.archive_state = "VERIFIED".into();
                return Ok(());
            }
            health.archive_state = "INCOMPLETE".into();
            bail!("archive verification failed during runtime shutdown")
        }
        let summary = journal.flush_archive_with_summary(2000)?;
        if summary.rows == 0 {
            health.archive_state = "INCOMPLETE".into();
            bail!("runtime archive finalization made no progress")
        }
        observe_archive_flush(health, &summary);
    }
    health.archive_state = "INCOMPLETE".into();
    bail!("runtime archive finalization exceeded bounded flush limit")
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub series: Vec<String>,
    pub discovery_interval: Duration,
    pub book_interval: Duration,
    pub trade_interval: Duration,
    pub opening_pre_seconds: i64,
    pub opening_post_seconds: i64,
    pub max_pages: usize,
    pub queue_capacity: usize,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<()> {
        if self.series.len() > 100 {
            bail!("continuous collection accepts at most 100 explicit series tickers")
        }
        if self.max_pages == 0 || self.max_pages > 20 {
            bail!("page limit outside 1..=20")
        }
        if self.queue_capacity < self.max_pages.saturating_mul(1000).saturating_add(64)
            || self.queue_capacity > 12_000
        {
            bail!("event queue capacity outside 1..=12000")
        }
        for series in &self.series {
            if series.len() > 128
                || !series
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                bail!("invalid series ticker")
            }
        }
        Ok(())
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            series: Vec::new(),
            discovery_interval: Duration::from_secs(60),
            book_interval: Duration::from_secs(15),
            trade_interval: Duration::from_secs(120),
            opening_pre_seconds: 300,
            opening_post_seconds: 180,
            max_pages: 4,
            queue_capacity: 4096,
        }
    }
}

#[derive(Debug, Default)]
pub struct InventoryBatch {
    pub markets: Vec<Value>,
    pub failures: Vec<Value>,
}

#[derive(Debug)]
pub struct TradeBatch {
    pub trades: Vec<Value>,
    pub complete: bool,
    pub pages: usize,
}

/// Mockable interface for all exchange reads. Implementations must be
/// read-only and must return raw market metadata without dropping fields.
pub trait ExchangeSource: Send + Sync {
    fn series_catalog<'a>(&'a self) -> impl Future<Output = Result<Vec<Value>>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }
    fn discover<'a>(
        &'a self,
        series: &'a [String],
        max_pages: usize,
    ) -> impl Future<Output = Result<InventoryBatch>> + Send + 'a;
    fn orderbook<'a>(
        &'a self,
        ticker: &'a str,
    ) -> impl Future<Output = Result<Option<Response>>> + Send + 'a;
    fn trades<'a>(
        &'a self,
        ticker: &'a str,
        since: DateTime<Utc>,
        max_pages: usize,
    ) -> impl Future<Output = Result<TradeBatch>> + Send + 'a;
}

/// Public REST source. Construction is denied unless the caller passes the
/// explicit network gate; tests should implement `ExchangeSource` directly.
pub struct RestSource {
    rest: PublicRest,
    network_allowed: bool,
}

impl RestSource {
    pub fn new(rest: PublicRest, allow_network: bool) -> Result<Self> {
        if !allow_network {
            bail!("continuous REST network access is disabled without --allow-network")
        }
        Ok(Self {
            rest,
            network_allowed: true,
        })
    }
}

impl ExchangeSource for RestSource {
    async fn series_catalog(&self) -> Result<Vec<Value>> {
        if !self.network_allowed {
            bail!("REST network gate is closed")
        }
        let (series, _) = self.rest.pages("/series", "series", &[], 1).await?;
        Ok(series)
    }

    async fn discover(&self, series: &[String], max_pages: usize) -> Result<InventoryBatch> {
        if !self.network_allowed {
            bail!("REST network gate is closed")
        }
        let mut batch = InventoryBatch::default();
        for series_ticker in series {
            for status in ["unopened", "open"] {
                let params = vec![
                    ("series_ticker".to_string(), series_ticker.clone()),
                    ("status".to_string(), status.to_string()),
                    ("limit".to_string(), "200".to_string()),
                ];
                match self
                    .rest
                    .pages("/markets", "markets", &params, max_pages)
                    .await
                {
                    Ok((mut markets, _)) => {
                        let remaining = 1000usize.saturating_sub(batch.markets.len());
                        if markets.len() > remaining {
                            let omitted = markets.len() - remaining;
                            markets.truncate(remaining);
                            batch.markets.append(&mut markets);
                            batch.failures.push(json!({"series_ticker":series_ticker,
                                "status":status,"complete":false,"omitted_rows":omitted,
                                "error":"bounded market inventory buffer reached 1000 rows"}));
                        } else {
                            batch.markets.append(&mut markets);
                        }
                    }
                    Err(error) => batch.failures.push(json!({
                        "series_ticker":series_ticker,"status":status,
                        "error":error.to_string(),"complete":false
                    })),
                }
            }
        }
        Ok(batch)
    }

    async fn orderbook(&self, ticker: &str) -> Result<Option<Response>> {
        let response = self
            .rest
            .get(
                "/markets/orderbooks",
                &[("tickers".into(), ticker.to_string())],
            )
            .await?;
        let rows = response
            .body
            .get("orderbooks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("orderbooks response missing array"))?;
        Ok(rows
            .iter()
            .find(|v| v.get("ticker").and_then(Value::as_str) == Some(ticker))
            .map(|row| Response {
                body: row.clone(),
                metadata: response.metadata.clone(),
            }))
    }

    async fn trades(
        &self,
        ticker: &str,
        since: DateTime<Utc>,
        max_pages: usize,
    ) -> Result<TradeBatch> {
        let params = vec![
            ("ticker".to_string(), ticker.to_string()),
            ("min_ts".to_string(), since.timestamp().to_string()),
            ("limit".to_string(), "1000".to_string()),
        ];
        let (trades, pages) = self
            .rest
            .pages("/markets/trades", "trades", &params, max_pages)
            .await?;
        Ok(TradeBatch {
            trades,
            complete: true,
            pages: pages.len(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketHealth {
    pub ticker: String,
    pub market_state: String,
    pub seconds_to_open: i64,
    pub last_book_poll_utc: Option<String>,
    pub last_websocket_frame_utc: Option<String>,
    pub last_websocket_trade_utc: Option<String>,
    pub book_source: String,
    pub book_state: String,
    pub websocket_book_state: String,
    pub websocket_events_observed: u64,
    pub websocket_raw_frames: RawFrameHealth,
    pub websocket_events_journaled: u64,
    pub websocket_events_duplicate: u64,
    pub websocket_events_spooled: u64,
    pub websocket_events_unpersisted: u64,
    pub websocket_events_archived: u64,
    pub websocket_orderbook_deltas_archived: u64,
    pub websocket_sequence_gaps_archived: u64,
    pub last_websocket_journaled_event_utc: Option<String>,
    pub last_websocket_journal_ack_utc: Option<String>,
    pub last_websocket_archived_event_utc: Option<String>,
    pub last_websocket_archive_ack_utc: Option<String>,
    pub last_websocket_spooled_event_utc: Option<String>,
    pub last_trade_poll_utc: Option<String>,
    pub trades_complete: Option<bool>,
    pub last_error: Option<String>,
}

impl MarketHealth {
    fn unknown(ticker: &str, market_state: &str) -> Self {
        Self {
            ticker: ticker.to_owned(),
            market_state: market_state.to_owned(),
            seconds_to_open: 0,
            last_book_poll_utc: None,
            last_websocket_frame_utc: None,
            last_websocket_trade_utc: None,
            book_source: "NOT_POLLED".into(),
            book_state: "NOT_YET_OBSERVED".into(),
            websocket_book_state: "NOT_YET_OBSERVED".into(),
            websocket_events_observed: 0,
            websocket_raw_frames: RawFrameHealth::default(),
            websocket_events_journaled: 0,
            websocket_events_duplicate: 0,
            websocket_events_spooled: 0,
            websocket_events_unpersisted: 0,
            websocket_events_archived: 0,
            websocket_orderbook_deltas_archived: 0,
            websocket_sequence_gaps_archived: 0,
            last_websocket_journaled_event_utc: None,
            last_websocket_journal_ack_utc: None,
            last_websocket_archived_event_utc: None,
            last_websocket_archive_ack_utc: None,
            last_websocket_spooled_event_utc: None,
            last_trade_poll_utc: None,
            trades_complete: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeHealth {
    pub process_liveness: String,
    pub rest_state: String,
    pub websocket_state: String,
    pub websocket_reconnect_count: u64,
    pub websocket_sequence_gap_count: u64,
    pub websocket_malformed_frame_count: u64,
    pub websocket_last_frame_utc: Option<String>,
    pub websocket_events_observed: u64,
    pub websocket_raw_frames: RawFrameHealth,
    pub websocket_subscriptions: BTreeMap<String, WebSocketSubscriptionHealth>,
    pub websocket_events_journaled: u64,
    pub websocket_events_duplicate: u64,
    pub websocket_events_spooled: u64,
    pub websocket_events_unpersisted: u64,
    pub websocket_events_archived: u64,
    pub websocket_orderbook_deltas_archived: u64,
    pub websocket_sequence_gaps_archived: u64,
    pub last_websocket_archive_ack_utc: Option<String>,
    pub archive_counter_scope: String,
    pub sqlite_state: String,
    pub archive_state: String,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub queue_saturation_count: u64,
    pub events_acknowledged: u64,
    pub events_duplicate: u64,
    pub poll_failures: u64,
    pub disk_free_bytes: u64,
    pub sqlite_bytes: u64,
    pub wal_bytes: u64,
    pub capacity_state: String,
    pub updated_utc: String,
    pub markets: BTreeMap<String, MarketHealth>,
}

impl RuntimeHealth {
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            process_liveness: "RUNNING".into(),
            rest_state: "NOT_YET_POLLED".into(),
            websocket_state: "DISABLED".into(),
            websocket_reconnect_count: 0,
            websocket_sequence_gap_count: 0,
            websocket_malformed_frame_count: 0,
            websocket_last_frame_utc: None,
            websocket_events_observed: 0,
            websocket_raw_frames: RawFrameHealth::default(),
            websocket_subscriptions: BTreeMap::new(),
            websocket_events_journaled: 0,
            websocket_events_duplicate: 0,
            websocket_events_spooled: 0,
            websocket_events_unpersisted: 0,
            websocket_events_archived: 0,
            websocket_orderbook_deltas_archived: 0,
            websocket_sequence_gaps_archived: 0,
            last_websocket_archive_ack_utc: None,
            archive_counter_scope: "CURRENT_PROCESS_ARCHIVE_ACKS_ONLY".into(),
            sqlite_state: "OPEN".into(),
            archive_state: "UNVERIFIED".into(),
            queue_capacity,
            queue_depth: 0,
            queue_saturation_count: 0,
            events_acknowledged: 0,
            events_duplicate: 0,
            poll_failures: 0,
            disk_free_bytes: 0,
            sqlite_bytes: 0,
            wal_bytes: 0,
            capacity_state: "NOT_MEASURED".into(),
            updated_utc: utc_now(),
            markets: BTreeMap::new(),
        }
    }

    pub fn publish(&mut self, home: &Path) -> Result<()> {
        self.updated_utc = utc_now();
        let path = home.join("runtime").join("status.json");
        std::fs::create_dir_all(path.parent().context("status parent")?)?;
        // Status is a replaceable staging observation, never a durability
        // marker. The journal and archive manifests remain authoritative.
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CycleSummary {
    pub discovered: usize,
    pub book_polls: usize,
    pub trade_polls: usize,
    pub failures: usize,
    pub opening_observations: usize,
}

#[derive(Default)]
pub struct ContinuousRuntime {
    discovery: Discovery,
    market_health: BTreeMap<String, MarketHealth>,
    book_due: BTreeMap<String, DateTime<Utc>>,
    trades_due: BTreeMap<String, DateTime<Utc>>,
    opening_minutes: BTreeMap<String, BTreeSet<u8>>,
    last_discovery: Option<DateTime<Utc>>,
    last_open_state: BTreeMap<String, bool>,
    queue_saturation_count: u64,
    candidate_series: BTreeSet<String>,
    series_rotation_cursor: usize,
}

fn rotating_series_batch(
    configured: &[String],
    candidates: &BTreeSet<String>,
    cursor: &mut usize,
    limit: usize,
) -> Vec<String> {
    let all_series: Vec<String> = configured
        .iter()
        .cloned()
        .chain(candidates.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if all_series.is_empty() || limit == 0 {
        *cursor = 0;
        return Vec::new();
    }
    let start = *cursor % all_series.len();
    let count = limit.min(all_series.len());
    let batch = (0..count)
        .map(|offset| all_series[(start + offset) % all_series.len()].clone())
        .collect();
    *cursor = (start + count) % all_series.len();
    batch
}

impl ContinuousRuntime {
    /// Run the REST scheduler and authenticated WebSocket on one journal-owning
    /// task. WebSocket reads wait for SQLite's synchronous durable commit ACK;
    /// both inputs share one bounded channel and one cancellation lifecycle.
    pub async fn run_with_websocket<S: ExchangeSource>(
        &mut self,
        source: &S,
        journal: &mut Journal,
        config: &RuntimeConfig,
        transport: AuthenticatedWebSocket,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        config.validate()?;
        let capacity = journal.capacity_snapshot()?;
        if capacity.capacity_state == "CRITICAL_DISK_PRESSURE" {
            bail!("critical staging disk pressure; refusing combined collector startup")
        }

        let mut health = RuntimeHealth::new(config.queue_capacity);
        health.rest_state = "CONNECTING".into();
        health.websocket_state = "CONNECTING".into();
        health.archive_state = "PENDING".into();
        health.disk_free_bytes = capacity.disk_free_bytes;
        health.sqlite_bytes = capacity.sqlite_bytes;
        health.wal_bytes = capacity.wal_bytes;
        health.capacity_state = capacity.capacity_state;
        health.publish(&journal.home)?;

        let (event_tx, mut event_rx) = mpsc::channel(config.queue_capacity);
        let (websocket_stop_tx, websocket_stop_rx) = watch::channel(false);
        let mut websocket_task = tokio::spawn(async move {
            transport
                .run_acknowledged(websocket_stop_rx, event_tx)
                .await
        });
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        let mut cycle_error = None;
        let mut persist_error = None;
        let mut spool_drain = false;
        let mut websocket_done = false;
        let mut capacity_counter = 0_u64;

        'collect: loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                }
                result = &mut websocket_task, if !websocket_done => {
                    websocket_done = true;
                    match result {
                        Ok(Ok(())) if *stop.borrow() => break,
                        Ok(Ok(())) => {
                            persist_error = Some(anyhow!("WebSocket transport exited before coordinated shutdown"));
                            break;
                        }
                        Ok(Err(error)) => {
                            persist_error = Some(error.context("WebSocket transport failed"));
                            break;
                        }
                        Err(error) => {
                            persist_error = Some(anyhow!("WebSocket task failed: {error}"));
                            break;
                        }
                    }
                }
                item = event_rx.recv() => {
                    let Some(item) = item else {
                        persist_error = Some(anyhow!("WebSocket event channel closed unexpectedly"));
                        break;
                    };
                    let queue_depth = event_rx.len();
                    observe_websocket_event(&mut health, &item.event, queue_depth);
                    match journal.ingest(&item.event) {
                        Ok(IngestResult::Inserted | IngestResult::Versioned) => {
                            health.events_acknowledged += 1;
                            health.sqlite_state = "HEALTHY".into();
                            capacity_counter += 1;
                            observe_websocket_persistence(
                                &mut health,
                                &item.event,
                                WebSocketPersistenceOutcome::Journaled,
                            );
                            if let Err(error) = item.acknowledge(Ok(())) {
                                persist_error = Some(error.context("acknowledge durable WebSocket event"));
                                break 'collect;
                            }
                        }
                        Ok(IngestResult::Duplicate) => {
                            health.events_duplicate += 1;
                            capacity_counter += 1;
                            observe_websocket_persistence(
                                &mut health,
                                &item.event,
                                WebSocketPersistenceOutcome::Duplicate,
                            );
                            if let Err(error) = item.acknowledge(Ok(())) {
                                persist_error = Some(error.context("acknowledge duplicate durable event"));
                                break 'collect;
                            }
                        }
                        Err(error) => {
                            health.sqlite_state = "DEGRADED_SPOOLED".into();
                            let message = match journal.spool(&item.event) {
                                Ok(spool) => {
                                    observe_websocket_persistence(
                                        &mut health,
                                        &item.event,
                                        WebSocketPersistenceOutcome::Spooled,
                                    );
                                    format!("SQLite write failed; full event spooled at {}: {error}", spool.display())
                                }
                                Err(spool_error) => {
                                    observe_websocket_persistence(
                                        &mut health,
                                        &item.event,
                                        WebSocketPersistenceOutcome::Failed,
                                    );
                                    health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                                    format!("SQLite write failed ({error}); emergency spool also failed ({spool_error})")
                                }
                            };
                            let _ = item.acknowledge(Err(message.clone()));
                            persist_error = Some(anyhow!(message));
                            spool_drain = true;
                            break 'collect;
                        }
                    }
                    if capacity_counter >= 100 {
                        capacity_counter = 0;
                        match journal.capacity_snapshot() {
                            Ok(snapshot) => {
                                health.disk_free_bytes = snapshot.disk_free_bytes;
                                health.sqlite_bytes = snapshot.sqlite_bytes;
                                health.wal_bytes = snapshot.wal_bytes;
                                health.capacity_state = snapshot.capacity_state.clone();
                                if snapshot.capacity_state == "CRITICAL_DISK_PRESSURE" {
                                    health.sqlite_state = "BLOCKED_DISK_PRESSURE".into();
                                    persist_error = Some(anyhow!("critical disk pressure during combined collection"));
                                    spool_drain = true;
                                    break 'collect;
                                }
                            }
                            Err(error) => {
                                persist_error = Some(error.context("check combined collector disk capacity"));
                                spool_drain = true;
                                break 'collect;
                            }
                        }
                    }
                    let unarchived = match journal.unarchived_count() {
                        Ok(count) => count,
                        Err(error) => {
                            persist_error = Some(error.context("count unarchived combined collector events"));
                            break 'collect;
                        }
                    };
                    if unarchived >= 500 {
                        match journal.flush_archive_with_summary(500) {
                            Ok(summary) => {
                                observe_archive_flush(&mut health, &summary);
                                health.archive_state = "RECONCILIATION_PENDING".into();
                            }
                            Err(error) => {
                                health.archive_state = "INCOMPLETE".into();
                                health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
                                // SQLite already committed this event. Do not
                                // falsely turn durable persistence into loss.
                                persist_error = Some(error.context("flush combined collector Parquet archive"));
                                break 'collect;
                            }
                        }
                    }
                    health.queue_depth = event_rx.len();
                    if let Err(error) = health.publish(&journal.home) {
                        persist_error = Some(error.context("publish combined collector status"));
                        break 'collect;
                    }
                }
                _ = interval.tick() => {
                    if let Err(error) = self.run_cycle(source, journal, config, &mut health, Utc::now()).await {
                        health.rest_state = "DEGRADED".into();
                        health.poll_failures += 1;
                        cycle_error = Some(error);
                        break;
                    }
                    health.websocket_state = if health.websocket_last_frame_utc.is_some() {
                        health.websocket_state.clone()
                    } else {
                        "CONNECTING".into()
                    };
                    if let Err(error) = health.publish(&journal.home) {
                        persist_error = Some(error.context("publish combined collector status after REST cycle"));
                        break 'collect;
                    }
                }
            }
        }

        let _ = websocket_stop_tx.send(true);
        // The transport waits for the current durable ACK and then observes
        // cancellation; drain any already-enqueued events before joining it.
        while let Some(item) = event_rx.recv().await {
            observe_websocket_event(&mut health, &item.event, event_rx.len());
            if spool_drain {
                match journal.spool(&item.event) {
                    Ok(_) => {
                        health.sqlite_state = "DEGRADED_SPOOLED".into();
                        health.events_acknowledged += 1;
                        observe_websocket_persistence(
                            &mut health,
                            &item.event,
                            WebSocketPersistenceOutcome::Spooled,
                        );
                        let _ = item.acknowledge(Ok(()));
                    }
                    Err(error) => {
                        health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                        observe_websocket_persistence(
                            &mut health,
                            &item.event,
                            WebSocketPersistenceOutcome::Failed,
                        );
                        let message =
                            format!("shutdown spool failed; event remains unacknowledged: {error}");
                        let _ = item.acknowledge(Err(message.clone()));
                        persist_error.get_or_insert_with(|| anyhow!(message));
                    }
                }
                continue;
            }
            match journal.ingest(&item.event) {
                Ok(IngestResult::Inserted | IngestResult::Versioned) => {
                    health.events_acknowledged += 1;
                    observe_websocket_persistence(
                        &mut health,
                        &item.event,
                        WebSocketPersistenceOutcome::Journaled,
                    );
                    item.acknowledge(Ok(()))?;
                }
                Ok(IngestResult::Duplicate) => {
                    health.events_duplicate += 1;
                    observe_websocket_persistence(
                        &mut health,
                        &item.event,
                        WebSocketPersistenceOutcome::Duplicate,
                    );
                    item.acknowledge(Ok(()))?;
                }
                Err(error) => {
                    let message = match journal.spool(&item.event) {
                        Ok(path) => {
                            observe_websocket_persistence(
                                &mut health,
                                &item.event,
                                WebSocketPersistenceOutcome::Spooled,
                            );
                            format!(
                                "shutdown drain write failed; event spooled at {}: {error}",
                                path.display()
                            )
                        }
                        Err(spool_error) => {
                            observe_websocket_persistence(
                                &mut health,
                                &item.event,
                                WebSocketPersistenceOutcome::Failed,
                            );
                            health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                            format!("shutdown drain SQLite write failed ({error}); emergency spool also failed ({spool_error})")
                        }
                    };
                    let _ = item.acknowledge(Err(message.clone()));
                    persist_error.get_or_insert_with(|| anyhow!(message));
                }
            }
        }
        if !websocket_done {
            match tokio::time::timeout(Duration::from_secs(10), &mut websocket_task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    persist_error.get_or_insert(error);
                }
                Ok(Err(error)) => {
                    persist_error
                        .get_or_insert_with(|| anyhow!("WebSocket task join failed: {error}"));
                }
                Err(_) => {
                    websocket_task.abort();
                    let _ = websocket_task.await;
                    persist_error.get_or_insert_with(|| {
                        anyhow!("WebSocket transport did not stop within 10 seconds")
                    });
                }
            }
        }
        if let Err(error) = finalize_runtime_archive(journal, &mut health) {
            health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
            health.archive_state = "INCOMPLETE".into();
            persist_error
                .get_or_insert_with(|| error.context("finalize combined collector archive"));
        }
        health.process_liveness = "STOPPING".into();
        health.queue_depth = 0;
        health.publish(&journal.home)?;
        if let Some(error) = cycle_error.or(persist_error) {
            return Err(error);
        }
        Ok(())
    }

    pub async fn run_cycle<S: ExchangeSource>(
        &mut self,
        source: &S,
        journal: &mut Journal,
        config: &RuntimeConfig,
        health: &mut RuntimeHealth,
        now: DateTime<Utc>,
    ) -> Result<CycleSummary> {
        config.validate()?;
        let capacity = journal.capacity_snapshot()?;
        health.disk_free_bytes = capacity.disk_free_bytes;
        health.sqlite_bytes = capacity.sqlite_bytes;
        health.wal_bytes = capacity.wal_bytes;
        health.capacity_state = capacity.capacity_state.clone();
        if capacity.capacity_state == "CRITICAL_DISK_PRESSURE" {
            health.sqlite_state = "BLOCKED_DISK_PRESSURE".into();
            health.rest_state = "BLOCKED_DISK_PRESSURE".into();
            health.publish(&journal.home)?;
            bail!("critical staging disk pressure; refusing new market polls")
        }
        let mut summary = CycleSummary::default();
        let mut pending = Vec::new();

        let should_discover = self
            .last_discovery
            .map(|last| {
                ChronoDuration::from_std(config.discovery_interval)
                    .map(|interval| now - last >= interval)
                    .unwrap_or(false)
            })
            .unwrap_or(true);
        if should_discover {
            match source.series_catalog().await {
                Ok(rows) => {
                    self.candidate_series = rows
                        .iter()
                        .filter(|row| crate::discovery::series_is_15m_candidate(row))
                        .filter_map(|row| {
                            row.get("ticker")
                                .or_else(|| row.get("series_ticker"))
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        })
                        .collect();
                }
                Err(error) => {
                    summary.failures += 1;
                    health.poll_failures += 1;
                    pending.push(Event::new(
                        "rest_market",
                        "series_catalog_failure",
                        None,
                        json!({"error":error.to_string(),"complete":false}),
                        None,
                    ));
                }
            }
            let series_batch = rotating_series_batch(
                &config.series,
                &self.candidate_series,
                &mut self.series_rotation_cursor,
                4,
            );
            let inventory = source.discover(&series_batch, config.max_pages).await?;
            summary.failures += inventory.failures.len();
            health.poll_failures += inventory.failures.len() as u64;
            for failure in inventory.failures {
                let ticker = failure
                    .get("series_ticker")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                pending.push(Event::new(
                    "rest_market",
                    "market_discovery_failure",
                    ticker.as_deref(),
                    failure,
                    None,
                ));
            }
            summary.discovered = inventory.markets.len();
            for row in &inventory.markets {
                let Some(ticker) = row.get("ticker").and_then(Value::as_str) else {
                    pending.push(Event::new(
                        "rest_market",
                        "market_metadata_invalid",
                        None,
                        row.clone(),
                        None,
                    ));
                    continue;
                };
                let digest = Event::new(
                    "rest_market",
                    "market_metadata",
                    Some(ticker),
                    row.clone(),
                    None,
                )
                .payload_hash()?;
                pending.push(Event::new(
                    "rest_market",
                    "market_metadata",
                    Some(ticker),
                    row.clone(),
                    Some(format!("market-meta|{ticker}|{digest}")),
                ));
            }
            pending.extend(self.discovery.add_rows(&inventory.markets, now));
            self.last_discovery = Some(now);
            health.rest_state = if summary.failures == 0 {
                "HEALTHY"
            } else {
                "DEGRADED"
            }
            .into();
        }

        // Commit inventory before per-market polling so a single saturated
        // trade page cannot combine with thousands of metadata rows.
        if !pending.is_empty() {
            let batch = std::mem::take(&mut pending);
            self.commit_bounded(journal, health, batch).await?;
        }

        let mut due_markets: Vec<Market> = self
            .discovery
            .scheduled(now, config.opening_pre_seconds, config.opening_post_seconds)
            .into_iter()
            .cloned()
            .collect();
        due_markets.sort_by_key(|market| {
            let seconds = market.seconds_to_open(now);
            let active_opening = seconds <= 0 && seconds > -180;
            (!active_opening, seconds.abs(), market.ticker.clone())
        });

        for market in due_markets {
            let ticker = market.ticker.as_str();
            let is_open = market.open <= now && now < market.close;
            let previous_open = self.last_open_state.insert(ticker.to_string(), is_open);
            if is_open && previous_open != Some(true) {
                pending.push(Event::new(
                    "market_lifecycle",
                    "market_opened_observed",
                    Some(ticker),
                    json!({"scheduled_open_utc":market.open.to_rfc3339(),"observed_utc":now.to_rfc3339(),
                        "source":"REST_DISCOVERY","opening_detection_uncertainty_seconds":config.discovery_interval.as_secs()}),
                    Some(format!("market-open|{ticker}|{}", market.open.timestamp())),
                ));
            }
            self.market_health
                .entry(ticker.to_string())
                .or_insert_with(|| MarketHealth {
                    ticker: ticker.to_string(),
                    market_state: if is_open { "OPEN" } else { "UPCOMING" }.into(),
                    seconds_to_open: market.seconds_to_open(now),
                    last_book_poll_utc: None,
                    last_websocket_frame_utc: None,
                    last_websocket_trade_utc: None,
                    book_source: "REST_SAMPLED_NOT_WS_SYNCHRONIZED".into(),
                    book_state: "NOT_YET_OBSERVED".into(),
                    websocket_book_state: "NOT_YET_OBSERVED".into(),
                    websocket_events_observed: 0,
                    websocket_raw_frames: RawFrameHealth::default(),
                    websocket_events_journaled: 0,
                    websocket_events_duplicate: 0,
                    websocket_events_spooled: 0,
                    websocket_events_unpersisted: 0,
                    websocket_events_archived: 0,
                    websocket_orderbook_deltas_archived: 0,
                    websocket_sequence_gaps_archived: 0,
                    last_websocket_journaled_event_utc: None,
                    last_websocket_journal_ack_utc: None,
                    last_websocket_archived_event_utc: None,
                    last_websocket_archive_ack_utc: None,
                    last_websocket_spooled_event_utc: None,
                    last_trade_poll_utc: None,
                    trades_complete: None,
                    last_error: None,
                });

            let book_is_due = self
                .book_due
                .get(ticker)
                .map(|next| now >= *next)
                .unwrap_or(true);
            if book_is_due {
                self.book_due.insert(
                    ticker.to_string(),
                    now + ChronoDuration::from_std(config.book_interval)?,
                );
                summary.book_polls += 1;
                let observed = utc_now();
                match source.orderbook(ticker).await {
                    Ok(Some(response)) => {
                        let book_data = response
                            .body
                            .get("orderbook_fp")
                            .or_else(|| response.body.get("orderbook"))
                            .unwrap_or(&response.body);
                        let mut book = OrderBook::default();
                        let validation = if book.snapshot(book_data).is_ok() {
                            "VALID_REST_SAMPLED"
                        } else {
                            "INVALID_REST_BOOK"
                        };
                        pending.push(Event::new(
                            "rest_orderbook",
                            "orderbook_snapshot",
                            Some(ticker),
                            json!({"raw":response.body,"validation":validation,"http":response.metadata,
                                "observed_utc":observed,"source":"REST_SAMPLED_NOT_WS_SYNCHRONIZED"}),
                            Some(format!("rest-book|{ticker}|{observed}")),
                        ));
                        if let Some(item) = self.market_health.get_mut(ticker) {
                            item.last_book_poll_utc = Some(observed.clone());
                            item.book_source = "REST_SAMPLED_NOT_WS_SYNCHRONIZED".into();
                            item.book_state = validation.into();
                            item.last_error = None;
                        }
                        self.track_opening(ticker, market.open, now, &mut pending, &mut summary);
                    }
                    Ok(None) => {
                        let error = "requested ticker absent from orderbook response";
                        pending.push(Event::new(
                            "rest_orderbook",
                            "orderbook_missing",
                            Some(ticker),
                            json!({"requested_ticker":ticker,"observed_utc":observed,"error":error}),
                            Some(format!("rest-book-missing|{ticker}|{observed}")),
                        ));
                        health.poll_failures += 1;
                        summary.failures += 1;
                        if let Some(item) = self.market_health.get_mut(ticker) {
                            item.last_book_poll_utc = Some(observed);
                            item.book_state = "MISSING".into();
                            item.last_error = Some(error.into());
                        }
                    }
                    Err(error) => {
                        self.poll_failure(
                            ticker,
                            "orderbook",
                            &error,
                            observed,
                            health,
                            &mut pending,
                        );
                        summary.failures += 1;
                    }
                }
            }

            let trade_is_due = self
                .trades_due
                .get(ticker)
                .map(|next| now >= *next)
                .unwrap_or(true);
            if trade_is_due {
                self.trades_due.insert(
                    ticker.to_string(),
                    now + ChronoDuration::from_std(config.trade_interval)?,
                );
                summary.trade_polls += 1;
                let observed = utc_now();
                match source
                    .trades(ticker, now - ChronoDuration::hours(1), config.max_pages)
                    .await
                {
                    Ok(batch) => {
                        for trade in &batch.trades {
                            let trade_id = trade
                                .get("trade_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .unwrap_or_else(|| {
                                    Event::new(
                                        "rest_public_trades",
                                        "public_trade",
                                        Some(ticker),
                                        trade.clone(),
                                        None,
                                    )
                                    .payload_hash()
                                    .unwrap_or_default()
                                });
                            let mut event = Event::new(
                                "rest_public_trades",
                                "public_trade",
                                Some(ticker),
                                trade.clone(),
                                Some(format!("trade|{trade_id}")),
                            );
                            event.exchange_ts = trade
                                .get("created_time")
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                            pending.push(event);
                        }
                        pending.push(Event::new(
                            "rest_public_trades",
                            "trade_poll_result",
                            Some(ticker),
                            json!({"trade_rows":batch.trades.len(),"pages":batch.pages,"complete":batch.complete,
                                "observed_utc":observed,"coverage":"REST_POLL_NOT_CONTINUOUS_STREAM"}),
                            Some(format!("trade-poll|{ticker}|{observed}")),
                        ));
                        if let Some(item) = self.market_health.get_mut(ticker) {
                            item.last_trade_poll_utc = Some(observed);
                            item.trades_complete = Some(batch.complete);
                            item.last_error = None;
                        }
                    }
                    Err(error) => {
                        self.poll_failure(ticker, "trades", &error, observed, health, &mut pending);
                        summary.failures += 1;
                        if let Some(item) = self.market_health.get_mut(ticker) {
                            item.trades_complete = Some(false);
                        }
                    }
                }
            }

            self.market_health
                .entry(ticker.to_string())
                .or_insert_with(|| MarketHealth {
                    ticker: ticker.to_string(),
                    market_state: if is_open { "OPEN" } else { "UPCOMING" }.into(),
                    seconds_to_open: market.seconds_to_open(now),
                    last_book_poll_utc: None,
                    last_websocket_frame_utc: None,
                    last_websocket_trade_utc: None,
                    book_source: "REST_SAMPLED_NOT_WS_SYNCHRONIZED".into(),
                    book_state: "NOT_YET_OBSERVED".into(),
                    websocket_book_state: "NOT_YET_OBSERVED".into(),
                    websocket_events_observed: 0,
                    websocket_raw_frames: RawFrameHealth::default(),
                    websocket_events_journaled: 0,
                    websocket_events_duplicate: 0,
                    websocket_events_spooled: 0,
                    websocket_events_unpersisted: 0,
                    websocket_events_archived: 0,
                    websocket_orderbook_deltas_archived: 0,
                    websocket_sequence_gaps_archived: 0,
                    last_websocket_journaled_event_utc: None,
                    last_websocket_journal_ack_utc: None,
                    last_websocket_archived_event_utc: None,
                    last_websocket_archive_ack_utc: None,
                    last_websocket_spooled_event_utc: None,
                    last_trade_poll_utc: None,
                    trades_complete: None,
                    last_error: None,
                });
            if let Some(item) = self.market_health.get_mut(ticker) {
                item.market_state = if market.close <= now {
                    "CLOSED"
                } else if is_open {
                    "OPEN"
                } else {
                    "UPCOMING"
                }
                .into();
                item.seconds_to_open = market.seconds_to_open(now);
            }
            if !pending.is_empty() {
                let batch = std::mem::take(&mut pending);
                self.commit_bounded(journal, health, batch).await?;
            }
        }

        // Active-market polling ends at close_time, so detect close transitions
        // from the small set of markets known open on the prior cycle.
        let newly_closed = self
            .last_open_state
            .iter()
            .filter(|(ticker, was_open)| {
                **was_open
                    && self
                        .discovery
                        .get(ticker)
                        .is_some_and(|market| market.close <= now)
            })
            .map(|(ticker, _)| ticker.clone())
            .collect::<Vec<_>>();
        for ticker in newly_closed {
            if let Some(market) = self.discovery.get(&ticker) {
                pending.push(Event::new(
                    "market_lifecycle",
                    "market_closed_observed",
                    Some(&ticker),
                    json!({"scheduled_close_utc":market.close.to_rfc3339(),"observed_utc":now.to_rfc3339()}),
                    Some(format!("market-close|{ticker}|{}", market.close.timestamp())),
                ));
            }
            self.last_open_state.remove(&ticker);
            if let Some(item) = self.market_health.get_mut(&ticker) {
                item.market_state = "CLOSED".into();
            }
        }

        self.commit_bounded(journal, health, pending).await?;
        health.archive_state = if journal.verify_archive()? {
            "VERIFIED".into()
        } else {
            "INCOMPLETE".into()
        };
        let websocket_markets = std::mem::take(&mut health.markets);
        health.markets = self.market_health.clone();
        for (ticker, websocket) in websocket_markets {
            if websocket.last_websocket_frame_utc.is_none()
                && websocket.last_websocket_trade_utc.is_none()
            {
                continue;
            }
            let market = health.markets.entry(ticker).or_insert(websocket.clone());
            market.last_websocket_frame_utc = websocket.last_websocket_frame_utc;
            market.last_websocket_trade_utc = websocket.last_websocket_trade_utc;
            market.websocket_book_state = websocket.websocket_book_state;
            market.websocket_events_observed = websocket.websocket_events_observed;
            market.websocket_raw_frames = websocket.websocket_raw_frames;
            market.websocket_events_journaled = websocket.websocket_events_journaled;
            market.websocket_events_duplicate = websocket.websocket_events_duplicate;
            market.websocket_events_spooled = websocket.websocket_events_spooled;
            market.websocket_events_unpersisted = websocket.websocket_events_unpersisted;
            market.last_websocket_journaled_event_utc =
                websocket.last_websocket_journaled_event_utc;
            market.last_websocket_journal_ack_utc = websocket.last_websocket_journal_ack_utc;
            market.last_websocket_spooled_event_utc = websocket.last_websocket_spooled_event_utc;
        }
        let capacity = journal.capacity_snapshot()?;
        health.disk_free_bytes = capacity.disk_free_bytes;
        health.sqlite_bytes = capacity.sqlite_bytes;
        health.wal_bytes = capacity.wal_bytes;
        health.capacity_state = capacity.capacity_state;
        health.publish(&journal.home)?;
        Ok(summary)
    }

    fn track_opening(
        &mut self,
        ticker: &str,
        open: DateTime<Utc>,
        now: DateTime<Utc>,
        pending: &mut Vec<Event>,
        summary: &mut CycleSummary,
    ) {
        let elapsed = (now - open).num_seconds();
        if !(0..180).contains(&elapsed) {
            return;
        }
        let minute = (elapsed / 60) as u8;
        let seen = self.opening_minutes.entry(ticker.to_string()).or_default();
        if seen.insert(minute) {
            summary.opening_observations += 1;
            pending.push(Event::new(
                "opening_coverage",
                "rest_opening_minute_observed",
                Some(ticker),
                json!({"scheduled_open_utc":open.to_rfc3339(),"observed_utc":now.to_rfc3339(),
                    "minute_from_open":minute,"source":"REST_SAMPLED_NOT_CONTINUOUS"}),
                Some(format!(
                    "opening-minute|{ticker}|{}|{minute}",
                    open.timestamp()
                )),
            ));
        }
    }

    fn poll_failure(
        &mut self,
        ticker: &str,
        channel: &str,
        error: &anyhow::Error,
        observed: String,
        health: &mut RuntimeHealth,
        pending: &mut Vec<Event>,
    ) {
        health.poll_failures += 1;
        pending.push(Event::new(
            "rest_health",
            "market_poll_failure",
            Some(ticker),
            json!({"channel":channel,"error":error.to_string(),"observed_utc":observed}),
            None,
        ));
        let item = self
            .market_health
            .entry(ticker.to_string())
            .or_insert_with(|| MarketHealth {
                ticker: ticker.to_string(),
                market_state: "UNKNOWN".into(),
                seconds_to_open: 0,
                last_book_poll_utc: None,
                last_websocket_frame_utc: None,
                last_websocket_trade_utc: None,
                book_source: "REST_SAMPLED_NOT_WS_SYNCHRONIZED".into(),
                book_state: "NOT_YET_OBSERVED".into(),
                websocket_book_state: "NOT_YET_OBSERVED".into(),
                websocket_events_observed: 0,
                websocket_raw_frames: RawFrameHealth::default(),
                websocket_events_journaled: 0,
                websocket_events_duplicate: 0,
                websocket_events_spooled: 0,
                websocket_events_unpersisted: 0,
                websocket_events_archived: 0,
                websocket_orderbook_deltas_archived: 0,
                websocket_sequence_gaps_archived: 0,
                last_websocket_journaled_event_utc: None,
                last_websocket_journal_ack_utc: None,
                last_websocket_archived_event_utc: None,
                last_websocket_archive_ack_utc: None,
                last_websocket_spooled_event_utc: None,
                last_trade_poll_utc: None,
                trades_complete: None,
                last_error: None,
            });
        {
            item.last_error = Some(error.to_string());
            if channel == "orderbook" {
                item.book_source = "REST_SAMPLED_NOT_WS_SYNCHRONIZED".into();
                item.book_state = "ERROR".into();
                item.last_book_poll_utc = Some(observed);
            } else {
                item.trades_complete = Some(false);
                item.last_trade_poll_utc = Some(observed);
            }
        }
    }

    async fn commit_bounded(
        &mut self,
        journal: &mut Journal,
        health: &mut RuntimeHealth,
        events: Vec<Event>,
    ) -> Result<()> {
        if events.len() > health.queue_capacity {
            self.queue_saturation_count += 1;
            health.queue_saturation_count = self.queue_saturation_count;
            health.queue_depth = health.queue_capacity;
            for event in events {
                if let Err(error) = journal.spool(&event) {
                    health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                    health.queue_depth = 0;
                    return Err(error).context("spool oversized runtime batch");
                }
            }
            health.sqlite_state = "DEGRADED_SPOOLED".into();
            bail!("cycle exceeded bounded event queue; full batch durably spooled")
        }
        health.queue_depth = events.len();
        if health.queue_depth == health.queue_capacity {
            self.queue_saturation_count += 1;
            health.queue_saturation_count = self.queue_saturation_count;
        }
        let mut events = events.into_iter();
        while let Some(event) = events.next() {
            match journal.ingest(&event) {
                Ok(IngestResult::Inserted | IngestResult::Versioned) => {
                    health.events_acknowledged += 1;
                }
                Ok(IngestResult::Duplicate) => health.events_duplicate += 1,
                Err(error) => {
                    // The failed event is durably spooled before the cycle
                    // reports an error; callers never receive a false ACK.
                    if let Err(spool_error) = journal.spool(&event) {
                        health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                        health.queue_depth = 0;
                        return Err(spool_error).context("durably spool failed event");
                    }
                    for remaining in events {
                        if let Err(spool_error) = journal.spool(&remaining) {
                            health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                            health.queue_depth = 0;
                            return Err(spool_error).context("durably spool queued event");
                        }
                    }
                    health.sqlite_state = "DEGRADED_SPOOLED".into();
                    health.queue_depth = 0;
                    return Err(error).context("SQLite write failed; event spooled");
                }
            }
            health.queue_depth = health.queue_depth.saturating_sub(1);
            if journal.unarchived_count()? >= 500 {
                match journal.flush_archive_with_summary(500) {
                    Ok(summary) => observe_archive_flush(health, &summary),
                    Err(error) => {
                        for remaining in events {
                            if let Err(spool_error) = journal.spool(&remaining) {
                                health.sqlite_state = "FAILED_UNSPOOLED_EVENT".into();
                                health.queue_depth = 0;
                                return Err(spool_error)
                                    .context("durably spool after archive failure");
                            }
                        }
                        health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
                        health.archive_state = "INCOMPLETE".into();
                        health.queue_depth = 0;
                        return Err(error).context("Parquet flush failed after SQLite commit");
                    }
                }
            }
        }
        match journal.flush_archive_with_summary(500) {
            Ok(summary) => observe_archive_flush(health, &summary),
            Err(error) => {
                health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
                health.archive_state = "INCOMPLETE".into();
                health.queue_depth = 0;
                return Err(error).context("Parquet flush failed after SQLite commit");
            }
        }
        health.sqlite_state = "HEALTHY".into();
        health.archive_state = "RECONCILIATION_PENDING".into();
        Ok(())
    }

    pub async fn run<S: ExchangeSource>(
        &mut self,
        source: &S,
        journal: &mut Journal,
        config: &RuntimeConfig,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        config.validate()?;
        let mut health = RuntimeHealth::new(config.queue_capacity);
        loop {
            if *stop.borrow() {
                break;
            }
            let now = Utc::now();
            if let Err(error) = self
                .run_cycle(source, journal, config, &mut health, now)
                .await
            {
                health.rest_state = "DEGRADED".into();
                health.poll_failures += 1;
                if let Err(archive_error) = finalize_runtime_archive(journal, &mut health) {
                    health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
                    health.archive_state = "INCOMPLETE".into();
                    let _ = archive_error;
                }
                health.publish(&journal.home)?;
                return Err(error);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                }
            }
        }
        health.process_liveness = "STOPPING".into();
        if let Err(error) = finalize_runtime_archive(journal, &mut health) {
            health.sqlite_state = "DEGRADED_ARCHIVE_PENDING".into();
            health.archive_state = "INCOMPLETE".into();
            health.publish(&journal.home)?;
            return Err(error).context("finalize continuous collector archive");
        }
        health.publish(&journal.home)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use ed25519_dalek::pkcs8::EncodePrivateKey as EdEncodePrivateKey;
    use rusqlite::{Connection, TransactionBehavior};
    use std::fs;
    use std::sync::Mutex;

    #[test]
    fn catalog_rotation_is_bounded_deduplicated_and_fair() {
        let configured = vec!["B".to_string()];
        let candidates = ["A", "B", "C", "D", "E"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let mut cursor = 0;
        assert_eq!(
            rotating_series_batch(&configured, &candidates, &mut cursor, 2),
            ["A", "B"]
        );
        assert_eq!(
            rotating_series_batch(&configured, &candidates, &mut cursor, 2),
            ["C", "D"]
        );
        assert_eq!(
            rotating_series_batch(&configured, &candidates, &mut cursor, 2),
            ["E", "A"]
        );
        assert_eq!(
            rotating_series_batch(&[], &BTreeSet::new(), &mut cursor, 2),
            Vec::<String>::new()
        );
    }

    #[test]
    fn websocket_status_separates_observed_journaled_duplicate_and_spooled_events() {
        let mut health = RuntimeHealth::new(8);
        let event = Event::new(
            "websocket_orderbook",
            "orderbook_delta",
            Some("KXBTC15M-STATUS-MOCK"),
            json!({"seq":12}),
            Some("ws-status-12".into()),
        );
        observe_websocket_event(&mut health, &event, 0);
        observe_websocket_persistence(&mut health, &event, WebSocketPersistenceOutcome::Journaled);
        observe_websocket_persistence(&mut health, &event, WebSocketPersistenceOutcome::Duplicate);
        observe_websocket_persistence(&mut health, &event, WebSocketPersistenceOutcome::Spooled);
        observe_websocket_persistence(&mut health, &event, WebSocketPersistenceOutcome::Failed);
        let summary = ArchiveFlushSummary {
            rows: 3,
            websocket_events: 3,
            websocket_raw_frames: 0,
            orderbook_delta_frames: 0,
            orderbook_deltas: 1,
            sequence_gaps: 1,
            latest_delta_received_utc: None,
            by_market: BTreeMap::from([(
                "KXBTC15M-STATUS-MOCK".into(),
                crate::storage::MarketArchiveSummary {
                    websocket_events: 3,
                    raw_frames: 0,
                    orderbook_delta_frames: 0,
                    orderbook_deltas: 1,
                    sequence_gaps: 1,
                    latest_event_received_utc: Some(event.received_utc.clone()),
                    latest_delta_received_utc: None,
                },
            )]),
            by_subscription: BTreeMap::new(),
        };
        observe_archive_flush(&mut health, &summary);

        let market = &health.markets["KXBTC15M-STATUS-MOCK"];
        assert_eq!(health.websocket_events_observed, 1);
        assert_eq!(health.websocket_events_journaled, 1);
        assert_eq!(health.websocket_events_duplicate, 1);
        assert_eq!(health.websocket_events_spooled, 1);
        assert_eq!(health.websocket_events_unpersisted, 1);
        assert_eq!(health.websocket_events_archived, 3);
        assert_eq!(health.websocket_orderbook_deltas_archived, 1);
        assert_eq!(health.websocket_sequence_gaps_archived, 1);
        assert!(health.last_websocket_archive_ack_utc.is_some());
        assert_eq!(
            health.archive_counter_scope,
            "CURRENT_PROCESS_ARCHIVE_ACKS_ONLY"
        );
        assert_eq!(market.websocket_events_observed, 1);
        assert_eq!(market.websocket_events_journaled, 1);
        assert_eq!(market.websocket_events_duplicate, 1);
        assert_eq!(market.websocket_events_spooled, 1);
        assert_eq!(market.websocket_events_unpersisted, 1);
        assert_eq!(market.websocket_events_archived, 3);
        assert_eq!(market.websocket_orderbook_deltas_archived, 1);
        assert_eq!(market.websocket_sequence_gaps_archived, 1);
        assert_eq!(
            market.last_websocket_archived_event_utc.as_deref(),
            Some(event.received_utc.as_str())
        );
        assert!(market.last_websocket_archive_ack_utc.is_some());
        assert_eq!(
            market.last_websocket_journaled_event_utc.as_deref(),
            Some(event.received_utc.as_str())
        );
        assert!(market.last_websocket_journal_ack_utc.is_some());
        assert_eq!(
            market.last_websocket_spooled_event_utc.as_deref(),
            Some(event.received_utc.as_str())
        );
        let status = serde_json::to_value(&health).unwrap();
        assert_eq!(
            status["markets"]["KXBTC15M-STATUS-MOCK"]["websocket_orderbook_deltas_archived"],
            1
        );
    }

    #[test]
    fn final_archive_flush_records_per_market_acknowledgments_before_clean_status() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(8);
        let snapshot = Event::new(
            "websocket_orderbook",
            "orderbook_snapshot",
            Some("KXBTC15M-FINALIZE-MOCK"),
            json!({"seq":10,"yes":[],"no":[]}),
            Some("finalize-snapshot".into()),
        );
        let delta = Event::new(
            "websocket_orderbook",
            "orderbook_delta",
            Some("KXBTC15M-FINALIZE-MOCK"),
            json!({"seq":11,"side":"yes","price":"0.5","delta":1}),
            Some("finalize-delta".into()),
        );
        for event in [&snapshot, &delta] {
            observe_websocket_event(&mut health, event, 0);
            assert_eq!(journal.ingest(event).unwrap(), IngestResult::Inserted);
            observe_websocket_persistence(
                &mut health,
                event,
                WebSocketPersistenceOutcome::Journaled,
            );
        }
        assert_eq!(health.websocket_events_journaled, 2);
        assert_eq!(health.websocket_events_archived, 0);

        finalize_runtime_archive(&mut journal, &mut health).unwrap();

        assert_eq!(health.archive_state, "VERIFIED");
        assert_eq!(health.websocket_events_archived, 2);
        assert_eq!(health.websocket_orderbook_deltas_archived, 1);
        let market = &health.markets["KXBTC15M-FINALIZE-MOCK"];
        assert_eq!(market.websocket_events_archived, 2);
        assert_eq!(market.websocket_orderbook_deltas_archived, 1);
        assert!(market.last_websocket_archive_ack_utc.is_some());
        assert_eq!(journal.unarchived_count().unwrap(), 0);
        assert!(journal.verify_archive().unwrap());
        journal.close().unwrap();
    }

    struct MockSource {
        market: Value,
        books: Mutex<Vec<Option<Value>>>,
        trade_complete: bool,
    }

    struct CatalogMock {
        rows: Vec<Value>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    struct PriorityMock {
        markets: Vec<Value>,
        book_order: Mutex<Vec<String>>,
    }

    impl ExchangeSource for PriorityMock {
        async fn discover(&self, _series: &[String], _max_pages: usize) -> Result<InventoryBatch> {
            Ok(InventoryBatch {
                markets: self.markets.clone(),
                failures: Vec::new(),
            })
        }

        async fn orderbook(&self, ticker: &str) -> Result<Option<Response>> {
            self.book_order.lock().unwrap().push(ticker.to_owned());
            Ok(Some(Response {
                body: json!({"ticker":ticker,"orderbook_fp":{"yes_dollars_fp":[],"no_dollars_fp":[]}}),
                metadata: json!({"http_status":200}),
            }))
        }

        async fn trades(
            &self,
            _ticker: &str,
            _since: DateTime<Utc>,
            _max_pages: usize,
        ) -> Result<TradeBatch> {
            Ok(TradeBatch {
                trades: Vec::new(),
                complete: true,
                pages: 1,
            })
        }
    }

    impl ExchangeSource for CatalogMock {
        async fn series_catalog(&self) -> Result<Vec<Value>> {
            Ok(self.rows.clone())
        }

        async fn discover(&self, series: &[String], _max_pages: usize) -> Result<InventoryBatch> {
            self.calls.lock().unwrap().push(series.to_vec());
            Ok(InventoryBatch::default())
        }

        async fn orderbook(&self, _ticker: &str) -> Result<Option<Response>> {
            Ok(None)
        }

        async fn trades(
            &self,
            _ticker: &str,
            _since: DateTime<Utc>,
            _max_pages: usize,
        ) -> Result<TradeBatch> {
            Ok(TradeBatch {
                trades: Vec::new(),
                complete: true,
                pages: 1,
            })
        }
    }

    impl ExchangeSource for MockSource {
        async fn discover(&self, _series: &[String], _max_pages: usize) -> Result<InventoryBatch> {
            Ok(InventoryBatch {
                markets: vec![self.market.clone()],
                failures: Vec::new(),
            })
        }

        async fn orderbook(&self, ticker: &str) -> Result<Option<Response>> {
            let value = self.books.lock().unwrap().remove(0);
            Ok(value.map(|body| Response {
                body: json!({"ticker":ticker,"orderbook_fp":body}),
                metadata: json!({"http_status":200}),
            }))
        }

        async fn trades(
            &self,
            _ticker: &str,
            _since: DateTime<Utc>,
            _max_pages: usize,
        ) -> Result<TradeBatch> {
            Ok(TradeBatch {
                trades: Vec::new(),
                complete: self.trade_complete,
                pages: 1,
            })
        }
    }

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            series: vec!["KXTEST15M".into()],
            discovery_interval: Duration::from_secs(15),
            book_interval: Duration::from_secs(15),
            trade_interval: Duration::from_secs(120),
            opening_pre_seconds: 300,
            opening_post_seconds: 180,
            max_pages: 4,
            queue_capacity: 4096,
        }
    }

    #[tokio::test]
    async fn runtime_discovers_candidate_series_with_fair_four_series_rotation() {
        let home = tempfile::tempdir().unwrap();
        let source = CatalogMock {
            rows: ["A", "B", "C", "D", "E", "F"]
                .into_iter()
                .map(|ticker| json!({"ticker":ticker,"frequency":"15_minutes"}))
                .chain(std::iter::once(json!({"ticker":"FALSE15M"})))
                .collect(),
            calls: Mutex::new(Vec::new()),
        };
        let mut runtime = ContinuousRuntime::default();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(config().queue_capacity);
        let mut runtime_config = config();
        runtime_config.series.clear();
        let at = Utc.with_ymd_and_hms(2026, 11, 7, 12, 0, 0).unwrap();
        runtime
            .run_cycle(&source, &mut journal, &runtime_config, &mut health, at)
            .await
            .unwrap();
        runtime
            .run_cycle(
                &source,
                &mut journal,
                &runtime_config,
                &mut health,
                at + ChronoDuration::seconds(16),
            )
            .await
            .unwrap();
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![vec!["A", "B", "C", "D"], vec!["E", "F", "A", "B"]]
        );
        journal.close().unwrap();
    }

    #[tokio::test]
    async fn scheduler_prioritizes_first_three_minutes_before_imminent_contract() {
        let home = tempfile::tempdir().unwrap();
        let now = Utc.with_ymd_and_hms(2026, 11, 7, 12, 0, 0).unwrap();
        let open = now - ChronoDuration::seconds(30);
        let upcoming = now + ChronoDuration::seconds(10);
        let source = PriorityMock {
            markets: vec![
                json!({"ticker":"ACTIVE-OPENING","status":"open","open_time":open.to_rfc3339(),
                    "close_time":(open+ChronoDuration::seconds(900)).to_rfc3339()}),
                json!({"ticker":"UPCOMING-NEXT","status":"unopened","open_time":upcoming.to_rfc3339(),
                    "close_time":(upcoming+ChronoDuration::seconds(900)).to_rfc3339()}),
            ],
            book_order: Mutex::new(Vec::new()),
        };
        let mut runtime = ContinuousRuntime::default();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(config().queue_capacity);
        let summary = runtime
            .run_cycle(&source, &mut journal, &config(), &mut health, now)
            .await
            .unwrap();
        assert_eq!(summary.book_polls, 2);
        assert_eq!(
            *source.book_order.lock().unwrap(),
            ["ACTIVE-OPENING", "UPCOMING-NEXT"]
        );
        journal.close().unwrap();
    }

    #[tokio::test]
    async fn scheduler_persists_empty_books_opening_coverage_and_health() {
        let home = tempfile::tempdir().unwrap();
        let open = Utc.with_ymd_and_hms(2026, 11, 7, 12, 0, 0).unwrap();
        let market = json!({"ticker":"KXTEST15M-ONE","status":"open",
            "open_time":open.to_rfc3339(),"close_time":(open+ChronoDuration::seconds(900)).to_rfc3339(),
            "floor_strike":123,"expiration_time":(open+ChronoDuration::seconds(900)).to_rfc3339()});
        let source = MockSource {
            market,
            books: Mutex::new(vec![Some(json!({"yes_dollars_fp":[],"no_dollars_fp":[]}))]),
            trade_complete: true,
        };
        let mut runtime = ContinuousRuntime::default();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(config().queue_capacity);
        let at = open + ChronoDuration::seconds(30);
        let summary = runtime
            .run_cycle(&source, &mut journal, &config(), &mut health, at)
            .await
            .unwrap();
        assert_eq!(summary.discovered, 1);
        assert_eq!(summary.book_polls, 1);
        assert_eq!(summary.trade_polls, 1);
        assert_eq!(summary.opening_observations, 1);
        assert_eq!(
            health.markets["KXTEST15M-ONE"].book_state,
            "VALID_REST_SAMPLED"
        );
        assert_eq!(health.markets["KXTEST15M-ONE"].trades_complete, Some(true));
        assert!(journal.verify_archive().unwrap());
        assert_eq!(journal.audit().unwrap()["canonical_events"], 6);
        journal.close().unwrap();
    }

    #[tokio::test]
    async fn scheduler_polls_active_market_after_opening_window_and_records_close() {
        let home = tempfile::tempdir().unwrap();
        let open = Utc.with_ymd_and_hms(2026, 11, 7, 12, 0, 0).unwrap();
        let close = open + ChronoDuration::seconds(900);
        let source = MockSource {
            market: json!({"ticker":"KXTEST15M-ACTIVE","status":"open",
                "open_time":open.to_rfc3339(),"close_time":close.to_rfc3339()}),
            books: Mutex::new(vec![Some(json!({"yes_dollars_fp":[],"no_dollars_fp":[]}))]),
            trade_complete: true,
        };
        let mut runtime = ContinuousRuntime::default();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(config().queue_capacity);

        // Six minutes after opening is beyond the three-minute opening
        // priority window, but the contract remains active for nine more.
        let active = open + ChronoDuration::seconds(360);
        let sampled = runtime
            .run_cycle(&source, &mut journal, &config(), &mut health, active)
            .await
            .unwrap();
        assert_eq!(sampled.book_polls, 1);
        assert_eq!(sampled.trade_polls, 1);
        assert_eq!(health.markets["KXTEST15M-ACTIVE"].market_state, "OPEN");

        // After scheduled close the market is no longer polled, but its
        // lifecycle transition is still durably observed.
        let closed = runtime
            .run_cycle(
                &source,
                &mut journal,
                &config(),
                &mut health,
                close + ChronoDuration::seconds(1),
            )
            .await
            .unwrap();
        assert_eq!(closed.book_polls, 0);
        assert_eq!(closed.trade_polls, 0);
        assert_eq!(health.markets["KXTEST15M-ACTIVE"].market_state, "CLOSED");
        let close_events: i64 = journal
            .conn
            .query_row(
                "SELECT COUNT(*) FROM market_events WHERE kind='market_closed_observed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(close_events, 1);
        journal.close().unwrap();
    }

    #[tokio::test]
    async fn scheduler_records_missing_books_and_incomplete_failures() {
        let home = tempfile::tempdir().unwrap();
        let open = Utc.with_ymd_and_hms(2026, 11, 7, 12, 0, 0).unwrap();
        let source = MockSource {
            market: json!({"ticker":"ONE","status":"open","open_time":open.to_rfc3339(),
                "close_time":(open+ChronoDuration::seconds(900)).to_rfc3339()}),
            books: Mutex::new(vec![None]),
            trade_complete: false,
        };
        let mut runtime = ContinuousRuntime::default();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut health = RuntimeHealth::new(config().queue_capacity);
        let summary = runtime
            .run_cycle(&source, &mut journal, &config(), &mut health, open)
            .await
            .unwrap();
        assert_eq!(summary.failures, 1);
        assert_eq!(health.markets["ONE"].book_state, "MISSING");
        assert_eq!(health.markets["ONE"].trades_complete, Some(false));
        let missing: i64 = journal
            .conn
            .query_row(
                "SELECT count(*) FROM market_events WHERE kind='orderbook_missing'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(missing, 1);
        journal.close().unwrap();
    }

    #[tokio::test]
    async fn queue_overflow_spools_entire_batch_and_restart_recovers_it() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let mut runtime = ContinuousRuntime::default();
        // A tiny internal capacity keeps this branch test bounded. RuntimeConfig
        // separately enforces its production minimum based on page limits.
        let mut health = RuntimeHealth::new(2);
        let events = (0..3)
            .map(|id| {
                Event::new(
                    "rest",
                    "trade",
                    Some("QUEUE-TEST"),
                    json!({"id":id}),
                    Some(format!("queue-overflow-{id}")),
                )
            })
            .collect();

        let error = runtime
            .commit_bounded(&mut journal, &mut health, events)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("full batch durably spooled"));
        assert_eq!(health.queue_saturation_count, 1);
        assert_eq!(health.queue_depth, 2);
        assert_eq!(health.sqlite_state, "DEGRADED_SPOOLED");
        assert_eq!(journal.audit().unwrap()["canonical_events"], 0);
        assert_eq!(journal.audit().unwrap()["pending_spool_entries"], 3);
        drop(journal);

        let recovered = Journal::open(home.path()).unwrap();
        assert_eq!(recovered.audit().unwrap()["canonical_events"], 3);
        assert_eq!(recovered.audit().unwrap()["pending_spool_entries"], 0);
        assert_eq!(recovered.stats.spool_replayed, 3);
        recovered.close().unwrap();
    }

    #[tokio::test]
    async fn unavailable_emergency_spool_marks_event_unrecoverable_and_never_clean() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let spool_path = home.path().join("data/rust_pending");
        fs::write(&spool_path, b"simulate unavailable staging volume path").unwrap();

        let mut runtime = ContinuousRuntime::default();
        let mut health = RuntimeHealth::new(1);
        let events = (0..2)
            .map(|id| {
                Event::new(
                    "rest",
                    "trade",
                    Some("SPOOL-FAILURE"),
                    json!({"id":id}),
                    Some(format!("spool-failure-{id}")),
                )
            })
            .collect();

        let error = runtime
            .commit_bounded(&mut journal, &mut health, events)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("spool oversized runtime batch"));
        assert_eq!(health.sqlite_state, "FAILED_UNSPOOLED_EVENT");
        assert_eq!(health.events_acknowledged, 0);
        assert_eq!(
            journal.audit().unwrap()["capture_failure"],
            "UNSPOOLED_EVENT"
        );
        assert!(!journal.verify_archive().unwrap());
        assert!(journal.close().is_err());

        let readonly = Journal::open_readonly(home.path()).unwrap();
        let audit = readonly.audit().unwrap();
        assert_eq!(audit["capture_failure"], "UNSPOOLED_EVENT");
        assert_eq!(audit["archive_marker"], "CLOSED_WITH_UNSPOOLED_EVENT");
        assert!(!readonly.verify_archive().unwrap());
    }

    #[tokio::test]
    async fn sqlite_lock_spools_unacknowledged_event_and_restart_recovers_it() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        journal
            .conn
            .busy_timeout(Duration::from_millis(25))
            .unwrap();
        let database = home.path().join("data/paper_ledger.sqlite3");
        let mut competing_writer = Connection::open(database).unwrap();
        let lock = competing_writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();

        let mut runtime = ContinuousRuntime::default();
        let mut health = RuntimeHealth::new(2);
        let event = Event::new(
            "rest",
            "trade",
            Some("LOCK-TEST"),
            json!({"id":"must-not-be-acknowledged-early"}),
            Some("sqlite-lock-spool-test".into()),
        );
        let error = runtime
            .commit_bounded(&mut journal, &mut health, vec![event])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("SQLite write failed; event spooled"));
        assert_eq!(health.events_acknowledged, 0);
        assert_eq!(health.sqlite_state, "DEGRADED_SPOOLED");
        assert_eq!(journal.audit().unwrap()["canonical_events"], 0);
        assert_eq!(journal.audit().unwrap()["pending_spool_entries"], 1);

        drop(lock);
        drop(competing_writer);
        drop(journal);

        let recovered = Journal::open(home.path()).unwrap();
        assert_eq!(recovered.audit().unwrap()["canonical_events"], 1);
        assert_eq!(recovered.audit().unwrap()["pending_spool_entries"], 0);
        assert_eq!(recovered.stats.spool_replayed, 1);
        recovered.close().unwrap();
    }

    #[tokio::test]
    async fn unavailable_archive_path_reports_incomplete_then_recovers() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let archive_root = journal.archive_root.clone();
        let parked_root = archive_root.with_extension("parked");
        fs::rename(&archive_root, &parked_root).unwrap();
        fs::write(&archive_root, b"simulate unavailable archive directory").unwrap();

        let mut runtime = ContinuousRuntime::default();
        let mut health = RuntimeHealth::new(2);
        let event = Event::new(
            "websocket_orderbook",
            "orderbook_delta",
            Some("ARCHIVE-UNAVAILABLE"),
            json!({"id":"durable-in-sqlite"}),
            Some("archive-unavailable-test".into()),
        );
        assert!(runtime
            .commit_bounded(&mut journal, &mut health, vec![event.clone()])
            .await
            .is_err());
        assert_eq!(health.sqlite_state, "DEGRADED_ARCHIVE_PENDING");
        assert_eq!(health.archive_state, "INCOMPLETE");
        assert_eq!(journal.audit().unwrap()["canonical_events"], 1);
        assert_eq!(journal.unarchived_count().unwrap(), 1);
        assert_eq!(journal.audit().unwrap()["pending_spool_entries"], 0);
        observe_websocket_event(&mut health, &event, 0);
        observe_websocket_persistence(&mut health, &event, WebSocketPersistenceOutcome::Journaled);
        assert!(finalize_runtime_archive(&mut journal, &mut health).is_err());
        assert_eq!(health.archive_state, "INCOMPLETE");
        assert_eq!(health.sqlite_state, "DEGRADED_ARCHIVE_PENDING");
        assert_eq!(health.websocket_events_archived, 0);
        assert_eq!(
            health.markets["ARCHIVE-UNAVAILABLE"].websocket_events_journaled,
            1
        );
        assert_eq!(
            health.markets["ARCHIVE-UNAVAILABLE"].websocket_orderbook_deltas_archived,
            0
        );

        fs::remove_file(&archive_root).unwrap();
        fs::rename(&parked_root, &archive_root).unwrap();
        drop(journal);

        let recovered = Journal::open(home.path()).unwrap();
        assert_eq!(recovered.unarchived_count().unwrap(), 1);
        recovered.close().unwrap();
        let verified = Journal::open_readonly(home.path()).unwrap();
        assert!(verified.verify_archive().unwrap());
        drop(verified);
    }

    struct EmptySource;

    impl ExchangeSource for EmptySource {
        async fn discover(&self, _series: &[String], _max_pages: usize) -> Result<InventoryBatch> {
            Ok(InventoryBatch::default())
        }

        async fn orderbook(&self, _ticker: &str) -> Result<Option<Response>> {
            Ok(None)
        }

        async fn trades(
            &self,
            _ticker: &str,
            _since: DateTime<Utc>,
            _max_pages: usize,
        ) -> Result<TradeBatch> {
            Ok(TradeBatch {
                trades: Vec::new(),
                complete: true,
                pages: 0,
            })
        }
    }

    #[tokio::test]
    async fn combined_runtime_durably_acks_mock_websocket_and_shuts_down_cleanly() -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{accept_async, tungstenite::Message};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let url = format!("ws://{address}/trade-api/ws/v2");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let subscription = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await?
                .ok_or_else(|| anyhow!("client closed before WebSocket subscribe"))??;
            let Message::Text(subscription) = subscription else {
                bail!("expected WebSocket text subscription")
            };
            let subscription: Value = serde_json::from_str(&subscription)?;
            if subscription["cmd"] != "subscribe" {
                bail!("unexpected WebSocket command: {subscription}")
            }
            socket
                .send(Message::Text(
                    json!({
                        "type":"subscribed","msg":{"channel":"orderbook_delta","sid":17}
                    })
                    .to_string()
                    .into(),
                ))
                .await?;
            socket.send(Message::Text(json!({
                "type":"orderbook_snapshot","sid":17,"seq":2,
                "msg":{"market_ticker":"KXBTC15M-COORDINATOR-MOCK","yes_dollars_fp":[],"no_dollars_fp":[]}
            }).to_string().into())).await?;
            while let Some(message) = socket.next().await {
                if matches!(message?, Message::Close(_)) {
                    break;
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let home = tempfile::tempdir()?;
        let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)?;
        let key_path = home.path().join("mock-key.pem");
        tokio::fs::write(&key_path, pem.as_bytes()).await?;
        let transport =
            AuthenticatedWebSocket::new_loopback_mock(crate::websocket::WebSocketConfigStage {
                url,
                key_id: "offline-coordinator-test".into(),
                private_key_path: key_path,
                market_tickers: vec!["KXBTC15M-COORDINATOR-MOCK".into()],
                allow_network: true,
                reconnect_initial: Duration::from_millis(5),
                reconnect_max: Duration::from_millis(20),
                output_capacity: 4096,
            })?;
        let mut journal = Journal::open(home.path())?;
        let config = RuntimeConfig {
            series: Vec::new(),
            ..RuntimeConfig::default()
        };
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let stage = home.path().to_path_buf();
        let runner = tokio::spawn(async move {
            let mut runtime = ContinuousRuntime::default();
            let result = runtime
                .run_with_websocket(&EmptySource, &mut journal, &config, transport, &mut stop_rx)
                .await;
            let close = journal.close();
            result?;
            close?;
            Ok::<(), anyhow::Error>(())
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status_path = stage.join("runtime/status.json");
                if let Ok(bytes) = tokio::fs::read(status_path).await {
                    if serde_json::from_slice::<Value>(&bytes)
                        .ok()
                        .and_then(|status| {
                            let market = &status["markets"]["KXBTC15M-COORDINATOR-MOCK"];
                            (market["websocket_book_state"].as_str() == Some("SYNCHRONIZED")
                                && market["websocket_events_observed"].as_u64().unwrap_or(0) >= 2
                                && market["websocket_events_journaled"].as_u64().unwrap_or(0) >= 2
                                && market["last_websocket_journal_ack_utc"].is_string())
                            .then(|| "SYNCHRONIZED_AND_JOURNALED".to_string())
                        })
                        .as_deref()
                        == Some("SYNCHRONIZED_AND_JOURNALED")
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mock WebSocket events should receive durable journal acknowledgments");
        stop_tx.send(true)?;
        tokio::time::timeout(Duration::from_secs(5), runner)
            .await
            .expect("combined runtime should drain and stop promptly")??;
        server.await??;

        let verified = Journal::open_readonly(home.path())?;
        let report = verified.audit()?;
        assert!(report["canonical_events"].as_u64().unwrap_or_default() >= 2);
        assert!(verified.verify_archive()?);
        let conn = rusqlite::Connection::open_with_flags(
            home.path().join("data/paper_ledger.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let snapshots: i64 = conn.query_row(
            "SELECT COUNT(*) FROM market_events WHERE channel='websocket_raw' AND kind='orderbook_snapshot'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(snapshots, 1);
        drop(conn);
        drop(verified);
        Ok(())
    }
}
