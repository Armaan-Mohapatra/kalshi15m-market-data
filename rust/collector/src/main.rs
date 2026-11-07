use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use kalshi15m_rust::{
    api::{PublicRest, DEFAULT_BASE},
    event::Event,
    replay::{collect_once, replay_file},
    runtime::{ContinuousRuntime, MarketHealth, RestSource, RuntimeConfig, RuntimeHealth},
    storage::{IngestResult, Journal},
    websocket::{AuthenticatedWebSocket, WebSocketConfigStage, KALSHI_WS_URL},
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

fn observe_websocket_event(health: &mut RuntimeHealth, event: &Event, queue_depth: usize) {
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
            .or_insert_with(|| MarketHealth {
                ticker: ticker.to_owned(),
                market_state: "UNKNOWN_FROM_WEBSOCKET".into(),
                seconds_to_open: 0,
                last_book_poll_utc: None,
                last_websocket_frame_utc: None,
                last_websocket_trade_utc: None,
                book_source: "WEBSOCKET_SEQUENCE_STATE".into(),
                book_state: "NOT_YET_OBSERVED".into(),
                last_trade_poll_utc: None,
                trades_complete: None,
                last_error: None,
            });
        market.last_websocket_frame_utc = Some(event.received_utc.clone());
        match event.kind.as_str() {
            "orderbook_snapshot" | "orderbook_delta" => {
                market.book_source = "WEBSOCKET_SEQUENCE_STATE".into();
            }
            "book_synchronized" => {
                market.book_source = "WEBSOCKET_SEQUENCE_VALIDATED".into();
                market.book_state = "SYNCHRONIZED_WSS".into();
                market.last_error = None;
            }
            "sequence_gap" => {
                market.book_state = "INVALIDATED_SEQUENCE_GAP".into();
                market.last_error =
                    Some("subscription sequence gap; intermediate deltas unrecovered".into());
            }
            "invalid_snapshot" => {
                market.book_state = "INVALID_SNAPSHOT".into();
                market.last_error = Some("invalid WebSocket snapshot".into());
            }
            "delta_not_applied" => market.book_state = "UNSYNCHRONIZED".into(),
            "trade" => market.last_websocket_trade_utc = Some(event.received_utc.clone()),
            _ => {}
        }
    }
}

#[derive(Parser)]
#[command(
    name = "kalshi15m-rust",
    version,
    about = "Isolated read-only Rust Kalshi market-data replay and staging capture"
)]
struct Cli {
    /// REQUIRED isolated staging directory; never point at live Python/Phoenix files.
    #[arg(long)]
    home: PathBuf,
    #[command(subcommand)]
    action: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Replay explicitly supplied, finalized or synthetic NDJSON without network.
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
    },
    /// One explicit, bounded public GET pass; network disabled unless opted in.
    CaptureOnce {
        #[arg(
            long,
            required = true,
            help = "Explicit consent for public HTTP GET requests"
        )]
        allow_network: bool,
        #[arg(long)]
        series: Vec<String>,
        #[arg(long)]
        ticker: Vec<String>,
        #[arg(long, default_value_t = 4)]
        max_pages: usize,
        #[arg(long, default_value_t = 2.0)]
        min_spacing_seconds: f64,
        #[arg(long, default_value=DEFAULT_BASE)]
        base_url: String,
    },
    /// Start the continuously scheduled, read-only REST collector.
    /// Network access requires `--allow-network true`; no trading routes exist.
    RunContinuous {
        #[arg(long, num_args = 0..)]
        series: Vec<String>,
        #[arg(long, required = true)]
        allow_network: bool,
        #[arg(long, default_value_t = 2.0)]
        min_spacing_seconds: f64,
        #[arg(long, default_value = DEFAULT_BASE)]
        base_url: String,
    },
    /// Explicitly gated authenticated market-data WebSocket capture.
    /// Never run during ordinary offline staging validation.
    CaptureWebsocket {
        #[arg(long, required = true)]
        allow_websocket: bool,
        #[arg(long, required = true)]
        key_id: String,
        #[arg(long, required = true)]
        private_key: PathBuf,
        #[arg(long, required = true, num_args = 1..)]
        ticker: Vec<String>,
        #[arg(long, default_value_t = 4096)]
        queue_capacity: usize,
    },
    /// Audit staging journal and verify archive integrity without network.
    Audit,
}

fn validate_staging_home(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        bail!("--home must be an explicit absolute path")
    }
    let raw = root
        .to_string_lossy()
        .to_ascii_lowercase()
        .replace('/', "\\");
    for prohibited in [
        "tradingnextdata",
        "kalshiruntime",
        "\\kalshi15mnative",
        "\\e:\\",
        "\\production\\",
        "\\paper_ledger.sqlite3",
    ] {
        if raw.contains(prohibited) {
            bail!("refusing production/collector data path: {prohibited}")
        }
    }
    if root.is_file() {
        bail!("--home points to a file")
    }
    std::fs::create_dir_all(root)?;
    let home = root.canonicalize().context("resolve staging root")?;
    let resolved = home
        .to_string_lossy()
        .to_ascii_lowercase()
        .replace('/', "\\");
    for prohibited in [
        "tradingnextdata",
        "kalshiruntime",
        "\\kalshi15mnative",
        "\\production\\",
    ] {
        if resolved.contains(prohibited) {
            bail!("canonical staging path resolves inside a protected source root")
        }
    }
    if home.components().count() < 3 {
        bail!("staging root too broad; choose a dedicated child directory")
    }
    Ok(home)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = validate_staging_home(&cli.home)?;
    let mut journal = if matches!(&cli.action, Commands::Audit) {
        Journal::open_readonly(&home)?
    } else {
        Journal::open(&home)?
    };
    match cli.action {
        Commands::Replay { input, batch_size } => {
            let input = input.canonicalize().context("resolve replay input")?;
            if input.starts_with(&home) {
                bail!("replay input must be separate from writable stage output")
            }
            let summary = replay_file(&mut journal, &input, batch_size)?;
            let stats = journal.close()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mode":"OFFLINE_REPLAY",
                    "lines":summary.lines,"source_events":summary.source_events,
                    "durable_events":stats.inserted + stats.variants,
                    "duplicates":stats.duplicates,"sequence_gaps":summary.ws_gap_events,
                    "status":"ARCHIVE_VERIFIED"
                }))?
            );
        }
        Commands::CaptureOnce {
            allow_network,
            series,
            ticker,
            max_pages,
            min_spacing_seconds,
            base_url,
        } => {
            if !allow_network {
                bail!("network disabled unless --allow-network explicitly supplied")
            }
            if !min_spacing_seconds.is_finite() || min_spacing_seconds < 2.0 {
                bail!("minimum REST pacing is 2 seconds without a separate limit review")
            }
            let client = PublicRest::new(&base_url, Duration::from_secs_f64(min_spacing_seconds))?;
            let report = collect_once(&mut journal, &client, &series, &ticker, max_pages).await?;
            journal.close()?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::RunContinuous {
            series,
            allow_network,
            min_spacing_seconds,
            base_url,
        } => {
            if !allow_network {
                bail!("continuous REST network access requires --allow-network true")
            }
            if !min_spacing_seconds.is_finite() || min_spacing_seconds < 2.0 {
                bail!("minimum REST pacing is 2 seconds without a separate limit review")
            }
            let rest = PublicRest::new(&base_url, Duration::from_secs_f64(min_spacing_seconds))?;
            let source = RestSource::new(rest, allow_network)?;
            let config = RuntimeConfig {
                series,
                ..RuntimeConfig::default()
            };
            config.validate()?;
            let (stop_tx, mut stop_rx) = watch::channel(false);
            let signal_task = tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                let _ = stop_tx.send(true);
            });
            let mut runtime = ContinuousRuntime::default();
            let result = runtime
                .run(&source, &mut journal, &config, &mut stop_rx)
                .await;
            signal_task.abort();
            result?;
            let stats = journal.close()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mode":"CONTINUOUS_READ_ONLY_REST",
                    "process_state":"STOPPED_GRACEFULLY",
                    "network_used":true,
                    "durable_events":stats.inserted + stats.variants,
                    "duplicates":stats.duplicates,
                }))?
            );
        }
        Commands::CaptureWebsocket {
            allow_websocket,
            key_id,
            private_key,
            ticker,
            queue_capacity,
        } => {
            if !allow_websocket {
                bail!("authenticated WebSocket access requires --allow-websocket true")
            }
            let initial_capacity = journal.capacity_snapshot()?;
            if initial_capacity.capacity_state == "CRITICAL_DISK_PRESSURE" {
                let _ = journal.close();
                bail!("critical staging disk pressure; refusing authenticated WebSocket startup")
            }
            let transport = AuthenticatedWebSocket::new(WebSocketConfigStage {
                url: KALSHI_WS_URL.to_owned(),
                key_id,
                private_key_path: private_key,
                market_tickers: ticker,
                allow_network: allow_websocket,
                reconnect_initial: Duration::from_secs(1),
                reconnect_max: Duration::from_secs(60),
                output_capacity: queue_capacity,
            })?;
            let home = journal.home.clone();
            let mut health = RuntimeHealth::new(queue_capacity);
            health.rest_state = "NOT_USED_BY_WS_CAPTURE".into();
            health.websocket_state = "CONNECTING".into();
            health.archive_state = "PENDING".into();
            health.disk_free_bytes = initial_capacity.disk_free_bytes;
            health.sqlite_bytes = initial_capacity.sqlite_bytes;
            health.wal_bytes = initial_capacity.wal_bytes;
            health.capacity_state = initial_capacity.capacity_state;
            health.publish(&home)?;
            let (events_tx, mut events_rx) = mpsc::channel(queue_capacity);
            let (stop_tx, stop_rx) = watch::channel(false);
            let transport_task =
                tokio::spawn(async move { transport.run(stop_rx, events_tx).await });
            let ctrl_c = tokio::signal::ctrl_c();
            tokio::pin!(ctrl_c);
            let mut stopping = false;
            let mut spooled = 0_u64;
            let mut persisted = 0_u64;
            let mut events_since_capacity_check = 0_u64;
            let mut stop_ingest = false;
            let mut persistence_error = None;
            let mut status_interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    signal = &mut ctrl_c, if !stopping => {
                        signal.context("wait for Ctrl-C")?;
                        stopping = true;
                        let _ = stop_tx.send(true);
                    }
                    _ = status_interval.tick() => {
                        health.queue_depth = events_rx.len();
                        match journal.capacity_snapshot() {
                            Ok(capacity) => {
                                health.disk_free_bytes = capacity.disk_free_bytes;
                                health.sqlite_bytes = capacity.sqlite_bytes;
                                health.wal_bytes = capacity.wal_bytes;
                                health.capacity_state = capacity.capacity_state.clone();
                                if capacity.capacity_state == "CRITICAL_DISK_PRESSURE" && !stop_ingest {
                                    persistence_error = Some(anyhow::anyhow!("critical disk capacity during WebSocket capture"));
                                    health.websocket_state = "STOPPING_DISK_PRESSURE".into();
                                    stop_ingest = true;
                                    stopping = true;
                                    let _ = stop_tx.send(true);
                                }
                            }
                            Err(error) => {
                                persistence_error = Some(error);
                                health.websocket_state = "STOPPING_CAPACITY_CHECK_FAILED".into();
                                stop_ingest = true;
                                stopping = true;
                                let _ = stop_tx.send(true);
                            }
                        }
                        if let Err(error) = health.publish(&home) {
                            persistence_error = Some(error);
                            stop_ingest = true;
                            stopping = true;
                            let _ = stop_tx.send(true);
                        }
                    }
                    event = events_rx.recv() => match event {
                        Some(event) => {
                            observe_websocket_event(&mut health, &event, events_rx.len());
                            if stop_ingest {
                                match journal.spool(&event) {
                                    Ok(_) => spooled += 1,
                                    Err(error) => {
                                        health.sqlite_state = "SPOOL_WRITE_FAILED".into();
                                        persistence_error = Some(error);
                                    }
                                }
                            } else {
                                match journal.ingest(&event) {
                                    Ok(IngestResult::Inserted | IngestResult::Versioned) => {
                                        health.events_acknowledged += 1;
                                        health.sqlite_state = "HEALTHY".into();
                                        persisted += 1;
                                        events_since_capacity_check += 1;
                                        if events_since_capacity_check >= 100 {
                                            events_since_capacity_check = 0;
                                            match journal.capacity_snapshot() {
                                                Ok(capacity) => {
                                                    health.disk_free_bytes = capacity.disk_free_bytes;
                                                    health.sqlite_bytes = capacity.sqlite_bytes;
                                                    health.wal_bytes = capacity.wal_bytes;
                                                    health.capacity_state = capacity.capacity_state.clone();
                                                    if capacity.capacity_state == "CRITICAL_DISK_PRESSURE" {
                                                        persistence_error = Some(anyhow::anyhow!("critical staging disk pressure; new WebSocket events are being spooled"));
                                                        health.websocket_state = "STOPPING_DISK_PRESSURE".into();
                                                        stop_ingest = true;
                                                    }
                                                    if !stop_ingest {
                                                    match journal.unarchived_count() {
                                                        Ok(count) if count >= 500 => {
                                                            if let Err(error) = journal.flush_archive(500) {
                                                                persistence_error = Some(error);
                                                                stop_ingest = true;
                                                                health.archive_state = "DEGRADED_ARCHIVE_PENDING".into();
                                                            } else {
                                                                health.archive_state = if journal.verify_archive().unwrap_or(false) { "VERIFIED" } else { "INCOMPLETE" }.into();
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(error) => {
                                                            persistence_error = Some(error);
                                                            stop_ingest = true;
                                                        }
                                                    }
                                                    }
                                                }
                                                Err(error) => {
                                                    persistence_error = Some(error);
                                                    stop_ingest = true;
                                                }
                                            }
                                            if stop_ingest {
                                                stopping = true;
                                                let _ = stop_tx.send(true);
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        match journal.spool(&event) {
                                            Ok(_) => spooled += 1,
                                            Err(spool_error) => {
                                                health.sqlite_state = "SPOOL_WRITE_FAILED".into();
                                                persistence_error = Some(anyhow::anyhow!(
                                                    "journal failed ({error}); emergency spool failed ({spool_error})"
                                                ));
                                            }
                                        }
                                        stop_ingest = true;
                                        stopping = true;
                                        let _ = stop_tx.send(true);
                                    }
                                    Ok(IngestResult::Duplicate) => {
                                        health.events_duplicate += 1;
                                        persisted += 1;
                                        events_since_capacity_check += 1;
                                    }
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
            let transport_result = transport_task
                .await
                .context("WebSocket transport task panicked")?;
            health.process_liveness = "STOPPED".into();
            health.websocket_state = if transport_result.is_ok() {
                "STOPPED_GRACEFULLY".into()
            } else {
                "FAILED".into()
            };
            health.queue_depth = events_rx.len();
            health.archive_state = if journal.verify_archive().unwrap_or(false) {
                "VERIFIED".into()
            } else {
                "INCOMPLETE".into()
            };
            let close_result = journal.close();
            if close_result.is_ok() {
                health.archive_state = "VERIFIED_ON_CLOSE".into();
            } else {
                health.archive_state = "INCOMPLETE_ON_CLOSE".into();
            }
            let _ = health.publish(&home);
            let stats = close_result?;
            if let Some(error) = persistence_error {
                return Err(error);
            }
            transport_result?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mode":"AUTHENTICATED_WEBSOCKET_MARKET_DATA",
                    "process_state":"STOPPED_GRACEFULLY",
                    "network_used":true,
                    "durable_events":stats.inserted + stats.variants,
                    "events_seen":persisted,
                    "duplicates":stats.duplicates,
                    "spooled_events":spooled,
                    "archive_state":"VERIFIED_ON_CLOSE"
                }))?
            );
        }
        Commands::Audit => {
            let report = journal.audit()?;
            let verified = journal.verify_archive()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "journal":report,"archive_verified":verified,
                    "mode":"ISOLATED_STAGING_ONLY"
                }))?
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn continuous_cli_allows_catalog_discovery_without_explicit_series() {
        let parsed = Cli::try_parse_from([
            "kalshi15m-rust",
            "--home",
            "C:\\stage",
            "run-continuous",
            "--allow-network",
            "true",
        ])
        .unwrap();
        assert!(matches!(
            parsed.action,
            Commands::RunContinuous { series, .. } if series.is_empty()
        ));
    }

    #[test]
    fn websocket_health_keeps_rest_and_sequence_validated_freshness_separate() {
        let mut health = RuntimeHealth::new(64);
        health.rest_state = "NOT_USED_BY_WS_CAPTURE".into();
        health.websocket_state = "CONNECTING".into();
        let snapshot = Event::new(
            "websocket_raw",
            "orderbook_snapshot",
            Some("KXBTC15M-FIXTURE"),
            json!({"raw":{}}),
            None,
        );
        observe_websocket_event(&mut health, &snapshot, 0);
        assert_eq!(health.rest_state, "NOT_USED_BY_WS_CAPTURE");
        assert_eq!(health.markets["KXBTC15M-FIXTURE"].last_book_poll_utc, None);
        assert!(health.markets["KXBTC15M-FIXTURE"]
            .last_websocket_frame_utc
            .is_some());

        let synchronized = Event::new(
            "websocket_health",
            "book_synchronized",
            Some("KXBTC15M-FIXTURE"),
            json!({"state":"SYNCHRONIZED_AFTER_FRESH_SNAPSHOT"}),
            None,
        );
        observe_websocket_event(&mut health, &synchronized, 0);
        assert_eq!(health.websocket_state, "HEALTHY");
        assert_eq!(
            health.markets["KXBTC15M-FIXTURE"].book_source,
            "WEBSOCKET_SEQUENCE_VALIDATED"
        );

        let gap = Event::new(
            "websocket_health",
            "sequence_gap",
            Some("KXBTC15M-FIXTURE"),
            json!({"intermediate_deltas":"UNRECOVERED"}),
            None,
        );
        observe_websocket_event(&mut health, &gap, 63);
        assert_eq!(health.websocket_state, "DEGRADED_SEQUENCE_GAP");
        assert_eq!(health.websocket_sequence_gap_count, 1);
        assert_eq!(health.queue_saturation_count, 1);
        assert_eq!(
            health.markets["KXBTC15M-FIXTURE"].book_state,
            "INVALIDATED_SEQUENCE_GAP"
        );
    }
}
