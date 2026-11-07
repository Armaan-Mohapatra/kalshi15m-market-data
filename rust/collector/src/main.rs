use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use kalshi15m_rust::{
    api::{PublicRest, DEFAULT_BASE},
    replay::{collect_once, replay_file},
    runtime::{ContinuousRuntime, RestSource, RuntimeConfig},
    storage::Journal,
    websocket::{AuthenticatedWebSocket, WebSocketConfigStage, KALSHI_WS_URL},
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

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
        #[arg(long, required = true, num_args = 1..)]
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
    let mut journal = Journal::open(&home)?;
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
            let (events_tx, mut events_rx) = mpsc::channel(queue_capacity);
            let (stop_tx, stop_rx) = watch::channel(false);
            let transport_task =
                tokio::spawn(async move { transport.run(stop_rx, events_tx).await });
            let ctrl_c = tokio::signal::ctrl_c();
            tokio::pin!(ctrl_c);
            let mut stopping = false;
            let mut spooled = 0_u64;
            let mut persistence_error = None;
            loop {
                tokio::select! {
                    signal = &mut ctrl_c, if !stopping => {
                        signal.context("wait for Ctrl-C")?;
                        stopping = true;
                        let _ = stop_tx.send(true);
                    }
                    event = events_rx.recv() => match event {
                        Some(event) => {
                            if let Err(error) = journal.ingest(&event) {
                                match journal.spool(&event) {
                                    Ok(_) => spooled += 1,
                                    Err(spool_error) => {
                                        persistence_error = Some(anyhow::anyhow!(
                                            "journal failed ({error}); emergency spool failed ({spool_error})"
                                        ));
                                        stopping = true;
                                        let _ = stop_tx.send(true);
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
            if let Some(error) = persistence_error {
                return Err(error);
            }
            transport_result?;
            let stats = journal.close()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mode":"AUTHENTICATED_WEBSOCKET_MARKET_DATA",
                    "process_state":"STOPPED_GRACEFULLY",
                    "network_used":true,
                    "durable_events":stats.inserted + stats.variants,
                    "duplicates":stats.duplicates,
                    "spooled_events":spooled,
                    "archive_state":"VERIFIED_ON_CLOSE"
                }))?
            );
        }
        Commands::Audit => {
            let report = journal.audit()?;
            let verified = journal.verify_archive()?;
            journal.close()?;
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
