use crate::archive::{self, ArchiveRow, PartInfo};
use crate::event::{hash_bytes, utc_now, Event};
use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// This journal is intended ONLY for a separate Rust staging runtime. The
/// Python production SQLite database and WAL must never be opened by Rust.
pub struct Journal {
    pub home: PathBuf,
    pub conn: Connection,
    pub archive_root: PathBuf,
    pub stats: Stats,
}
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub inserted: u64,
    pub variants: u64,
    pub duplicates: u64,
    pub spool_replayed: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MarketArchiveSummary {
    pub websocket_events: u64,
    pub raw_frames: u64,
    pub orderbook_delta_frames: u64,
    pub orderbook_deltas: u64,
    pub sequence_gaps: u64,
    pub latest_event_received_utc: Option<String>,
    pub latest_delta_received_utc: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubscriptionArchiveSummary {
    pub session_id: String,
    pub sid: i64,
    pub raw_frames: u64,
    pub orderbook_delta_frames: u64,
    pub sequence_gaps: u64,
    pub latest_frame_received_utc: Option<String>,
    pub latest_delta_received_utc: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArchiveFlushSummary {
    pub rows: usize,
    pub websocket_events: u64,
    pub websocket_raw_frames: u64,
    pub orderbook_delta_frames: u64,
    pub orderbook_deltas: u64,
    pub sequence_gaps: u64,
    pub latest_delta_received_utc: Option<String>,
    pub by_market: BTreeMap<String, MarketArchiveSummary>,
    pub by_subscription: BTreeMap<String, SubscriptionArchiveSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapacitySnapshot {
    pub disk_free_bytes: u64,
    pub sqlite_bytes: u64,
    pub wal_bytes: u64,
    pub capacity_state: String,
}

pub fn capacity_state(disk_free_bytes: u64, wal_bytes: u64) -> &'static str {
    const WARNING_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
    const CRITICAL_FREE_BYTES: u64 = 512 * 1024 * 1024;
    const WARNING_WAL_BYTES: u64 = 512 * 1024 * 1024;
    if disk_free_bytes < CRITICAL_FREE_BYTES {
        "CRITICAL_DISK_PRESSURE"
    } else if disk_free_bytes < WARNING_FREE_BYTES || wal_bytes >= WARNING_WAL_BYTES {
        "WARNING_CAPACITY_PRESSURE"
    } else {
        "NORMAL"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestResult {
    Inserted,
    Versioned,
    Duplicate,
}

impl Journal {
    pub fn open(home: &Path) -> Result<Self> {
        fs::create_dir_all(home.join("data"))?;
        fs::create_dir_all(home.join("runtime"))?;
        let archive_root = home.join("data").join("market_data");
        fs::create_dir_all(&archive_root)?;
        let conn = Connection::open(home.join("data").join("paper_ledger.sqlite3"))
            .context("open isolated Rust SQLite journal")?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA busy_timeout=15000;
            CREATE TABLE IF NOT EXISTS market_events (
                dedupe_key TEXT PRIMARY KEY,
                received_utc TEXT NOT NULL,
                exchange_ts TEXT,
                channel TEXT NOT NULL,
                kind TEXT NOT NULL,
                market_ticker TEXT,
                raw_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS market_event_versions (
                source_generation TEXT NOT NULL,
                row_sha256 TEXT NOT NULL,
                captured_utc TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                received_utc TEXT NOT NULL,
                exchange_ts TEXT,
                channel TEXT NOT NULL,
                kind TEXT NOT NULL,
                market_ticker TEXT,
                raw_json BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (source_generation,row_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS archive_manifests (
                part_path TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL,
                byte_count INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                committed_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS collector_markers (
                marker TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rust_archive_issues (
                part_path TEXT PRIMARY KEY,
                failure TEXT NOT NULL,
                detected_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rust_archive_index (
                dedupe_key TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                part_path TEXT NOT NULL,
                PRIMARY KEY (dedupe_key, payload_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS paper_ledger (
                idempotency_key TEXT PRIMARY KEY,
                created_utc TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_market_time
            ON market_events(market_ticker, received_utc);
        "#,
        )?;
        let mut journal = Self {
            home: home.to_path_buf(),
            conn,
            archive_root,
            stats: Stats::default(),
        };
        journal.mark("RUNNING_UNVERIFIED")?;
        // A past crash may leave a finalized file without a manifest commit.
        // All operations below are bounded to the *staging* archive root.
        journal.discover_existing_parts(10_000)?;
        journal.replay_spool(10_000)?;
        Ok(journal)
    }

    /// Open an existing staging journal for audit without changing markers,
    /// replaying spool files, or creating database objects.
    pub fn open_readonly(home: &Path) -> Result<Self> {
        let db_path = home.join("data").join("paper_ledger.sqlite3");
        let archive_root = home.join("data").join("market_data");
        if !db_path.is_file() || !archive_root.is_dir() {
            bail!("staging journal or archive is missing; read-only audit cannot initialize it")
        }
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("open staging SQLite journal read-only")?;
        Ok(Self {
            home: home.to_path_buf(),
            conn,
            archive_root,
            stats: Stats::default(),
        })
    }

    /// Metadata-only capacity check for the isolated journal volume.
    pub fn capacity_snapshot(&self) -> Result<CapacitySnapshot> {
        self.capacity_snapshot_with_probe(|path| fs2::available_space(path))
    }

    fn capacity_snapshot_with_probe(
        &self,
        probe_available_space: impl FnOnce(&Path) -> std::io::Result<u64>,
    ) -> Result<CapacitySnapshot> {
        let db = self.home.join("data").join("paper_ledger.sqlite3");
        let wal = self.home.join("data").join("paper_ledger.sqlite3-wal");
        let size = |path: &Path| -> Result<u64> {
            Ok(if path.exists() {
                path.metadata()?.len()
            } else {
                0
            })
        };
        let disk_free_bytes = probe_available_space(&self.home)
            .context("query available space for isolated staging volume")?;
        let sqlite_bytes = size(&db)?;
        let wal_bytes = size(&wal)?;
        Ok(CapacitySnapshot {
            disk_free_bytes,
            sqlite_bytes,
            wal_bytes,
            capacity_state: capacity_state(disk_free_bytes, wal_bytes).to_owned(),
        })
    }

    fn mark(&self, state: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO collector_markers (marker, value, updated_utc) VALUES ('archive_shutdown', ?1, ?2)",
            params![state, utc_now()],
        )?;
        Ok(())
    }

    pub fn ingest(&mut self, event: &Event) -> Result<IngestResult> {
        event.validate()?;
        let key = event.key()?;
        let raw = event.canonical_payload()?;
        let digest = event.payload_hash()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT payload_sha256 FROM market_events WHERE dedupe_key = ?1",
                [&key],
                |row| row.get(0),
            )
            .optional()?;
        let outcome = match prior {
            None => {
                tx.execute(
                    "INSERT INTO market_events VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        key,
                        event.received_utc,
                        event.exchange_ts,
                        event.channel,
                        event.kind,
                        event.market_ticker,
                        raw,
                        digest
                    ],
                )?;
                IngestResult::Inserted
            }
            Some(previous) if previous == digest => IngestResult::Duplicate,
            Some(_) => {
                let identity = hash_bytes(format!("{key}|{digest}").as_bytes());
                let added = tx.execute(
                    "INSERT OR IGNORE INTO market_event_versions VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    params!["collector-runtime", identity, utc_now(), key,
                        event.received_utc, event.exchange_ts, event.channel,
                        event.kind, event.market_ticker, raw, digest],
                )?;
                if added > 0 {
                    IngestResult::Versioned
                } else {
                    IngestResult::Duplicate
                }
            }
        };
        tx.commit()?; // synchronous=FULL. Nothing acknowledged before this.
        match outcome {
            IngestResult::Inserted => self.stats.inserted += 1,
            IngestResult::Versioned => self.stats.variants += 1,
            IngestResult::Duplicate => self.stats.duplicates += 1,
        };
        Ok(outcome)
    }

    /// FSYNC a full canonical event into a staged emergency spool. This must
    /// succeed before a network producer can acknowledge a storage outage.
    pub fn spool(&self, event: &Event) -> Result<PathBuf> {
        let result = (|| {
            let spool = self.home.join("data").join("rust_pending");
            fs::create_dir_all(&spool)?;
            let id = Uuid::new_v4().simple().to_string();
            let temporary = spool.join(format!("{id}.partial"));
            let output = spool.join(format!("{id}.json"));
            let bytes = serde_json::to_vec(event)?;
            {
                let mut target = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)?;
                target.write_all(&bytes)?;
                target.sync_all()?;
            }
            fs::rename(&temporary, &output)?;
            Ok(output)
        })();
        if result.is_err() {
            self.record_unspooled_event_failure()
                .context("persist unspooled-event failure marker")?;
        }
        result
    }

    /// Persist evidence when an observed event could not be committed to
    /// SQLite or the emergency spool. This marker is intentionally sticky:
    /// recovery cannot claim completeness without an operator reconciling the
    /// unpersisted event.
    pub fn record_unspooled_event_failure(&self) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO collector_markers (marker, value, updated_utc) VALUES ('capture_failure', 'UNSPOOLED_EVENT', ?1)",
            [utc_now()],
        )?;
        Ok(())
    }

    fn replay_spool(&mut self, limit: usize) -> Result<()> {
        let spool = self.home.join("data").join("rust_pending");
        if !spool.exists() {
            return Ok(());
        }
        // Enumeration failures must fail closed. Silently skipping an entry
        // could otherwise make recovery appear complete while a spool file
        // remains unobserved.
        let mut files = fs::read_dir(&spool)?
            .map(|item| item.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        files.sort();
        if files.len() > limit {
            bail!("pending spool exceeds safe replay bound")
        }
        for path in files {
            let raw = fs::read_to_string(&path)?;
            let event: Event = serde_json::from_str(&raw)?;
            self.ingest(&event)?;
            fs::remove_file(&path)?;
            self.stats.spool_replayed += 1;
        }
        Ok(())
    }

    pub fn unarchived_count(&self) -> Result<u64> {
        let canonical: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM market_events m LEFT JOIN rust_archive_index a ON a.dedupe_key=m.dedupe_key AND a.payload_sha256=m.payload_sha256 WHERE a.dedupe_key IS NULL",
            [], |r| r.get(0),
        )?;
        let variants: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM market_event_versions m LEFT JOIN rust_archive_index a ON a.dedupe_key=m.dedupe_key AND a.payload_sha256=m.payload_sha256 WHERE a.dedupe_key IS NULL",
            [], |r| r.get(0),
        )?;
        Ok(canonical + variants)
    }

    fn outstanding_rows(&self, limit: usize) -> Result<Vec<ArchiveRow>> {
        let mut query = self.conn.prepare(
            r#"
            SELECT m.dedupe_key,m.received_utc,m.exchange_ts,m.channel,m.kind,m.market_ticker,m.payload_sha256,m.raw_json
            FROM market_events m LEFT JOIN rust_archive_index a
                ON a.dedupe_key=m.dedupe_key AND a.payload_sha256=m.payload_sha256
            WHERE a.dedupe_key IS NULL
            UNION ALL
            SELECT v.dedupe_key,v.received_utc,v.exchange_ts,v.channel,v.kind,v.market_ticker,v.payload_sha256,v.raw_json
            FROM market_event_versions v LEFT JOIN rust_archive_index a
                ON a.dedupe_key=v.dedupe_key AND a.payload_sha256=v.payload_sha256
            WHERE a.dedupe_key IS NULL
            LIMIT ?1
            "#
        )?;
        let rows = query.query_map([limit as i64], |row| {
            Ok(ArchiveRow {
                dedupe_key: row.get(0)?,
                received_utc: row.get(1)?,
                exchange_ts: row.get(2)?,
                channel: row.get(3)?,
                kind: row.get(4)?,
                market_ticker: row.get(5)?,
                payload_sha256: row.get(6)?,
                raw_json: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn register_part(&mut self, part: &PartInfo, rows: &[ArchiveRow]) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO archive_manifests VALUES (?1,?2,?3,?4,?5)",
            params![
                part.path,
                part.rows as i64,
                part.bytes as i64,
                part.sha256,
                utc_now()
            ],
        )?;
        for row in rows {
            tx.execute(
                "INSERT OR IGNORE INTO rust_archive_index VALUES (?1,?2,?3)",
                params![row.dedupe_key, row.payload_sha256, part.path],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn flush_archive(&mut self, limit: usize) -> Result<usize> {
        Ok(self.flush_archive_with_summary(limit)?.rows)
    }

    /// Finalize archive parts and return only rows whose manifest/index
    /// transaction committed. Runtime health can therefore distinguish
    /// SQLite journal acknowledgments from Parquet archive acknowledgments.
    pub fn flush_archive_with_summary(&mut self, limit: usize) -> Result<ArchiveFlushSummary> {
        if limit == 0 || limit > 5000 {
            bail!("archive chunk size outside 1..=5000")
        }
        let pending = self.outstanding_rows(limit)?;
        if pending.is_empty() {
            return Ok(ArchiveFlushSummary::default());
        }
        let mut partitions = BTreeMap::<PathBuf, Vec<ArchiveRow>>::new();
        for row in pending {
            partitions
                .entry(archive::partition_path(&self.archive_root, &row))
                .or_default()
                .push(row);
        }
        let mut summary = ArchiveFlushSummary::default();
        for (_path, rows) in partitions {
            // On disk errors, the original SQLite source remains intact. A
            // completed part without index will be discovered at next open.
            let part = archive::write_part(&self.archive_root, &rows)?;
            self.register_part(&part, &rows)?;
            summary.rows += rows.len();
            for row in rows
                .iter()
                .filter(|row| row.channel.starts_with("websocket_"))
            {
                summary.websocket_events += 1;
                let market = row
                    .market_ticker
                    .as_deref()
                    .map(|ticker| summary.by_market.entry(ticker.to_owned()).or_default());
                if row.kind == "orderbook_delta" {
                    summary.orderbook_deltas += 1;
                }
                if row.kind == "sequence_gap" {
                    summary.sequence_gaps += 1;
                }
                let is_raw_frame = row.channel == "websocket_raw";
                let is_delta_frame = is_raw_frame && row.kind == "orderbook_delta";
                if is_raw_frame {
                    summary.websocket_raw_frames += 1;
                }
                if is_delta_frame {
                    summary.orderbook_delta_frames += 1;
                    if summary
                        .latest_delta_received_utc
                        .as_ref()
                        .is_none_or(|latest| row.received_utc > *latest)
                    {
                        summary.latest_delta_received_utc = Some(row.received_utc.clone());
                    }
                }
                if let Some(market) = market {
                    market.websocket_events += 1;
                    if is_raw_frame {
                        market.raw_frames += 1;
                    }
                    if is_delta_frame {
                        market.orderbook_delta_frames += 1;
                    }
                    if row.kind == "orderbook_delta" {
                        market.orderbook_deltas += 1;
                        if market
                            .latest_delta_received_utc
                            .as_ref()
                            .is_none_or(|latest| row.received_utc > *latest)
                        {
                            market.latest_delta_received_utc = Some(row.received_utc.clone());
                        }
                    }
                    if row.kind == "sequence_gap" {
                        market.sequence_gaps += 1;
                    }
                    if market
                        .latest_event_received_utc
                        .as_ref()
                        .is_none_or(|latest| row.received_utc > *latest)
                    {
                        market.latest_event_received_utc = Some(row.received_utc.clone());
                    }
                }
                if let Some((session_id, sid)) = Self::archive_subscription_identity(row) {
                    let key = format!("{session_id}:{sid}");
                    let subscription = summary.by_subscription.entry(key).or_insert_with(|| {
                        SubscriptionArchiveSummary {
                            session_id,
                            sid,
                            ..SubscriptionArchiveSummary::default()
                        }
                    });
                    if is_raw_frame {
                        subscription.raw_frames += 1;
                        if subscription
                            .latest_frame_received_utc
                            .as_ref()
                            .is_none_or(|latest| row.received_utc > *latest)
                        {
                            subscription.latest_frame_received_utc = Some(row.received_utc.clone());
                        }
                    }
                    if is_delta_frame {
                        subscription.orderbook_delta_frames += 1;
                        if subscription
                            .latest_delta_received_utc
                            .as_ref()
                            .is_none_or(|latest| row.received_utc > *latest)
                        {
                            subscription.latest_delta_received_utc = Some(row.received_utc.clone());
                        }
                    }
                    if row.kind == "sequence_gap" {
                        subscription.sequence_gaps += 1;
                    }
                }
            }
        }
        Ok(summary)
    }

    fn archive_subscription_identity(row: &ArchiveRow) -> Option<(String, i64)> {
        let payload: Value = serde_json::from_str(&row.raw_json).ok()?;
        let session_id = payload.get("session_id")?.as_str()?.to_owned();
        let sid = payload.get("sid").and_then(Value::as_i64).or_else(|| {
            let raw = payload.get("raw")?;
            raw.get("sid")
                .and_then(Value::as_i64)
                .or_else(|| raw.get("msg")?.get("sid")?.as_i64())
        })?;
        Some((session_id, sid))
    }

    fn revoke_archive_part(&mut self, name: &str, reason: &str) -> Result<()> {
        // Do not delete or move any source/archive bytes. Only revoke the
        // staging index that previously claimed an unavailable/corrupt part.
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM rust_archive_index WHERE part_path=?1", [name])?;
        tx.execute("DELETE FROM archive_manifests WHERE part_path=?1", [name])?;
        tx.execute(
            "INSERT OR REPLACE INTO rust_archive_issues VALUES (?1,?2,?3)",
            params![name, reason, utc_now()],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn discover_existing_parts(&mut self, max_files: usize) -> Result<()> {
        let parts = archive::list_final_parts(&self.archive_root, max_files)?;
        // Verify manifests and index targets even when the file is absent.
        // An orphaned index must not hide missing data from reconciliation.
        let tracked = {
            let mut stmt = self.conn.prepare(
                "SELECT part_path FROM archive_manifests UNION SELECT part_path FROM rust_archive_index"
            )?;
            let result = stmt.query_map([], |r| r.get::<_, String>(0))?;
            result.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if tracked.len() > max_files {
            bail!("too many staged archive manifest entries")
        }
        for name in tracked {
            if !self.archive_root.join(&name).is_file() {
                self.revoke_archive_part(&name, "MISSING_FINALIZED_PART")?;
            }
        }
        for part in parts {
            let name = part
                .strip_prefix(&self.archive_root)?
                .to_string_lossy()
                .replace('\\', "/");
            let actual_sha = match archive::hash_file(&part) {
                Ok(digest) => digest,
                Err(_) => {
                    self.revoke_archive_part(&name, "UNREADABLE_FINALIZED_PART")?;
                    continue;
                }
            };
            let expected: Option<String> = self
                .conn
                .query_row(
                    "SELECT sha256 FROM archive_manifests WHERE part_path=?1",
                    [&name],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(expected_sha) = expected {
                if actual_sha != expected_sha {
                    self.revoke_archive_part(&name, "CORRUPT_SHA256_MISMATCH")?;
                    continue;
                }
            }
            let rows = match archive::read_part(&part) {
                Ok(rows) => rows,
                Err(_) => {
                    self.revoke_archive_part(&name, "CORRUPT_PARQUET_DATA")?;
                    continue;
                }
            };
            let meta = PartInfo {
                path: name,
                rows: rows.len(),
                bytes: part.metadata()?.len(),
                sha256: actual_sha,
            };
            self.register_part(&meta, &rows)?;
        }
        Ok(())
    }

    pub fn verify_archive(&self) -> Result<bool> {
        let capture_failure: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM collector_markers WHERE marker='capture_failure'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if capture_failure.is_some() {
            return Ok(false);
        }
        let pending_spool = self.home.join("data").join("rust_pending");
        if pending_spool.exists() && fs::read_dir(pending_spool)?.next().is_some() {
            return Ok(false);
        }
        if self.unarchived_count()? > 0 {
            return Ok(false);
        }
        let broken: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM rust_archive_issues WHERE failure LIKE 'CORRUPT_%' OR failure='UNREADABLE_FINALIZED_PART'",
            [],|r|r.get(0)
        )?;
        if broken > 0 {
            return Ok(false);
        }
        let parts = archive::list_final_parts(&self.archive_root, 10_000)?;
        let mut expected_rows: i64 = 0;
        let count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM rust_archive_index", [], |r| r.get(0))?;
        let canonical: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM market_events", [], |r| r.get(0))?;
        let conflicts: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM market_event_versions", [], |r| {
                    r.get(0)
                })?;
        if count != canonical + conflicts {
            return Ok(false);
        }
        if parts.len()
            != self
                .conn
                .query_row("SELECT COUNT(*) FROM archive_manifests", [], |r| {
                    r.get::<_, i64>(0)
                })? as usize
        {
            return Ok(false);
        }
        // Presence of any incomplete temporary part prevents a clean marker.
        for date in std::fs::read_dir(&self.archive_root)? {
            let date = date?;
            if !date.file_type()?.is_dir() {
                continue;
            }
            for kind in std::fs::read_dir(date.path())? {
                let kind = kind?;
                if !kind.file_type()?.is_dir() {
                    continue;
                }
                for file in std::fs::read_dir(kind.path())? {
                    if file?.path().extension().and_then(|e| e.to_str()) == Some("partial") {
                        return Ok(false);
                    }
                }
            }
        }
        let mut statement = self
            .conn
            .prepare("SELECT part_path, row_count, byte_count, sha256 FROM archive_manifests")?;
        let checks = statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        for check in checks {
            let (name, rows, bytes, checksum) = check?;
            expected_rows += rows;
            let path = self.archive_root.join(name);
            if !path.is_file()
                || path.metadata()?.len() != bytes as u64
                || archive::hash_file(&path)? != checksum
            {
                return Ok(false);
            }
        }
        Ok(count == expected_rows)
    }

    pub fn close(mut self) -> Result<Stats> {
        let capture_failure: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM collector_markers WHERE marker='capture_failure'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if capture_failure.is_some() {
            self.mark("CLOSED_WITH_UNSPOOLED_EVENT")?;
            bail!("capture contains an event that was not durably persisted")
        }
        let pending_spool = self.home.join("data").join("rust_pending");
        if pending_spool.exists() && fs::read_dir(&pending_spool)?.next().is_some() {
            self.mark("CLOSED_WITH_PENDING_SPOOL_UNVERIFIED")?;
            bail!("emergency spool still contains unacknowledged events")
        }
        for _ in 0..200 {
            if self.unarchived_count()? == 0 {
                break;
            }
            let n = self.flush_archive(2000)?;
            if n == 0 {
                bail!("archive reconciliation made no progress")
            }
        }
        let verified = self.verify_archive()?;
        self.mark(if verified {
            "CLEAN_ARCHIVE_VERIFIED"
        } else {
            "CLOSED_WITH_ARCHIVE_UNVERIFIED"
        })?;
        if !verified {
            bail!("journal complete but archive is not independently verified")
        }
        Ok(self.stats)
    }

    pub fn audit(&self) -> Result<serde_json::Value> {
        let canonical: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM market_events", [], |r| r.get(0))?;
        let variants: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM market_event_versions", [], |r| {
                    r.get(0)
                })?;
        let unarchived = self.unarchived_count()?;
        let marker: String = self.conn.query_row(
            "SELECT value FROM collector_markers WHERE marker='archive_shutdown'",
            [],
            |r| r.get(0),
        )?;
        let capture_failure: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM collector_markers WHERE marker='capture_failure'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let spool_dir = self.home.join("data").join("rust_pending");
        let pending_spool_entries = if spool_dir.is_dir() {
            fs::read_dir(spool_dir)?.take(10_001).count()
        } else {
            0
        };
        Ok(json!({
            "canonical_events":canonical, "conflicting_versions":variants,
            "unarchived":unarchived, "archive_marker":marker,
            "pending_spool_entries":pending_spool_entries,
            "capture_failure":capture_failure,
            "staging_home":self.home.to_string_lossy()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn capacity_thresholds_warn_and_fail_closed_at_low_free_space() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(capacity_state(3 * GIB, 0), "NORMAL");
        assert_eq!(
            capacity_state(1500 * 1024 * 1024, 0),
            "WARNING_CAPACITY_PRESSURE"
        );
        assert_eq!(
            capacity_state(3 * GIB, 512 * 1024 * 1024),
            "WARNING_CAPACITY_PRESSURE"
        );
        assert_eq!(
            capacity_state(511 * 1024 * 1024, 0),
            "CRITICAL_DISK_PRESSURE"
        );
    }

    #[test]
    fn capacity_snapshot_reads_only_the_staging_journal_and_volume() {
        let home = tempfile::tempdir().unwrap();
        let journal = Journal::open(home.path()).unwrap();
        let snapshot = journal.capacity_snapshot().unwrap();
        assert!(snapshot.disk_free_bytes > 0);
        assert!(snapshot.sqlite_bytes > 0);
        assert_eq!(
            snapshot.capacity_state,
            capacity_state(snapshot.disk_free_bytes, snapshot.wal_bytes)
        );
        journal.close().unwrap();
    }

    #[test]
    fn unavailable_capacity_volume_fails_explicitly_and_preserves_journaled_data() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "websocket_orderbook",
            "orderbook_delta",
            Some("KXBTC15M-DISK-MOCK"),
            json!({"seq":31}),
            Some("disk-capacity-probe-mock".into()),
        );
        assert_eq!(journal.ingest(&event).unwrap(), IngestResult::Inserted);

        let capacity_error = journal
            .capacity_snapshot_with_probe(|_| Err(std::io::Error::other("injected volume outage")))
            .unwrap_err();
        assert!(format!("{capacity_error:#}").contains("injected volume outage"));
        assert_eq!(journal.audit().unwrap()["canonical_events"], 1);
        journal.close().unwrap();
    }

    #[test]
    fn critical_capacity_snapshot_is_reported_without_losing_journaled_data() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "websocket_orderbook",
            "orderbook_delta",
            Some("KXBTC15M-LOW-CAPACITY-MOCK"),
            json!({"seq":32}),
            Some("low-capacity-snapshot-mock".into()),
        );
        assert_eq!(journal.ingest(&event).unwrap(), IngestResult::Inserted);

        let snapshot = journal
            .capacity_snapshot_with_probe(|_| Ok(256 * 1024 * 1024))
            .unwrap();
        assert_eq!(snapshot.capacity_state, "CRITICAL_DISK_PRESSURE");
        assert!(snapshot.sqlite_bytes > 0);
        assert_eq!(journal.audit().unwrap()["canonical_events"], 1);
        journal.close().unwrap();
    }

    #[test]
    fn readonly_audit_preserves_clean_marker_and_does_not_replay_spool() {
        let home = tempfile::tempdir().unwrap();
        let mut writer = Journal::open(home.path()).unwrap();
        let event = Event::new("rest", "trade", None, json!({"id":9}), Some("audit".into()));
        writer.ingest(&event).unwrap();
        writer.close().unwrap();

        let pending_dir = home.path().join("data").join("rust_pending");
        fs::create_dir_all(&pending_dir).unwrap();
        let pending = Event::new(
            "rest",
            "trade",
            None,
            json!({"id":10}),
            Some("pending".into()),
        );
        let pending_path = pending_dir.join("pending.json");
        fs::write(&pending_path, serde_json::to_vec(&pending).unwrap()).unwrap();

        let readonly = Journal::open_readonly(home.path()).unwrap();
        assert_eq!(
            readonly.audit().unwrap()["archive_marker"],
            "CLEAN_ARCHIVE_VERIFIED"
        );
        assert_eq!(readonly.audit().unwrap()["canonical_events"], 1);
        assert_eq!(readonly.audit().unwrap()["pending_spool_entries"], 1);
        assert!(!readonly.verify_archive().unwrap());
        drop(readonly);
        assert!(pending_path.is_file());
    }

    #[test]
    fn readonly_archive_verification_fails_for_missing_or_corrupt_parts() {
        for corrupt in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let mut writer = Journal::open(home.path()).unwrap();
            let event = Event::new(
                "rest",
                "trade",
                Some("ARCHIVE-TEST"),
                json!({"id":"integrity"}),
                Some("archive-integrity-test".into()),
            );
            assert_eq!(writer.ingest(&event).unwrap(), IngestResult::Inserted);
            writer.close().unwrap();

            let archive_root = home.path().join("data").join("market_data");
            let parts = archive::list_final_parts(&archive_root, 10).unwrap();
            assert_eq!(parts.len(), 1);
            if corrupt {
                let mut bytes = fs::read(&parts[0]).unwrap();
                bytes[0] ^= 0x01;
                fs::write(&parts[0], bytes).unwrap();
            } else {
                fs::remove_file(&parts[0]).unwrap();
            }

            let readonly = Journal::open_readonly(home.path()).unwrap();
            assert!(!readonly.verify_archive().unwrap());
            assert_eq!(
                readonly.audit().unwrap()["archive_marker"],
                "CLEAN_ARCHIVE_VERIFIED"
            );
            drop(readonly);
            assert_eq!(parts[0].exists(), corrupt);
        }
    }

    #[test]
    fn journal_retains_conflicts_and_archives_exactly_once() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let first = Event::new(
            "rest",
            "metadata",
            Some("TEST"),
            json!({"price":1}),
            Some("K".into()),
        );
        let changed = Event::new(
            "rest",
            "metadata",
            Some("TEST"),
            json!({"price":2}),
            Some("K".into()),
        );
        assert_eq!(store.ingest(&first).unwrap(), IngestResult::Inserted);
        assert_eq!(store.ingest(&changed).unwrap(), IngestResult::Versioned);
        assert_eq!(store.ingest(&changed).unwrap(), IngestResult::Duplicate);
        assert_eq!(store.unarchived_count().unwrap(), 2);
        store.flush_archive(200).unwrap();
        assert_eq!(store.unarchived_count().unwrap(), 0);
        assert!(store.verify_archive().unwrap());
        store.close().unwrap();
        let reopened = Journal::open(home.path()).unwrap();
        assert_eq!(reopened.audit().unwrap()["conflicting_versions"], 1);
        reopened.close().unwrap();
    }

    #[test]
    fn archive_flush_summary_counts_only_manifest_committed_websocket_rows() {
        let home = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(home.path()).unwrap();
        let events = [
            Event::new(
                "websocket_orderbook",
                "orderbook_snapshot",
                Some("KXBTC15M-ARCHIVE-HEALTH"),
                json!({"seq":10}),
                Some("archive-health-snapshot".into()),
            ),
            Event::new(
                "websocket_orderbook",
                "orderbook_delta",
                Some("KXBTC15M-ARCHIVE-HEALTH"),
                json!({"seq":11}),
                Some("archive-health-delta".into()),
            ),
            Event::new(
                "websocket_orderbook",
                "sequence_gap",
                Some("KXBTC15M-ARCHIVE-HEALTH"),
                json!({"prior_seq":11,"current_seq":13}),
                Some("archive-health-gap".into()),
            ),
            Event::new(
                "websocket_raw",
                "orderbook_delta",
                Some("KXBTC15M-ARCHIVE-HEALTH"),
                json!({"session_id":"mock-session","raw":{"sid":7,"seq":11,
                    "msg":{"market_ticker":"KXBTC15M-ARCHIVE-HEALTH"}}}),
                Some("archive-health-raw-delta".into()),
            ),
            Event::new(
                "rest",
                "trade",
                Some("KXBTC15M-ARCHIVE-HEALTH"),
                json!({"trade_id":"rest-only"}),
                Some("archive-health-rest".into()),
            ),
        ];
        for event in &events {
            assert_eq!(journal.ingest(event).unwrap(), IngestResult::Inserted);
        }

        let summary = journal.flush_archive_with_summary(10).unwrap();
        assert_eq!(summary.rows, 5);
        assert_eq!(summary.websocket_events, 4);
        assert_eq!(summary.websocket_raw_frames, 1);
        assert_eq!(summary.orderbook_delta_frames, 1);
        assert_eq!(summary.orderbook_deltas, 2);
        assert_eq!(summary.sequence_gaps, 1);
        let market = &summary.by_market["KXBTC15M-ARCHIVE-HEALTH"];
        assert_eq!(market.websocket_events, 4);
        assert_eq!(market.raw_frames, 1);
        assert_eq!(market.orderbook_delta_frames, 1);
        assert_eq!(market.orderbook_deltas, 2);
        assert_eq!(market.sequence_gaps, 1);
        assert!(market.latest_event_received_utc.is_some());
        let subscription = &summary.by_subscription["mock-session:7"];
        assert_eq!(subscription.raw_frames, 1);
        assert_eq!(subscription.orderbook_delta_frames, 1);
        assert_eq!(journal.unarchived_count().unwrap(), 0);
        assert!(journal.verify_archive().unwrap());
        journal.close().unwrap();
    }

    #[test]
    fn interrupted_after_sqlite_commit_is_reconciled() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "rest",
            "trade",
            Some("T"),
            json!({"id":"a"}),
            Some("T-a".into()),
        );
        store.ingest(&event).unwrap();
        drop(store); // crash before Parquet finalize
        let reopened = Journal::open(home.path()).unwrap();
        assert_eq!(reopened.unarchived_count().unwrap(), 1);
        reopened.close().unwrap();
        let check = Journal::open(home.path()).unwrap();
        assert!(check.verify_archive().unwrap());
        check.close().unwrap();
    }

    #[test]
    fn interrupted_after_parquet_finalization_reconciles_orphan_part() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "rest",
            "trade",
            Some("T"),
            json!({"id":"finalized-before-manifest"}),
            Some("T-finalized-before-manifest".into()),
        );
        assert_eq!(store.ingest(&event).unwrap(), IngestResult::Inserted);

        // Simulate a process exit after the part is atomically finalized but
        // before its SQLite manifest/index transaction commits.
        let pending = store.outstanding_rows(10).unwrap();
        assert_eq!(pending.len(), 1);
        let part = archive::write_part(&store.archive_root, &pending).unwrap();
        assert!(store.archive_root.join(&part.path).is_file());
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM archive_manifests", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(store);

        let recovered = Journal::open(home.path()).unwrap();
        assert_eq!(recovered.unarchived_count().unwrap(), 0);
        assert_eq!(recovered.audit().unwrap()["canonical_events"], 1);
        assert!(recovered.verify_archive().unwrap());
        assert_eq!(
            recovered
                .conn
                .query_row("SELECT COUNT(*) FROM archive_manifests", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        recovered.close().unwrap();
    }

    #[test]
    fn interrupted_partial_parquet_prevents_clean_shutdown_and_archive_verification() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "rest",
            "trade",
            Some("PARTIAL-PARQUET-TEST"),
            json!({"id":"partial-parquet-must-not-be-clean"}),
            Some("partial-parquet-test".into()),
        );
        assert_eq!(store.ingest(&event).unwrap(), IngestResult::Inserted);
        assert_eq!(store.flush_archive(10).unwrap(), 1);

        let parts = archive::list_final_parts(&store.archive_root, 10).unwrap();
        assert_eq!(parts.len(), 1);
        let partial = parts[0]
            .parent()
            .unwrap()
            .join("interrupted-partial.parquet.partial");
        fs::write(&partial, b"simulated interrupted parquet write").unwrap();

        assert!(!store.verify_archive().unwrap());
        assert!(store.close().is_err());

        let readonly = Journal::open_readonly(home.path()).unwrap();
        assert_eq!(
            readonly.audit().unwrap()["archive_marker"],
            "CLOSED_WITH_ARCHIVE_UNVERIFIED"
        );
        assert!(!readonly.verify_archive().unwrap());
        assert!(partial.is_file());
    }

    #[test]
    fn temporary_archive_unavailability_never_reports_clean_and_recovers_from_sqlite() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "rest",
            "trade",
            Some("DISK-FAILURE-TEST"),
            json!({"id":"archive-volume-unavailable"}),
            Some("archive-volume-unavailable".into()),
        );
        assert_eq!(store.ingest(&event).unwrap(), IngestResult::Inserted);
        let archive_root = home.path().join("data").join("market_data");

        // Isolated fault injection: make the staging archive path unavailable
        // while leaving the durable SQLite journal intact.
        fs::remove_dir(&archive_root).unwrap();
        fs::write(&archive_root, b"simulated unavailable archive mount").unwrap();
        assert!(store.flush_archive(10).is_err());
        assert_eq!(store.unarchived_count().unwrap(), 1);
        assert!(store.close().is_err());

        let conn = Connection::open(home.path().join("data/paper_ledger.sqlite3")).unwrap();
        let canonical: i64 = conn
            .query_row("SELECT COUNT(*) FROM market_events", [], |row| row.get(0))
            .unwrap();
        let marker: String = conn
            .query_row(
                "SELECT value FROM collector_markers WHERE marker='archive_shutdown'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(canonical, 1);
        assert_ne!(marker, "CLEAN_ARCHIVE_VERIFIED");
        drop(conn);

        fs::remove_file(&archive_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let recovered = Journal::open(home.path()).unwrap();
        assert_eq!(recovered.unarchived_count().unwrap(), 1);
        recovered.close().unwrap();
        let verified = Journal::open_readonly(home.path()).unwrap();
        assert!(verified.verify_archive().unwrap());
        assert_eq!(verified.audit().unwrap()["canonical_events"], 1);
    }

    #[test]
    fn spool_survives_restart_and_acknowledges_only_after_replay() {
        let home = tempfile::tempdir().unwrap();
        let store = Journal::open(home.path()).unwrap();
        let event = Event::new("rest", "trade", None, json!({"id":7}), Some("a".into()));
        let path = store.spool(&event).unwrap();
        assert!(path.exists());
        drop(store);
        let restarted = Journal::open(home.path()).unwrap();
        assert!(!path.exists());
        assert_eq!(restarted.audit().unwrap()["canonical_events"], 1);
        restarted.close().unwrap();
    }

    #[test]
    fn close_cannot_mark_clean_while_emergency_spool_is_pending() {
        let home = tempfile::tempdir().unwrap();
        let store = Journal::open(home.path()).unwrap();
        let event = Event::new(
            "rest",
            "trade",
            None,
            json!({"id":8}),
            Some("pending".into()),
        );
        store.spool(&event).unwrap();
        assert!(store.close().is_err());
        let conn = Connection::open(home.path().join("data/paper_ledger.sqlite3")).unwrap();
        let marker: String = conn
            .query_row(
                "SELECT value FROM collector_markers WHERE marker='archive_shutdown'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(marker, "CLOSED_WITH_PENDING_SPOOL_UNVERIFIED");
    }

    #[test]
    fn interrupted_partial_spool_prevents_clean_shutdown_and_archive_verification() {
        let home = tempfile::tempdir().unwrap();
        let store = Journal::open(home.path()).unwrap();
        let spool_dir = home.path().join("data").join("rust_pending");
        fs::create_dir_all(&spool_dir).unwrap();
        let partial = spool_dir.join("interrupted.partial");
        fs::write(&partial, b"{truncated emergency spool").unwrap();

        assert!(store.close().is_err());
        assert!(partial.is_file());

        let readonly = Journal::open_readonly(home.path()).unwrap();
        let audit = readonly.audit().unwrap();
        assert_eq!(audit["pending_spool_entries"], 1);
        assert_eq!(
            audit["archive_marker"],
            "CLOSED_WITH_PENDING_SPOOL_UNVERIFIED"
        );
        assert!(!readonly.verify_archive().unwrap());
        drop(readonly);
        assert!(partial.is_file());
    }

    #[test]
    fn corrupt_spool_fails_open_and_preserves_evidence() {
        let home = tempfile::tempdir().unwrap();
        let spool_dir = home.path().join("data").join("rust_pending");
        fs::create_dir_all(&spool_dir).unwrap();
        let corrupt = spool_dir.join("000-corrupt.json");
        fs::write(&corrupt, b"{not valid event json").unwrap();

        let opened = Journal::open(home.path());
        assert!(opened.is_err());
        assert!(corrupt.is_file(), "corrupt spool evidence must be retained");

        let readonly = Journal::open_readonly(home.path()).unwrap();
        assert_eq!(readonly.audit().unwrap()["pending_spool_entries"], 1);
        assert!(!readonly.verify_archive().unwrap());
        drop(readonly);
        assert!(corrupt.is_file());
    }

    #[test]
    fn spool_replay_limit_fails_before_ingesting_or_removing_entries() {
        let home = tempfile::tempdir().unwrap();
        let mut store = Journal::open(home.path()).unwrap();
        let first = store
            .spool(&Event::new(
                "rest",
                "trade",
                None,
                json!({"id":1}),
                Some("one".into()),
            ))
            .unwrap();
        let second = store
            .spool(&Event::new(
                "rest",
                "trade",
                None,
                json!({"id":2}),
                Some("two".into()),
            ))
            .unwrap();

        assert!(store.replay_spool(1).is_err());
        assert!(first.is_file());
        assert!(second.is_file());
        assert_eq!(store.unarchived_count().unwrap(), 0);
        assert_eq!(store.stats.spool_replayed, 0);
        drop(store);

        let recovered = Journal::open(home.path()).unwrap();
        assert!(!first.exists());
        assert!(!second.exists());
        assert_eq!(recovered.audit().unwrap()["canonical_events"], 2);
        recovered.close().unwrap();
    }
}
