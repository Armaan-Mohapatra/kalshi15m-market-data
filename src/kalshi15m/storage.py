from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings, settings

LOG = logging.getLogger(__name__)
_JOURNAL_ZSTD_PREFIX = b"K15Z1"
_JOURNAL_ZSTD_HEADER_BYTES = len(_JOURNAL_ZSTD_PREFIX) + 4
_JOURNAL_ZSTD_CODEC = pa.Codec("zstd", compression_level=3)


def encode_journal_payload(raw_json: str) -> str | bytes:
    """Compress worthwhile SQLite journal payloads without changing Parquet JSON."""
    raw = raw_json.encode("utf-8")
    compressed = bytes(_JOURNAL_ZSTD_CODEC.compress(raw))
    encoded = (
        _JOURNAL_ZSTD_PREFIX
        + len(raw).to_bytes(4, "big")
        + compressed
    )
    # Small or incompressible payloads remain ordinary SQLite TEXT.
    return encoded if len(encoded) + 16 < len(raw) else raw_json


def decode_journal_payload(value: str | bytes) -> str:
    """Decode legacy TEXT or the versioned ZSTD BLOB form in raw_json."""
    if isinstance(value, str):
        return value
    if not isinstance(value, bytes):
        raise TypeError("journal raw_json must be TEXT or BLOB")
    if value.startswith(_JOURNAL_ZSTD_PREFIX):
        if len(value) <= _JOURNAL_ZSTD_HEADER_BYTES:
            raise ValueError("compressed journal payload is truncated")
        size = int.from_bytes(
            value[len(_JOURNAL_ZSTD_PREFIX):_JOURNAL_ZSTD_HEADER_BYTES], "big"
        )
        decoded = bytes(
            _JOURNAL_ZSTD_CODEC.decompress(
                value[_JOURNAL_ZSTD_HEADER_BYTES:], decompressed_size=size
            )
        )
        if len(decoded) != size:
            raise ValueError("compressed journal payload size does not match its header")
        return decoded.decode("utf-8")
    # Preserve compatibility with any pre-existing UTF-8 BLOB rows.
    return value.decode("utf-8")


def event_archive_fingerprint(row: dict[str, Any]) -> str:
    """Hash the complete event row as it appears in the Parquet archive."""
    values = [
        row.get("dedupe_key"),
        row.get("received_utc"),
        row.get("exchange_ts"),
        row.get("channel"),
        row.get("kind"),
        row.get("market_ticker"),
        decode_journal_payload(row["raw_json"]),
        row.get("payload_sha256"),
    ]
    raw = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class Storage:
    """Single-writer SQLite WAL journal plus partitioned ZSTD Parquet capture."""

    def __init__(self, cfg: Settings = settings) -> None:
        self.cfg = cfg
        self.cfg.ensure_dirs()
        self.previous_service_state: str | None = None
        self.previous_raw_capture_state: str | None = None
        self.previous_parquet_dropped_rows = 0
        status_path = self.cfg.runtime_dir / "status.json"
        try:
            previous = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(previous, dict):
                prior_state = previous.get("service_state")
                self.previous_service_state = str(prior_state) if prior_state else None
                prior_capture_state = previous.get("raw_capture_state")
                self.previous_raw_capture_state = (
                    str(prior_capture_state) if prior_capture_state else None
                )
                try:
                    self.previous_parquet_dropped_rows = max(
                        0, int(previous.get("parquet_dropped_rows") or 0)
                    )
                except (TypeError, ValueError):
                    self.previous_parquet_dropped_rows = 0
        except (OSError, json.JSONDecodeError):
            pass
        self.connection: sqlite3.Connection | None = None
        self.sqlite_open_path: Path | None = None
        self.sqlite_open_file_identity: tuple[int, int] | None = None
        self.paper_ledger_record_count = 0
        self.latest_paper_record_utc: str | None = None
        self.latest_market_event_received_utc: str | None = None
        self.raw_capture_enabled = True
        self.parquet_bytes = 0
        self.sqlite_bytes = 0
        self.events_inserted = 0
        self.events_duplicate = 0
        self.events_key_conflict = 0
        self.events_quarantined = 0
        self.events_spooled = 0
        self.events_replayed = 0
        self.parquet_buffer: list[dict[str, Any]] = []
        self.parquet_buffer_bytes_estimate = 0
        self.parquet_buffer_started_monotonic: float | None = None
        self.parquet_flush_rows = 2000
        self.parquet_flush_interval_seconds = 60.0
        self.parquet_flush_count = 0
        self.parquet_last_flush_utc: str | None = None
        self.parquet_last_flush_rows = 0
        self.parquet_last_flush_reason: str | None = None
        self.parquet_dropped_rows = self.previous_parquet_dropped_rows
        self.parquet_recovery_state = "NOT_RUN"
        self.parquet_recovery_rows = 0
        self.corrupt_parquet_parts: list[str] = []
        self.orphaned_partial_parts = 0
        self.persistence_audit_interval_seconds = 15.0
        self.persistence_audit_last_monotonic = 0.0
        self.persistence_audit_cache: dict[str, Any] = {}

    def open(self) -> None:
        self.connection = sqlite3.connect(self.cfg.ledger_path, timeout=15)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=15000")
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS market_events (
                dedupe_key TEXT PRIMARY KEY,
                received_utc TEXT NOT NULL,
                exchange_ts TEXT,
                channel TEXT NOT NULL,
                kind TEXT NOT NULL,
                market_ticker TEXT,
                raw_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            )"""
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS market_event_versions (
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
                PRIMARY KEY (source_generation, row_sha256)
            ) WITHOUT ROWID"""
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS archive_manifests (
                part_path TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL,
                byte_count INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                committed_utc TEXT NOT NULL
            )"""
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS collector_markers (
                marker TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            )"""
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS quarantined_market_events (
                quarantine_id TEXT PRIMARY KEY,
                captured_utc TEXT NOT NULL,
                reason TEXT NOT NULL,
                original_dedupe_key TEXT,
                event_diagnostic_json TEXT NOT NULL,
                original_payload_fidelity TEXT NOT NULL
            )"""
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS paper_ledger (
                idempotency_key TEXT PRIMARY KEY,
                created_utc TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )"""
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_market_time ON market_events(market_ticker, received_utc)"
        )
        self.connection.commit()
        database_rows = self.connection.execute("PRAGMA database_list").fetchall()
        main_database = next((row for row in database_rows if row[1] == "main"), None)
        if main_database is None or not main_database[2]:
            self.connection.close()
            self.connection = None
            raise RuntimeError("SQLite main database path is unavailable")
        self.sqlite_open_path = Path(str(main_database[2])).resolve()
        configured_path = self.cfg.ledger_path.resolve()
        if os.path.normcase(str(self.sqlite_open_path)) != os.path.normcase(str(configured_path)):
            self.connection.close()
            self.connection = None
            raise RuntimeError("SQLite opened a database outside the configured paper ledger path")
        self.sqlite_open_file_identity = self._file_identity(self.sqlite_open_path)
        self.paper_ledger_record_count = int(
            self.connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0]
        )
        latest_paper = self.connection.execute(
            "SELECT created_utc FROM paper_ledger ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        self.latest_paper_record_utc = str(latest_paper[0]) if latest_paper else None
        latest_event = self.connection.execute(
            "SELECT received_utc FROM market_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        self.latest_market_event_received_utc = str(latest_event[0]) if latest_event else None
        self.parquet_bytes = self._parquet_directory_size()
        self.sqlite_bytes = self._sqlite_size()
        # A status.json label is not an archival commit marker. The previous
        # process must have durably verified its final archive before skip is
        # allowed. Missing/old schemas always go through reconciliation.
        previous_marker = self.connection.execute(
            "SELECT value FROM collector_markers WHERE marker = 'archive_shutdown'"
        ).fetchone()
        self.connection.execute(
            "INSERT OR REPLACE INTO collector_markers VALUES (?, ?, ?)",
            ("archive_shutdown", "RUNNING_UNVERIFIED", datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()
        self.orphaned_partial_parts = sum(
            1 for _ in self.cfg.parquet_dir.rglob("*.partial")
        )
        if (
            previous_marker
            and previous_marker[0] == "CLEAN_ARCHIVE_VERIFIED"
            and self._archive_manifest_files_present()
        ):
            self.parquet_recovery_state = "SKIPPED_VERIFIED_SHUTDOWN_MARKER"
        else:
            try:
                self.parquet_recovery_rows = self.recover_parquet_archive()
                if self.parquet_recovery_state == "NOT_RUN":
                    self.parquet_recovery_state = "VERIFIED_NO_GAP"
            except Exception as exc:
                self.parquet_recovery_state = f"FAILED_{type(exc).__name__}"
                LOG.exception("Parquet archive recovery failed; SQLite journal remains available")

    @staticmethod
    def _file_identity(path: Path) -> tuple[int, int] | None:
        try:
            stat = path.stat()
        except OSError:
            return None
        if not stat.st_ino:
            return None
        return int(stat.st_dev), int(stat.st_ino)

    def connection_status(self) -> dict[str, Any]:
        self.sqlite_bytes = self._sqlite_size()
        now = time.monotonic()
        if now - self.persistence_audit_last_monotonic >= self.persistence_audit_interval_seconds:
            self._refresh_persistence_audit()
            self.persistence_audit_last_monotonic = now
        disk_audit = self.persistence_audit_cache
        configured_path = self.cfg.ledger_path.resolve()
        opened_path = self.sqlite_open_path
        path_matches = (
            os.path.normcase(str(opened_path)) == os.path.normcase(str(configured_path))
            if opened_path is not None
            else None
        )
        current_identity = self._file_identity(configured_path)
        if self.sqlite_open_file_identity is None or current_identity is None:
            identity_state = "UNKNOWN"
        elif current_identity == self.sqlite_open_file_identity:
            identity_state = "UNCHANGED_SINCE_OPEN"
        else:
            identity_state = "CHANGED_SINCE_OPEN"
        return {
            "storage_configured_ledger_path": str(configured_path),
            "storage_open_database_path": str(opened_path) if opened_path else None,
            "storage_database_path_matches_config": path_matches,
            "storage_path_file_identity_state": identity_state,
            "storage_open_file_identity_at_open": (
                list(self.sqlite_open_file_identity)
                if self.sqlite_open_file_identity is not None
                else None
            ),
            "storage_configured_file_identity_now": (
                list(current_identity) if current_identity is not None else None
            ),
            "storage_paper_ledger_record_count": self.paper_ledger_record_count,
            "storage_latest_paper_record_utc": self.latest_paper_record_utc,
            "storage_latest_market_event_received_utc": self.latest_market_event_received_utc,
            **disk_audit,
            "sqlite_bytes": self.sqlite_bytes,
            "sqlite_growth_warning": self.sqlite_bytes >= 8 * 1024**3,
            "parquet_bytes": self.parquet_bytes,
            "events_key_conflict": self.events_key_conflict,
            "events_quarantined": self.events_quarantined,
            "events_spooled": self.events_spooled,
            "events_replayed": self.events_replayed,
            "spool_pending_batch_count": sum(
                1 for _ in self.pending_spool_dir.glob("pending-*.json")
            ),
            "archive_corrupt_part_count": len(self.corrupt_parquet_parts),
            "archive_corrupt_part_paths": [Path(path).name for path in self.corrupt_parquet_parts[:20]],
            "archive_orphaned_partial_count": self.orphaned_partial_parts,
            "parquet_buffered_rows": len(self.parquet_buffer),
            "parquet_buffer_estimated_bytes": self.parquet_buffer_bytes_estimate,
            "parquet_flush_count": self.parquet_flush_count,
            "parquet_last_flush_utc": self.parquet_last_flush_utc,
            "parquet_last_flush_rows": self.parquet_last_flush_rows,
            "parquet_last_flush_reason": self.parquet_last_flush_reason,
            "parquet_dropped_rows": self.parquet_dropped_rows,
        }

    def _refresh_persistence_audit(self) -> None:
        """Compare in-memory watermarks with a separate read-only SQLite view."""
        path = self.cfg.ledger_path.resolve()
        probe: sqlite3.Connection | None = None
        try:
            probe = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
            latest_event = probe.execute(
                "SELECT rowid, received_utc FROM market_events ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            latest_paper = probe.execute(
                "SELECT rowid, created_utc FROM paper_ledger ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            disk_event_rowid = int(latest_event[0]) if latest_event else None
            disk_paper_rowid = int(latest_paper[0]) if latest_paper else None
            disk_event = str(latest_event[1]) if latest_event else None
            disk_paper = str(latest_paper[1]) if latest_paper else None
            self.parquet_bytes = self._parquet_directory_size()
            self.persistence_audit_cache = {
                "storage_latest_disk_market_event_rowid": disk_event_rowid,
                "storage_latest_disk_paper_record_rowid": disk_paper_rowid,
                "storage_latest_disk_market_event_received_utc": disk_event,
                "storage_latest_disk_paper_record_utc": disk_paper,
                "storage_market_event_watermark_matches_disk": (
                    disk_event == self.latest_market_event_received_utc
                ),
                "storage_paper_watermark_matches_disk": (
                    disk_paper == self.latest_paper_record_utc
                ),
                "storage_persistence_audit_error": None,
            }
        except (OSError, sqlite3.Error) as exc:
            self.persistence_audit_cache = {
                "storage_latest_disk_market_event_rowid": None,
                "storage_latest_disk_paper_record_rowid": None,
                "storage_latest_disk_market_event_received_utc": None,
                "storage_latest_disk_paper_record_utc": None,
                "storage_market_event_watermark_matches_disk": None,
                "storage_paper_watermark_matches_disk": None,
                "storage_persistence_audit_error": type(exc).__name__,
            }
        finally:
            if probe is not None:
                probe.close()

    def recover_parquet_archive(self) -> int:
        """Rebuild missing Parquet event variants from the durable SQLite journal."""
        self.corrupt_parquet_parts = []
        if self.connection is None:
            raise RuntimeError("Storage is not open")
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.raw_capture_enabled = False
            self.parquet_recovery_state = "SKIPPED_RAW_BUDGET"
            return 0
        try:
            free_bytes = shutil.disk_usage(self.cfg.home).free
        except OSError:
            free_bytes = 0
        if free_bytes < self.cfg.min_free_bytes:
            self.raw_capture_enabled = False
            self.parquet_recovery_state = "SKIPPED_LOW_DISK"
            return 0

        # Recovery may scan a long-running journal. Keep the archive index on
        # disk instead of retaining every fingerprint in a Python set (which
        # can consume several times the archive size in RAM).
        fingerprint_db = sqlite3.connect("")
        try:
            fingerprint_db.execute("PRAGMA journal_mode=OFF")
            fingerprint_db.execute("PRAGMA synchronous=OFF")
            fingerprint_db.execute("PRAGMA temp_store=FILE")
            fingerprint_db.execute("PRAGMA cache_size=-8192")
            fingerprint_db.execute(
                "CREATE TABLE archived_fingerprints "
                "(fingerprint TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            insert_fingerprint = "INSERT OR IGNORE INTO archived_fingerprints VALUES (?)"
            for path in self.cfg.parquet_dir.rglob("*.parquet"):
                try:
                    parquet_file = pq.ParquetFile(path)
                except Exception as exc:
                    # Preserve source files intact; the journal is canonical.
                    self.corrupt_parquet_parts.append(str(path))
                    LOG.error("unreadable Parquet part recorded for repair: %s (%s)", path, type(exc).__name__)
                    continue
                names = set(parquet_file.schema_arrow.names)
                columns = [
                    name
                    for name in (
                        "dedupe_key",
                        "received_utc",
                        "exchange_ts",
                        "channel",
                        "kind",
                        "market_ticker",
                        "raw_json",
                        "payload_sha256",
                    )
                    if name in names
                ]
                partition_kind = (
                    path.parent.name.removeprefix("kind=")
                    if path.parent.name.startswith("kind=")
                    else None
                )
                try:
                    fingerprint_db.execute("SAVEPOINT parquet_part")
                    for batch in parquet_file.iter_batches(columns=columns, batch_size=5000):
                        fingerprints = []
                        for row in batch.to_pylist():
                            if "kind" not in row:
                                row["kind"] = partition_kind
                            if row.get("dedupe_key") is not None:
                                fingerprints.append((event_archive_fingerprint(row),))
                        fingerprint_db.executemany(insert_fingerprint, fingerprints)
                    fingerprint_db.execute("RELEASE SAVEPOINT parquet_part")
                    fingerprint_db.commit()
                except Exception as exc:
                    fingerprint_db.execute("ROLLBACK TO SAVEPOINT parquet_part")
                    fingerprint_db.execute("RELEASE SAVEPOINT parquet_part")
                    self.corrupt_parquet_parts.append(str(path))
                    LOG.error("unreadable Parquet row group: %s (%s)", path, type(exc).__name__)
                    continue

            assert self.connection is not None
            tables = ["market_events"]
            version_table = self.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_event_versions'"
            ).fetchone()
            if version_table:
                tables.append("market_event_versions")
            columns = (
                "dedupe_key, received_utc, exchange_ts, channel, kind, "
                "market_ticker, raw_json, payload_sha256"
            )
            recovered = 0
            archive_rows: list[dict[str, Any]] = []
            stop_recovery = False
            for table in tables:
                cursor = self.connection.execute(
                    f"SELECT {columns} FROM {table} ORDER BY received_utc, dedupe_key"
                )
                while True:
                    rows = cursor.fetchmany(1000)
                    if not rows:
                        break
                    if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
                        self.parquet_recovery_state = "PARTIAL_RECOVERY_RAW_BUDGET"
                        self.raw_capture_enabled = False
                        stop_recovery = True
                        break
                    for row in rows:
                        event = {
                            "dedupe_key": row[0],
                            "received_utc": row[1],
                            "exchange_ts": row[2],
                            "channel": row[3],
                            "kind": row[4],
                            "market_ticker": row[5],
                            "raw_json": decode_journal_payload(row[6]),
                            "payload_sha256": row[7],
                        }
                        fingerprint = event_archive_fingerprint(event)
                        was_new = fingerprint_db.execute(
                            insert_fingerprint, (fingerprint,)
                        ).rowcount
                        if was_new:
                            archive_rows.append(event)
                    fingerprint_db.commit()
                    if len(archive_rows) >= 1000:
                        self._write_partitioned_parquet(archive_rows)
                        recovered += len(archive_rows)
                        archive_rows = []
                if stop_recovery:
                    break
            if archive_rows:
                self._write_partitioned_parquet(archive_rows)
                recovered += len(archive_rows)
        finally:
            fingerprint_db.close()
        self.parquet_bytes = sum(
            path.stat().st_size for path in self.cfg.parquet_dir.rglob("*.parquet")
        )
        if self.corrupt_parquet_parts:
            self.parquet_recovery_state = "REPAIRED_WITH_CORRUPT_PARTS_STILL_PRESENT"
        elif self.parquet_recovery_state == "NOT_RUN":
            self.parquet_recovery_state = "RECOVERED_MISSING_ROWS" if recovered else "VERIFIED_NO_GAP"
        return recovered

    def resume_raw_capture_if_safe(self) -> bool:
        """Resume Parquet capture after disk pressure clears, repairing gaps first."""
        if self.raw_capture_enabled:
            return True
        self.parquet_bytes = self._parquet_directory_size()
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.parquet_recovery_state = "SKIPPED_RAW_BUDGET"
            return False
        try:
            free_bytes = shutil.disk_usage(self.cfg.home).free
        except OSError:
            free_bytes = 0
        if free_bytes < self.cfg.min_free_bytes:
            self.parquet_recovery_state = "SKIPPED_LOW_DISK"
            return False

        self.parquet_recovery_state = "NOT_RUN"
        self.raw_capture_enabled = True
        try:
            recovered = self.recover_parquet_archive()
        except Exception as exc:
            self.raw_capture_enabled = False
            self.parquet_recovery_state = f"FAILED_{type(exc).__name__}"
            LOG.exception("Parquet recovery before capture resume failed")
            return False
        self.parquet_recovery_rows += recovered
        if self.raw_capture_enabled:
            # append_batch commits to SQLite before buffering each row, so a
            # successful full-journal recovery makes this memory buffer redundant.
            self.parquet_buffer.clear()
            self.parquet_buffer_bytes_estimate = 0
            self.parquet_buffer_started_monotonic = None
        return self.raw_capture_enabled

    def _sqlite_size(self) -> int:
        total = 0
        for path in (
            self.cfg.ledger_path,
            Path(str(self.cfg.ledger_path) + "-wal"),
            Path(str(self.cfg.ledger_path) + "-shm"),
        ):
            if path.exists():
                total += path.stat().st_size
        return total

    def _parquet_directory_size(self) -> int:
        """Return the actual committed archive bytes under the configured root."""
        total = 0
        for path in self.cfg.parquet_dir.rglob("*.parquet"):
            try:
                total += path.stat().st_size
            except OSError:
                # A concurrent file rotation/removal should not break ingest;
                # the next flush reconciles the byte count again.
                continue
        return total

    def _archive_manifest_files_present(self) -> bool:
        """Cheap restart guard: reject removed/resized parts and new journal rows.

        The verified shutdown marker attests to full SHA-256 checks at exit.
        This fast check does not replace a periodic full integrity audit.
        """
        if self.connection is None:
            return False
        expected = int(self.connection.execute("SELECT COUNT(*) FROM market_events").fetchone()[0])
        expected += int(self.connection.execute("SELECT COUNT(*) FROM market_event_versions").fetchone()[0])
        rows = self.connection.execute(
            "SELECT part_path, row_count, byte_count FROM archive_manifests"
        ).fetchall()
        if sum(int(row[1]) for row in rows) != expected:
            return False
        for part_path, _count, size in rows:
            path = self.cfg.parquet_dir / str(part_path)
            try:
                if not path.is_file() or path.stat().st_size != int(size):
                    return False
            except OSError:
                return False
        return True

    def verify_archive_manifest(self) -> bool:
        """Bounded-memory, full-file integrity check before marking a clean shutdown.

        The existing journal remains canonical. A verified marker is only
        committed if *all* event versions are represented in archive manifests,
        and every referenced archive part has the expected size and hash.
        """
        if self.connection is None:
            return False
        if (
            self.parquet_buffer or not self.raw_capture_enabled
            or self.parquet_dropped_rows > 0
            or any(self.pending_spool_dir.glob("pending-*.json"))
            or self.corrupt_parquet_parts or self.orphaned_partial_parts > 0
        ):
            return False
        expected = int(self.connection.execute(
            "SELECT COUNT(*) FROM market_events"
        ).fetchone()[0])
        expected += int(self.connection.execute(
            "SELECT COUNT(*) FROM market_event_versions"
        ).fetchone()[0])
        manifest_rows = self.connection.execute(
            "SELECT part_path, row_count, byte_count, sha256 FROM archive_manifests"
        ).fetchall()
        if sum(int(row[1]) for row in manifest_rows) != expected:
            return False
        for name, _count, byte_count, checksum in manifest_rows:
            path = self.cfg.parquet_dir / str(name)
            try:
                if not path.is_file() or path.stat().st_size != int(byte_count):
                    return False
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                if digest.hexdigest() != checksum:
                    return False
            except OSError:
                return False
        return True

    def close(self) -> None:
        self.flush_parquet(reason="SHUTDOWN")
        if self.connection is not None:
            verified = self.verify_archive_manifest()
            self.connection.execute(
                "INSERT OR REPLACE INTO collector_markers VALUES (?, ?, ?)",
                (
                    "archive_shutdown",
                    "CLEAN_ARCHIVE_VERIFIED" if verified else "CLOSED_WITH_ARCHIVE_UNVERIFIED",
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self.connection.commit()
            self.connection.close()
            self.connection = None

    def flush_parquet(self, *, reason: str = "MANUAL") -> int:
        """Write one buffered batch, leaving the SQLite journal immediately durable."""
        if not self.parquet_buffer:
            return 0
        if not self.raw_capture_enabled:
            count = len(self.parquet_buffer)
            self.parquet_dropped_rows += count
            self.parquet_buffer.clear()
            self.parquet_buffer_bytes_estimate = 0
            self.parquet_buffer_started_monotonic = None
            return 0
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.raw_capture_enabled = False
            LOG.warning("raw Parquet budget reached; SQLite critical journal remains enabled")
            return self.flush_parquet(reason="RAW_CAPTURE_DISABLED")
        rows = self.parquet_buffer
        self._write_partitioned_parquet(rows)
        # Reconcile after every flush so budget checks/status include any
        # archive files created while this worker was running.
        self.parquet_bytes = self._parquet_directory_size()
        self.parquet_buffer = []
        self.parquet_buffer_bytes_estimate = 0
        self.parquet_buffer_started_monotonic = None
        self.parquet_flush_count += 1
        self.parquet_last_flush_utc = datetime.now(timezone.utc).isoformat()
        self.parquet_last_flush_rows = len(rows)
        self.parquet_last_flush_reason = reason
        return self.parquet_last_flush_rows

    def flush_parquet_if_due(self, reason: str = "AGED_BUFFER") -> int:
        """Flush an aged buffer regardless of whether the event queue is busy."""
        started = self.parquet_buffer_started_monotonic
        if not self.parquet_buffer or started is None:
            return 0
        if time.monotonic() - started < self.parquet_flush_interval_seconds:
            return 0
        return self.flush_parquet(reason=reason)

    @staticmethod
    def _normalized(event: dict[str, Any]) -> tuple[str, str, str]:
        raw = json.dumps(
            event.get("payload", {}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        supplied = event.get("dedupe_key")
        dedupe = str(supplied) if supplied else hashlib.sha256(
            (
                str(event.get("channel", ""))
                + "|"
                + str(event.get("kind", ""))
                + "|"
                + raw
            ).encode("utf-8")
        ).hexdigest()
        return dedupe, digest, raw

    def append_paper_record(
        self,
        idempotency_key: str,
        profile_id: str,
        record_type: str,
        payload: dict[str, Any],
    ) -> bool:
        if self.connection is None:
            raise RuntimeError("Storage is not open")
        now = datetime.now(timezone.utc).isoformat()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO paper_ledger VALUES (?, ?, ?, ?, ?)",
                (
                    idempotency_key,
                    now,
                    profile_id,
                    record_type,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise
        self.sqlite_bytes = self._sqlite_size()
        inserted = cursor.rowcount == 1
        if inserted:
            self.paper_ledger_record_count += 1
            self.latest_paper_record_utc = now
        return inserted

    def paper_records(self, profile_id: str) -> list[dict[str, Any]]:
        """Read one isolated paper profile's durable append-only records."""
        if self.connection is None:
            raise RuntimeError("Storage is not open")
        rows = self.connection.execute(
            """SELECT idempotency_key, created_utc, record_type, payload_json
               FROM paper_ledger WHERE profile_id = ? ORDER BY created_utc, idempotency_key""",
            (profile_id,),
        ).fetchall()
        return [
            {
                "idempotency_key": str(row[0]),
                "created_utc": str(row[1]),
                "record_type": str(row[2]),
                "payload": json.loads(row[3]),
            }
            for row in rows
        ]

    @property
    def pending_spool_dir(self) -> Path:
        return self.cfg.data_dir / "pending_market_batches"

    def spool_batch(self, events: list[dict[str, Any]]) -> Path:
        """Durable last-resort spool for a temporarily unavailable journal.

        This is not a successful journal commit. Callers must report degraded
        state and stop normal capture. The spool uses exclusive, atomically
        finalized files and cannot silently overwrite another batch.
        """
        directory = self.pending_spool_dir
        directory.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(
            events,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        output = directory / f"pending-{uuid.uuid4().hex}.json"
        temporary = output.with_suffix(".partial")
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
        self.events_spooled += len(events)
        return output

    def replay_spooled_batches(self) -> int:
        """Idempotently replay fully serialized batches after SQLite is available.

        Never remove source spool data until its append (including conflict
        version handling) returns without an exception.
        """
        directory = self.pending_spool_dir
        if not directory.exists():
            return 0
        recovered = 0
        for path in sorted(directory.glob("pending-*.json")):
            with path.open("rb") as source:
                payload = json.loads(source.read())
            if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
                raise ValueError(f"spooled event batch is malformed: {path.name}")
            self.append_batch_resilient(payload)
            path.unlink()
            recovered += len(payload)
        self.events_replayed += recovered
        return recovered

    def _quarantine_invalid_event(self, event: dict[str, Any], exc: Exception) -> None:
        """Durably preserve the diagnostic representation before acknowledging an invalid event.

        This is not a lossless capture of arbitrary unserializable Python objects.
        The diagnostic explicitly marks that limitation rather than claiming the
        original market event has been archived.
        """
        if self.connection is None:
            raise RuntimeError("storage is not open")
        diagnostic = json.dumps(
            event,
            sort_keys=True,
            ensure_ascii=True,
            default=lambda item: {"unserializable_type": type(item).__name__, "repr": repr(item)},
        )
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO quarantined_market_events VALUES (?, ?, ?, ?, ?, ?)",
            (
                hashlib.sha256(diagnostic.encode("utf-8")).hexdigest(),
                datetime.now(timezone.utc).isoformat(),
                type(exc).__name__,
                str(event.get("dedupe_key") or ""),
                diagnostic,
                "DIAGNOSTIC_ONLY_NOT_ORIGINAL_EVENT",
            ),
        )
        self.connection.commit()
        self.events_quarantined += max(0, cursor.rowcount)

    def append_batch_resilient(self, events: list[dict[str, Any]]) -> int:
        """Isolate unencodable input; never misclassify a DB/disk failure as poison.

        Persistent I/O errors propagate to the worker and must not be
        acknowledged. Only individual, proven encoding failures are
        quarantined into the same durable SQLite store.
        """
        valid: list[dict[str, Any]] = []
        for event in events:
            try:
                _, _, raw = self._normalized(event)
                encode_journal_payload(raw)
            except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
                self._quarantine_invalid_event(event, exc)
                continue
            valid.append(event)
        return self.append_batch(valid) if valid else 0

    def append_batch(self, events: list[dict[str, Any]]) -> int:
        if self.connection is None:
            self.open()
        assert self.connection is not None
        # Serialize before opening the SQL transaction; partial input errors
        # cannot leave a half-applied batch behind.
        prepared: list[tuple[dict[str, Any], str, str, str]] = []
        for event in events:
            dedupe, digest, raw = self._normalized(event)
            prepared.append((event, dedupe, digest, raw))

        keys = list(dict.fromkeys(item[1] for item in prepared))
        # Prior versions are already committed; do not rearchive them on
        # overlapping REST observations, even though the canonical key differs.
        prior_versions = {
            str(row[0]) for row in self.connection.execute(
                "SELECT row_sha256 FROM market_event_versions WHERE source_generation = ?",
                ("collector-runtime",),
            )
        }
        existing: dict[str, str] = {}
        seen: dict[str, str] = {}
        for start in range(0, len(keys), 500):
            batch_keys = keys[start : start + 500]
            placeholders = ",".join("?" for _ in batch_keys)
            existing.update(
                (str(row[0]), str(row[1]))
                for row in self.connection.execute(
                    f"SELECT dedupe_key, payload_sha256 FROM market_events WHERE dedupe_key IN ({placeholders})",
                    batch_keys,
                )
            )

        rows: list[tuple[Any, ...]] = []
        version_rows: list[tuple[Any, ...]] = []
        new_parquet_rows: list[dict[str, Any]] = []
        archived_candidates: set[tuple[str, str]] = set()
        for event, dedupe, digest, raw in prepared:
            canonical = existing.get(dedupe, seen.get(dedupe))
            if canonical == digest:
                continue
            received = str(event.get("received_utc") or datetime.now(timezone.utc).isoformat())
            exchange_ts = event.get("exchange_ts")
            channel = str(event.get("channel", "unknown"))
            kind = str(event.get("kind", "unknown"))
            ticker = event.get("market_ticker")
            encoded = encode_journal_payload(raw)
            row = (dedupe, received, exchange_ts, channel, kind, ticker, encoded, digest)
            if canonical is not None:
                # Deduplicate the conflicting version by source key and payload,
                # not by time of this observation.
                version_hash = hashlib.sha256(
                    (dedupe + "|" + digest).encode("utf-8")
                ).hexdigest()
                if version_hash in prior_versions:
                    continue
                prior_versions.add(version_hash)
                version_rows.append(
                    ("collector-runtime", version_hash, datetime.now(timezone.utc).isoformat(), *row)
                )
            else:
                seen[dedupe] = digest
                rows.append(row)
            pair = (dedupe, digest)
            if pair in archived_candidates:
                continue
            archived_candidates.add(pair)
            new_parquet_rows.append(
                {
                    "dedupe_key": dedupe,
                    "received_utc": received,
                    "exchange_ts": exchange_ts,
                    "channel": channel,
                    "kind": kind,
                    "market_ticker": ticker,
                    "payload_sha256": digest,
                    "raw_json": raw,
                }
            )

        inserted = 0
        inserted_versions = 0
        try:
            if rows or version_rows:
                self.connection.execute("BEGIN IMMEDIATE")
                if rows:
                    inserted = max(0, int(self.connection.executemany(
                        "INSERT OR IGNORE INTO market_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        rows,
                    ).rowcount))
                if version_rows:
                    inserted_versions = max(0, int(self.connection.executemany(
                        "INSERT OR IGNORE INTO market_event_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        version_rows,
                    ).rowcount))
                self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        self.events_inserted += inserted
        self.events_key_conflict += inserted_versions
        self.events_duplicate += max(0, len(events) - inserted - inserted_versions)
        self.sqlite_bytes = self._sqlite_size()
        latest_event = self.connection.execute(
            "SELECT received_utc FROM market_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        self.latest_market_event_received_utc = str(latest_event[0]) if latest_event else None

        # All candidates have stable archive fingerprints, but only committed
        # canonical/variant rows should enter Parquet.
        committed = set((row[0], row[7]) for row in rows)
        committed.update((row[3], row[10]) for row in version_rows)
        new_parquet_rows = [
            row for row in new_parquet_rows
            if (row["dedupe_key"], row["payload_sha256"]) in committed
        ]
        if self.raw_capture_enabled and new_parquet_rows:
            incoming_estimate = sum(
                len(row["raw_json"].encode("utf-8")) + 192 for row in new_parquet_rows
            )
            if self.parquet_bytes + self.parquet_buffer_bytes_estimate + incoming_estimate > self.cfg.raw_data_budget_bytes:
                self.flush_parquet(reason="BUDGET_HEADROOM")
            if self.parquet_bytes + incoming_estimate > self.cfg.raw_data_budget_bytes:
                self.raw_capture_enabled = False
                self.parquet_dropped_rows += len(new_parquet_rows)
                LOG.warning("raw Parquet budget reached; SQLite critical journal remains enabled")
            else:
                if not self.parquet_buffer:
                    self.parquet_buffer_started_monotonic = time.monotonic()
                self.parquet_buffer.extend(new_parquet_rows)
                self.parquet_buffer_bytes_estimate += incoming_estimate
                if len(self.parquet_buffer) >= self.parquet_flush_rows:
                    self.flush_parquet(reason="ROW_THRESHOLD")
        elif new_parquet_rows:
            self.parquet_dropped_rows += len(new_parquet_rows)
        return inserted + inserted_versions

    def _write_partitioned_parquet(self, rows: list[dict[str, Any]]) -> None:
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            try:
                received = datetime.fromisoformat(row["received_utc"].replace("Z", "+00:00"))
            except ValueError:
                received = datetime.now(timezone.utc)
            groups[(received.astimezone(timezone.utc).strftime("%Y-%m-%d"), row["kind"])].append(row)
        for (date_partition, kind), batch in groups.items():
            partition = (
                self.cfg.parquet_dir
                / f"date={date_partition}"
                / f"kind={kind.replace('/', '_')}"
            )
            partition.mkdir(parents=True, exist_ok=True)
            output = partition / f"part-{uuid.uuid4().hex}.parquet"
            temporary = output.with_suffix(".partial")
            file_rows = [{key: value for key, value in row.items() if key != "kind"} for row in batch]
            try:
                pq.write_table(
                    pa.Table.from_pylist(file_rows),
                    temporary,
                    compression="zstd",
                    compression_level=3,
                )
                # Keep incomplete files out of the archive glob. A crash before
                # the rename leaves the durable SQLite source available to replay.
                with temporary.open("rb+") as handle:
                    os.fsync(handle.fileno())
                os.replace(temporary, output)
            except Exception:
                # Leave the .partial file for forensic recovery; never silently
                # remove evidence of an interrupted archive commit.
                raise
            size = output.stat().st_size
            digest = hashlib.sha256()
            with output.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            if self.connection is not None:
                self.connection.execute(
                    "INSERT OR REPLACE INTO archive_manifests VALUES (?, ?, ?, ?, ?)",
                    (
                        str(output.relative_to(self.cfg.parquet_dir)),
                        len(batch),
                        size,
                        digest.hexdigest(),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                self.connection.commit()
            self.parquet_bytes += size

    def integrity_check(self) -> str:
        if self.connection is None:
            self.open()
        assert self.connection is not None
        result = self.connection.execute("PRAGMA integrity_check").fetchone()
        return str(result[0]) if result else "UNKNOWN"

    def database_metrics(self) -> dict[str, Any]:
        if self.connection is None:
            self.open()
        assert self.connection is not None
        event_count = self.connection.execute("SELECT count(*) FROM market_events").fetchone()[0]
        ledger_count = self.connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0]
        return {
            "event_count": event_count,
            "paper_ledger_count": ledger_count,
            "sqlite_bytes_including_wal": self._sqlite_size(),
            "parquet_bytes": self.parquet_bytes,
            "integrity_check": self.integrity_check(),
            **self.connection_status(),
        }

