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
        self.parquet_recovery_mode = "NOT_RUN"
        # Highest SQLite rowid committed so far, and highest rowid sitting in
        # parquet_buffer. A flush publishes the checkpoint only when no rows were
        # dropped since the last verified checkpoint (archive_gap_pending).
        self.latest_event: tuple[int, str] | None = None
        self.parquet_buffer_through: tuple[int, str] | None = None
        self.archive_gap_pending = False
        self._recent_keys: dict[str, None] = {}
        self.phase_ms_ema: dict[str, float] = {}
        self.parquet_recovery_reason: str | None = None
        self.parquet_recovery_files_scanned = 0
        self.parquet_recovery_rows_scanned = 0
        self.parquet_recovery_seconds = 0.0
        self.archive_checkpoint_reason = "NOT_EVALUATED"
        self.archive_scan_paths: set[Path] | None = None
        self.archive_bad_files: list[tuple[Path, str]] = []
        self.archive_manifest_failures = 0
        self.archive_checkpoint_write_failures = 0
        self.archive_quarantined_files = 0
        self.parquet_size_reconcile_interval_seconds = 300.0
        self.parquet_size_reconcile_last_monotonic = 0.0
        self.persistence_audit_interval_seconds = 15.0
        self.persistence_audit_last_monotonic = 0.0
        self.persistence_audit_cache: dict[str, Any] = {}

    def open(self) -> None:
        if self.cfg.event_synchronous not in ("NORMAL", "FULL"):  # before a handle exists
            raise ValueError(f"unsupported synchronous mode {self.cfg.event_synchronous!r}")
        self.connection = sqlite3.connect(self.cfg.ledger_path, timeout=15)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.synchronous_mode = ""
        self._set_synchronous(self.cfg.event_synchronous)
        self.connection.execute("PRAGMA busy_timeout=15000")
        # Default 2 MiB cache re-reads B-tree interior pages from disk on every
        # dedupe lookup once the journal reaches GBs (slow on the USB data volume).
        self.connection.execute(f"PRAGMA cache_size=-{self.cfg.sqlite_cache_kib}")
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
            "SELECT received_utc, rowid, dedupe_key FROM market_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        self.latest_market_event_received_utc = str(latest_event[0]) if latest_event else None
        self.latest_event = (int(latest_event[1]), str(latest_event[2])) if latest_event else None
        self.parquet_bytes = self._parquet_directory_size()
        self.parquet_size_reconcile_last_monotonic = time.monotonic()
        self.sqlite_bytes = self._sqlite_size()
        previous_capture_incomplete = (
            self.previous_parquet_dropped_rows > 0
            or self.previous_raw_capture_state not in (None, "ENABLED")
        )
        if (
            (
                self.previous_service_state
                and self.previous_service_state != "STOPPED"
            )
            or previous_capture_incomplete
            or not self._checkpoint_current()
        ):
            try:
                self.parquet_recovery_rows = self.recover_parquet_archive()
                if self.parquet_recovery_state == "NOT_RUN":
                    self.parquet_recovery_state = "VERIFIED_NO_GAP"
            except Exception as exc:
                self.parquet_recovery_state = f"FAILED_{type(exc).__name__}"
                LOG.exception("Parquet archive recovery failed; SQLite journal remains available")
        else:
            self.parquet_recovery_state = "SKIPPED_PREVIOUS_RUN_STOPPED"

    def _set_synchronous(self, mode: str) -> None:
        """Switch the journal's fsync policy between transactions (WAL: per connection)."""
        if mode not in ("NORMAL", "FULL"):
            raise ValueError(f"unsupported synchronous mode {mode!r}")
        if mode != self.synchronous_mode:
            assert self.connection is not None
            self.connection.execute(f"PRAGMA synchronous={mode}")
            self.synchronous_mode = mode

    def _make_journal_durable(self) -> bool:
        """fsync everything committed so far; the archive checkpoint must not outrun it."""
        assert self.connection is not None
        try:
            self.connection.execute("PRAGMA wal_checkpoint(FULL)").fetchall()
            return True
        except sqlite3.Error:
            LOG.warning("journal durability barrier failed; checkpoint not advanced", exc_info=True)
            return False

    def _phase(self, name: str, since: float) -> float:
        """Record how long a writer phase took (EMA, alpha 0.2); returns now for chaining."""
        now = time.perf_counter()
        ms = (now - since) * 1000
        ema = self.phase_ms_ema
        ema[name] = ms if name not in ema else 0.8 * ema[name] + 0.2 * ms
        return now

    def _checkpoint_current(self) -> bool:
        """True only if a manifest-verified checkpoint already covers the journal head.

        Cheap (stat + tail hash), so a clean stop is verified instead of assumed.
        """
        if self.latest_event is None:
            return True
        record = self._read_archive_checkpoint()
        return record is not None and int(record["through_rowid"]) == self.latest_event[0]

    @property
    def archive_integrity_state(self) -> str:
        """VERIFIED_COMPLETE only with positive evidence; anything else blocks certification."""
        if self.archive_gap_pending or self.archive_manifest_failures:
            return "INCOMPLETE"
        if self.parquet_recovery_state in ("RECOVERED_MISSING_ROWS", "VERIFIED_NO_GAP"):
            return "VERIFIED_COMPLETE"
        if self.parquet_recovery_state == "SKIPPED_PREVIOUS_RUN_STOPPED":
            return "VERIFIED_COMPLETE_CLEAN_STOP_CHECKPOINT"
        return "UNKNOWN"

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
            "parquet_bytes": self.parquet_bytes,
            "parquet_buffered_rows": len(self.parquet_buffer),
            "parquet_buffer_estimated_bytes": self.parquet_buffer_bytes_estimate,
            "parquet_flush_count": self.parquet_flush_count,
            "parquet_last_flush_utc": self.parquet_last_flush_utc,
            "parquet_last_flush_rows": self.parquet_last_flush_rows,
            "parquet_last_flush_reason": self.parquet_last_flush_reason,
            "parquet_dropped_rows": self.parquet_dropped_rows,
            "writer_phase_ms_ema": {k: round(v, 1) for k, v in self.phase_ms_ema.items()},
            "archive_integrity_state": self.archive_integrity_state,
            "archive_recovery_reason": self.parquet_recovery_reason,
            "archive_recovery_files_scanned": self.parquet_recovery_files_scanned,
            "archive_recovery_rows_scanned": self.parquet_recovery_rows_scanned,
            "archive_recovery_seconds": self.parquet_recovery_seconds,
            "archive_manifest_failures": self.archive_manifest_failures,
            "archive_checkpoint_write_failures": self.archive_checkpoint_write_failures,
            "archive_quarantined_files": self.archive_quarantined_files,
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
            now = time.monotonic()
            if now - self.parquet_size_reconcile_last_monotonic >= self.parquet_size_reconcile_interval_seconds:
                # Full directory walk is O(files); flushes track bytes incrementally.
                self.parquet_bytes = self._parquet_directory_size()
                self.parquet_size_reconcile_last_monotonic = now
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

    @property
    def archive_checkpoint_path(self) -> Path:
        return self.cfg.parquet_dir / "_archive_checkpoint.json"

    # --- hash-chained archive manifest -------------------------------------
    # One fsync'd line per published Parquet file: size, mtime and SHA-256, each
    # line chained to the previous by hash. The checkpoint pins (seq, head), so
    # it only vouches for files that are listed, present and unchanged. Startup
    # stats every listed file but hashes just the newest MANIFEST_TAIL_HASH_FILES.
    # Older same-size corruption needs verify_archive(full=True).
    RECENT_KEYS_MAX = 100_000
    MANIFEST_GENESIS = "0" * 64
    MANIFEST_TAIL_HASH_FILES = 32

    @property
    def archive_manifest_path(self) -> Path:
        return self.cfg.parquet_dir / "_archive_manifest.jsonl"

    @staticmethod
    def _manifest_hash(prev: str, record: dict[str, Any]) -> str:
        body = json.dumps(record, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256((prev + body).encode("utf-8")).hexdigest()

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        return digest.hexdigest()

    def _manifest_read(self) -> tuple[list[dict[str, Any]], int, bool]:
        """Valid chain prefix, its byte length, and whether junk follows it."""
        try:
            data = self.archive_manifest_path.read_bytes()
        except FileNotFoundError:
            return [], 0, False
        entries: list[dict[str, Any]] = []
        prev = self.MANIFEST_GENESIS
        good = 0
        for line in data.splitlines(keepends=True):
            try:
                if not line.endswith(b"\n"):
                    break  # torn final append
                record = json.loads(line)
                claimed = record.pop("hash")
                if (
                    record["prev"] != prev
                    or record["seq"] != len(entries) + 1
                    or self._manifest_hash(prev, record) != claimed
                ):
                    break
            except (ValueError, KeyError, TypeError):
                break
            record["hash"] = claimed
            entries.append(record)
            prev = claimed
            good += len(line)
        return entries, good, good < len(data)

    def _manifest_append(self, bodies: list[dict[str, Any]]) -> tuple[int, str]:
        """Append and fsync chained entries; returns the new (seq, head)."""
        entries, good, _ = self._manifest_read()
        prev = entries[-1]["hash"] if entries else self.MANIFEST_GENESIS
        seq = len(entries)
        lines = []
        for body in bodies:
            seq += 1
            record = {"seq": seq, "prev": prev, **body}
            prev = self._manifest_hash(prev, record)
            lines.append(json.dumps({**record, "hash": prev}, sort_keys=True).encode("utf-8") + b"\n")
        path = self.archive_manifest_path
        with path.open("r+b" if path.exists() else "wb") as handle:
            handle.truncate(good)  # drop any torn tail before extending the chain
            handle.seek(good)
            handle.write(b"".join(lines))
            handle.flush()
            os.fsync(handle.fileno())
        return seq, prev

    def _manifest_file_body(self, path: Path, *, adopted: bool = False) -> dict[str, Any]:
        info = path.stat()
        return {
            "t": "file",
            "path": path.relative_to(self.cfg.parquet_dir).as_posix(),
            "size": info.st_size,
            "mtime_ns": info.st_mtime_ns,
            "sha256": self._file_sha256(path),
            "adopted": adopted,
        }

    def _record_published_files(self, paths: list[Path]) -> bool:
        """List just-published files in the manifest. False => checkpoint must not advance."""
        if not paths:
            return True
        try:
            self._manifest_append([self._manifest_file_body(path) for path in paths])
            return True
        except OSError:
            self.archive_manifest_failures += 1
            self.archive_gap_pending = True
            LOG.error("archive manifest append failed; checkpoint frozen until recovery", exc_info=True)
            return False

    def _quarantine(self, path: Path, reason: str) -> None:
        """Move a suspect archive file out of the *.parquet glob, keeping it as evidence."""
        target = path.with_name(f"{path.name}.corrupt-{int(time.time())}")
        try:
            os.replace(path, target)
        except OSError:
            LOG.error("could not quarantine %s (%s)", path, reason, exc_info=True)
            return
        self.archive_quarantined_files += 1
        LOG.error("quarantined archive file %s: %s", path.name, reason)

    def verify_archive(self, *, full: bool = False) -> dict[str, Any]:
        """Check manifest chain, presence and size of every listed file.

        full=True also re-hashes every file (offline audit; reads the whole archive).
        Returns {"ok": bool, "violations": [...], "files": n, "chain_entries": n}.
        """
        entries, _, junk = self._manifest_read()
        files = [e for e in entries if e.get("t") == "file"]
        bad = self._manifest_file_violations(files, full=full)
        violations = [reason for _, reason in bad]
        if junk:
            violations.insert(0, "MANIFEST_CHAIN_JUNK_AFTER_VALID_PREFIX")
        return {
            "ok": not violations,
            "violations": violations,
            "files": len(files),
            "chain_entries": len(entries),
        }

    def _manifest_file_violations(
        self, files: list[dict[str, Any]], *, full: bool
    ) -> list[tuple[Path, str]]:
        """Stat every listed file; hash the newest tail, any whose mtime moved, or all if full."""
        tail = {id(e) for e in files[-self.MANIFEST_TAIL_HASH_FILES:]}
        bad: list[tuple[Path, str]] = []
        for entry in files:
            path = self.cfg.parquet_dir / entry["path"]
            try:
                info = path.stat()
            except OSError:
                bad.append((path, f"MANIFEST_FILE_MISSING:{entry['path']}"))
                continue
            if info.st_size != entry["size"]:
                bad.append((path, f"MANIFEST_FILE_SIZE_MISMATCH:{entry['path']}"))
            elif full or id(entry) in tail or info.st_mtime_ns != entry["mtime_ns"]:
                try:
                    if self._file_sha256(path) != entry["sha256"]:
                        bad.append((path, f"MANIFEST_FILE_HASH_MISMATCH:{entry['path']}"))
                except OSError:
                    bad.append((path, f"MANIFEST_FILE_UNREADABLE:{entry['path']}"))
        return bad

    def _write_archive_checkpoint(self, through: tuple[int, str]) -> None:
        """Durably record that every market_events row <= through[0] is archived.

        Failure leaves the older checkpoint (still valid, just more to rescan),
        but is counted and reported so it is never silent.
        """
        try:
            self._write_archive_checkpoint_file(through)
        except OSError:
            self.archive_checkpoint_write_failures += 1
            LOG.warning("archive checkpoint write failed; older checkpoint kept", exc_info=True)

    def _write_archive_checkpoint_file(self, through: tuple[int, str]) -> None:
        entries, _, _ = self._manifest_read()
        record = {
            "version": 2,
            "through_rowid": int(through[0]),
            "through_dedupe_key": str(through[1]),
            "written_epoch": time.time(),
            "written_utc": datetime.now(timezone.utc).isoformat(),
            "archive_bytes": int(self.parquet_bytes),
            "manifest_seq": len(entries),
            "manifest_head": entries[-1]["hash"] if entries else self.MANIFEST_GENESIS,
        }
        path = self.archive_checkpoint_path
        temporary = path.with_suffix(".json.partial")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(record, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    def _read_archive_checkpoint(self) -> dict[str, Any] | None:
        """Return the checkpoint only if the journal row AND the manifest still back it.

        Also sets archive_checkpoint_reason, archive_bad_files (to quarantine) and
        archive_scan_paths: the listed-after-checkpoint plus unlisted files, which
        are the only ones an incremental pass must read.
        """
        assert self.connection is not None
        self.archive_scan_paths = None
        self.archive_bad_files = []
        self.archive_checkpoint_reason = "NO_VALID_CHECKPOINT"
        try:
            record = json.loads(self.archive_checkpoint_path.read_text(encoding="utf-8"))
            rowid = int(record["through_rowid"])
            key = str(record["through_dedupe_key"])
            float(record["written_epoch"])
            seq = int(record["manifest_seq"])
            head = str(record["manifest_head"])
        except (OSError, ValueError, KeyError, TypeError):
            self.archive_checkpoint_reason = "CHECKPOINT_MISSING_UNREADABLE_OR_PRE_MANIFEST"
            return None
        row = self.connection.execute(
            "SELECT dedupe_key FROM market_events WHERE rowid = ?", (rowid,)
        ).fetchone()
        if row is None or str(row[0]) != key:
            self.archive_checkpoint_reason = "CHECKPOINT_ROW_MISMATCH"
            return None
        entries, _, _ = self._manifest_read()
        pinned = entries[seq - 1]["hash"] if 0 < seq <= len(entries) else (
            self.MANIFEST_GENESIS if seq == 0 else None
        )
        if pinned != head:
            self.archive_checkpoint_reason = "MANIFEST_CHAIN_MISMATCH"
            return None
        bad = self._manifest_file_violations(
            [e for e in entries[:seq] if e.get("t") == "file"], full=False
        )
        if bad:
            self.archive_bad_files = bad
            self.archive_checkpoint_reason = bad[0][1]
            return None
        listed = {e["path"] for e in entries if e.get("t") == "file"}
        scan = {self.cfg.parquet_dir / e["path"] for e in entries[seq:] if e.get("t") == "file"}
        for path in self.cfg.parquet_dir.rglob("*.parquet"):
            if path.relative_to(self.cfg.parquet_dir).as_posix() not in listed:
                scan.add(path)
        self.archive_scan_paths = scan
        self.archive_checkpoint_reason = "VERIFIED"
        return record

    def _adopt_archive_into_manifest(self, *, rebuild: bool) -> bool:
        """List every archive file the manifest does not yet cover.

        rebuild=True (after a full scan, which just re-read everything) first sets
        the old manifest aside as evidence and starts a fresh chain. False => failed.
        """
        try:
            path = self.archive_manifest_path
            if rebuild and path.exists():
                os.replace(path, path.with_name(f"{path.name}.superseded-{int(time.time())}"))
            entries, _, _ = self._manifest_read()
            listed = {e["path"] for e in entries if e.get("t") == "file"}
            orphans = sorted(
                p
                for p in self.cfg.parquet_dir.rglob("*.parquet")
                if p.relative_to(self.cfg.parquet_dir).as_posix() not in listed
            )
            if orphans:
                self._manifest_append([self._manifest_file_body(p, adopted=True) for p in orphans])
            return True
        except OSError:
            self.archive_manifest_failures += 1
            LOG.error("archive manifest adoption failed; recovery stays incomplete", exc_info=True)
            return False

    def recover_parquet_archive(self) -> int:
        """Rebuild missing Parquet event variants from the durable SQLite journal.

        With a valid checkpoint only journal rows after it, and Parquet files
        written after it, are compared. Otherwise the full history is scanned.
        """
        if self.connection is None:
            raise RuntimeError("Storage is not open")
        # Until this run completes, the archive may be missing journal rows, so
        # flushes must not advance the checkpoint (cleared only on success).
        self.archive_gap_pending = True
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.raw_capture_enabled = False
            self.parquet_recovery_state = "SKIPPED_RAW_BUDGET"
            self.parquet_recovery_mode = "SKIPPED_RAW_BUDGET"
            return 0
        try:
            free_bytes = shutil.disk_usage(self.cfg.home).free
        except OSError:
            free_bytes = 0
        if free_bytes < self.cfg.min_free_bytes:
            self.raw_capture_enabled = False
            self.parquet_recovery_state = "SKIPPED_LOW_DISK"
            self.parquet_recovery_mode = "SKIPPED_LOW_DISK"
            return 0

        has_versions = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_event_versions'"
        ).fetchone()
        # The legacy merge tool writes market_event_versions out of band, so a
        # checkpoint cannot vouch for it; such journals always take the full scan.
        started = time.monotonic()
        self.parquet_recovery_files_scanned = 0
        self.parquet_recovery_rows_scanned = 0
        checkpoint = None if has_versions else self._read_archive_checkpoint()
        self.parquet_recovery_reason = (
            "LEGACY_VERSIONS_TABLE" if has_versions else self.archive_checkpoint_reason
        )
        for bad_path, why in self.archive_bad_files:
            if bad_path.exists():
                self._quarantine(bad_path, why)
        since_rowid = int(checkpoint["through_rowid"]) if checkpoint else 0
        scan_paths = self.archive_scan_paths if checkpoint else None
        self.parquet_recovery_mode = (
            f"INCREMENTAL_FROM_ROWID_{since_rowid}" if checkpoint else "FULL_SCAN"
        )
        max_scanned: tuple[int, str] | None = None
        # Recovery may scan a long-running journal. Keep the archive index on
        # disk (not a Python set), in a file under the research volume with a hard
        # size cap, instead of sqlite3.connect("") which spills into %TEMP% on C:.
        workspace = self.cfg.recovery_workspace_dir
        workspace.mkdir(parents=True, exist_ok=True)
        for stale in workspace.glob("archive-fingerprints-*.sqlite3"):
            stale.unlink(missing_ok=True)  # scratch index of a dead run, not evidence
        index_path = workspace / f"archive-fingerprints-{os.getpid()}.sqlite3"
        fingerprint_db = sqlite3.connect(index_path)
        archive_rows: list[dict[str, Any]] = []
        recovered = 0
        stop_recovery = False
        try:
            fingerprint_db.execute("PRAGMA journal_mode=OFF")
            fingerprint_db.execute("PRAGMA synchronous=OFF")
            # Random-key B-tree: it must stay cache-resident or every probe is a
            # random read on the (USB) research volume. 32-byte BLOB keys, not hex.
            fingerprint_db.execute("PRAGMA cache_size=-131072")
            page_size = int(fingerprint_db.execute("PRAGMA page_size").fetchone()[0])
            fingerprint_db.execute(
                f"PRAGMA max_page_count={max(1, self.cfg.recovery_index_max_bytes // page_size)}"
            )
            fingerprint_db.execute(
                "CREATE TABLE archived_fingerprints "
                "(fingerprint BLOB PRIMARY KEY) WITHOUT ROWID"
            )
            insert_fingerprint = "INSERT OR IGNORE INTO archived_fingerprints VALUES (?)"
            for path in (
                scan_paths if scan_paths is not None else self.cfg.parquet_dir.rglob("*.parquet")
            ):
                if not path.exists():
                    continue
                file_fingerprints: list[tuple[str]] = []
                try:
                    parquet_file = pq.ParquetFile(path)
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
                    for batch in parquet_file.iter_batches(columns=columns, batch_size=5000):
                        for row in batch.to_pylist():
                            if "kind" not in row:
                                row["kind"] = partition_kind
                            if row.get("dedupe_key") is not None:
                                file_fingerprints.append(
                                    (bytes.fromhex(event_archive_fingerprint(row)),)
                                )
                except (pa.ArrowException, OSError, ValueError) as exc:
                    # Nothing from an unreadable file enters the index, so the
                    # journal re-archives its rows; the file is kept as evidence.
                    self._quarantine(path, f"UNREADABLE_PARQUET:{type(exc).__name__}")
                    continue
                fingerprint_db.executemany(insert_fingerprint, file_fingerprints)
                self.parquet_recovery_files_scanned += 1
                # The index is disposable scratch: batching commits avoids flushing
                # ~1000 random dirty pages to the USB volume for every tiny file.
                if self.parquet_recovery_files_scanned % 100 == 0:
                    fingerprint_db.commit()
            fingerprint_db.commit()

            assert self.connection is not None
            tables = ["market_events"]
            if has_versions:
                tables.append("market_event_versions")
            columns = (
                "dedupe_key, received_utc, exchange_ts, channel, kind, "
                "market_ticker, raw_json, payload_sha256"
            )
            for table in tables:
                if table == "market_events":
                    cursor = self.connection.execute(
                        f"SELECT {columns}, rowid FROM {table} WHERE rowid > ? ORDER BY rowid",
                        (since_rowid,),
                    )
                else:
                    # WITHOUT ROWID legacy table.
                    cursor = self.connection.execute(
                        f"SELECT {columns} FROM {table} ORDER BY received_utc, dedupe_key"
                    )
                while True:
                    rows = cursor.fetchmany(1000)
                    if not rows:
                        break
                    if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
                        self.parquet_recovery_state = "PARTIAL_RECOVERY_RAW_BUDGET"
                        self.parquet_recovery_mode = "PARTIAL_RECOVERY_RAW_BUDGET"
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
                        self.parquet_recovery_rows_scanned += 1
                        if table == "market_events":
                            # ORDER BY rowid: the last row seen is the maximum.
                            max_scanned = (int(row[8]), str(row[0]))
                        fingerprint = bytes.fromhex(event_archive_fingerprint(event))
                        if has_versions:
                            # The legacy versions table can repeat market_events rows.
                            was_new = fingerprint_db.execute(
                                insert_fingerprint, (fingerprint,)
                            ).rowcount
                        else:
                            # market_events.dedupe_key is unique, so journal rows never
                            # collide with each other: probe only, keep the index small.
                            was_new = (
                                fingerprint_db.execute(
                                    "SELECT 1 FROM archived_fingerprints WHERE fingerprint = ?",
                                    (fingerprint,),
                                ).fetchone()
                                is None
                            )
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
                archive_rows = []
            if not stop_recovery:
                # Files are listed (and, after a full scan, the chain is rebuilt)
                # before the checkpoint can pin them. Failure keeps the gap open.
                if self._adopt_archive_into_manifest(rebuild=checkpoint is None):
                    if max_scanned is not None:
                        self._write_archive_checkpoint(max_scanned)
                    self.archive_gap_pending = False
                else:
                    self.parquet_recovery_state = "PARTIAL_MANIFEST_WRITE_FAILED"
        except sqlite3.OperationalError as exc:
            if "full" not in str(exc).lower():
                raise
            # Index cap reached: keep what was found, never claim completeness.
            if archive_rows:
                self._write_partitioned_parquet(archive_rows)
                recovered += len(archive_rows)
            self.parquet_recovery_state = "PARTIAL_RECOVERY_INDEX_BUDGET"
            self.parquet_recovery_mode = "PARTIAL_RECOVERY_INDEX_BUDGET"
            LOG.error("archive recovery index cap reached; recovery is partial")
        finally:
            fingerprint_db.close()
            index_path.unlink(missing_ok=True)
            self.parquet_recovery_seconds = round(time.monotonic() - started, 3)
        self.parquet_bytes = self._parquet_directory_size()
        self.parquet_size_reconcile_last_monotonic = time.monotonic()
        if self.parquet_recovery_state == "NOT_RUN":
            self.parquet_recovery_state = "RECOVERED_MISSING_ROWS" if recovered else "VERIFIED_NO_GAP"
        return recovered

    def resume_raw_capture_if_safe(self) -> bool:
        """Resume Parquet capture after disk pressure clears, repairing gaps first."""
        if self.raw_capture_enabled:
            return True
        self.parquet_bytes = self._parquet_directory_size()
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.parquet_recovery_state = "SKIPPED_RAW_BUDGET"
            self.parquet_recovery_mode = "SKIPPED_RAW_BUDGET"
            return False
        try:
            free_bytes = shutil.disk_usage(self.cfg.home).free
        except OSError:
            free_bytes = 0
        if free_bytes < self.cfg.min_free_bytes:
            self.parquet_recovery_state = "SKIPPED_LOW_DISK"
            self.parquet_recovery_mode = "SKIPPED_LOW_DISK"
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

    def close(self) -> None:
        self.flush_parquet(reason="SHUTDOWN")
        if self.connection is not None:
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
            self.archive_gap_pending = True
            self.parquet_buffer_through = None
            self.parquet_buffer.clear()
            self.parquet_buffer_bytes_estimate = 0
            self.parquet_buffer_started_monotonic = None
            return 0
        if self.parquet_bytes >= self.cfg.raw_data_budget_bytes:
            self.raw_capture_enabled = False
            LOG.warning("raw Parquet budget reached; SQLite critical journal remains enabled")
            return self.flush_parquet(reason="RAW_CAPTURE_DISABLED")
        rows = self.parquet_buffer
        # _write_partitioned_parquet adds each published file's size to
        # parquet_bytes; the periodic audit reconciles with a directory walk.
        flush_clock = time.perf_counter()
        published = self._write_partitioned_parquet(rows)
        flush_clock = self._phase("flush_write_files", flush_clock)
        through = self.parquet_buffer_through
        self.parquet_buffer_through = None
        # Files are fsynced and published, then listed (fsynced) in the manifest,
        # and only then can the checkpoint move. A failed listing freezes the
        # checkpoint (archive_gap_pending); the next recovery adopts the files.
        listed = self._record_published_files(published)
        flush_clock = self._phase("flush_manifest", flush_clock)
        # With synchronous=NORMAL for events, the checkpoint may only name journal
        # rows that are already on disk: force the WAL out first.
        if through is not None and listed and not self.archive_gap_pending:
            durable = self._make_journal_durable()
            flush_clock = self._phase("flush_wal_barrier", flush_clock)
            if durable:
                self._write_archive_checkpoint(through)
                self._phase("flush_checkpoint_file", flush_clock)
            else:
                self.archive_checkpoint_write_failures += 1
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
        # Paper records are never allowed the weaker policy; a FULL commit also syncs
        # every earlier NORMAL event commit in the same WAL. SQLite refuses to change
        # the level inside a transaction, so it is set before BEGIN.
        self._set_synchronous("FULL")
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
        finally:
            self._set_synchronous(self.cfg.event_synchronous)
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

    def append_batch(self, events: list[dict[str, Any]]) -> int:
        if self.connection is None:
            self.open()
        assert self.connection is not None
        phase_clock = time.perf_counter()
        rows: list[tuple[Any, ...]] = []
        parquet_rows: list[dict[str, Any]] = []
        prepared: list[tuple[dict[str, Any], str, str | None, str | None]] = []
        for event in events:
            supplied = event.get("dedupe_key")
            if supplied:
                prepared.append((event, str(supplied), None, None))
            else:
                dedupe, digest, raw = self._normalized(event)
                prepared.append((event, dedupe, digest, raw))

        # No separate dedupe SELECT: dedupe_key is a random SHA-256, so every probe
        # is a cold random B-tree read. INSERT OR IGNORE ... RETURNING does the
        # global uniqueness check and reports which rows were really new in one pass.
        seen: set[str] = set()
        new_parquet_rows: list[dict[str, Any]] = []
        for event, dedupe, digest, raw in prepared:
            # Recently committed keys (reconnect replays) skip normalization; anything
            # older or after a restart is settled by INSERT OR IGNORE below.
            if dedupe in seen or dedupe in self._recent_keys:
                continue
            seen.add(dedupe)
            if digest is None or raw is None:
                _, digest, raw = self._normalized(event)
            received = str(event.get("received_utc") or datetime.now(timezone.utc).isoformat())
            rows.append(
                (
                    dedupe,
                    received,
                    event.get("exchange_ts"),
                    str(event.get("channel", "unknown")),
                    str(event.get("kind", "unknown")),
                    event.get("market_ticker"),
                    encode_journal_payload(raw),
                    digest,
                )
            )
            new_parquet_rows.append(
                {
                    "dedupe_key": dedupe,
                    "received_utc": received,
                    "exchange_ts": event.get("exchange_ts"),
                    "channel": str(event.get("channel", "unknown")),
                    "kind": str(event.get("kind", "unknown")),
                    "market_ticker": event.get("market_ticker"),
                    "payload_sha256": digest,
                    "raw_json": raw,
                }
            )

        phase_clock = self._phase("append_prepare", phase_clock)
        inserted_keys: set[str] = set()
        try:
            if rows:
                self.connection.execute("BEGIN IMMEDIATE")
                for start in range(0, len(rows), 100):  # 800 bound variables per statement
                    chunk = rows[start : start + 100]
                    returned = self.connection.execute(
                        "INSERT OR IGNORE INTO market_events VALUES "
                        + ",".join(["(?,?,?,?,?,?,?,?)"] * len(chunk))
                        + " RETURNING dedupe_key",
                        [value for row in chunk for value in row],
                    )
                    inserted_keys.update(item[0] for item in returned.fetchall())
                phase_clock = self._phase("append_insert", phase_clock)
                self.connection.commit()
                phase_clock = self._phase("append_commit", phase_clock)
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            raise
        inserted = len(inserted_keys)
        new_parquet_rows = [row for row in new_parquet_rows if row["dedupe_key"] in inserted_keys]
        # shortcut: bounded window of committed keys, not a full index; upgrade only
        # if duplicate normalization CPU ever shows up in a profile.
        self._recent_keys.update(dict.fromkeys(row[0] for row in rows))
        while len(self._recent_keys) > self.RECENT_KEYS_MAX:
            del self._recent_keys[next(iter(self._recent_keys))]
        self.events_inserted += inserted
        self.events_duplicate += max(0, len(events) - inserted)
        self.sqlite_bytes = self._sqlite_size()
        latest_event = self.connection.execute(
            "SELECT received_utc, rowid, dedupe_key FROM market_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        self.latest_market_event_received_utc = str(latest_event[0]) if latest_event else None
        self.latest_event = (int(latest_event[1]), str(latest_event[2])) if latest_event else None
        phase_clock = self._phase("append_tail", phase_clock)

        if self.raw_capture_enabled and new_parquet_rows:
            incoming_estimate = sum(
                len(row["raw_json"].encode("utf-8")) + 192 for row in new_parquet_rows
            )
            if self.parquet_bytes + self.parquet_buffer_bytes_estimate + incoming_estimate > self.cfg.raw_data_budget_bytes:
                self.flush_parquet(reason="BUDGET_HEADROOM")
            if self.parquet_bytes + incoming_estimate > self.cfg.raw_data_budget_bytes:
                self.raw_capture_enabled = False
                self.parquet_dropped_rows += len(new_parquet_rows)
                self.archive_gap_pending = True
                LOG.warning("raw Parquet budget reached; SQLite critical journal remains enabled")
            else:
                if not self.parquet_buffer:
                    self.parquet_buffer_started_monotonic = time.monotonic()
                self.parquet_buffer.extend(new_parquet_rows)
                self.parquet_buffer_through = self.latest_event
                self.parquet_buffer_bytes_estimate += incoming_estimate
                if len(self.parquet_buffer) >= self.parquet_flush_rows:
                    self.flush_parquet(reason="ROW_THRESHOLD")
        elif new_parquet_rows:
            self.parquet_dropped_rows += len(new_parquet_rows)
            self.archive_gap_pending = True
        self._phase("append_parquet_buffer_and_flush", phase_clock)
        return inserted

    def _write_partitioned_parquet(self, rows: list[dict[str, Any]]) -> list[Path]:
        published: list[Path] = []
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
                temporary.unlink(missing_ok=True)
                raise
            self.parquet_bytes += output.stat().st_size
            published.append(output)
        return published

    def integrity_check(self) -> str:
        if self.connection is None:
            self.open()
        assert self.connection is not None
        result = self.connection.execute("PRAGMA integrity_check").fetchone()
        return str(result[0]) if result else "UNKNOWN"

    def database_metrics(self, *, full_integrity_check: bool = False) -> dict[str, Any]:
        """Cheap shutdown metrics; the full-page integrity scan is opt-in.

        market_events rows are never deleted and INSERT OR IGNORE consumes no
        rowid when it ignores, so max(rowid) equals the row count without a scan.
        """
        if self.connection is None:
            self.open()
        assert self.connection is not None
        event_count = self.connection.execute("SELECT max(rowid) FROM market_events").fetchone()[0] or 0
        ledger_count = self.connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0]
        return {
            "event_count": event_count,
            "paper_ledger_count": ledger_count,
            "sqlite_bytes_including_wal": self._sqlite_size(),
            "parquet_bytes": self.parquet_bytes,
            "integrity_check": self.integrity_check() if full_integrity_check else "SKIPPED_FULL_SCAN",
            **self.connection_status(),
        }

