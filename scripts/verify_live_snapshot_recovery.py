from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pyarrow.parquet as pq

from kalshi15m.config import settings
from kalshi15m.storage import (
    Storage,
    decode_journal_payload,
    event_archive_fingerprint,
)


def _status_snapshot(status_url: str) -> dict[str, Any]:
    with urlopen(status_url, timeout=5) as response:
        return json.loads(response.read())


def _copy_rows(root: Path) -> tuple[int, int]:
    rows = 0
    files = 0
    for path in root.rglob("*.parquet"):
        files += 1
        rows += int(pq.ParquetFile(path).metadata.num_rows)
    return files, rows


def _select_recovery_probe_part(
    root: Path,
) -> tuple[Path, int] | None:
    """Choose the largest non-empty committed part for an isolated rebuild."""
    parts = [
        (path, int(pq.ParquetFile(path).metadata.num_rows))
        for path in root.rglob("*.parquet")
    ]
    return max((item for item in parts if item[1] > 0), key=lambda item: item[1], default=None)


def _fingerprint_index(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA cache_size=-8192")
    connection.execute(
        "CREATE TABLE journal_fingerprints "
        "(fingerprint TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL, "
        "payload_sha256 TEXT) WITHOUT ROWID"
    )
    connection.execute(
        "CREATE INDEX journal_fingerprints_by_key "
        "ON journal_fingerprints(dedupe_key, payload_sha256)"
    )
    connection.execute(
        "CREATE TABLE archive_fingerprints "
        "(fingerprint TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL, "
        "payload_sha256 TEXT, occurrences INTEGER NOT NULL) WITHOUT ROWID"
    )
    connection.commit()
    return connection


def _populate_journal_index(index: sqlite3.Connection, path: Path) -> tuple[str, int]:
    """Index every journal variant in bounded batches on a temporary disk DB."""
    uri = path.resolve().as_uri() + "?mode=ro"
    source = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        integrity = str(source.execute("PRAGMA integrity_check").fetchone()[0])
        tables = ["market_events"]
        if source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_event_versions'"
        ).fetchone():
            tables.append("market_event_versions")
        columns = (
            "dedupe_key, received_utc, exchange_ts, channel, kind, "
            "market_ticker, raw_json, payload_sha256"
        )
        for table in tables:
            cursor = source.execute(f"SELECT {columns} FROM {table}")
            while True:
                records = cursor.fetchmany(2000)
                if not records:
                    break
                values = []
                for record in records:
                    event = {
                        "dedupe_key": record[0],
                        "received_utc": record[1],
                        "exchange_ts": record[2],
                        "channel": record[3],
                        "kind": record[4],
                        "market_ticker": record[5],
                        "raw_json": decode_journal_payload(record[6]),
                        "payload_sha256": record[7],
                    }
                    values.append(
                        (event_archive_fingerprint(event), str(record[0]), str(record[7]))
                    )
                index.executemany(
                    "INSERT OR IGNORE INTO journal_fingerprints VALUES (?, ?, ?)",
                    values,
                )
                index.commit()
        count = int(index.execute("SELECT count(*) FROM journal_fingerprints").fetchone()[0])
        return integrity, count
    finally:
        source.close()


def _populate_archive_index(index: sqlite3.Connection, root: Path) -> tuple[int, int, int]:
    """Index Parquet variants in bounded batches and count duplicate rows."""
    index.execute("DELETE FROM archive_fingerprints")
    index.commit()
    rows = 0
    files = 0
    for path in root.rglob("*.parquet"):
        files += 1
        parquet_file = pq.ParquetFile(path)
        rows += int(parquet_file.metadata.num_rows)
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
        for batch in parquet_file.iter_batches(columns=columns, batch_size=2000):
            values = []
            for row in batch.to_pylist():
                if "kind" not in row:
                    row["kind"] = partition_kind
                if row.get("dedupe_key") is not None:
                    values.append(
                        (
                            event_archive_fingerprint(row),
                            str(row["dedupe_key"]),
                            str(row["payload_sha256"]),
                        )
                    )
            index.executemany(
                "INSERT INTO archive_fingerprints VALUES (?, ?, ?, 1) "
                "ON CONFLICT(fingerprint) DO UPDATE SET "
                "occurrences=occurrences+1",
                values,
            )
            index.commit()
    return files, rows, int(
        index.execute(
            "SELECT coalesce(sum(occurrences-1), 0) FROM archive_fingerprints"
        ).fetchone()[0]
    )


def _compare_index(index: sqlite3.Connection, archive_rows: int, duplicates: int) -> dict[str, int]:
    journal_variants = int(
        index.execute("SELECT count(*) FROM journal_fingerprints").fetchone()[0]
    )
    archive_variants = int(
        index.execute("SELECT count(*) FROM archive_fingerprints").fetchone()[0]
    )
    missing = int(
        index.execute(
            "SELECT count(*) FROM journal_fingerprints j "
            "WHERE NOT EXISTS (SELECT 1 FROM archive_fingerprints a "
            "WHERE a.fingerprint=j.fingerprint)"
        ).fetchone()[0]
    )
    extra = int(
        index.execute(
            "SELECT count(*) FROM archive_fingerprints a "
            "WHERE NOT EXISTS (SELECT 1 FROM journal_fingerprints j "
            "WHERE j.fingerprint=a.fingerprint)"
        ).fetchone()[0]
    )
    mismatches = int(
        index.execute(
            "SELECT count(*) FROM archive_fingerprints a "
            "WHERE NOT EXISTS (SELECT 1 FROM journal_fingerprints j "
            "WHERE j.fingerprint=a.fingerprint) "
            "AND EXISTS (SELECT 1 FROM journal_fingerprints j "
            "WHERE j.dedupe_key=a.dedupe_key "
            "AND j.payload_sha256<>a.payload_sha256)"
        ).fetchone()[0]
    )
    journal_rows = int(
        index.execute(
            "SELECT count(DISTINCT dedupe_key) FROM journal_fingerprints"
        ).fetchone()[0]
    )
    archive_keys = int(
        index.execute(
            "SELECT count(DISTINCT dedupe_key) FROM archive_fingerprints"
        ).fetchone()[0]
    )
    return {
        "journal_rows": journal_rows,
        "journal_variants": journal_variants,
        "parquet_rows": archive_rows,
        "parquet_unique_keys": archive_keys,
        "parquet_unique_variants": archive_variants,
        "parquet_duplicates": duplicates,
        "missing_from_parquet": missing,
        "extra_in_parquet": extra,
        "payload_hash_mismatches": mismatches,
    }


def _backup_journal(
    source_path: Path,
    destination_path: Path,
) -> None:
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=30)
    destination = sqlite3.connect(destination_path)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()


def _paper_ledger_rows(path: Path) -> int:
    """Count durable paper records through a read-only SQLite connection."""
    source_uri = path.resolve().as_uri() + "?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=30)
    try:
        return int(source.execute("SELECT count(*) FROM paper_ledger").fetchone()[0])
    finally:
        source.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Parquet rebuild from a durable standalone SQLite journal snapshot."
    )
    parser.add_argument(
        "--journal-snapshot",
        type=Path,
        help="use a previously verified SQLite snapshot and rebuild its Parquet archive in an isolated clone",
    )
    parser.add_argument(
        "--status-url",
        default=f"http://127.0.0.1:{settings.status_port}/api/v1/status",
        help="read-only standalone worker status endpoint",
    )
    parser.add_argument("--report", type=Path, help="write the JSON receipt to this path")
    args = parser.parse_args()

    before = _status_snapshot(args.status_url)
    journal_path = Path(before.get("paper_ledger_path") or settings.ledger_path)
    archive_path = Path(before.get("parquet_dir") or settings.parquet_dir)
    result: dict[str, Any] = {
        "snapshot_method": (
            "VERIFIED_SQLITE_SNAPSHOT_THEN_PARQUET_REBUILD"
            if args.journal_snapshot
            else "COPY_IMMUTABLE_PARQUET_PARTS_THEN_SQLITE_ONLINE_BACKUP"
        ),
        "worker_pid_before": before.get("pid"),
        "worker_state_before": before.get("service_state"),
        "worker_buffered_rows_before": before.get("parquet_buffered_rows"),
        "worker_queue_depth_before": before.get("queue_depth"),
        "exchange_write_calls": before.get("exchange_write_calls"),
        "paper_ledger_path": str(journal_path),
        "parquet_dir": str(archive_path),
    }

    with tempfile.TemporaryDirectory(prefix="kalshi15m-live-recovery-") as scratch:
        clone_home = Path(scratch) / "home"
        clone_data = clone_home / "data"
        clone_archive = clone_data / "market_data"
        clone_runtime = clone_home / "runtime"
        clone_archive.parent.mkdir(parents=True)
        clone_runtime.mkdir(parents=True)
        (clone_runtime / "status.json").write_text(
            json.dumps({"service_state": "RUNNING"}), encoding="utf-8"
        )
        index = _fingerprint_index(Path(scratch) / "fingerprints.sqlite3")

        try:
            # Archive files are immutable after atomic publication. Copy them
            # before SQLite's online backup so the clone journal covers every
            # archive part present at the start of the backup.
            if args.journal_snapshot:
                source_snapshot = args.journal_snapshot.expanduser().resolve(strict=True)
                source_bytes = source_snapshot.stat().st_size
                with source_snapshot.open("rb") as source_file:
                    source_sha256 = hashlib.file_digest(source_file, "sha256").hexdigest()
                shutil.copy2(source_snapshot, clone_data / "paper_ledger.sqlite3")
                result["source_journal_snapshot"] = {
                    "path": str(source_snapshot),
                    "bytes": source_bytes,
                    "sha256": source_sha256,
                }
                result["snapshot_method"] = "VERIFIED_SQLITE_SNAPSHOT_TO_EMPTY_PARQUET_CLONE"
            else:
                shutil.copytree(
                    archive_path,
                    clone_archive,
                    ignore=shutil.ignore_patterns("*.partial"),
                )
                _backup_journal(journal_path, clone_data / "paper_ledger.sqlite3")

            clone_integrity, journal_variants = _populate_journal_index(
                index, clone_data / "paper_ledger.sqlite3"
            )
            archive_files_before, archive_rows_before, archive_duplicates_before = (
                _populate_archive_index(index, clone_archive)
            )
            before_recovery = _compare_index(
                index, archive_rows_before, archive_duplicates_before
            )
            paper_ledger_rows_before = _paper_ledger_rows(
                clone_data / "paper_ledger.sqlite3"
            )
            result["snapshot"] = {
                "sqlite_integrity_before_recovery": clone_integrity,
                "journal_rows": before_recovery["journal_rows"],
                "journal_variants": journal_variants,
                "paper_ledger_rows": paper_ledger_rows_before,
                "parquet_files_copied": archive_files_before,
                "parquet_rows_copied": archive_rows_before,
                "missing_rows_before_recovery": before_recovery["missing_from_parquet"],
                "extra_rows_before_recovery": before_recovery["extra_in_parquet"],
                "duplicates_before_recovery": before_recovery["parquet_duplicates"],
                "payload_hash_mismatches_before_recovery": before_recovery[
                    "payload_hash_mismatches"
                ],
                "fingerprint_index": "TEMPORARY_DISK_BACKED_SQLITE",
            }

            removed_part_name = None
            removed_part_rows = 0
            removed_unique_journal_variants = 0
            if args.journal_snapshot:
                expected_recovery_rows = before_recovery["missing_from_parquet"]
            else:
                probe_part = _select_recovery_probe_part(clone_archive)
                if probe_part is None:
                    result["recovery"] = {
                        "passed": False,
                        "reason": "NO_NONEMPTY_PARQUET_PART",
                    }
                    return _finish(result, before, args.status_url, args.report)
                removed_part, removed_part_rows = probe_part
                removed_part_name = removed_part.name
                removed_part.unlink()
                _, remaining_rows, remaining_duplicates = _populate_archive_index(
                    index, clone_archive
                )
                before_recovery_trial = _compare_index(
                    index, remaining_rows, remaining_duplicates
                )
                expected_recovery_rows = before_recovery_trial["missing_from_parquet"]
                removed_unique_journal_variants = expected_recovery_rows

            clone_settings = replace(settings, home=clone_home)
            recovery = Storage(clone_settings)
            recovery.open()
            recovered_rows = int(recovery.parquet_recovery_rows)
            recovery_state = str(recovery.parquet_recovery_state)
            recovery.close()

            after_integrity, _ = _populate_journal_index(
                index, clone_data / "paper_ledger.sqlite3"
            )
            archive_files_after, archive_rows_after, archive_duplicates_after = (
                _populate_archive_index(index, clone_archive)
            )
            after = _compare_index(index, archive_rows_after, archive_duplicates_after)
            paper_ledger_rows_after = _paper_ledger_rows(
                clone_data / "paper_ledger.sqlite3"
            )
            structural_clean = (
                clone_integrity == "ok"
                and before_recovery["extra_in_parquet"] == 0
                and before_recovery["parquet_duplicates"] == 0
                and before_recovery["payload_hash_mismatches"] == 0
            )
            passed = (
                expected_recovery_rows > 0
                and after_integrity == "ok"
                and recovered_rows == expected_recovery_rows
                and after["missing_from_parquet"] == 0
                and after["extra_in_parquet"] == 0
                and after["parquet_duplicates"] == 0
                and after["payload_hash_mismatches"] == 0
                and paper_ledger_rows_after == paper_ledger_rows_before
                and structural_clean
            )
            result["recovery"] = {
                "removed_part": removed_part_name,
                "removed_part_rows": removed_part_rows,
                "probe_mode": "FULL_REBUILD_FROM_SQLITE_SNAPSHOT" if args.journal_snapshot else "REMOVE_PARQUET_PART",
                "removed_unique_journal_variants": removed_unique_journal_variants,
                "expected_recovery_rows": expected_recovery_rows,
                "recovered_rows": recovered_rows,
                "recovery_state": recovery_state,
                "sqlite_integrity_after_recovery": after_integrity,
                "paper_ledger_rows_before": paper_ledger_rows_before,
                "paper_ledger_rows_after": paper_ledger_rows_after,
                "paper_ledger_preserved": paper_ledger_rows_after == paper_ledger_rows_before,
                "archive_files_after_recovery": archive_files_after,
                "archive_structure_clean_before_trial": structural_clean,
                "after_recovery": after,
                "passed": passed,
            }
        finally:
            index.close()

    return _finish(result, before, args.status_url, args.report)


def _finish(
    result: dict[str, Any], before: dict[str, Any], status_url: str, report_path: Path | None
) -> int:
    after = _status_snapshot(status_url)
    result["worker_pid_after"] = after.get("pid")
    result["worker_state_after"] = after.get("service_state")
    result["worker_buffered_rows_after"] = after.get("parquet_buffered_rows")
    result["worker_queue_depth_after"] = after.get("queue_depth")
    result["exchange_write_calls_after"] = after.get("exchange_write_calls")
    result["worker_unchanged"] = (
        before.get("pid") == after.get("pid")
        and after.get("service_state") == "RUNNING"
        and before.get("exchange_write_calls") == 0
        and after.get("exchange_write_calls") == 0
    )
    rendered = json.dumps(result, indent=2)
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    recovery = result.get("recovery", {})
    return 0 if recovery.get("passed") and result["worker_unchanged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
