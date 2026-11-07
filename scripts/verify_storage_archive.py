from __future__ import annotations

import argparse
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
from kalshi15m.storage import Storage, decode_journal_payload, event_archive_fingerprint


def _archive_rows(root: Path) -> tuple[dict[str, dict[str, str | None]], int, int]:
    rows_by_fingerprint: dict[str, dict[str, str | None]] = {}
    rows = 0
    duplicates = 0
    for path in root.rglob("*.parquet"):
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
        for batch in parquet_file.iter_batches(columns=columns, batch_size=10_000):
            for row in batch.to_pylist():
                rows += 1
                if "kind" not in row:
                    row["kind"] = partition_kind
                if row.get("dedupe_key") is None:
                    continue
                fingerprint = event_archive_fingerprint(row)
                if fingerprint in rows_by_fingerprint:
                    duplicates += 1
                else:
                    rows_by_fingerprint[fingerprint] = {
                        "dedupe_key": str(row["dedupe_key"]),
                        "payload_sha256": str(row["payload_sha256"]),
                    }
    return rows_by_fingerprint, rows, duplicates


def _journal_rows(path: Path) -> tuple[str, dict[str, dict[str, str | None]]]:
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        rows: dict[str, dict[str, str | None]] = {}
        tables = ["market_events"]
        has_versions = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_event_versions'"
        ).fetchone()
        if has_versions:
            tables.append("market_event_versions")
        columns = (
            "dedupe_key, received_utc, exchange_ts, channel, kind, "
            "market_ticker, raw_json, payload_sha256"
        )
        for table in tables:
            for record in connection.execute(f"SELECT {columns} FROM {table}"):
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
                fingerprint = event_archive_fingerprint(event)
                rows[fingerprint] = {
                    "dedupe_key": str(record[0]),
                    "payload_sha256": str(record[7]),
                }
    finally:
        connection.close()
    return integrity, rows


def _compare(
    journal: dict[str, dict[str, str | None]], archive_root: Path
) -> dict[str, Any]:
    archive_rows, rows, duplicates = _archive_rows(archive_root)
    journal_fingerprints = set(journal)
    archive_fingerprints = set(archive_rows)
    missing = journal_fingerprints - archive_fingerprints
    extra = archive_fingerprints - journal_fingerprints
    journal_keys = {str(record["dedupe_key"]) for record in journal.values()}
    archive_keys = {str(record["dedupe_key"]) for record in archive_rows.values()}
    journal_digests_by_key: dict[str, set[str]] = {}
    for record in journal.values():
        journal_digests_by_key.setdefault(str(record["dedupe_key"]), set()).add(
            str(record["payload_sha256"])
        )
    mismatches = {
        fingerprint
        for fingerprint in extra
        if str(archive_rows[fingerprint]["dedupe_key"]) in journal_digests_by_key
        and str(archive_rows[fingerprint]["payload_sha256"])
        not in journal_digests_by_key[str(archive_rows[fingerprint]["dedupe_key"])]
    }
    return {
        "journal_rows": len(journal_keys),
        "journal_variants": len(journal_fingerprints),
        "parquet_rows": rows,
        "parquet_unique_keys": len(archive_keys),
        "parquet_unique_variants": len(archive_fingerprints),
        "parquet_duplicates": duplicates,
        "missing_from_parquet": len(missing),
        "extra_in_parquet": len(extra),
        "payload_hash_mismatches": len(mismatches),
        "missing_keys": sorted(
            {str(journal[fingerprint]["dedupe_key"]) for fingerprint in missing}
        ),
        "extra_keys": sorted(
            {str(archive_rows[fingerprint]["dedupe_key"]) for fingerprint in extra}
        ),
        "mismatch_keys": sorted(
            {str(archive_rows[fingerprint]["dedupe_key"]) for fingerprint in mismatches}
        ),
    }


def _status_snapshot(status_url: str) -> dict[str, Any]:
    with urlopen(status_url, timeout=5) as response:
        return json.loads(response.read())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare the live SQLite event journal with its Parquet archive."
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help="verify current archive parity while the worker is running; skip recovery clone",
    )
    parser.add_argument(
        "--show-keys",
        action="store_true",
        help="include full missing/extra/hash-mismatch key arrays in JSON output",
    )
    parser.add_argument(
        "--status-url",
        default=f"http://127.0.0.1:{settings.status_port}/api/v1/status",
        help="read the live worker status instead of a possibly stale status file",
    )
    args = parser.parse_args()

    status = _status_snapshot(args.status_url)
    journal_path = Path(status.get("paper_ledger_path") or settings.ledger_path)
    archive_path = Path(status.get("parquet_dir") or settings.parquet_dir)
    integrity, journal = _journal_rows(journal_path)
    live = _compare(journal, archive_path)
    pending = int(status.get("parquet_buffered_rows", 0))
    queue_depth = int(status.get("queue_depth", 0))
    report: dict[str, Any] = {
        "status_source": "LIVE_READ_ONLY_HTTP_ENDPOINT",
        "worker_pid": status.get("pid"),
        "worker_heartbeat_utc": status.get("heartbeat_utc"),
        "paper_ledger_path": str(journal_path),
        "parquet_dir": str(archive_path),
        "live_integrity": integrity,
        "live_parquet_files": sum(1 for _ in archive_path.rglob("*.parquet")),
        "live_parquet_bytes": sum(p.stat().st_size for p in archive_path.rglob("*.parquet")),
        "live_partial_files": sum(1 for _ in archive_path.rglob("*.partial")),
        "worker_buffered_rows": pending,
        "worker_queue_depth": queue_depth,
        "live_archive": live,
        "crash_recovery_clone": "SKIPPED_LIVE_ONLY" if args.live_only else "SKIPPED_LIVE_ARCHIVE_NOT_CAUGHT_UP",
    }
    caught_up = (
        integrity == "ok"
        and queue_depth == 0
        and live["missing_from_parquet"] == pending
        and live["extra_in_parquet"] == 0
        and live["parquet_duplicates"] == 0
        and live["payload_hash_mismatches"] == 0
        and report["live_partial_files"] == 0
    )
    if args.live_only:
        after_status = _status_snapshot(args.status_url)
        signature_fields = (
            "events_inserted",
            "queue_depth",
            "parquet_buffered_rows",
            "sqlite_bytes",
            "parquet_bytes",
        )
        before_signature = {key: status.get(key) for key in signature_fields}
        after_signature = {key: after_status.get(key) for key in signature_fields}
        progressed = before_signature != after_signature
        structural_issues = (
            integrity != "ok"
            or report["live_partial_files"] != 0
            or live["extra_in_parquet"] != 0
            or live["parquet_duplicates"] != 0
            or live["payload_hash_mismatches"] != 0
        )
        if progressed:
            live_scan_state = (
                "INCONCLUSIVE_CONCURRENT_WRITES_WITH_OBSERVED_ANOMALIES"
                if structural_issues
                else "INCONCLUSIVE_CONCURRENT_WRITES"
            )
        elif caught_up:
            live_scan_state = "EXACT_LIVE_MATCH"
        else:
            live_scan_state = "FAIL_INCONSISTENT"
        report["worker_progress_during_scan"] = progressed
        report["structural_anomalies_observed_during_scan"] = structural_issues
        report["worker_status_before_scan"] = before_signature
        report["worker_status_after_scan"] = after_signature
        report["live_scan_state"] = live_scan_state
        report["crash_recovery_clone"] = "SKIPPED_LIVE_ONLY"
    elif caught_up and pending == 0:
        with tempfile.TemporaryDirectory(prefix="kalshi15m-archive-recovery-") as scratch:
            clone_home = Path(scratch) / "home"
            clone_data = clone_home / "data"
            clone_archive = clone_data / "market_data"
            clone_runtime = clone_home / "runtime"
            clone_archive.parent.mkdir(parents=True)
            clone_runtime.mkdir(parents=True)
            (clone_runtime / "status.json").write_text(
                json.dumps({"service_state": "RUNNING"}), encoding="utf-8"
            )
            shutil.copytree(
                archive_path,
                clone_archive,
                ignore=shutil.ignore_patterns("*.partial"),
            )
            source_uri = journal_path.resolve().as_uri() + "?mode=ro"
            source = sqlite3.connect(source_uri, uri=True, timeout=10)
            destination = sqlite3.connect(clone_data / "paper_ledger.sqlite3")
            try:
                source.backup(destination)
            finally:
                destination.close()
                source.close()
            parts = [
                (pq.ParquetFile(path).metadata.num_rows, path)
                for path in clone_archive.rglob("*.parquet")
            ]
            if not parts:
                report["crash_recovery_clone"] = "FAIL_EMPTY_PARQUET_ARCHIVE"
            else:
                lost_rows, removed_part = min(parts, key=lambda item: item[0])
                removed_part.unlink()
                clone_settings = replace(settings, home=clone_home)
                recovery = Storage(clone_settings)
                recovery.open()
                recovered = recovery.parquet_recovery_rows
                recovery.close()
                clone_integrity, clone_journal = _journal_rows(clone_data / "paper_ledger.sqlite3")
                clone_result = _compare(clone_journal, clone_archive)
                report["crash_recovery_clone"] = {
                    "removed_part_rows": lost_rows,
                    "recovered_rows": recovered,
                    "recovery_state": recovery.parquet_recovery_state,
                    "journal_integrity": clone_integrity,
                    "after_recovery": {
                        key: value
                        for key, value in clone_result.items()
                        if key not in {"missing_keys", "extra_keys", "mismatch_keys"}
                    },
                }
                report["crash_recovery_clone"]["passed"] = (
                    recovered == lost_rows
                    and clone_integrity == "ok"
                    and clone_result["missing_from_parquet"] == 0
                    and clone_result["extra_in_parquet"] == 0
                    and clone_result["parquet_duplicates"] == 0
                    and clone_result["payload_hash_mismatches"] == 0
                )
    if not args.show_keys:
        for key in ("missing_keys", "extra_keys", "mismatch_keys"):
            report["live_archive"].pop(key, None)
    print(json.dumps(report, indent=2))
    recovery = report["crash_recovery_clone"]
    if args.live_only:
        if report["live_scan_state"] == "EXACT_LIVE_MATCH":
            return 0
        if report["live_scan_state"].startswith("INCONCLUSIVE_CONCURRENT_WRITES"):
            return 2
        return 1
    return 0 if caught_up and pending == 0 and isinstance(recovery, dict) and recovery.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
