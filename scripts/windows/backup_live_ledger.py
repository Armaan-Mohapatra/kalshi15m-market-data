from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def latest_rows(connection: sqlite3.Connection) -> dict[str, object]:
    event = connection.execute(
        "SELECT rowid, received_utc FROM market_events ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    paper = connection.execute(
        "SELECT rowid, created_utc FROM paper_ledger ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    return {
        "market_events_max_rowid": int(event[0]) if event else None,
        "market_events_latest_utc": str(event[1]) if event else None,
        "paper_ledger_max_rowid": int(paper[0]) if paper else None,
        "paper_ledger_latest_utc": str(paper[1]) if paper else None,
        "paper_ledger_rows": int(
            connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0]
        ),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--max-seconds", type=float, default=60.0)
    args = parser.parse_args()

    source_path = args.ledger.resolve()
    destination_dir = args.destination.resolve()
    destination_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = destination_dir / "paper_ledger.sqlite3"
    manifest_path = destination_dir / "manifest.json"
    started_utc = datetime.now(timezone.utc).isoformat()
    sidecars: list[dict[str, object]] = []
    for suffix in ("-wal", "-shm"):
        source_sidecar = Path(str(source_path) + suffix)
        if not source_sidecar.is_file():
            continue
        target_sidecar = destination_dir / (source_path.name + suffix)
        try:
            size_at_open = source_sidecar.stat().st_size
            copied = 0
            digest = hashlib.sha256()
            with source_sidecar.open("rb") as reader, target_sidecar.open("wb") as writer:
                while copied < size_at_open:
                    chunk = reader.read(min(8 * 1024 * 1024, size_at_open - copied))
                    if not chunk:
                        break
                    writer.write(chunk)
                    digest.update(chunk)
                    copied += len(chunk)
                writer.flush()
            sidecars.append(
                {
                    "source_name": source_sidecar.name,
                    "snapshot_name": target_sidecar.name,
                    "bytes_at_open": size_at_open,
                    "bytes_copied": copied,
                    "sha256_of_copied_bytes": digest.hexdigest(),
                    "best_effort_while_worker_open": True,
                }
            )
        except OSError as exc:
            sidecars.append(
                {
                    "source_name": source_sidecar.name,
                    "snapshot_name": target_sidecar.name,
                    "error_category": type(exc).__name__,
                    "best_effort_while_worker_open": True,
                }
            )

    source_uri = source_path.as_uri() + "?mode=ro"
    source = sqlite3.connect(source_uri, uri=True, timeout=10)
    destination: sqlite3.Connection | None = None
    report: dict[str, object] = {
        "schema": "kalshi15m.live_ledger_snapshot.v1",
        "started_utc": started_utc,
        "source_path": str(source_path),
        "snapshot_path": str(snapshot_path),
        "sidecar_copies": sidecars,
        "max_backup_seconds": args.max_seconds,
        "snapshot_state": "STARTED",
    }
    try:
        report["source_before"] = latest_rows(source)
        destination = sqlite3.connect(snapshot_path, timeout=10)
        deadline = time.monotonic() + args.max_seconds

        def progress(status: int, remaining: int, total: int) -> None:
            report["backup_total_pages"] = int(total)
            report["backup_remaining_pages"] = int(remaining)
            if time.monotonic() > deadline:
                raise TimeoutError("SQLite online backup exceeded configured time bound")

        source.backup(destination, pages=4096, progress=progress, sleep=0.01)
        destination.commit()
        report["snapshot_state"] = "COPIED"
        report["snapshot_rows"] = latest_rows(destination)
        report["snapshot_quick_check"] = str(
            destination.execute("PRAGMA quick_check").fetchone()[0]
        )
        destination.close()
        destination = None
        report["snapshot_bytes"] = snapshot_path.stat().st_size
        report["snapshot_sha256"] = sha256_file(snapshot_path)
        report["source_after"] = latest_rows(source)
        report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        report["snapshot_state"] = "VERIFIED"
        write_manifest(manifest_path, report)
        print(json.dumps(report, separators=(",", ":")))
        return 0
    except Exception as exc:
        report["snapshot_state"] = f"INCOMPLETE_{type(exc).__name__}"
        report["error_category"] = type(exc).__name__
        report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        if snapshot_path.exists():
            report["partial_snapshot_bytes"] = snapshot_path.stat().st_size
            report["partial_snapshot_sha256"] = sha256_file(snapshot_path)
        write_manifest(manifest_path, report)
        print(json.dumps(report, separators=(",", ":")))
        return 2
    finally:
        if destination is not None:
            destination.close()
        source.close()


if __name__ == "__main__":
    raise SystemExit(main())
