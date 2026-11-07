from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kalshi15m.config import settings
from kalshi15m.storage import decode_journal_payload, encode_journal_payload


EVENT_COLUMNS = (
    "dedupe_key", "received_utc", "exchange_ts", "channel", "kind",
    "market_ticker", "raw_json", "payload_sha256",
)


def _load_fixture(path: Path, row_limit: int) -> tuple[list[tuple[Any, ...]], int]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        connection.execute("BEGIN")
        total = int(connection.execute("SELECT count(*) FROM market_events").fetchone()[0])
        query = (
            f"SELECT {', '.join(EVENT_COLUMNS)} FROM market_events "
            "ORDER BY received_utc DESC, dedupe_key DESC LIMIT ?"
        )
        rows = connection.execute(query, (row_limit,)).fetchall()
        rows.reverse()
        decoded = [
            (*row[:6], decode_journal_payload(row[6]), *row[7:])
            for row in rows
        ]
        return decoded, total
    finally:
        connection.close()


def _benchmark(root: Path, rows: list[tuple[Any, ...]], compressed: bool) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "market_events.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """CREATE TABLE market_events (
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
    connection.execute(
        "CREATE INDEX idx_events_market_time ON market_events(market_ticker, received_utc)"
    )
    cpu_start = time.process_time()
    started = time.perf_counter()
    stored = rows if not compressed else [
        (*row[:6], encode_journal_payload(row[6]), *row[7:])
        for row in rows
    ]
    connection.executemany(
        "INSERT INTO market_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", stored
    )
    connection.commit()
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    write_seconds = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_start
    roundtrip = {
        key: decode_journal_payload(raw)
        for key, raw in connection.execute(
            "SELECT dedupe_key, raw_json FROM market_events"
        )
    }
    expected = {row[0]: row[6] for row in rows}
    blob_rows = int(
        connection.execute(
            "SELECT count(*) FROM market_events WHERE typeof(raw_json)='blob'"
        ).fetchone()[0]
    )
    connection.close()
    database_bytes = sum(item.stat().st_size for item in root.rglob("*") if item.is_file())
    return {
        "database_bytes": database_bytes,
        "write_seconds_including_codec": round(write_seconds, 6),
        "cpu_seconds_including_codec": round(cpu_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "blob_rows": blob_rows,
        "roundtrip_exact": roundtrip == expected,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare raw TEXT and ZSTD BLOB SQLite journals on one captured fixture."
    )
    parser.add_argument("--source-db", type=Path, default=settings.ledger_path)
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rows, total = _load_fixture(args.source_db, max(1, args.rows))
    with tempfile.TemporaryDirectory(prefix="kalshi15m-journal-compression-") as scratch:
        scratch_root = Path(scratch)
        text = _benchmark(scratch_root / "text", rows, compressed=False)
        zstd = _benchmark(scratch_root / "zstd_blob", rows, compressed=True)
    before = text["database_bytes"]
    after = zstd["database_bytes"]
    result = {
        "measured_utc": datetime.now(timezone.utc).isoformat(),
        "source_db": str(args.source_db.resolve()),
        "source_total_rows": total,
        "fixture_rows": len(rows),
        "fixture_raw_json_bytes": sum(len(row[6].encode("utf-8")) for row in rows),
        "text_sqlite": text,
        "zstd_blob_sqlite": zstd,
        "bytes_saved": before - after,
        "size_reduction_percent": round(100 * (before - after) / before, 3) if before else 0,
        "measurement_note": "Same captured rows, schema, index, WAL mode, and FULL synchronous setting; codec cost is included in the ZSTD write time.",
    }
    rendered = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if text["roundtrip_exact"] and zstd["roundtrip_exact"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
