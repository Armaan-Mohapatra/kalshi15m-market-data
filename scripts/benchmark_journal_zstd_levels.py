from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa

from kalshi15m.config import settings
from kalshi15m.storage import _JOURNAL_ZSTD_PREFIX, decode_journal_payload


CREATE_SQL = """CREATE TABLE market_events (
    dedupe_key TEXT PRIMARY KEY,
    received_utc TEXT NOT NULL,
    exchange_ts TEXT,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    market_ticker TEXT,
    raw_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
)"""
INSERT_SQL = "INSERT INTO market_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
COLUMNS = (
    "dedupe_key", "received_utc", "exchange_ts", "channel", "kind",
    "market_ticker", "raw_json", "payload_sha256",
)


def _load_fixture(source: Path, limit: int) -> tuple[list[tuple[Any, ...]], int]:
    uri = source.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=15)
    try:
        connection.execute("BEGIN")
        total = int(connection.execute("SELECT count(*) FROM market_events").fetchone()[0])
        rows = connection.execute(
            f"SELECT {', '.join(COLUMNS)} FROM market_events "
            "ORDER BY received_utc DESC, dedupe_key DESC LIMIT ?",
            (limit,),
        ).fetchall()
        rows.reverse()
        decoded = [(*row[:6], decode_journal_payload(row[6]), *row[7:]) for row in rows]
        return decoded, total
    finally:
        connection.close()


def _encode(raw_json: str, codec: pa.Codec | None) -> str | bytes:
    if codec is None:
        return raw_json
    raw = raw_json.encode("utf-8")
    compressed = bytes(codec.compress(raw))
    encoded = _JOURNAL_ZSTD_PREFIX + len(raw).to_bytes(4, "big") + compressed
    return encoded if len(encoded) + 16 < len(raw) else raw_json


def _benchmark(root: Path, rows: list[tuple[Any, ...]], level: int | None) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(root / "market_events.sqlite3")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(CREATE_SQL)
    connection.execute(
        "CREATE INDEX idx_events_market_time ON market_events(market_ticker, received_utc)"
    )
    codec = None if level is None else pa.Codec("zstd", compression_level=level)
    stored = [(*row[:6], _encode(row[6], codec), *row[7:]) for row in rows]
    cpu_start = time.process_time()
    started = time.perf_counter()
    connection.executemany(INSERT_SQL, stored)
    connection.commit()
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    write_seconds = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_start
    roundtrip = {
        key: decode_journal_payload(raw)
        for key, raw in connection.execute("SELECT dedupe_key, raw_json FROM market_events")
    }
    expected = {row[0]: row[6] for row in rows}
    blob_rows = int(
        connection.execute(
            "SELECT count(*) FROM market_events WHERE typeof(raw_json)='blob'"
        ).fetchone()[0]
    )
    connection.close()
    database_bytes = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    return {
        "compression_level": level,
        "database_bytes": database_bytes,
        "bytes_per_event": round(database_bytes / len(rows), 3) if rows else None,
        "write_seconds_including_codec": round(write_seconds, 6),
        "cpu_seconds_including_codec": round(cpu_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "blob_rows": blob_rows,
        "roundtrip_exact": roundtrip == expected,
        "roundtrip_sha256": hashlib.sha256(
            "".join(f"{key}:{value}\n" for key, value in sorted(roundtrip.items())).encode()
        ).hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare SQLite journal ZSTD levels on one captured live-event fixture."
    )
    parser.add_argument("--source-db", type=Path, default=settings.ledger_path)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--levels", default="1,3,6,9,12")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    levels = [int(value) for value in args.levels.split(",") if value.strip()]
    if any(level < 1 or level > 22 for level in levels):
        parser.error("ZSTD levels must be between 1 and 22")
    rows, total = _load_fixture(args.source_db, max(1, args.rows))
    with tempfile.TemporaryDirectory(prefix="kalshi15m-zstd-levels-") as scratch:
        scratch_root = Path(scratch)
        results = {"text": _benchmark(scratch_root / "text", rows, None)}
        for level in levels:
            results[f"zstd_{level}"] = _benchmark(
                scratch_root / f"zstd_{level}", rows, level
            )
    text_bytes = results["text"]["database_bytes"]
    for result in results.values():
        result["savings_vs_text_bytes"] = text_bytes - result["database_bytes"]
        result["reduction_vs_text_percent"] = round(
            100 * (text_bytes - result["database_bytes"]) / text_bytes, 3
        ) if text_bytes else 0
    report = {
        "measured_utc": datetime.now(timezone.utc).isoformat(),
        "source_db": str(args.source_db.resolve()),
        "source_rows_total": total,
        "fixture_rows": len(rows),
        "fixture_first_received_utc": rows[0][1] if rows else None,
        "fixture_last_received_utc": rows[-1][1] if rows else None,
        "fixture_raw_json_bytes": sum(len(row[6].encode("utf-8")) for row in rows),
        "results": results,
        "measurement_note": (
            "Each level uses the same captured rows, schema, index, WAL FULL setting, and "
            "temporary database. Read-only fixture load is outside the write timing."
        ),
    }
    rendered = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if all(result["roundtrip_exact"] for result in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
