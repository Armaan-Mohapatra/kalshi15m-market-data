from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi15m.config import settings
from kalshi15m.storage import decode_journal_payload

EVENT_COLUMNS = (
    "dedupe_key", "received_utc", "exchange_ts", "channel", "kind",
    "market_ticker", "raw_json", "payload_sha256",
)
CREATE_SQLITE = """CREATE TABLE market_events (
    dedupe_key TEXT PRIMARY KEY,
    received_utc TEXT NOT NULL,
    exchange_ts TEXT,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    market_ticker TEXT,
    raw_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
)"""


def _sizes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _median_query_ms(fn: Any, repetitions: int = 20) -> dict[str, float]:
    samples = []
    for _ in range(repetitions):
        began = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - began) * 1000)
    ordered = sorted(samples)
    return {
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 3),
        "samples": repetitions,
    }


def _load_source(source_db: Path, max_rows: int) -> tuple[list[tuple[Any, ...]], int]:
    uri = f"file:{source_db.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("BEGIN")
        total = int(connection.execute("SELECT count(*) FROM market_events").fetchone()[0])
        query = (
            f"SELECT {', '.join(EVENT_COLUMNS)} FROM market_events "
            "ORDER BY received_utc DESC, dedupe_key DESC LIMIT ?"
        )
        rows = connection.execute(query, (max_rows,)).fetchall()
        rows.reverse()
        rows = [
            (*row[:6], decode_journal_payload(row[6]), *row[7:])
            for row in rows
        ]
        connection.commit()
        return rows, total
    finally:
        connection.close()


def _benchmark_sqlite(root: Path, rows: list[tuple[Any, ...]], ticker: str | None) -> dict[str, Any]:
    path = root / "market_events.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=15000")
    connection.execute(CREATE_SQLITE)
    began = time.perf_counter()
    connection.executemany(
        "INSERT INTO market_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    connection.execute(
        "CREATE INDEX idx_events_market_time ON market_events(market_ticker, received_utc)"
    )
    connection.commit()
    write_seconds = time.perf_counter() - began

    def query() -> None:
        connection.execute(
            "SELECT count(*) FROM market_events WHERE market_ticker = ?", (ticker,)
        ).fetchone()

    query_metrics = _median_query_ms(query) if ticker else None
    connection.execute("PRAGMA wal_checkpoint(FULL)")
    connection.close()
    return {
        "rows": len(rows),
        "ingestion_method": "sqlite3 executemany and one commit with WAL FULL; index built in transaction",
        "write_seconds": round(write_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "query_market_ticker": query_metrics,
        "database_bytes": _sizes(root),
    }


def _benchmark_duckdb(root: Path, rows: list[tuple[Any, ...]], ticker: str | None) -> dict[str, Any]:
    path = root / "market_events.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute(
        "CREATE TABLE market_events (dedupe_key VARCHAR PRIMARY KEY, received_utc VARCHAR NOT NULL, "
        "exchange_ts VARCHAR, channel VARCHAR NOT NULL, kind VARCHAR NOT NULL, market_ticker VARCHAR, "
        "raw_json VARCHAR NOT NULL, payload_sha256 VARCHAR NOT NULL)"
    )
    began = time.perf_counter()
    table = pa.Table.from_pylist([dict(zip(EVENT_COLUMNS, row, strict=True)) for row in rows])
    connection.register("captured_rows", table)
    connection.execute("BEGIN TRANSACTION")
    connection.execute("INSERT INTO market_events SELECT * FROM captured_rows")
    connection.execute(
        "CREATE INDEX idx_events_market_time ON market_events(market_ticker, received_utc)"
    )
    connection.execute("COMMIT")
    write_seconds = time.perf_counter() - began
    connection.unregister("captured_rows")

    def query() -> None:
        connection.execute(
            "SELECT count(*) FROM market_events WHERE market_ticker = ?", [ticker]
        ).fetchone()

    query_metrics = _median_query_ms(query) if ticker else None
    count = connection.execute("SELECT count(*) FROM market_events").fetchone()[0]
    connection.close()
    return {
        "rows": int(count),
        "ingestion_method": "Arrow table batch insert plus index build",
        "write_seconds": round(write_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "query_market_ticker": query_metrics,
        "database_bytes": _sizes(root),
    }


def _benchmark_parquet(root: Path, rows: list[tuple[Any, ...]], ticker: str | None) -> dict[str, Any]:
    path = root / "market_events.parquet"
    began = time.perf_counter()
    records = [dict(zip(EVENT_COLUMNS, row, strict=True)) for row in rows]
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path, compression="zstd", compression_level=3)
    write_seconds = time.perf_counter() - began
    connection = duckdb.connect()

    def query() -> None:
        connection.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE market_ticker = ?",
            [str(path), ticker],
        ).fetchone()

    query_metrics = _median_query_ms(query) if ticker else None
    count = pq.ParquetFile(path).metadata.num_rows
    connection.close()
    return {
        "rows": int(count),
        "ingestion_method": "PyArrow table creation and ZSTD Parquet write",
        "write_seconds": round(write_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "query_market_ticker": query_metrics,
        "database_bytes": _sizes(root),
        "compression": "ZSTD level 3",
    }


def _psql(psql: Path, port: int, command: str) -> str:
    result = subprocess.run(
        [
            str(psql), "-X", "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
            "-p", str(port), "-U", "postgres", "-d", "postgres", "-A", "-t", "-c", command,
        ],
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"psql command failed: {result.stderr.strip()[:500]}")
    return result.stdout.strip()


def _benchmark_postgres(
    root: Path, rows: list[tuple[Any, ...]], ticker: str | None, psql: Path, port: int
) -> dict[str, Any]:
    csv_path = root / "market_events.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(rows)

    _psql(
        psql,
        port,
        "DROP TABLE IF EXISTS market_events; CREATE TABLE market_events ("
        "dedupe_key TEXT PRIMARY KEY, received_utc TEXT NOT NULL, exchange_ts TEXT, "
        "channel TEXT NOT NULL, kind TEXT NOT NULL, market_ticker TEXT, raw_json TEXT NOT NULL, "
        "payload_sha256 TEXT NOT NULL)",
    )
    copy_path = csv_path.resolve().as_posix().replace("'", "''")
    began = time.perf_counter()
    _psql(psql, port, f"\\copy market_events FROM '{copy_path}' WITH (FORMAT csv)")
    _psql(psql, port, "CREATE INDEX idx_events_market_time ON market_events(market_ticker, received_utc)")
    write_seconds = time.perf_counter() - began
    _psql(psql, port, "ANALYZE market_events")
    query_metrics = None
    if ticker:
        escaped_ticker = ticker.replace("'", "''")
        statement = (
            "EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON) "
            f"SELECT count(*) FROM market_events WHERE market_ticker = '{escaped_ticker}'"
        )
        plan = _psql(psql, port, ";".join([statement] * 20))
        execution_ms = [
            float(line.split(":", 1)[1].strip().split()[0])
            for line in plan.splitlines()
            if line.strip().startswith("Execution Time:")
        ]
        query_metrics = {
            "median_server_execution_ms": round(statistics.median(execution_ms), 3) if execution_ms else None,
            "p95_server_execution_ms": round(sorted(execution_ms)[min(len(execution_ms) - 1, int(len(execution_ms) * 0.95))], 3) if execution_ms else None,
            "samples": len(execution_ms),
        }
    count = int(_psql(psql, port, "SELECT count(*) FROM market_events"))
    data_directory = Path(_psql(psql, port, "SHOW data_directory"))
    table_bytes = int(_psql(psql, port, "SELECT pg_total_relation_size('market_events')"))
    return {
        "rows": count,
        "ingestion_method": "psql COPY FROM CSV followed by index build",
        "write_seconds": round(write_seconds, 6),
        "rows_per_second": round(len(rows) / write_seconds, 2) if write_seconds else None,
        "query_market_ticker": query_metrics,
        "database_bytes": table_bytes,
        "cluster_data_directory": str(data_directory),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark storage on captured Kalshi REST/WS journal rows only.")
    parser.add_argument("--source-db", type=Path, default=settings.ledger_path)
    parser.add_argument("--output-root", type=Path, default=settings.home / "benchmarks")
    parser.add_argument("--max-rows", type=int, default=50000)
    parser.add_argument("--psql", type=Path, help="PostgreSQL psql.exe from a dedicated temporary benchmark cluster")
    parser.add_argument("--postgres-port", type=int)
    args = parser.parse_args()

    if args.max_rows < 1:
        parser.error("--max-rows must be positive")
    rows, source_total = _load_source(args.source_db, args.max_rows)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = args.output_root / f"storage-benchmark-{run_id}"
    root.mkdir(parents=True, exist_ok=False)
    if not rows:
        result = {
            "result": "INSUFFICIENT_CAPTURED_ROWS",
            "source_db": str(args.source_db),
            "source_rows": source_total,
            "benchmarks": {},
        }
    else:
        ticker = next((str(row[5]) for row in rows if row[5]), None)
        kinds = Counter(str(row[4]) for row in rows)
        channels = Counter(str(row[3]) for row in rows)
        parquet_root = root / "parquet"
        sqlite_root = root / "sqlite"
        duckdb_root = root / "duckdb"
        for engine_root in (parquet_root, sqlite_root, duckdb_root):
            engine_root.mkdir()
        benchmarks = {
            "sqlite_wal_full": _benchmark_sqlite(sqlite_root, rows, ticker),
            "duckdb": _benchmark_duckdb(duckdb_root, rows, ticker),
            "parquet_zstd": _benchmark_parquet(parquet_root, rows, ticker),
        }
        if args.psql or args.postgres_port:
            if not args.psql or not args.postgres_port:
                parser.error("--psql and --postgres-port must be supplied together")
            pg_root = root / "postgres"
            pg_root.mkdir()
            benchmarks["postgresql"] = _benchmark_postgres(
                pg_root, rows, ticker, args.psql, args.postgres_port
            )
        result = {
            "result": "CAPTURED_DATA_SAMPLE",
            "source_db": str(args.source_db),
            "source_rows_total": source_total,
            "benchmarked_rows": len(rows),
            "first_received_utc": rows[0][1],
            "last_received_utc": rows[-1][1],
            "capture_span_seconds": round(
                max(0.0, (datetime.fromisoformat(rows[-1][1].replace("Z", "+00:00")) - datetime.fromisoformat(rows[0][1].replace("Z", "+00:00"))).total_seconds()),
                3,
            ),
            "sample_market_ticker_for_read_query": ticker,
            "row_kind_counts": dict(sorted(kinds.items())),
            "channel_counts": dict(sorted(channels.items())),
            "write_fixture": "captured market_events rows from the project's own exchange-read-only runtime",
            "measurement_note": "Write timings include each engine's listed bulk loader and index or compression work. SQLite and DuckDB query timings are Python client wall times; PostgreSQL query timing is server execution from EXPLAIN ANALYZE.",
            "benchmarks": benchmarks,
        }
    result["run_utc"] = datetime.now(timezone.utc).isoformat()
    result["benchmark_root"] = str(root)
    report = root / "benchmark.json"
    report.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
