"""Cross-language compatibility check of *isolated Rust stage* only.

Use with a newly generated synthetic replay stage; never run against live
Phoenix journals, SQLite/WAL or E:\\TradingNextData.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pyarrow.parquet as pq


def check(home: Path) -> None:
    home = home.resolve()
    db = home / "data" / "paper_ledger.sqlite3"
    archive = home / "data" / "market_data"
    assert db.is_file(), "missing Rust staging SQLite"
    con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    try:
        canonical = con.execute("SELECT COUNT(*) FROM market_events").fetchone()[0]
        versions = con.execute("SELECT COUNT(*) FROM market_event_versions").fetchone()[0]
        assert canonical >= 4, (canonical, versions)
        assert versions == 1, f"expected one metadata conflict, got {versions}"
        gap = con.execute(
            "SELECT COUNT(*) FROM market_events WHERE kind = 'sequence_gap'"
        ).fetchone()[0]
        assert gap == 1, f"expected one recorded subscription gap, got {gap}"
        stored = con.execute(
            "SELECT dedupe_key,payload_sha256,raw_json FROM market_events"
        ).fetchall()
        for key, digest, raw in stored:
            assert hashlib.sha256(raw.encode("utf-8")).hexdigest() == digest, key
            json.loads(raw)
        manifests = con.execute(
            "SELECT part_path,row_count,byte_count,sha256 FROM archive_manifests"
        ).fetchall()
        assert manifests, "Rust archive manifests absent"
        archived = 0
        seen = set()
        for rel, count, size, digest in manifests:
            path = archive / rel
            assert path.is_file(), rel
            assert path.stat().st_size == size, rel
            assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, rel
            parquet = pq.read_table(path)
            assert parquet.num_rows == count, rel
            assert {
                "dedupe_key", "received_utc", "exchange_ts", "channel",
                "market_ticker", "payload_sha256", "raw_json",
            }.issubset(set(parquet.column_names)), rel
            for key, d in zip(
                parquet["dedupe_key"].to_pylist(),
                parquet["payload_sha256"].to_pylist(),
            ):
                assert (key, d) not in seen, (rel, key, d)
                seen.add((key, d))
            archived += count
        assert archived == canonical + versions, (archived, canonical, versions)
        print(json.dumps({
            "status": "PASS_RUST_OUTPUT_READABLE_IN_PYTHON",
            "canonical": canonical, "versions": versions,
            "subscription_gaps": gap, "parquet_rows": archived,
            "manifest_parts": len(manifests),
        }, sort_keys=True))
    finally:
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python check_python_parity.py <rust-staging-home>")
    check(Path(sys.argv[1]))
