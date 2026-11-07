from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EVENT_COLUMNS = (
    "dedupe_key",
    "received_utc",
    "exchange_ts",
    "channel",
    "kind",
    "market_ticker",
    "raw_json",
    "payload_sha256",
)
PAPER_COLUMNS = (
    "idempotency_key",
    "created_utc",
    "profile_id",
    "record_type",
    "payload_json",
)


def _readonly(path: Path) -> sqlite3.Connection:
    uri = "file:" + path.resolve().as_posix() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _source_id(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"blob_hex": value.hex()}
    return value


def _row_hash(row: tuple[Any, ...]) -> str:
    raw = json.dumps(
        [_json_value(value) for value in row],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _schema_columns(connection: sqlite3.Connection, schema: str, table: str) -> tuple[str, ...]:
    rows = connection.execute(f"PRAGMA {schema}.table_info('{table}')").fetchall()
    return tuple(str(row[1]) for row in rows)


def _alternate_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    key: str,
    columns: tuple[str, ...],
    version_table: str,
    source_id: str,
    captured_utc: str,
) -> int:
    compare_columns = columns[1:]
    where = " OR ".join(f"current_row.{column} IS NOT source.{column}" for column in compare_columns)
    select_columns = ", ".join(f"source.{column}" for column in columns)
    rows = connection.execute(
        f"SELECT {select_columns} FROM generation.{table} AS source "
        f"JOIN main.{table} AS current_row USING ({key}) WHERE {where}"
    ).fetchall()
    placeholders = ", ".join("?" for _ in range(len(columns) + 3))
    insert = (
        f"INSERT OR IGNORE INTO {version_table} "
        f"(source_generation, row_sha256, captured_utc, {', '.join(columns)}) "
        f"VALUES ({placeholders})"
    )
    connection.executemany(
        insert,
        (
            (source_id, _row_hash(row), captured_utc, *row)
            for row in rows
        ),
    )
    return len(rows)


def merge_generations(target: Path, source: Path, output: Path) -> dict[str, Any]:
    target = target.resolve()
    source = source.resolve()
    output = output.resolve()
    if not target.is_file() or not source.is_file():
        raise FileNotFoundError("target and source must be existing database files")
    if target == source or output in {target, source}:
        raise ValueError("target, source, and output must be different paths")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".partial")
    if temporary.exists():
        raise FileExistsError(f"refusing to overwrite {temporary}")

    source_id = _source_id(source)
    captured_utc = datetime.now(timezone.utc).isoformat()
    target_connection = _readonly(target)
    result_connection: sqlite3.Connection | None = None
    try:
        result_connection = sqlite3.connect(temporary, timeout=30, uri=True)
        target_connection.backup(result_connection, pages=2048, sleep=0.05)
        result_connection.execute(
            "ATTACH DATABASE ? AS generation",
            ("file:" + source.as_posix() + "?mode=ro",),
        )
        for table, expected in (("market_events", EVENT_COLUMNS), ("paper_ledger", PAPER_COLUMNS)):
            if _schema_columns(result_connection, "main", table) != expected:
                raise ValueError(f"target schema mismatch for {table}")
            if _schema_columns(result_connection, "generation", table) != expected:
                raise ValueError(f"source schema mismatch for {table}")

        result_connection.executescript(
            """
            CREATE TABLE market_event_versions (
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
            ) WITHOUT ROWID;
            CREATE TABLE paper_ledger_versions (
                source_generation TEXT NOT NULL,
                row_sha256 TEXT NOT NULL,
                captured_utc TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (source_generation, row_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE ledger_generation_merge_audit (
                source_generation TEXT PRIMARY KEY,
                captured_utc TEXT NOT NULL,
                source_market_event_rows INTEGER NOT NULL,
                source_paper_ledger_rows INTEGER NOT NULL,
                imported_market_event_rows INTEGER NOT NULL,
                imported_paper_ledger_rows INTEGER NOT NULL,
                preserved_market_event_versions INTEGER NOT NULL,
                preserved_paper_ledger_versions INTEGER NOT NULL
            ) WITHOUT ROWID;
            """
        )
        result_connection.execute("BEGIN IMMEDIATE")
        event_versions = _alternate_rows(
            result_connection,
            table="market_events",
            key="dedupe_key",
            columns=EVENT_COLUMNS,
            version_table="market_event_versions",
            source_id=source_id,
            captured_utc=captured_utc,
        )
        paper_versions = _alternate_rows(
            result_connection,
            table="paper_ledger",
            key="idempotency_key",
            columns=PAPER_COLUMNS,
            version_table="paper_ledger_versions",
            source_id=source_id,
            captured_utc=captured_utc,
        )

        event_select = ", ".join(EVENT_COLUMNS)
        paper_select = ", ".join(PAPER_COLUMNS)
        result_connection.execute(
            f"INSERT OR IGNORE INTO main.market_events ({event_select}) "
            f"SELECT {event_select} FROM generation.market_events"
        )
        imported_events = int(result_connection.execute("SELECT changes()").fetchone()[0])
        result_connection.execute(
            f"INSERT OR IGNORE INTO main.paper_ledger ({paper_select}) "
            f"SELECT {paper_select} FROM generation.paper_ledger"
        )
        imported_paper = int(result_connection.execute("SELECT changes()").fetchone()[0])
        result_connection.execute(
            "INSERT INTO ledger_generation_merge_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source_id,
                captured_utc,
                int(result_connection.execute("SELECT count(*) FROM generation.market_events").fetchone()[0]),
                int(result_connection.execute("SELECT count(*) FROM generation.paper_ledger").fetchone()[0]),
                imported_events,
                imported_paper,
                event_versions,
                paper_versions,
            ),
        )
        result_connection.commit()

        uncovered_events = result_connection.execute(
            "SELECT count(*) FROM generation.market_events source "
            "LEFT JOIN main.market_events current_row USING (dedupe_key) "
            "WHERE current_row.dedupe_key IS NULL"
        ).fetchone()[0]
        uncovered_paper = result_connection.execute(
            "SELECT count(*) FROM generation.paper_ledger source "
            "LEFT JOIN main.paper_ledger current_row USING (idempotency_key) "
            "WHERE current_row.idempotency_key IS NULL"
        ).fetchone()[0]
        if uncovered_events or uncovered_paper:
            raise RuntimeError("merge left source primary keys uncovered")
        integrity = str(result_connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise RuntimeError(f"merged database integrity check failed: {integrity}")

        summary = {
            "source_generation_sha256": source_id,
            "captured_utc": captured_utc,
            "target": str(target),
            "source": str(source),
            "output": str(output),
            "market_events": int(result_connection.execute("SELECT count(*) FROM main.market_events").fetchone()[0]),
            "paper_ledger": int(result_connection.execute("SELECT count(*) FROM main.paper_ledger").fetchone()[0]),
            "imported_market_events": imported_events,
            "imported_paper_ledger": imported_paper,
            "preserved_market_event_versions": int(result_connection.execute("SELECT count(*) FROM market_event_versions").fetchone()[0]),
            "preserved_paper_ledger_versions": int(result_connection.execute("SELECT count(*) FROM paper_ledger_versions").fetchone()[0]),
            "integrity": integrity,
        }
        result_connection.execute("DETACH DATABASE generation")
        result_connection.close()
        result_connection = None
        with temporary.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        return summary
    except BaseException:
        if result_connection is not None:
            result_connection.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                Path(str(temporary) + suffix).unlink(missing_ok=True)
            except OSError:
                pass
        raise
    finally:
        target_connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge two SQLite journal generations without dropping conflicting rows.")
    parser.add_argument("--target", required=True, type=Path, help="Existing canonical ledger to preserve")
    parser.add_argument("--source", required=True, type=Path, help="Additional SQLite ledger generation")
    parser.add_argument("--output", required=True, type=Path, help="New output database; must not exist")
    args = parser.parse_args()
    print(json.dumps(merge_generations(args.target, args.source, args.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
