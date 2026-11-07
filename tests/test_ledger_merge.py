import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.merge_ledger_generations import merge_generations


SCHEMA = """
CREATE TABLE market_events (
    dedupe_key TEXT PRIMARY KEY,
    received_utc TEXT NOT NULL,
    exchange_ts TEXT,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    market_ticker TEXT,
    raw_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
);
CREATE TABLE paper_ledger (
    idempotency_key TEXT PRIMARY KEY,
    created_utc TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    record_type TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
"""


def create_ledger(path: Path, events: list[tuple], paper: list[tuple]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA)
        connection.executemany("INSERT INTO market_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", events)
        connection.executemany("INSERT INTO paper_ledger VALUES (?, ?, ?, ?, ?)", paper)
        connection.commit()
    finally:
        connection.close()


class LedgerMergeTests(unittest.TestCase):
    def test_imports_missing_rows_and_preserves_conflicting_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.sqlite3"
            source = root / "source.sqlite3"
            output = root / "merged.sqlite3"
            create_ledger(
                target,
                [("event-1", "2026-10-15T01:00:00Z", None, "rest", "trade", "MKT", "{\"v\":1}", "hash1")],
                [("paper-1", "2026-10-15T01:00:00Z", "btc", "decision", "{\"v\":1}")],
            )
            create_ledger(
                source,
                [
                    ("event-1", "2026-10-15T01:01:00Z", None, "rest", "trade", "MKT", "{\"v\":2}", "hash2"),
                    ("event-2", "2026-10-15T01:02:00Z", None, "rest", "book", "MKT", "{}", "hash3"),
                ],
                [
                    ("paper-1", "2026-10-15T01:01:00Z", "btc", "decision", "{\"v\":2}"),
                    ("paper-2", "2026-10-15T01:02:00Z", "btc", "fill", "{}"),
                ],
            )

            summary = merge_generations(target, source, output)

            self.assertEqual(summary["integrity"], "ok")
            self.assertEqual(summary["market_events"], 2)
            self.assertEqual(summary["paper_ledger"], 2)
            self.assertEqual(summary["imported_market_events"], 1)
            self.assertEqual(summary["imported_paper_ledger"], 1)
            self.assertEqual(summary["preserved_market_event_versions"], 1)
            self.assertEqual(summary["preserved_paper_ledger_versions"], 1)
            connection = sqlite3.connect(output)
            try:
                alternate_event = connection.execute(
                    "SELECT received_utc, raw_json FROM market_event_versions"
                ).fetchone()
                alternate_paper = connection.execute(
                    "SELECT created_utc, payload_json FROM paper_ledger_versions"
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual(alternate_event, ("2026-10-15T01:01:00Z", "{\"v\":2}"))
            self.assertEqual(alternate_paper, ("2026-10-15T01:01:00Z", "{\"v\":2}"))

    def test_refuses_to_overwrite_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.sqlite3"
            source = root / "source.sqlite3"
            output = root / "merged.sqlite3"
            create_ledger(target, [], [])
            create_ledger(source, [], [])
            output.write_bytes(b"preserve")
            with self.assertRaises(FileExistsError):
                merge_generations(target, source, output)
            self.assertEqual(output.read_bytes(), b"preserve")


if __name__ == "__main__":
    unittest.main()
