"""Archive writer durability regressions using only temporary storage."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow.parquet as pq

from kalshi15m.config import Settings
from kalshi15m.storage import Storage


class ArchiveManifestRetryTests(unittest.TestCase):
    def test_retry_after_manifest_commit_failure_reuses_renamed_part(self):
        with TemporaryDirectory() as directory:
            store = Storage(Settings(home=Path(directory), min_free_bytes=0))
            store.open()
            store.append_batch(
                [
                    {
                        "dedupe_key": "retry-one",
                        "channel": "test",
                        "kind": "observation",
                        "market_ticker": "TEST",
                        "received_utc": datetime.now(timezone.utc).isoformat(),
                        "payload": {"price": "0.42"},
                    },
                    {
                        "dedupe_key": "retry-two",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "TEST",
                        "received_utc": datetime.now(timezone.utc).isoformat(),
                        "payload": {"price": "0.43"},
                    },
                ]
            )
            real_connection = store.connection
            store.parquet_budget_last_reconcile_monotonic = 100.0

            class FailSecondCommit:
                calls = 0

                def __getattr__(self, name):
                    return getattr(real_connection, name)

                def commit(self):
                    self.calls += 1
                    if self.calls == 2:
                        raise sqlite3.OperationalError("injected manifest commit failure")
                    return real_connection.commit()

            try:
                store.connection = FailSecondCommit()
                with self.assertRaisesRegex(sqlite3.OperationalError, "injected manifest"):
                    with patch("kalshi15m.storage.time.monotonic", return_value=101.0):
                        store.flush_parquet(reason="INJECTED_COMMIT_FAILURE")

                failed_parts = list(store.cfg.parquet_dir.rglob("*.parquet"))
                self.assertEqual(len(failed_parts), 2)
                self.assertEqual(
                    store.parquet_bytes,
                    sum(part.stat().st_size for part in failed_parts),
                )
                self.assertEqual(len(store.parquet_buffer), 2)
                with patch("kalshi15m.storage.time.monotonic", return_value=101.0):
                    self.assertEqual(store.flush_parquet(reason="RETRY"), 2)

                store.connection = real_connection
                parts = list(store.cfg.parquet_dir.rglob("*.parquet"))
                self.assertEqual(len(parts), 2)
                self.assertEqual(sum(pq.read_table(part).num_rows for part in parts), 2)
                self.assertFalse(list(store.cfg.parquet_dir.rglob("*.partial")))
                self.assertEqual(store.parquet_buffer, [])
                manifest_rows = real_connection.execute(
                    "SELECT COALESCE(SUM(row_count), 0) FROM archive_manifests"
                ).fetchone()[0]
                self.assertEqual(manifest_rows, 2)
                self.assertEqual(store.parquet_bytes, sum(part.stat().st_size for part in parts))
                self.assertTrue(store.verify_archive_manifest())
            finally:
                store.connection = real_connection
                store.close()

    def test_restart_after_renamed_part_and_uncommitted_manifest_recovers_once(self):
        with TemporaryDirectory() as directory:
            cfg = Settings(home=Path(directory), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            event = {
                "dedupe_key": "restart-one",
                "channel": "test",
                "kind": "observation",
                "market_ticker": "TEST",
                "received_utc": "2026-11-07T12:00:00+00:00",
                "payload": {"price": "0.42"},
            }
            store.append_batch([event])
            real_connection = store.connection

            class FailManifestCommit:
                def __getattr__(self, name):
                    return getattr(real_connection, name)

                def commit(self):
                    raise sqlite3.OperationalError("injected manifest commit failure")

            store.connection = FailManifestCommit()
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected manifest"):
                store.flush_parquet(reason="INJECTED_COMMIT_FAILURE")
            self.assertEqual(len(list(cfg.parquet_dir.rglob("*.parquet"))), 1)
            self.assertEqual(len(store.parquet_buffer), 1)

            # Closing SQLite without Storage.close rolls back the uncommitted manifest,
            # as process termination would, while leaving the renamed part on disk.
            real_connection.close()
            store.connection = None
            restarted = Storage(cfg)
            try:
                restarted.open()
                parts = list(cfg.parquet_dir.rglob("*.parquet"))
                self.assertEqual(len(parts), 1)
                self.assertEqual(sum(pq.read_table(part).num_rows for part in parts), 1)
                self.assertEqual(
                    restarted.connection.execute(
                        "SELECT COALESCE(SUM(row_count), 0) FROM archive_manifests"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    restarted.parquet_bytes,
                    sum(part.stat().st_size for part in parts),
                )
                self.assertTrue(restarted.verify_archive_manifest())
                self.assertEqual(restarted.append_batch([event]), 0)
                self.assertEqual(restarted.flush_parquet(), 0)
                self.assertEqual(len(list(cfg.parquet_dir.rglob("*.parquet"))), 1)
            finally:
                restarted.close()

    def test_repeated_identical_batch_reuses_its_part_and_dedupes_ingest(self):
        with TemporaryDirectory() as directory:
            store = Storage(Settings(home=Path(directory), min_free_bytes=0))
            store.open()
            store.parquet_flush_rows = 1
            events = [
                {
                    "dedupe_key": f"repeat-{i}",
                    "channel": "test",
                    "kind": "observation",
                    "market_ticker": "TEST",
                    "received_utc": "2026-11-07T12:00:00+00:00",
                    "payload": {"price": price},
                }
                for i, price in enumerate(("0.42", "0.43"))
            ]
            captured = []
            writer = store._write_partitioned_parquet

            def capture(rows):
                captured.extend(dict(row) for row in rows)
                return writer(rows)

            try:
                with patch.object(store, "_write_partitioned_parquet", side_effect=capture):
                    self.assertEqual(store.append_batch(events), 2)
                part = next(store.cfg.parquet_dir.rglob("*.parquet"))
                store._write_partitioned_parquet(list(reversed(captured)))
                self.assertEqual(len(list(store.cfg.parquet_dir.rglob("*.parquet"))), 1)
                self.assertEqual(next(store.cfg.parquet_dir.rglob("*.parquet")), part)
                self.assertEqual(pq.read_table(part).num_rows, 2)
                self.assertEqual(store.parquet_bytes, part.stat().st_size)
                self.assertEqual(store.append_batch(events), 0)
                self.assertEqual(store.flush_parquet(), 0)
                self.assertTrue(store.verify_archive_manifest())
            finally:
                store.close()

    def test_part_name_hash_collision_preserves_both_batches(self):
        with TemporaryDirectory() as directory:
            store = Storage(Settings(home=Path(directory), min_free_bytes=0))
            store.open()
            store.parquet_flush_rows = 1
            events = [
                {
                    "dedupe_key": "collision-one",
                    "channel": "test",
                    "kind": "observation",
                    "market_ticker": "TEST",
                    "received_utc": "2026-11-07T12:00:00+00:00",
                    "payload": {"price": "0.42"},
                },
                {
                    "dedupe_key": "collision-two",
                    "channel": "test",
                    "kind": "observation",
                    "market_ticker": "TEST",
                    "received_utc": "2026-11-07T12:00:00+00:00",
                    "payload": {"price": "0.43"},
                },
            ]
            try:
                with patch("kalshi15m.storage.event_archive_fingerprint", return_value="0" * 64):
                    self.assertEqual(store.append_batch([events[0]]), 1)
                    self.assertEqual(store.append_batch([events[1]]), 1)
                parts = list(store.cfg.parquet_dir.rglob("*.parquet"))
                self.assertEqual(len(parts), 2)
                self.assertEqual(
                    sorted(row["dedupe_key"] for part in parts for row in pq.read_table(part).to_pylist()),
                    ["collision-one", "collision-two"],
                )
                self.assertEqual(store.parquet_bytes, sum(part.stat().st_size for part in parts))
                self.assertEqual(
                    store.connection.execute(
                        "SELECT COALESCE(SUM(row_count), 0) FROM archive_manifests"
                    ).fetchone()[0],
                    2,
                )
                self.assertTrue(store.verify_archive_manifest())
            finally:
                store.close()

    def test_budget_reconcile_uses_60_second_cadence_and_95_percent_override(self):
        with TemporaryDirectory() as directory:
            store = Storage(
                Settings(home=Path(directory), min_free_bytes=0, raw_data_budget_bytes=1000)
            )
            store.parquet_budget_reconcile_interval_seconds = 60.0
            store.parquet_budget_last_reconcile_monotonic = 100.0
            store.parquet_bytes = 100

            with (
                patch("kalshi15m.storage.time.monotonic", return_value=159.9),
                patch.object(store, "_parquet_directory_size", return_value=200) as scan,
            ):
                store._reconcile_parquet_size_if_due()
                scan.assert_not_called()

            with (
                patch("kalshi15m.storage.time.monotonic", return_value=160.0),
                patch.object(store, "_parquet_directory_size", return_value=200) as scan,
            ):
                store._reconcile_parquet_size_if_due()
                scan.assert_called_once_with()
                self.assertEqual(store.parquet_bytes, 200)
                self.assertEqual(store.parquet_budget_last_reconcile_monotonic, 160.0)

            store.parquet_budget_last_reconcile_monotonic = 160.0
            store.parquet_bytes = 949
            store.parquet_buffer_bytes_estimate = 0
            with (
                patch("kalshi15m.storage.time.monotonic", return_value=161.0),
                patch.object(store, "_parquet_directory_size", return_value=300) as scan,
            ):
                store._reconcile_parquet_size_if_due()
                scan.assert_not_called()

            store.parquet_bytes = 900
            store.parquet_buffer_bytes_estimate = 50
            with (
                patch("kalshi15m.storage.time.monotonic", return_value=162.0),
                patch.object(store, "_parquet_directory_size", return_value=300) as scan,
            ):
                store._reconcile_parquet_size_if_due()
                scan.assert_called_once_with()
                self.assertEqual(store.parquet_bytes, 300)


if __name__ == "__main__":
    unittest.main()
