from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from kalshi15m.config import Settings
from kalshi15m.storage import Storage
from scripts.verify_live_snapshot_recovery import (
    _compare_index,
    _fingerprint_index,
    _populate_archive_index,
    _populate_journal_index,
)


class DiskBackedRecoveryVerifierTests(unittest.TestCase):
    def test_index_compares_archives_and_detects_a_missing_part(self):
        with TemporaryDirectory(prefix="kalshi15m-recovery-verifier-test-") as root:
            home = Path(root) / "home"
            storage = Storage(Settings(home=home, min_free_bytes=0))
            storage.open()
            storage.append_batch(
                [
                    {
                        "dedupe_key": f"verifier-{index}",
                        "received_utc": f"2026-10-15T00:00:0{index}+00:00",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": {"id": index, "value": "x" * 80},
                    }
                    for index in range(3)
                ]
            )
            storage.close()

            index = _fingerprint_index(Path(root) / "fingerprints.sqlite3")
            try:
                integrity, journal_variants = _populate_journal_index(
                    index, storage.cfg.ledger_path
                )
                files, rows, duplicates = _populate_archive_index(
                    index, storage.cfg.parquet_dir
                )
                complete = _compare_index(index, rows, duplicates)
                self.assertEqual(integrity, "ok")
                self.assertEqual(journal_variants, 3)
                self.assertGreater(files, 0)
                self.assertEqual(complete["missing_from_parquet"], 0)
                self.assertEqual(complete["extra_in_parquet"], 0)
                self.assertEqual(complete["parquet_duplicates"], 0)

                part = next(storage.cfg.parquet_dir.rglob("*.parquet"))
                duplicate = part.with_name(f"duplicate-{part.name}")
                shutil.copy2(part, duplicate)
                _, duplicate_rows, duplicate_count = _populate_archive_index(
                    index, storage.cfg.parquet_dir
                )
                duplicate_comparison = _compare_index(
                    index, duplicate_rows, duplicate_count
                )
                self.assertEqual(duplicate_comparison["parquet_duplicates"], 3)
                duplicate.unlink()

                part.unlink()
                _, remaining_rows, remaining_duplicates = _populate_archive_index(
                    index, storage.cfg.parquet_dir
                )
                missing = _compare_index(index, remaining_rows, remaining_duplicates)
                self.assertEqual(missing["missing_from_parquet"], 3)
            finally:
                index.close()


if __name__ == "__main__":
    unittest.main()
