"""Bounded synthetic queue / actual storage-writer comparison for CI only.

No network calls, no production paths, no trading. This is NOT a live load test:
there is no wall-clock exchange arrival schedule or real HDD/SSD comparison.
"""
from __future__ import annotations

import asyncio
import json
import statistics
import tempfile
import time
import tracemalloc
from pathlib import Path

from kalshi15m.config import Settings
from kalshi15m.storage import Storage

TOTAL_EVENTS = 20_000
UNIQUE_EVENTS = 400
BATCH_SIZE = 500


def synthetic_event(index: int) -> dict:
    identity = index % UNIQUE_EVENTS
    return {
        "dedupe_key": f"ci-queue-{identity}",
        "channel": "synthetic_ws",
        "kind": "orderbook_delta",
        "market_ticker": "SYNTHETIC-ONLY",
        "received_utc": "2026-11-07T00:00:00+00:00",
        "exchange_ts": None,
        "payload": {"price": f"{identity % 100:02d}", "size": identity + 1},
    }


def run(capacity: int) -> dict:
    if capacity not in (12_000, 25_000):
        raise ValueError("Only the two agreed queue capacities are allowed")
    tracemalloc.start()
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    writer_durations_ms = []
    full_encounters = 0
    peak_queue = 0
    processed = 0

    with tempfile.TemporaryDirectory(prefix=f"phoenix-queue-{capacity}-") as directory:
        storage = Storage(Settings(home=Path(directory), min_free_bytes=0))
        storage.open()
        queue = asyncio.Queue(maxsize=capacity)

        def drain_batch() -> None:
            nonlocal processed
            batch = []
            while len(batch) < BATCH_SIZE and not queue.empty():
                batch.append(queue.get_nowait())
            started = time.perf_counter()
            storage.append_batch(batch)
            writer_durations_ms.append((time.perf_counter() - started) * 1_000)
            for _ in batch:
                queue.task_done()
            processed += len(batch)

        for i in range(TOTAL_EVENTS):
            if queue.full():
                full_encounters += 1
                drain_batch()
            queue.put_nowait(synthetic_event(i))
            peak_queue = max(peak_queue, queue.qsize())

        while not queue.empty():
            drain_batch()

        storage.flush_parquet(reason="SYNTHETIC_CAPACITY_CHECK")
        actual_unique = storage.connection.execute(
            "SELECT COUNT(*) FROM market_events"
        ).fetchone()[0]
        actual_duplicates = storage.events_duplicate
        archive_verified = storage.verify_archive_manifest()
        storage.close()

        assert queue.qsize() == 0
        assert processed == TOTAL_EVENTS
        assert actual_unique == UNIQUE_EVENTS, (capacity, actual_unique)
        assert actual_duplicates == TOTAL_EVENTS - UNIQUE_EVENTS, (
            capacity, actual_duplicates
        )
        assert archive_verified, f"archive manifest failed at capacity={capacity}"

    _current, peak_python_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "capacity_events": capacity,
        "synthetic_arrivals": TOTAL_EVENTS,
        "unique_journal_rows": actual_unique,
        "duplicate_observations": actual_duplicates,
        "peak_pending_queue_events": peak_queue,
        "times_queue_reached_capacity": full_encounters,
        "processed_queue_events": processed,
        "archive_manifest_verified": archive_verified,
        "writer_batch_count": len(writer_durations_ms),
        "batch_p95_ms": round(
            sorted(writer_durations_ms)[min(
                len(writer_durations_ms) - 1,
                int(0.95 * len(writer_durations_ms))
            )], 3
        ),
        "wall_seconds": round(time.perf_counter() - wall_start, 3),
        "cpu_seconds": round(time.process_time() - cpu_start, 3),
        "tracemalloc_peak_bytes": peak_python_bytes,
    }


if __name__ == "__main__":
    results = [run(12_000), run(25_000)]
    assert all(r["archive_manifest_verified"] for r in results)
    assert results[0]["times_queue_reached_capacity"] > 0
    assert results[1]["times_queue_reached_capacity"] == 0
    assert all(r["processed_queue_events"] == TOTAL_EVENTS for r in results)
    report = {
        "method": "synthetic burst + real SQLite/Parquet writer on ephemeral CI runner",
        "limitations": [
            "single-threaded queue scheduling, NOT exchange-real-time ingestion",
            "no external USB HDD or local SSD available on hosted CI",
            "Python tracemalloc is not total process resident memory",
            "queue capacity does not change steady-state writer throughput",
        ],
        "runs": results,
    }
    print("PHOENIX_QUEUE_RESULT=" + json.dumps(report, sort_keys=True), flush=True)
    Path("queue_capacity_ci_results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
