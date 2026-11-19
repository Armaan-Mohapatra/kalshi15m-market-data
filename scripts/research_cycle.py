"""Incremental forward-paper research cycle (read-only on the ledger).

Triggered only by NEWLY finalized settlements: it remembers which settlement keys it
has already processed in a checkpoint, does nothing when there are none, and otherwise
recomputes per-profile net P&L, hit rate and an event-clustered bootstrap interval.
Scope: this evaluates strategies that are *already* paper-running on forward data. It
does not search hypotheses or touch discovery/validation/OOS partitions, and it never
activates anything. usage: python scripts/research_cycle.py [--home DIR] [--seed N]
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

# Task Scheduler runs `python -I script.py` (isolated: no cwd, no PYTHONPATH).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

BOOTSTRAP = 2000


def load_settlements(ledger: Path) -> list[tuple[str, str, dict]]:
    db = sqlite3.connect("file:" + ledger.as_posix() + "?mode=ro", uri=True, timeout=30)
    try:
        rows = db.execute(
            "select idempotency_key, profile_id, payload_json from paper_ledger "
            "where record_type = 'PAPER_SETTLEMENT_FINAL' order by created_utc"
        ).fetchall()
    finally:
        db.close()
    return [(k, p, json.loads(j)) for k, p, j in rows]


def cluster_ci(clusters: list[list[float]], seed: int) -> tuple[float, float] | None:
    """95% bootstrap interval of total net P&L, resampling whole 15-minute markets."""
    if len(clusters) < 5:
        return None  # too few independent events to say anything
    rng, totals = random.Random(seed), []
    for _ in range(BOOTSTRAP):
        totals.append(sum(sum(clusters[rng.randrange(len(clusters))]) for _ in clusters))
    totals.sort()
    return round(totals[int(0.025 * BOOTSTRAP)], 4), round(totals[int(0.975 * BOOTSTRAP)], 4)


MIN_MARKETS_FOR_VERDICT = 30  # below this the bootstrap interval is not worth reading


def verdict(markets: int, ci: tuple[float, float] | None) -> str:
    """Label from the numbers. Never promotes: every label keeps the profile paper-only."""
    if ci is None or markets < MIN_MARKETS_FOR_VERDICT:
        return "INSUFFICIENT_DATA_KEEP_PAPER_ONLY"
    if ci[0] > 0:
        return "POSITIVE_FORWARD_PAPER_NEEDS_OOS_VALIDATION_KEEP_PAPER_ONLY"
    if ci[1] < 0:
        return "NEGATIVE_NET_RETURN_KEEP_PAPER_ONLY"
    return "NO_DEMONSTRATED_EDGE_KEEP_PAPER_ONLY"


def cycle(home: Path, seed: int = 7) -> dict:
    checkpoint_path = home / "reports" / "research_checkpoint.json"
    try:
        done = set(json.loads(checkpoint_path.read_text("utf-8"))["processed_keys"])
    except (OSError, ValueError, KeyError):
        done = set()
    from kalshi15m.config import Settings  # ledger may live on another volume (pointer file)

    settlements = load_settlements(Settings(home=home).ledger_path)
    fresh = [k for k, _, _ in settlements if k not in done]
    if not fresh and checkpoint_path.exists():
        return {"result": "NO_NEW_FINALIZED_MARKETS", "settlements_seen": len(settlements)}
    by_profile: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for _, profile, p in settlements:
        by_profile[profile][p["market_ticker"]].append(float(Decimal(p["net_pnl_dollars"])))
    profiles = {}
    for profile, markets in by_profile.items():
        values = [v for m in markets.values() for v in m]
        ci = cluster_ci(list(markets.values()), seed)
        profiles[profile] = {
            "settlements": len(values), "markets": len(markets), "net_pnl": round(sum(values), 4),
            "mean_per_settlement": round(sum(values) / len(values), 4),
            "hit_rate": round(sum(v > 0 for v in values) / len(values), 3),
            "net_pnl_ci95_cluster_bootstrap": ci,
            "verdict": verdict(len(markets), ci),
        }
    report = {"completed_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "result": "PROCESSED",
              "new_settlements": len(fresh), "profiles": profiles, "partition": "FORWARD_PAPER_ONLY",
              "processed_keys": sorted(done | set(fresh))}
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = checkpoint_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(report), encoding="utf-8")  # a crash before replace re-processes safely
    tmp.replace(checkpoint_path)
    return {k: v for k, v in report.items() if k != "processed_keys"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default=r"E:\PhoenixLab\runtime\k15m-native")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    result = cycle(Path(a.home), a.seed)
    # Heartbeat even for a no-op: Task Scheduler's "result 0" reflects the wrapper, not this
    # script, so the report alerts on this file's age instead.
    stamp = Path(a.home) / "reports" / "research_last_run.txt"
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), encoding="utf-8")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
