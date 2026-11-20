"""Pre-registered experiment H1: is the strategy's chosen DIRECTION informative?

H1: net P&L of the direction the strategy actually traded is greater than the net P&L of the opposite
direction taken at the same instant (same arrival book, same taker simulator, same fees, same official
outcome). Paired per fill, market-clustered bootstrap for the interval.

Protocol (fixed before any out-of-sample data exists):
  * Partitions are chronological by decision time. A seal file records oos_start_utc the first time this
    runs; fills decided at or after it are the untouched OUT-OF-SAMPLE partition and are never used for
    discovery or validation. Everything before is split 2/3 DISCOVERY, 1/3 VALIDATION.
  * H1 is SUPPORTED_NEEDS_OOS only if both discovery and validation intervals of the paired difference
    sit entirely above 0; CONFIRMED_OOS only if the OOS interval does too (and OOS has enough markets).
    Nothing here ever activates or promotes a strategy: the verdict is a label for the report.
  * The inverse is evaluated only when the opposite side can be filled in full at arrival (otherwise the
    pair is excluded and counted), so partial-fill artefacts do not leak into the comparison.
Read-only: it never writes to the ledger (it writes the seal file under reports/ once).
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from kalshi15m.paper import simulate_taker_ioc  # noqa: E402
from research_cycle import cluster_ci  # noqa: E402

MIN_MARKETS_FOR_CONFIRMATION = 30
D = Decimal


def load_pairs(ledger: Path) -> list[dict]:
    """Join each PAPER_FILL to its official settlement and price the opposite trade."""
    db = sqlite3.connect("file:" + ledger.as_posix() + "?mode=ro", uri=True, timeout=30)
    try:
        fills = db.execute(
            "select profile_id, payload_json from paper_ledger where record_type = 'PAPER_FILL' order by created_utc"
        ).fetchall()
        settled = {}
        for profile, raw in db.execute(
            "select profile_id, payload_json from paper_ledger where record_type = 'PAPER_SETTLEMENT_FINAL'"
        ):
            p = json.loads(raw)
            if p.get("entry_decision_id"):
                settled[(profile, p["entry_decision_id"])] = p
    finally:
        db.close()
    pairs = []
    for profile, raw in fills:
        fill = json.loads(raw)
        settlement = settled.get((profile, fill.get("decision_id")))
        if settlement is None or settlement.get("settlement_result") not in ("yes", "no"):
            continue  # not settled (or no official result yet): never invent an outcome
        side = str(fill["side"]).lower()
        other = "no" if side == "yes" else "yes"
        qty = D(str(fill["filled_quantity"]))
        sim = simulate_taker_ioc(
            fill.get("arrival_orderbook_snapshot") or {}, other, "0.9900", qty,
            fill.get("fee_type"), fill.get("fee_multiplier"),
        )
        inverse_net = None
        if sim["fill_state"] == "FULL_FILL" and sim["entry_fee_dollars"] is not None:
            payout = qty if settlement["settlement_result"] == other else D(0)
            inverse_net = payout - D(sim["total_cost_dollars"]) - D(sim["entry_fee_dollars"]) - D(
                str(settlement.get("settlement_fee_dollars") or "0")
            )
        pairs.append({
            "profile": profile, "market": fill["market_ticker"], "decision_utc": fill["decision_utc"],
            "actual_net": D(str(settlement["net_pnl_dollars"])), "inverse_net": inverse_net,
        })
    return sorted(pairs, key=lambda x: x["decision_utc"])


def seal(home: Path, now_utc: str | None = None) -> str:
    """Record the out-of-sample boundary once; later calls return the same value."""
    path = home / "reports" / "oos_seal.json"
    try:
        return json.loads(path.read_text("utf-8"))["oos_start_utc"]
    except (OSError, ValueError, KeyError):
        value = now_utc or time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"oos_start_utc": value, "protocol": "H1 inversion, sealed at first run"}), "utf-8")
        tmp.replace(path)
        return value


def partition(pairs: list[dict], oos_start: str) -> dict[str, list[dict]]:
    before = [p for p in pairs if p["decision_utc"] < oos_start]
    cut = (2 * len(before)) // 3
    return {"discovery": before[:cut], "validation": before[cut:],
            "oos": [p for p in pairs if p["decision_utc"] >= oos_start]}


def summarise(rows: list[dict], seed: int = 11) -> dict:
    usable = [r for r in rows if r["inverse_net"] is not None]
    clusters: dict[str, list[float]] = {}
    for r in usable:
        clusters.setdefault(r["market"], []).append(float(r["actual_net"] - r["inverse_net"]))
    if not usable:
        return {"fills": len(rows), "paired": 0, "excluded_inverse_unfillable": len(rows), "markets": 0}
    return {
        "fills": len(rows), "paired": len(usable), "excluded_inverse_unfillable": len(rows) - len(usable),
        "markets": len(clusters),
        "actual_net": round(float(sum(r["actual_net"] for r in usable)), 4),
        "inverse_net": round(float(sum(r["inverse_net"] for r in usable)), 4),
        "diff_actual_minus_inverse": round(sum(sum(v) for v in clusters.values()), 4),
        "diff_ci95_cluster_bootstrap": cluster_ci(list(clusters.values()), seed),
    }


def verdict(parts: dict[str, dict]) -> str:
    def above_zero(summary: dict) -> bool:
        ci = summary.get("diff_ci95_cluster_bootstrap")
        return bool(ci) and ci[0] > 0

    if above_zero(parts["discovery"]) and above_zero(parts["validation"]):
        oos = parts["oos"]
        if oos.get("markets", 0) >= MIN_MARKETS_FOR_CONFIRMATION and above_zero(oos):
            return "H1_CONFIRMED_OOS_STILL_PAPER_ONLY"
        return "H1_SUPPORTED_NEEDS_OOS_KEEP_PAPER_ONLY"
    if parts["discovery"].get("paired", 0) < 5 or parts["validation"].get("paired", 0) < 5:
        return "H1_INSUFFICIENT_DATA"
    return "H1_NOT_SUPPORTED"


def evaluate(ledger: Path, home: Path, now_utc: str | None = None) -> dict:
    oos_start = seal(home, now_utc)
    parts = {name: summarise(rows) for name, rows in partition(load_pairs(ledger), oos_start).items()}
    return {"hypothesis": "H1 direction is informative (actual vs inverted, paired)", "oos_start_utc": oos_start,
            "partitions": parts, "verdict": verdict(parts),
            "not_done": ["random-side control", "threshold/timing/momentum/mean-reversion/imbalance variants",
                         "regime splits", "ensembles"]}


def main() -> int:
    from kalshi15m.config import Settings

    home = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(r"E:\PhoenixLab\runtime\k15m-native")
    print(json.dumps(evaluate(Settings(home=home).ledger_path, home), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
