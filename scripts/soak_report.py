"""Soak report: compile the evidence for the 24 h soak and refuse to call it passed too early.

SOAK_COMPLETE only if ALL hold: the worker has been up >= 24 h under one PID and one source commit
(no restart inside the window), zero exchange writes/orders and PAPER_ONLY, no CRITICAL alert opened
inside the window, no rows dropped, archive integrity VERIFIED_COMPLETE now, and the supervisor issued
no restart. Anything else is SOAK_IN_PROGRESS (not yet 24 h) or SOAK_FAILED with the reasons listed.
This is P8 process-stability evidence, NOT P0 continuous-feed certification.\nRead-only. usage: python scripts/soak_report.py [home] [--port 8778]
"""
from __future__ import annotations

import csv
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SOAK_SECONDS = 24 * 3600


def judge(facts: dict) -> tuple[str, list[str]]:
    """facts -> (verdict, reasons). Pure so it can be tested without a running worker."""
    problems: list[str] = []
    if facts["mode"] != "PAPER_ONLY":
        problems.append(f"mode is {facts['mode']}")
    if facts["exchange_write_calls"] or facts["orders_submitted_to_exchange"]:
        problems.append("exchange writes or orders observed")
    if facts["critical_alerts_in_window"]:
        problems.append(f"{len(facts['critical_alerts_in_window'])} CRITICAL alert(s) opened in the window")
    if facts["parquet_dropped_rows"]:
        problems.append(f"{facts['parquet_dropped_rows']} Parquet rows dropped")
    if not str(facts["archive_integrity_state"]).startswith("VERIFIED_COMPLETE"):
        problems.append(f"archive integrity {facts['archive_integrity_state']}")
    if facts["restarts_in_window"]:
        problems.append(f"{facts['restarts_in_window']} supervisor restart(s) in the window")
    if facts["commits_seen_in_window"] > 1:
        problems.append("more than one source commit ran in the window")
    if facts.get("service_state") != "RUNNING":
        problems.append("collector is not RUNNING")
    if facts.get("journal_process_alive") is not True:
        problems.append("journal process is not confirmed alive")
    if facts.get("source_dirty") is not False or not facts.get("source_commit"):
        problems.append("source revision is dirty or unverified")
    heartbeat = facts.get("heartbeat_age_s")
    if heartbeat is None or heartbeat < -30 or heartbeat > 60:
        problems.append(f"worker heartbeat is stale or unavailable ({heartbeat}s)")
    if problems:
        return "SOAK_FAILED", problems
    if facts["uptime_seconds"] < SOAK_SECONDS:
        remaining = (SOAK_SECONDS - facts["uptime_seconds"]) / 3600
        return "SOAK_IN_PROGRESS", [f"{remaining:.1f} h of the 24 h window remain"]
    # A self-reported process start time is not sufficient evidence of an
    # unattended 24-hour soak. Verify coverage by independently scheduled
    # resource samples and durable alarm/supervisor logs.
    if facts.get("missing_evidence_files"):
        problems.append("missing soak evidence: " + ", ".join(facts["missing_evidence_files"]))
    if facts.get("resource_samples", 0) < 100:
        problems.append("too few independent resource samples for a 24 h soak")
    if facts.get("first_resource_offset_s") is None or facts["first_resource_offset_s"] > 5400:
        problems.append("resource monitoring began too late in the soak")
    if facts.get("last_resource_age_s") is None or facts["last_resource_age_s"] > 1800:
        problems.append("no recent resource monitoring sample")
    if facts.get("max_resource_gap_s") is None or facts["max_resource_gap_s"] > 1800:
        problems.append("a resource monitoring gap exceeded 30 minutes")
    if problems:
        return "SOAK_FAILED", problems
    return "SOAK_COMPLETE", []


def _series(path: Path) -> list[dict]:
    try:
        with path.open(encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def _stats(values: list[float]) -> dict:
    values = sorted(values)
    if not values:
        return {}
    return {"n": len(values), "min": values[0], "median": values[len(values) // 2], "max": values[-1]}


def collect(home: Path, port: int = 8778, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    status = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/v1/status", timeout=30))
    started = datetime.fromisoformat(status["started_utc"])
    reports = home / "reports"
    evidence_paths = {
        "alerts.log": reports / "alerts.log",
        "phoenix-supervisor.log": home / "logs" / "phoenix-supervisor.log",
        "resource_log.csv": reports / "resource_log.csv",
    }
    missing_evidence_files = [name for name, path in evidence_paths.items() if not path.is_file()]
    critical = []
    try:
        for line in (reports / "alerts.log").read_text(encoding="utf-8").splitlines():
            stamp, _, rest = line.partition(" ")
            if " NEW CRITICAL " in " " + rest and datetime.fromisoformat(stamp) >= started:
                critical.append(line)
    except OSError:
        pass
    restarts = 0
    try:
        for line in (home / "logs" / "phoenix-supervisor.log").read_text(encoding="utf-8").splitlines():
            # RESTART_COMPLETE is logged when the CURRENT worker became RUNNING, i.e. it ends the restart that
            # opened the window; only a new launch attempt or a frozen-worker recovery after it is a problem.
            if "WORKER_ABSENT_STARTING" in line or "FROZEN_WORKER_" in line:
                stamp = line.split(" ", 1)[0]
                if datetime.fromisoformat(stamp.replace("Z", "+00:00")) >= started:
                    restarts += 1
    except OSError:
        pass
    resources = []
    times: list[datetime] = []
    for row in _series(reports / "resource_log.csv"):
        try:
            stamp = datetime.fromisoformat(row["utc"].replace("Z", "+00:00"))
        except (KeyError, AttributeError, TypeError, ValueError):
            continue
        if started <= stamp <= now:
            resources.append(row)
            times.append(stamp)
    times.sort()
    monitoring = {
        "first_resource_offset_s": (times[0] - started).total_seconds() if times else None,
        "last_resource_age_s": (now - times[-1]).total_seconds() if times else None,
        "max_resource_gap_s": max(
            ((b - a).total_seconds() for a, b in zip(times, times[1:])), default=None
        ),
    }

    def column(name: str) -> list[float]:
        out = []
        for r in resources:
            try:
                out.append(float(r[name]))
            except (KeyError, TypeError, ValueError):
                pass
        return out

    return {
        "generated_utc": now.isoformat(timespec="seconds"), "worker_pid": status["pid"], "started_utc": status["started_utc"],
        "source_commit": status["source_commit"], "source_dirty": status["source_dirty"], "mode": status["mode"],
        "service_state": status.get("service_state"), "journal_process_alive": status.get("journal_process_alive"),
        "heartbeat_age_s": (
            (now - datetime.fromisoformat(status["heartbeat_utc"].replace("Z", "+00:00"))).total_seconds()
            if status.get("heartbeat_utc") else None
        ), "missing_evidence_files": missing_evidence_files, **monitoring,
        "uptime_seconds": (now - started).total_seconds(),
        "exchange_write_calls": status["exchange_write_calls"], "orders_submitted_to_exchange": status["orders_submitted_to_exchange"],
        "parquet_dropped_rows": status["parquet_dropped_rows"], "archive_integrity_state": status["archive_integrity_state"],
        "critical_alerts_in_window": critical, "restarts_in_window": restarts,
        "commits_seen_in_window": len({status["source_commit"]}),  # a restart would reset started_utc itself
        "reconnects": status["reconnect_count"], "sequence_gaps": status["sequence_gap_count"],
        "decisions": [status.get("strategy_decisions_persisted"), status.get("crossmarket_decisions_persisted")],
        "fills": [status.get("paper_fill_records"), status.get("crossmarket_paper_fill_records")],
        "settlements": [status.get("paper_settlement_records"), status.get("crossmarket_paper_settlement_records")],
        "crossmarket_realized_net_pnl": status.get("crossmarket_paper_realized_net_pnl_dollars"),
        "journal_bytes": status.get("sqlite_bytes"), "parquet_bytes": status.get("parquet_bytes"),
        "resource_samples": len(resources),
        "worker_rss_mb": _stats(column("worker_rss_mb")), "journal_rss_mb": _stats(column("journal_rss_mb")),
        "free_ram_mb": _stats(column("free_ram_mb")), "free_c_gb": _stats(column("free_c_gb")),
        "persisted_lag_s": _stats(column("persisted_lag_s")),
    }


def render(facts: dict, verdict: str, reasons: list[str]) -> str:
    lines = [f"# Soak report {facts['generated_utc']}", "", f"**{verdict}**" + ("".join(f"\n- {r}" for r in reasons)), "",
             "**Scope: P8 process stability only. This does not certify P0 uninterrupted WebSocket data or profitable strategies.**", "",
             f"- worker PID {facts['worker_pid']} started {facts['started_utc']} (uptime {facts['uptime_seconds']/3600:.2f} h), "
             f"commit {facts['source_commit'][:7]} dirty={facts['source_dirty']}, mode {facts['mode']}",
             f"- exchange writes {facts['exchange_write_calls']}, orders {facts['orders_submitted_to_exchange']}, "
             f"rows dropped {facts['parquet_dropped_rows']}, integrity {facts['archive_integrity_state']}",
             f"- reconnects {facts['reconnects']}, sequence gaps {facts['sequence_gaps']}, supervisor restarts in window {facts['restarts_in_window']}",
             f"- paper: decisions {facts['decisions']}, fills {facts['fills']}, settlements {facts['settlements']}, "
             f"cross-market net P&L {facts['crossmarket_realized_net_pnl']}",
             f"- storage: journal {facts['journal_bytes']} B, Parquet {facts['parquet_bytes']} B",
             f"- resource log ({facts['resource_samples']} samples since start, "
             f"max gap {facts.get('max_resource_gap_s')} s, newest age {facts.get('last_resource_age_s')} s):",
             f"- heartbeat age {facts.get('heartbeat_age_s')} s, process {facts.get('service_state')}, "
             f"journal child alive {facts.get('journal_process_alive')}, missing evidence {facts.get('missing_evidence_files')}"]
    for name in ("worker_rss_mb", "journal_rss_mb", "free_ram_mb", "free_c_gb", "persisted_lag_s"):
        lines.append(f"  - {name}: {facts[name] or 'no samples'}")
    if facts["critical_alerts_in_window"]:
        lines += ["- CRITICAL alerts in the window:"] + [f"  - {a}" for a in facts["critical_alerts_in_window"]]
    return "\n".join(lines)


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8778
    home = Path(args[0]) if args else Path(r"E:\PhoenixLab\runtime\k15m-native")
    try:
        facts = collect(home, port)
    except Exception as exc:
        # The one-shot task must leave a durable, explicit failure rather than
        # silently emitting no verdict when the status endpoint is unavailable.
        print(f"# Soak report {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n\n"
              f"**SOAK_FAILED**\n- evidence collection failed: {type(exc).__name__}")
        return 2
    verdict, reasons = judge(facts)
    print(render(facts, verdict, reasons))
    return 0 if verdict == "SOAK_COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
