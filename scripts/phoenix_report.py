"""Read-only dashboard + compact report for the isolated paper collector.

Writes dashboard.html and report.json (no secrets, no raw market data, no strategy
parameters) into <home>/reports. Never writes to the ledger or touches the worker.
usage: python scripts/phoenix_report.py [--home E:\\PhoenixLab\\runtime\\k15m-native] [--port 8778]
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Task Scheduler runs `python -I script.py` (isolated: no cwd, no PYTHONPATH), so the
# collector's own package must be added explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

STATUS_KEYS = (
    "service_state", "mode", "heartbeat_utc", "storage_latest_market_event_received_utc", "rest_state",
    "websocket_state", "orderbook_sequence_state", "sequence_gap_count", "reconnect_count", "queue_depth",
    "events_dropped", "archive_integrity_state", "strategy_decisions_persisted", "paper_fill_records",
    "paper_settlement_records", "crossmarket_decisions_persisted", "crossmarket_paper_fill_records",
    "crossmarket_paper_settlement_records", "crossmarket_paper_realized_net_pnl_dollars",
    "exchange_write_calls", "orders_submitted_to_exchange", "sqlite_bytes", "parquet_bytes",
    "disk_free_bytes", "active_market_rows",
)


def age(utc: str | None) -> float | None:
    if not utc:
        return None
    return round((datetime.now(timezone.utc) - datetime.fromisoformat(utc.replace("Z", "+00:00"))).total_seconds(), 1)


def get_status(home: Path, port: int) -> tuple[dict, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/v1/status", timeout=60) as r:
            return json.load(r), "status_api"
    except Exception:  # worker slow or down: say so, fall back to the supervisor heartbeat
        try:
            return json.loads((home / "runtime" / "phoenix-heartbeat.json").read_text("utf-8")), "supervisor_heartbeat_only"
        except Exception:
            return {}, "UNAVAILABLE"


def growth(home: Path) -> dict:
    try:
        rows = list(csv.DictReader((home / "logs" / "phoenix-growth.csv").open(encoding="utf-8")))[-7:]
        a, b = rows[0], rows[-1]
        hours = (datetime.fromisoformat(b["utc"]) - datetime.fromisoformat(a["utc"])).total_seconds() / 3600
        if hours <= 0:
            return {}
        return {"sqlite_gb_per_hour": round((int(b["sqlite_bytes"]) - int(a["sqlite_bytes"])) / 1e9 / hours, 3),
                "parquet_gb_per_hour": round((int(b["parquet_bytes"]) - int(a["parquet_bytes"])) / 1e9 / hours, 3)}
    except Exception:
        return {}


def ledger(home: Path) -> dict:
    from kalshi15m.config import Settings  # ledger may live on another volume (pointer file)

    db = sqlite3.connect("file:" + Settings(home=home).ledger_path.as_posix() + "?mode=ro", uri=True, timeout=30)
    try:
        counts = db.execute("select profile_id, record_type, count(*) from paper_ledger group by 1,2").fetchall()
        # invariant: every ineligible decision must say why (no silent no-trades)
        reasons = db.execute(
            "select profile_id, coalesce(json_extract(payload_json,'$.rejection_reason'),'<MISSING>'), count(*) "
            "from paper_ledger where record_type='DECISION' and json_extract(payload_json,'$.eligible') = 0 "
            "group by 1,2").fetchall()
        eligible = db.execute(
            "select profile_id, count(*) from paper_ledger where record_type='DECISION' "
            "and json_extract(payload_json,'$.eligible') = 1 group by 1").fetchall()
        recent = db.execute(
            "select created_utc, profile_id, record_type, payload_json from paper_ledger "
            "where record_type in ('PAPER_FILL','PAPER_SETTLEMENT_FINAL') order by created_utc desc limit 12").fetchall()
    finally:
        db.close()
    trades = []
    for created, profile, kind, raw in recent:
        p = json.loads(raw)
        trades.append({"utc": created, "profile": profile.split(":")[0], "type": kind, "ticker": p.get("market_ticker"),
                       "side": p.get("side"), "net_pnl": p.get("net_pnl_dollars")})
    decisions: dict[str, dict] = {}
    for profile, n in eligible:
        decisions.setdefault(profile.split(":")[0], {"eligible": 0, "ineligible": 0, "missing_reason": 0, "reasons": {}})["eligible"] = n
    for profile, reason, n in reasons:
        d = decisions.setdefault(profile.split(":")[0], {"eligible": 0, "ineligible": 0, "missing_reason": 0, "reasons": {}})
        d["ineligible"] += n
        d["reasons"][str(reason)] = n
        if reason == "<MISSING>":
            d["missing_reason"] += n
    return {"counts": {f"{p.split(':')[0]}/{t}": n for p, t, n in counts}, "recent": trades, "decisions": decisions}


JOB_STALE_SECONDS = 9000  # 2.5 h for jobs scheduled hourly


def job_ages(home: Path) -> dict:
    """Seconds since each hourly job last proved it ran (file mtimes; None if never)."""
    files = {"settlement_reconcile": home / "reports" / "settlement_reconciliation.json",
             "research_cycle": home / "reports" / "research_last_run.txt"}
    ages: dict = {}
    for name, path in files.items():
        try:
            ages[name] = round(time.time() - path.stat().st_mtime, 1)
        except OSError:
            ages[name] = None
    return ages


def alerts(report: dict) -> list[str]:
    """Explicit fault alarms. Unknown or missing evidence is an alarm, never silence."""
    out: list[str] = []
    s = report["status"]
    if report["status_source"] != "status_api":
        out.append(f"CRITICAL worker status unavailable ({report['status_source']})")
    if s.get("exchange_write_calls") not in (0, None) or s.get("orders_submitted_to_exchange") not in (0, None):
        out.append("CRITICAL exchange write or order observed; paper-only invariant broken")
    if s and s.get("mode") != "PAPER_ONLY":
        out.append(f"CRITICAL mode is {s.get('mode')}, expected PAPER_ONLY")
    age = report.get("persisted_event_age_s")
    if s and (age is None or age > 120):
        out.append(f"WARNING newest persisted market event is {age}s old (limit 120s)")
    if s and s.get("queue_depth", 0) >= 10800:
        out.append(f"WARNING writer queue at {s.get('queue_depth')} (cap 12000): persistence is behind")
    if s and s.get("ledger_volume_state") not in (None, "OK"):
        out.append(f"CRITICAL journal volume {s.get('ledger_volume_state')}")
    if s and s.get("archive_integrity_state", "VERIFIED_COMPLETE").startswith(("INCOMPLETE", "UNKNOWN")):
        out.append(f"CRITICAL archive integrity {s.get('archive_integrity_state')}")
    for profile, d in report.get("ledger", {}).get("decisions", {}).items():
        if d["missing_reason"]:
            out.append(f"CRITICAL {d['missing_reason']} no-trade decisions without a reason in {profile}")
    for job, hours in report.get("job_ages_s", {}).items():
        if hours is None or hours > JOB_STALE_SECONDS:
            out.append(f"WARNING scheduled job '{job}' has not run in {'ever' if hours is None else f'{hours / 3600:.1f} h'} (hourly)")
    recon = report.get("settlement_reconciliation", {})
    if recon.get("state") == "NOT_RUN":
        out.append("WARNING settlement reconciliation has not run")
    elif recon.get("counts", {}).get("MATCH", 0) != recon.get("settlements", 0):
        out.append(f"CRITICAL settlement reconciliation mismatches: {recon.get('counts')}")
    return out


def build(home: Path, port: int) -> dict:
    status, source = get_status(home, port)
    recon = {}
    try:
        recon = json.loads((home / "reports" / "settlement_reconciliation.json").read_text("utf-8"))
        recon.pop("unmatched", None) if not recon.get("unmatched") else None
    except Exception:
        recon = {"state": "NOT_RUN"}
    try:
        led = ledger(home)
    except Exception as exc:
        led = {"error": type(exc).__name__}
    s = {k: status.get(k) for k in STATUS_KEYS if k in status}
    report = {"generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "status_source": source,
              "persisted_event_age_s": age(status.get("storage_latest_market_event_received_utc")),
              "heartbeat_age_s": age(status.get("heartbeat_utc")), "status": s, "growth": growth(home),
              "settlement_reconciliation": recon, "ledger": led, "job_ages_s": job_ages(home)}
    report["alerts"] = alerts(report)
    return report


def render(report: dict) -> str:
    def table(rows, head):
        body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
        return f"<table><tr>{''.join(f'<th>{h}</th>' for h in head)}</tr>{body}</table>"
    st = report["status"]
    safe = st.get("exchange_write_calls") == 0 and st.get("orders_submitted_to_exchange") == 0
    banner = (
        "<div style='border:3px solid #b00;padding:8px;margin:8px 0'><b>ALERTS</b><ul>"
        + "".join(f"<li>{html.escape(a)}</li>" for a in report.get("alerts", [])) + "</ul></div>"
        if report.get("alerts") else "<div style='border:3px solid #080;padding:6px;margin:8px 0'>no active alerts</div>"
    )
    parts = [
        banner,
        f"<h1>Phoenix paper collector</h1><p>generated {report['generated_utc']} via {report['status_source']} "
        f"| persisted event age {report['persisted_event_age_s']}s | heartbeat age {report['heartbeat_age_s']}s "
        f"| exchange orders: <b>{'0 (paper only)' if safe else 'CHECK!'}</b></p>",
        "<h2>Status</h2>" + table(st.items(), ("field", "value")),
        "<h2>Growth</h2>" + table(report["growth"].items(), ("metric", "value")),
        "<h2>Settlement reconciliation</h2><pre>" + html.escape(json.dumps(report["settlement_reconciliation"], indent=1)) + "</pre>",
        "<h2>Ledger records</h2>" + table(report["ledger"].get("counts", {}).items(), ("profile/type", "count")),
        "<h2>Decisions: eligible vs explained no-trades</h2>" + table(
            [(p, d["eligible"], d["ineligible"], d["missing_reason"],
              "; ".join(f"{r}={n}" for r, n in sorted(d["reasons"].items(), key=lambda kv: -kv[1])[:6]))
             for p, d in report["ledger"].get("decisions", {}).items()],
            ("profile", "eligible", "no-trade", "no-trade WITHOUT reason (must be 0)", "top reasons")),
        "<h2>Recent fills and settlements</h2>" + table(
            [(t["utc"], t["profile"], t["type"], t["ticker"], t["side"], t["net_pnl"]) for t in report["ledger"].get("recent", [])],
            ("utc", "profile", "type", "ticker", "side", "net P&L")),
    ]
    return ("<!doctype html><meta charset=utf-8><meta http-equiv=refresh content=300><title>Phoenix paper</title>"
            "<style>body{font:14px system-ui;margin:16px}table{border-collapse:collapse}td,th{border:1px solid #888;padding:3px 8px}</style>"
            + "".join(parts))


RESOURCE_COLUMNS = (
    "utc", "worker_pid", "worker_rss_mb", "journal_pid", "journal_rss_mb", "free_ram_mb",
    "free_c_gb", "free_e_gb", "queue_depth", "persisted_lag_s",
)


def process_rss_mb(pid: int | None) -> float | None:
    """Working set of a process in MB (Windows psapi through ctypes; psutil is not installed)."""
    if not pid or sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]

    kernel32, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(0x1000 | 0x0010, False, int(pid))  # QUERY_LIMITED_INFORMATION | VM_READ
    if not handle:
        return None
    try:
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return None
        return round(counters.WorkingSetSize / 1048576, 1)
    finally:
        kernel32.CloseHandle(handle)


def free_ram_mb() -> float | None:
    if sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    class MemoryStatus(ctypes.Structure):
        _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

    status = MemoryStatus()
    status.dwLength = ctypes.sizeof(status)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return None
    return round(status.ullAvailPhys / 1048576, 0)


def resource_row(report: dict, home: Path) -> dict:
    """One resource sample for the soak report: memory, disk and persistence lag over time."""
    import shutil

    s = report.get("status") or {}

    def free_gb(path: str) -> float | None:
        try:
            return round(shutil.disk_usage(path).free / 1e9, 1)
        except OSError:
            return None

    return {
        "utc": report.get("generated_utc"), "worker_pid": s.get("pid"),
        "worker_rss_mb": process_rss_mb(s.get("pid")), "journal_pid": s.get("journal_process_pid"),
        "journal_rss_mb": process_rss_mb(s.get("journal_process_pid")), "free_ram_mb": free_ram_mb(),
        "free_c_gb": free_gb("C:\\"), "free_e_gb": free_gb(str(home.anchor or home)),
        "queue_depth": s.get("queue_depth"), "persisted_lag_s": report.get("persisted_event_age_s"),
    }


def append_resource_log(out: Path, row: dict) -> None:
    """Append-only CSV (header written once); best effort, never breaks the report."""
    try:
        path = out / "resource_log.csv"
        new = not path.exists()
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESOURCE_COLUMNS)
            if new:
                writer.writeheader()
            writer.writerow({k: row.get(k) for k in RESOURCE_COLUMNS})
    except OSError:
        pass


TOAST_SCRIPT = (
    "[void][Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime];"
    "$t=[Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent('ToastText02');"
    "$n=$t.GetElementsByTagName('text');"
    "$n.Item(0).AppendChild($t.CreateTextNode('Phoenix paper alert'))|Out-Null;"
    "$n.Item(1).AppendChild($t.CreateTextNode($env:PHOENIX_TOAST_TEXT))|Out-Null;"
    r"$id='{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe';"
    "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($id).Show("
    "[Windows.UI.Notifications.ToastNotification,Windows.UI.Notifications,ContentType=WindowsRuntime]::new($t))"
)


def notify(new_alerts: list[str], run=subprocess.run) -> bool:
    """Out-of-band signal: one Windows toast for the NEW CRITICAL alerts of this run.

    Best effort and never raises (a failed toast must not break the report). Only
    CRITICAL alerts notify, so a flapping WARNING (queue near cap) cannot spam. The text
    travels in an environment variable, not in the script. PHOENIX_NO_TOAST=1 disables it.
    """
    critical = [a for a in new_alerts if a.startswith("CRITICAL")]
    if not critical or os.environ.get("PHOENIX_NO_TOAST") == "1" or sys.platform != "win32":
        return False
    try:
        done = run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", TOAST_SCRIPT],
            env={**os.environ, "PHOENIX_TOAST_TEXT": "; ".join(critical)[:220]},
            timeout=20, capture_output=True,
        )
        return done.returncode == 0
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default=r"E:\PhoenixLab\runtime\k15m-native")
    ap.add_argument("--port", type=int, default=8778)
    a = ap.parse_args()
    home = Path(a.home)
    report = build(home, a.port)
    out = home / "reports"
    out.mkdir(parents=True, exist_ok=True)
    try:
        before = set(json.loads((out / "report.json").read_text("utf-8")).get("alerts", []))
    except Exception:
        before = set()
    now = set(report["alerts"])
    with (out / "alerts.log").open("a", encoding="utf-8") as log:  # history: when each alert appeared / cleared
        for a_ in sorted(now - before):
            log.write(f"{report['generated_utc']} NEW {a_}\n")
        for a_ in sorted(before - now):
            log.write(f"{report['generated_utc']} CLEARED {a_}\n")
    notify(sorted(now - before))
    append_resource_log(out, resource_row(report, home))
    for name, text in (("report.json", json.dumps(report, indent=1)), ("dashboard.html", render(report))):
        tmp = out / (name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(out / name)
    print(f"wrote {out / 'dashboard.html'} ({report['status_source']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
