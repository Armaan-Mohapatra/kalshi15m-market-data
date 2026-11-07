# Windows Codex staging handoff — reliability v2

**Release status:** Development branch awaiting independent review; not authorized for production deployment.

The GitHub repository is a sanitized source snapshot, not the active native Windows checkout. The active runtime, PAPER ledger, SQLite WAL and event archive are not bundled with this repository. Production roots are READ-ONLY for staging.

## Phase A — Establish baseline without modifying the collector

Inspect these bounded locations only:
- Existing project source: `C:\Users\dakin\Projects\kalshi-15m-native`
- Existing native worker data root: `%LOCALAPPDATA%\Kalshi15mNative` (read-only)
- Existing Phoenix finalized exports/manifests: `E:\TradingNextData\exports` (read-only; no recursive drive scans)

Read the existing project's `git rev-parse HEAD`, `git status --short`, interpreter version, requirements, Python process command line, supervisor identity, actual data directory, status endpoint, and local-only strategy authority references. Avoid copying or printing credentials or raw ledger data. Do not run any production script or connect to public Kalshi API. Do not assert parity until source drift is reviewed.

## Phase B — Clone a separate GitHub checkout

In a **normal Windows user** PowerShell session:

```powershell
$ErrorActionPreference = 'Stop'
$repo = 'https://github.com/Armaan-Mohapatra/kalshi15m-market-data.git'
$stage = Join-Path $env:USERPROFILE 'Projects\kalshi15m-reliability-stage'
if (Test-Path -LiteralPath $stage) { throw "Stage exists; inspect rather than overwrite: $stage" }
git clone --branch collector/reliability-v2 $repo $stage
Set-Location $stage
git rev-parse HEAD
git status --short

py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-lock.txt
$env:PYTHONPATH = Join-Path $stage 'src'
$env:KALSHI15M_HOME = Join-Path $env:LOCALAPPDATA 'Kalshi15mReliabilityStage'
$env:KALSHI15M_STATUS_PORT = '9876'
Remove-Item Env:KALSHI15M_MARKETDATA_API_KEY_ID -ErrorAction SilentlyContinue
Remove-Item Env:KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH -ErrorAction SilentlyContinue
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

This does not start a collector. The new branch's `start.ps1` also blocks development-branch network startup by default.

## Phase C — Validate the release candidate

1. Record the exact staging commit SHA, all test results and the number of tests skipped because the sanitized GitHub snapshot lacks local frozen-strategy authority.
2. Test with deterministic synthetic orderbook, trade and discovery fixtures; simulate a new market first observed after opening, 429/503/timeouts, duplicate WebSocket session IDs, sequence gaps, malformed/empty books, batches containing a non-serializable event, writer failure, crash between SQLite commit and Parquet finalization, corrupted parts and missing markers.
3. Test the minute-0 to minute-3 coverage summaries and verify that REST samples are not represented as continuous WSS order books.
4. If sanctioned finalized copies of relevant event segments exist, use bounded replay from copied files only. Do not directly open or modify active SQLite/WAL files; use verified read-only snapshots or exports.
5. Ensure the final reports contain all unresolved gaps, latency measurements, configuration hashes, row counts and storage limits. A test skip is not a pass.
6. Validate that the actual live checkout differs from the GitHub snapshot only in enumerated, understood ways. Stop staging at the approval boundary.

## Explicit prohibitions

- Do not execute `start.ps1`, `stop.ps1`, `register-supervisor.ps1`, or `supervise.ps1` against the active collector.
- Do not pull or check out this branch into the existing production checkout.
- Do not start a parallel live REST/WSS collector: it can consume the same API budget.
- Do not change Task Scheduler tasks, sleep plans, networking, credentials, or E:/C: production storage.
- Do not overwrite, migrate, rotate, repair, move or delete active SQLite, WAL, raw event segments or PAPER ledgers.
- Do not activate authenticated WebSocket until its separate authorization and protocol verification gate.

## Review and separately approved production release

A pull request is a code-review proposal. It does not deploy anything. Once code and offline tests are accepted, an operator may authorize a separate maintenance procedure covering backup/rollback, source drift, local authority dependencies, schema compatibility, supervisor behavior, and post-cutover monitoring. This document is **not** that authorization.
