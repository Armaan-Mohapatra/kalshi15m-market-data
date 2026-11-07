# Kalshi 15m Native

Independent native Windows project for read-only discovery and paper-only market research. The repository checkout is relocatable; use the Git branch and checkout that passed isolated testing. The **default** runtime data and SQLite WAL ledger live under %LOCALAPPDATA%\Kalshi15mNative, outside OneDrive. Point KALSHI15M_HOME at an independent temporary path for tests; do not run a second collector against the live root.

The worker has no exchange write client. REST access is GET-only. Paper ledger records remain local and are never sent to Kalshi.

## Current status

- Native Windows project is isolated from the REFLEX checkout.
- The original standalone PAPER-only REFLEX vNext config remains hash-frozen to `KXBTC15M`. A separately versioned cross-market PAPER profile inherits the exact entry and sizing rules and evaluates verified non-BTC binary 900-second contracts; its parent BTC config hash is pinned in its own authority manifest. Original executor parity is still unresolved.
- Public REST discovery scans the full series/event inventory every six hours. Startup reuses a valid local discovery snapshot younger than six hours and reconciles scheduled current, next, and recent unsettled markets before collecting. Known upcoming contracts are prewarmed up to 300 seconds early, with a scheduled reconciliation before and just after their opening boundary; recent-open market scans address discovery gaps.
- REST request pacing is 2.0 seconds by default, shared across public GET routes with priority given to order books and rollover reconciliation. It retries HTTP 429 and bounded transient 5xx/timeout errors. The 0.5-second figure in the original snapshot was stale documentation, not a safe rate-limit target. Final API failures remain visible in status.
- The broad REST orderbook batch is scheduled every 15 seconds; public trades for active markets are scheduled every 120 seconds and fetched with bounded cursor pagination and an incomplete-result state. These intervals are scheduling targets, not guarantees under rate limiting or long discovery passes. Status separates observed event age from poll age, marks incomplete REST book batches `DEGRADED`, and ages both `READY` and `DEGRADED` polls to `STALE` when they stop updating. A quiet but healthy trade market may legitimately return zero trades.
- Kalshi requires signed authentication for WebSocket sessions, including public ticker, trade, orderbook, and lifecycle channels. This worker reads only the explicitly namespaced `KALSHI15M_MARKETDATA_API_KEY_ID` and `KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH` variables; generic exchange credential variables do not enable WSS. Provide an independently authorized key reference for this read-only collector. It creates no key and never stores or prints credentials. Without those namespaced references, WSS stays blocked and REST remains active.
- Current market inventory is refreshed from series metadata, event/market APIs, and official rules. Classification does not use ticker text.
- Underlying reference sources are metadata only. This project does not purchase or query Pyth or another paid feed; missing sources stay missing.
- A timestamped operating and storage sample is in [reports/operational-sample-2026-10-14.md](reports/operational-sample-2026-10-14.md). The live endpoint remains the authority for current status.

## Reliability v2 development status

The `collector/reliability-v2` branch is a **development snapshot**, not an authorized production release. It introduces additive journal conflict versions, nondestructive quarantine diagnostics, opening coverage observations, priority-aware shared REST scheduling, and archive verification manifests. The active Windows environment is separate and has not been validated here. The source snapshot omits the private frozen strategy authority; isolated CI explicitly skips the tests that require it. Run the complete suite in a separate Windows checkout **only after** confirming the intended local authority and environment. See [the reliability development contract](docs/RELIABILITY_V2_PLAN.md).

The collector remains GET-only and PAPER_ONLY; no WSS credentials are installed or activated. REST book snapshots are sampled observations and **do not prove continuous order-book replay**. Market strike/contract definition metadata is not an independently observed underlying price-index feed. Source-capture completeness and Parquet-after-SQLite archive completeness are distinct properties.

## Setup

Use Python 3.14 and create an isolated virtual environment:

    py -3.14 -m venv .venv
    .venv\Scripts\python.exe -m pip install -r requirements.txt

Set the optional runtime data root before starting:

    $env:KALSHI15M_HOME = "$env:LOCALAPPDATA\Kalshi15mNative"
    $env:KALSHI15M_STATUS_PORT = "8768"

Start and inspect the independent REST collector:

    .\scripts\windows\start.ps1
    .\scripts\windows\status.ps1
    .\scripts\windows\verify.ps1

For persistence across shell or Codex interruptions, register the per-user Windows Task Scheduler supervisor:

    .\scripts\windows\register-supervisor.ps1
    .\scripts\windows\supervise.ps1 -CheckOnly

The supervisor uses the same project start script and runtime directory. It only restarts this worker when its PID and listener are absent, verifies project identity, PAPER_ONLY mode, and zero exchange writes, and fails closed on an unrelated listener or unsafe status. `stop.ps1` writes a manual-stop marker so the supervisor does not relaunch a deliberately stopped worker; a later explicit `start.ps1` clears that marker. Remove the watchdog with `unregister-supervisor.ps1`; that leaves the worker running.

The local read-only status endpoint binds to 127.0.0.1:8768/api/v1/status. It has no write routes. Stop through the managed script:

    .\scripts\windows\stop.ps1

## Data and retention

- SQLite WAL is the append-only idempotent journal and separate paper ledger. Compressible journal payloads use a versioned `K15Z1` ZSTD BLOB in `raw_json`; legacy and small payloads stay as TEXT. `kalshi15m.storage.decode_journal_payload` returns the original JSON. The Parquet archive remains full-fidelity UTF-8 JSON.
- ZSTD Parquet files are partitioned by UTC date and event kind; unique raw rows buffer to 2,000 rows or 60 seconds to reduce small files. SQLite commits stay immediate. On an unclean restart, missing Parquet keys are recovered from SQLite while the raw capture budget and free-space reserve permit it. DuckDB is used for local analytical queries.
- Raw capture has a 1.5 GiB project budget. C: free space below 20 GiB disables optional Parquet capture while preserving the critical journal, with disk and dropped-event state surfaced in status.
- Logs rotate at 8 MiB with five backups.
- Samples without a full-fidelity pre-entry/miss window are research data and cannot establish exact replay.
- The frozen BTC evaluator and separately versioned cross-market evaluator poll eligible markets during the configured prewarm window, persist decisions and full REST orderbooks, then simulate one taker-limit IOC at observed arrival depth after a measured REST round-trip latency proxy. The cross-market profile requires an official binary type, confirmed 900-second duration, a YES resolution rule, and a supported quadratic fee schedule; BTC remains reserved for the original profile and multi-outcome markets remain blocked. It uses no underlying reference feed because all inherited BRTI and optional reference gates are disabled. The latency proxy is not measured exchange order-submit latency. Supported quadratic fees use Decimal and upward $0.0001 rounding from the [Kalshi fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf) effective July 7, 2026, with event fee overrides. Binary fills are held to official REST settlement without a settlement fee. Unsupported fee types or unresolved results keep net P&L null. The only order path is local paper simulation.

## Storage benchmark

After the collector has captured rows, run `scripts/windows/benchmark-storage.ps1`. It copies only rows from this project's `market_events` journal into fresh SQLite WAL, DuckDB, ZSTD Parquet, and PostgreSQL benchmark stores. PostgreSQL uses a temporary cluster bound to `127.0.0.1`, then stops after the run. The JSON report records the sample time range, channel and event-kind counts, write time, file or relation size, and a market-ticker read query. Small or short capture windows are a sample benchmark, not a sustained capacity estimate.

The 2026-10-14 run benchmarked the newest 50,000 of 76,264 journal rows across a 61.2-minute capture span. ZSTD Parquet used 5.36 MB; SQLite WAL, DuckDB, and PostgreSQL used 45.85 MB, 46.41 MB, and 44.02 MB respectively. The full result, including timings and query samples, is [reports/storage-benchmark-2026-10-14-50k.json](reports/storage-benchmark-2026-10-14-50k.json). PostgreSQL query timing is server execution while the other engines use Python client wall time, so those query figures are not directly comparable.

## Strategy authority

The source snapshot is copied from the native Windows active configuration at revision `REFLEX_VNEXT_20261012T112726630113Z`; the standalone wrapper changes execution to PAPER and live authorization to false, then verifies that strategy rules and market-data settings still match the source snapshot. The standalone evaluator implements the active configured entry conditions and hold-to-settlement behavior. The original executor and visual details remain unavailable, so exact executor parity is UNKNOWN. The source checkout is read-only for this project.

## Remaining evaluation

Authenticated WebSocket collection remains blocked until this project receives a separately authorized market-data key reference. Exchange-side latency and live WebSocket sequence continuity are unknown; the current REST samples and zero disconnected gap counter do not prove either. As of 2026-10-14 16:46 PDT, 24 natural simulated fills were matched by decision ID to 24 official finalized settlements (18 wins, 6 losses, -$0.9735 net after fees); this small forward sample does not establish profitability. A five-minute rollover-inclusive storage sample measured 66.73 MB/hour when normalized. A separate one-hour storage/resource monitor was launched at 23:39:12Z with five-minute samples; its completed output is pending at `reports/storage-growth-full-hour-2026-10-14-2338Z.json` (the filename labels the preceding baseline sample; the JSON stores the exact interval). See [the operational evidence log](reports/operational-sample-2026-10-14.md) for current worker state, exact market coverage, archive recovery, drawdown, and sample limitations. The active worker must keep its paper ledger integrity check at `ok`, `exchange_write_calls=0`, and `orders_submitted_to_exchange=0`.

## Integration boundary

See [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md) for the read-only handoff format and isolation rules. No live order, credential, or primary REFLEX database is part of this interface.

## Verification

Run the local tests with the project environment and source path:

    $env:PYTHONPATH = (Join-Path (Get-Location) 'src')
    .\.venv\Scripts\python.exe -m unittest discover -s tests -v
