# Codex handoff: Rust collector (native Windows, isolated, offline first)

**Repository:** `Armaan-Mohapatra/kalshi15m-market-data`
**Branch:** `collector/rust-v1`
**Review:** Draft PR #2 at https://github.com/Armaan-Mohapatra/kalshi15m-market-data/pull/2
**Scope:** Only development and read-only validation. Do not deploy or activate any production collector.

## 1. Establish bounded read-only source identity

Without recursively scanning C: or E:, report:
- Actual active Python collector checkout path, Git SHA, git diff cleanliness, Python executable and process identity.
- Configured active collector runtime paths, health status and supervisor identity.
- Existence (not contents) of local-only strategy authority files.
- Manifest/export availability under `E:\TradingNextData\exports`, `kalshi`, `perps` only when their exact known paths are required.

Do not open active SQLite/WAL for writes, stop or restart any service, change Task Scheduler or credentials, or copy secrets.

## 2. Clone in a separate staging directory

Run in Windows PowerShell as the ordinary user. If a staged directory already exists, inspect it; DO NOT remove or overwrite it:

```powershell
$ErrorActionPreference = 'Stop'
$stage = Join-Path $env:USERPROFILE 'Projects\kalshi15m-rust-stage'
if (Test-Path -LiteralPath $stage) { throw "Inspect existing staging checkout first: $stage" }
git clone --branch collector/rust-v1 https://github.com/Armaan-Mohapatra/kalshi15m-market-data.git $stage
Set-Location (Join-Path $stage 'rust\collector')
git rev-parse HEAD
git status --short
rustc --version
cargo --version
cargo test --all-targets
```

If `rustc` is unavailable, report the missing toolchain. Do not alter the active collector's Python environment. Install the Rust stable MSVC toolchain and Windows SDK **only with appropriate user approval**.

## 3. Run a fully offline smoke replay

```powershell
$rustHome = Join-Path $env:LOCALAPPDATA 'Kalshi15mRustStage'
$fixture = Join-Path $stage 'rust\collector\fixtures\smoke.ndjson'
cargo run -- --home "$rustHome" replay --input "$fixture"
cargo run -- --home "$rustHome" audit
```

Validate all synthetic events are represented in SQLite, that the same-key metadata revision is recorded as a separate version, that the subscription sequence gap is durably recorded, and that Parquet files and manifest SHA-256 values match. The `tools/check_python_parity.py` script is the cross-language contract test. Use an isolated venv with `pyarrow==24.0.0` only if needed; do not install into the production Python interpreter.

## 4. Replay finalized exports, not active event segments

Use verified finalized, credential-sanitized exports and bounded batches. The current CLI accepts NDJSON event envelopes/raw WebSocket fixture frames only; format conversion from finalized Parquet must occur **into a separate staging output** preserving original source hashes and provenance. Never assume that direct active database access is safe.

Record counts, event keys, duplicate/version classifications, WSS sid/seq gaps, memory peaks, elapsed/durable latency, and stage disk growth. Compare to the Python implementation on the identical fixture with independently derived expected results. A Rust/ Python shared error is not proof of correctness.

## 5. Review and operational boundary

- The Rust crate is a working **offline capture/replay prototype**, not yet a replacement for the continuous Python process, PAPER engine or authenticated WSS feed.
- Public REST `capture-once` exists but **must not be executed in ordinary staging**: it can compete for the live Kalshi rate budget and requires independent authorization.
- No `start.ps1`, `stop.ps1`, `supervise.ps1`, process restarts, new collector services, SQLite migration, or production Git pull.
- No writes to `E:\TradingNextData`, `C:\KalshiRuntime` or `%LOCALAPPDATA%\Kalshi15mNative`.
- Record test results with exact Git SHA, effective config, fixture hashes, limitations, failed steps, and resource use. Do not call skipped tests passed.
- Request an independent code review and separate production-cutover plan. There is **no automatic approval to deploy or trade**.

The relevant docs are `rust/collector/README.md` and `docs/RELIABILITY_V2_PLAN.md`.
