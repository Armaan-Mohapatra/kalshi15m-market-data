# Rust Kalshi 15-minute capture engine — staging implementation

This crate is a **runnable, isolated Rust ingestion prototype**, not yet a production replacement. It lives in the same repository as the hardened Python collector, and never changes the running Python collector merely by being merged or built.

## Capabilities

- Deterministic NDJSON event and recorded WebSocket replay, with fixed-size streaming input.
- Exact fixed-point YES/NO book modeling and synchronized/desynchronized states; subscription-level sequence gap evidence, session identity, and invalid-snapshot rejection.
- Compatible base `market_events` and `market_event_versions` SQLite structures using WAL, synchronous FULL, and one owned writer connection.
- Full raw JSON market metadata including strike fields, plus an exchange-data version hash.
- Durable journal rows before acknowledgment; same-key different-payload variants, persistent event fingerprints, fsynced emergency spool and restart replay.
- Arrow/Parquet archive parts, bounded reconciliation from SQLite, independent manifest hashes and explicit clean/unverified shutdown.
- Public GET-only one-shot REST collector with 2-second minimum pacing, bounded 429/5xx/timeouts, discovery of open and unopened 900-second markets, REST book sampling, paginated public trades and incomplete-source telemetry.
- Authenticated Kalshi WebSocket transport with Ed25519/RSA-PSS handshake signing, explicit subscriptions, connection-qualified raw frames, sequence-gap invalidation, bounded backpressure, reconnect/resubscribe, fresh-snapshot recovery, malformed-frame evidence and graceful cancellation.
- Structured health separates process/REST/WebSocket state, queue and disk pressure, SQLite journal acknowledgments, and Parquet manifest-committed acknowledgments. WebSocket telemetry distinguishes raw frames from derived health records, tracks order-book deltas as observed/applied/rejected/journaled/spooled/archived, and scopes sequence-gap and archive counts by market and connection-qualified subscription session/SID. These runtime counters are process-scoped while the manifest remains authoritative across restarts. Graceful shutdown drains and verifies a bounded archive tail before publishing clean status.
- CI build, Rust unit tests and smoke replay on Windows and Linux.

**Limitations:** WebSocket signing and transport are validated offline only; no authenticated exchange connection has been attempted. The CLI requires `capture-websocket --allow-websocket true` and explicitly supplied credentials; never use it during routine staging validation while Phoenix collection is active. The continuous Rust runtime is not yet an independently accepted replacement for the Python strategy/paper runtime. REST snapshots cannot reconstruct lost intermediate orderbook deltas. The Rust journal uses staging-specific reconciliation tables and **must never target the production Python SQLite/WAL**. The Rust Parquet schema is explicitly checked by a separate Python compatibility test before any migration decision.

## Build and run — offline by default

Rust stable and its Windows MSVC toolchain must be installed in a **separate Windows staging checkout**. Run commands from the `rust/collector` directory:

```powershell
cargo test --all-targets
$stage = Join-Path $env:LOCALAPPDATA "Kalshi15mRustStage"
cargo run -- --home "$stage" replay --input "fixtures/smoke.ndjson"
cargo run -- --home "$stage" audit
```

No public request is made by these commands. `--home` is compulsory and paths matching Phoenix/production runtime roots are rejected. For captured research data, use *only* explicitly reviewed finalized exported NDJSON copies as input. Original E:/C: sources are READ-ONLY.

### Public REST one-shot staging mode (NOT automatically run)

The program has a single-pass GET-only path for explicit tickers or series. Running it is a network action that shares Kalshi rate limits with your active collector; **do not run it while Phoenix collection is active without a separate network scheduling authorization**.

```powershell
# Example syntax only, DO NOT run in ordinary validation:
cargo run -- --home "$stage" capture-once --allow-network --ticker KXBTC15M-EXAMPLE
```

This mode never places orders and does not install a service, configure Task Scheduler, activate WSS or change the active Windows collector.

### Continuous REST scheduler (network-gated)

`run-continuous` can take optional `--series` overrides. If no series are supplied, the runtime reads the public `/series` catalog and rotates through at most four metadata-confirmed 15-minute candidates per discovery cycle. It remains network-disabled unless `--allow-network true` is supplied, and must not be run against live Kalshi during ordinary Phoenix staging validation.

### Authenticated WebSocket staging mode (NOT automatically run)

The source includes authenticated WebSocket streaming for explicit market tickers. It writes only to the isolated `--home` journal/archive. Running it connects to Kalshi and requires a separate explicit operational decision; ordinary tests and staging validation must use local signing/protocol fixtures. Never point it at a production key path.

```powershell
# Syntax only. Do not run during ordinary staging validation.
cargo run -- --home "$stage" capture-websocket --allow-websocket true --key-id STAGING_KEY_ID --private-key "C:\staging\key.pem" --ticker KXBTC15M-EXAMPLE
```

## Data contract and recovery

The journal is authoritative. Parquet parts are staged to `.partial` and renamed after a complete writer close/fsync. The manifest is then committed in the journal along with event fingerprints. A crash between SQLite commit and Parquet finalization leaves unarchived records; a crash after the Parquet rename but before the manifest commit leaves a finalized orphan part. On the next staging startup, hashes are checked, finalized orphan parts are registered, and missing journal entries are replayed into new Parquet parts. Neither case rewrites source events. Corrupt part hashes are a fail-closed error, not silent success. The health output does not count archive rows until both the finalized part and manifest/index commit succeed; it reports archive failure separately from already-durable SQLite events.

Both the archive and the journal live under an isolated staging root. Python keeps responsibility for research/backtesting and any separately authorized PAPER strategy runtime.

### Replay a finalized Phoenix WebSocket Parquet segment

`tools/convert_recorded_ws_parquet.py` is a bounded, read-only adapter for finalized Phoenix Parquet exports. It verifies an adjacent source sidecar when present, preserves numeric-subscription raw protocol messages in source order, injects source-time/session replay metadata, excludes non-WebSocket rows, and writes a SHA-256 provenance manifest next to a new staging NDJSON file. It refuses to overwrite converter outputs or write under protected Phoenix/runtime roots. Never point `--output` into the source export tree.

```powershell
python tools/convert_recorded_ws_parquet.py `
  --input E:\TradingNextData\kalshi\parquet\segment-00000186.parquet `
  --output C:\Users\dakin\Projects\Kalshi15mRustStageResearch\segment186\segment186-ws.ndjson
```

Then replay the emitted raw frames with the offline `replay` command and an isolated `--home`. The adapter preserves all selected subscription frames so sequence checks are not computed from ticker-filtered fragments. Its output is a lossy projection of the source table: REST-only rows and source columns not needed by the WebSocket replay remain documented in the converter manifest and are not represented as WebSocket frames.

## Verification gates

The crate must compile and pass isolated Windows/Linux CI, smoke-replay synthetic events to SQLite/Parquet, preserve all expected event IDs and conflict versions, classify WSS gaps accurately and reconstruct interrupted archive parts. **Production is not authorized** by these checks. We then need finalized market replay parity, Python PyArrow/SQLite compatibility, native Windows CPU/RAM/latency benchmarks and independent review before any deployment decision.
