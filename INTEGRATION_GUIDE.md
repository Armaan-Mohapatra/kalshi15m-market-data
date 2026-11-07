# Read-only integration guide

This project is an independent Windows research and paper runtime. Its source is at `C:\Users\dakin\Projects\kalshi-15m-native`; the supervisor is configured with `%LOCALAPPDATA%\Kalshi15mNative`. The local status endpoint is `GET http://127.0.0.1:8768/api/v1/status` and binds only to loopback. As of 2026-10-15 05:12 UTC, the active worker's SQLite byte metric does not reconcile to the configured ledger file (about 76 MB reported versus 563 MB on disk), and the status file has not advanced since startup. Treat the active storage path as unresolved until a safe restart exposes the new `runtime_home` and `paper_ledger_path` status fields; do not snapshot the configured path as if it were confirmed live data.

## Separation rules

- Treat the native REFLEX source checkout and its databases as read-only from this project. The frozen source and paper configuration are copied under `strategy/`; no other bot configuration or process is changed here.
- Do not copy exchange credentials into this project or pass paper records to a live order path. The worker has no exchange write client.
- Do not share a SQLite file between projects. A downstream integration should read a verified snapshot and write into its own schema through a separately reviewed patch.
- Keep the REFLEX source revision and config hash with every paper record. The current frozen revision is `REFLEX_VNEXT_20261012T112726630113Z`; its hash is in `strategy/reflex_vnext_frozen_config.json`.

## Available read-only artifacts

Runtime files are outside OneDrive:

- `data/market_data/**/*.parquet`: ZSTD-compressed event archive. Fields are `dedupe_key`, `received_utc`, `exchange_ts`, `channel`, `kind`, `market_ticker`, `payload_sha256`, and `raw_json`.
- `data/paper_ledger.sqlite3`: SQLite WAL database. The `paper_ledger` table holds `idempotency_key`, `created_utc`, `profile_id`, `record_type`, and `payload_json`. The same database also contains the transactional `market_events` recovery journal.
- `runtime/status.json`: local worker state; prefer the loopback status endpoint for a current snapshot.

Use `dedupe_key` to deduplicate market events and `idempotency_key` to deduplicate paper records. Preserve `payload_sha256`, timestamps, market ticker, channel, event kind, and strategy revision/hash when importing. Keep `raw_json` intact if the downstream system needs full-fidelity replay.

In the SQLite recovery journal, new `raw_json` values that compress well are stored as a versioned `K15Z1`-prefixed ZSTD BLOB; older rows and small payloads remain TEXT. Use `kalshi15m.storage.decode_journal_payload` when reading SQLite rows. Parquet always contains the original UTF-8 JSON string, and archive recovery decodes the journal BLOB before writing Parquet.

## Snapshot handoff

1. Take a read-only snapshot of the source artifacts. Do not move or delete the live files.
2. Record the snapshot time, source worker PID, status state, strategy revision/hash, SQLite `PRAGMA integrity_check`, event-key count, Parquet file list, and SHA-256 for each exported file.
3. For an archive-complete export, wait for `parquet_buffered_rows=0` and verify the Parquet unique keys against the SQLite event journal. If the buffer is nonzero, include the SQLite journal or label the Parquet snapshot as partial.
4. Import only into a separate staging schema. Make import idempotent with the keys above, compare row counts and hashes, then review a code patch before any integration with another runtime.

The status endpoint is read-only. It does not expose a control path, exchange credentials, or permission to start, stop, or place orders in another system. Kalshi authenticates WebSocket sessions even for public market data. WSS remains unavailable until independently authorized market-data credential references are configured through `KALSHI15M_MARKETDATA_API_KEY_ID` and `KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH`; generic exchange variables and other bots' credential locations are ignored.

## Read-only local feed assessment

The local REFLEX Windows API's `/api` router requires its authenticated session dependency. Its `/api/reflex5/status` route reads `summary-cumulative.json` and returns a paper-status projection; it is not a raw market-data feed. No request was made to that API and no REFLEX database or credential was accessed. A loopback inventory found candidate Python listeners on ports 3001 and 8000 whose parent executables are under the REFLEX Windows virtual environment, but no separate unauthenticated book, trade, or lifecycle feed has been verified. Kalshi's current [WebSocket Quick Start](https://docs.kalshi.com/getting_started/quick_start_websockets) requires signed handshake headers for every WebSocket session; [Orderbook Updates](https://docs.kalshi.com/websockets/orderbook-updates) specifies a snapshot followed by deltas. Without an independently authorized key, REST remains the active source and WebSocket sequence integrity remains unverified.

## Current compatibility boundary

`REFLEX_VNEXT_PAPER` remains scoped to `KXBTC15M` and retains its frozen source hash. The separately versioned `REFLEX_CROSSMARKET_PAPER_V1` profile extends the inherited frozen entry rules to confirmed 900-second binary markets after binary settlement rules and fee semantics are checked; its hash records the BTC config hash as parent. BTC markets stay assigned to the frozen profile, and multi-outcome Crypto Lead markets remain blocked. Underlying reference gates inherited from the frozen config are disabled, so the cross-market profile uses executable Kalshi books and resolves P&L only from official finalized Kalshi settlements. Original executor parity is unresolved, and simulated paper fills must not be represented as exchange fills.
