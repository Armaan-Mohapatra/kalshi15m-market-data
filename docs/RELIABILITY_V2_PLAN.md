# Collector Reliability v2 — Development and Release Contract

**Source baseline:** `460e5065b629b40f42b1b5e58334861ff3ee583e`  
**Development branch:** `collector/reliability-v2`  
**Scope:** native Windows, read-only Kalshi 15-minute collector; source snapshot, not verified live runtime.

## Operational invariants

1. Preserve the original Windows checkout, active services, supervisor, task scheduler, SQLite/WAL, Parquet segments, paper ledger, secrets, and `E:\TradingNextData`. Development touches GitHub's new branch only.
2. No trading orders or exchange-write routes.
3. Do not start a second live collector. Windows staging uses its own checkout, environment, ports, and synthetic/finalized-copy data, without concurrent public REST collection.
4. Tests are not deployment approval. The running laptop source and environment must be compared read-only before any deployment decision.
5. Preserve old event records and source provenance; distinguish ingestion loss from archive loss, and state recovery incompleteness explicitly.
6. Do not claim any exchange reference/index value has been captured unless its authoritative source and timestamp are verified.
7. Keep discovery, paper strategy, and collection lifecycles isolated to avoid starving ingestion.

## Phase 0 — Baseline and isolated tests

- Inventory and pin the real Windows checkout SHA, dirty status, interpreter, dependency versions, runtime path, supervisor process identity, and required local-only strategy authority files.
- Establish deterministic fixtures: fake REST endpoint, WebSocket message stream, virtual clock, fake rate limiting, isolated SQLite and Parquet, injected filesystem exceptions, rollover schedules.
- Run existing `python -m unittest discover -s tests -v` using separate `KALSHI15M_HOME`; record baseline, failures, omissions, and resource use.
- Gate: baseline report, exact commit identity, independent staging root, zero production writes.

## Release 1 — Integrity, observability and silent-loss prevention

- Writer: separate serialization/validation faults from transient SQLite locks and disk failures. Bounded retry policy for nonrecoverable errors; isolate only persistently invalid individual events; durable quarantine must succeed before acknowledging and removing an event from the queue. Avoid discarding whole batches, and never claim a transient storage outage is a poison row.
- Event keys: retain source-specific event identity, add run and WSS session identity where required; detect same-key differing payload hashes before `INSERT OR IGNORE` suppresses them. The normal schema does **not** provision `market_event_versions`; design and test an additive conflict store explicitly. Keep existing records unchanged.
- Metadata: capture versioned raw market API fields including contractual strike/reference definitions; retain exact response source, request/receive timestamps, and official-versus-derived semantics. Use hashes of canonical source payloads to capture revisions, not transient observation metadata.
- Trades: retrieve bounded cursor pages and checkpoint watermarks; preserve stable exchange IDs and provenance; do not claim completeness after hitting page limits, timeouts, or rate limits.
- Telemetry: durable REST poll results, requested versus returned ticker sets, first-seen/synchronized-book/quote/trade distinctions, WSS session lifecycle, durable disk-write reconciliation outcomes, and receive/request RTT timestamps. HTTP Date is a coarse diagnostic, **not** an exact clock sync measurement.
- Gate: synthetic poison batch, retryable storage outage, event collision, metadata revision, >100 trade pages, bounded pagination failure, empty-trade legitimate result, partial order-book response, deterministic clocks.

## Release 2 — Opening coverage, scheduling, honest health

- Decouple long full discovery from scheduled depth and rollover processing without data races; publish immutable discovery snapshots or ownership-consistent state updates and retain only one compatible in-flight inventory job.
- Scan recently open as well as unopened candidates; handle markets never pre-discovered. Measure opening-to-discovery and per-market first observations.
- Retain safe shared rate limiting. **Do not adopt the stale README 0.5-second interval** over the actual 2-second configuration without verified exchange policy, measured demand, and rate-limit tests. Priority queue must be fair and must not starve archive or trade recovery.
- Distinguish REST sampled book from sequence-synchronized WSS book, valid empty book from invalid snapshots, market quietness from feed failures, and process liveness from market coverage.
- Keep supervisor identity and PAPER_ONLY safety gates; report data degradation independently, without unapproved restart or reconfiguration.
- Gate: long discovery does not block opening, undiscovered-to-open path, partial responses, 429 and timeout paths, starvation bounds, 0-60/60-120/120-180 coverage and unknown states.

## Release 3 — Persistence, recovery, capacity

- Verify SQLite journal versus finalized Parquet files independently of cached `status.json`. Durable clean-run marker plus archive commit manifest and row-range/hash checkpoint; missing/invalid markers require reconciliation rather than optimistic skip.
- Deal with corrupt Parquet and orphaned partial files via non-destructive reporting and independent recovery on staging copies; don't move/delete original production parts.
- Keep historical lifetime loss counts immutable. Track recovered rows and currently outstanding archive gaps separately.
- Implement incremental bounded checkpoints and storage-pressure alerts. Keep active SQLite journal rotation **out of scope** until independent design and regression justification.
- Gate: interrupted SQLite commit vs Parquet finalize, missing/corrupt status marker, malformed archive, orphaned partial, idempotent restart, capacity exhaustion, limits on memory and disk work.

## WebSocket readiness (separate activation gate)

- Fix WSS session-qualified journal identity while implementing release 1.
- Before ever enabling WSS: validate Kalshi subscription-level seq rules, per-market book synchronization, snapshot format, remote clock semantics, reconnect backoff, silent-link detection, and explicit unrecoverable order-book delta gaps with recorded fixtures.
- Read-only WSS credentials are not included in this snapshot; no automatic activation or credential configuration.

## Review and release gates

- Every finding gets a linked test or a documented, evidence-backed limitation.
- Each code patch includes a targeted diff, unit tests, and a backwards-compatibility assessment.
- Run automated tests and bounded fault-injection in a Windows isolated checkout before proposing a production cutover.
- Maintain one release commit/tag and changelog per independently releasable wave. This branch may contain staged patches; it must not be fast-forwarded into the production checkout merely because GitHub tests pass.
- A separate, explicitly approved operating procedure must govern any stop/start, migration, supervisor update, promotion, or rollback. Never use an unreviewed `git pull` in the active checkout.

## Status

- Baseline source verified; Linux/Windows tests not yet executed in this ChatGPT session.
- The branch's first patch makes incomplete REST orderbook batches DEGRADED and ages DEGRADED polls to STALE. Two focused regression cases were added.
- All other issues remain open until independently implemented and tested.
