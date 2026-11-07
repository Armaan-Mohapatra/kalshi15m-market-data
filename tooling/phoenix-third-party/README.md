# Phoenix isolated third-party tooling

**Development-only.** This branch and its bootstrap do not install any software on the Windows laptop until Codex explicitly runs it there. GitHub Actions only verifies syntax and inventory. A source clone is **not** a dependency install or an activated agent integration.

## Drive plan

- `E:\PhoenixTooling\src`: 23 third-party repository checkouts, fetched in bounded batches.
- `E:\PhoenixTooling\envs`, `models`, `indexes`, `cache`, `manifests`: optional isolated environments and bulk files. These are separate from `E:\TradingNextData` and `E:\KalshiData`.
- C: existing Windows, Rust toolchain, Codex, and active Phoenix project/source checkouts stay where they are. Avoid filling the SSD with Docker images, models, node_modules, or new large caches.
- Before installing development toolchains, check current C: and E: free space and drive connectivity. Do not change collectors, scheduled tasks, Windows services, global PATH, drivers or active environments.
- E: is an external HDD: source clones and cold indexes can live there, but hot graph queries may be slow and tools become unavailable if E: disconnects. Benchmark one lightweight active index on C: only if its measured size is small and sufficient free headroom exists; never auto-migrate without verification.

## One-time read-only inventory

```powershell
& .\tooling\phoenix-third-party\bootstrap.ps1
```

## Sequential source clone batches (no execution of third-party code)

```powershell
& .\tooling\phoenix-third-party\bootstrap.ps1 -CloneAll -StartIndex 0 -MaxItems 4
& .\tooling\phoenix-third-party\bootstrap.ps1 -CloneAll -StartIndex 4 -MaxItems 4
```

Continue 8, 12, 16, and 20 only after verifying resource headroom and prior checkout receipt. Every batch records exact Git commit SHA. If a target already exists, the script leaves it unchanged. It will not auto-pull, execute installation scripts, download LFS models, clone submodules, change global agent configuration or start services.

## Installation and activation policy

1. **High priority engineering:** inspect and configure codebase-memory-mcp, Serena, ast-grep, Repomix, Hyperfine, and reuse the existing codex-usage-tracker. Enable one code graph at a time, with indexes stored outside protected repos. Run a source-path retrieval trial on the Rust and Python staging checkouts.
2. **Optional agent policies:** Ponytail, official `affaan-m/ECC`, Trail-code and selected Agency-agents. Review every hook and config diff, activate only inside an isolated Codex home/workspace, and test for prompt/token overhead. Never run broad installers that overwrite the user's persistent Codex or Claude configuration.
3. **Research-only:** hftbacktest, TradingAgents, vibe-trading and laya. Set up isolated virtual environments only if dependencies and free resources permit; no exchange credentials, live APIs, orders or autostart.
4. **Cold/large/personal:** WeKnora, AFFiNE, DeepTutor, colibri, do-computer, BrowserSkill, jev-ultrafast and Career-Ops. Cloning is authorized but do not start Docker, large model downloads, browser extensions, privileged browser access, daemon services, GPU inference, or data ingestion without a specific need and safety review. Some are not Phoenix development tools.
5. **Unsupported hardware modification:** `Zagitta/gpu-unlock` is Linux-only and modifies GPU behavior; source checkout for reference only. **Never run the installer, apply drivers, patch firmware, or switch Windows to Linux** as part of this task.

Unverified screenshot identity: the earlier 'Cloudflare Computer' and 'Colibri' projects were not identifiable by exact URL from retained screenshots. `SawyerHood/do-computer` and `JustVugg/colibri` are plausible repository matches, kept as review candidates rather than approved automatic integrations. Earlier `pxrpxr/ecc` was a mirror; the canonical official source is `affaan-m/ECC` per the maintainer's README.

## Start using the tools on Phoenix immediately after safe activation

1. Using the existing `codex/rust-windows-integration` and Python staging code, build an exact source call graph for raw WS receive -> ticker membership -> event emit -> journal ack -> Parquet finalize; report gaps and instrumentation required. Preserve the actual live Python checkout read-only.
2. Use Serena/ast-grep/codebase-memory on only known code repositories and indexes to locate the authoritative `TNEVENT` format-v2 writer/decoder for `events.bin`. No whole-E recursive walk; do not guess binary framing or treat strings in raw data as schema.
3. Reconcile GitHub issue #6 using current safe evidence and bounded finalized Parquet/metadata; no active SQLite/WAL reads, service restart, rate limit changes, WSS subscriptions, or trading orders.
4. If a verified decoder is found, write isolated tests and a bounded converter against independently hashed `.ready` samples to advance issue #7. Keep provenance, connection identity and sequence continuity per source segment; do not fabricate full 180-second coverage.
5. After data correctness gates and resource-headroom check, run benchmark comparisons with Hyperfine and explore an isolated Kalshi execution-cost model inspired by hftbacktest. No performance claims from synthetic-only smoke tests.

## Explicit progress states

`SOURCE_CLONED`, `DEPENDENCIES_INSTALLED`, `AGENT_CONNECTED`, `SMOKE_TESTED`, `BENEFICIAL`, `DEFERRED_HEAVY`, `BLOCKED_SECURITY`, `UNSUPPORTED`, `NOT_APPLICABLE`. Record exact Git SHA, version, paths, startup effects, tests, measured RAM/disk and tokens separately for each repo.

Keep the verified existing Codex full-access preference scoped by these Phoenix read-only production safeguards. No automated startup or long-running background jobs.
