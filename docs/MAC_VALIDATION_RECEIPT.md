# Mac validation receipt (sanitized)

- Repository: kalshi15m-market-data · Branch: `mac/phase4-collector-integrity`
- Tested code SHA: `8ffc40bfa63ec184112836255deb7e38f75c76f0` (the branch tip is a docs-only descendant; verify with `git diff --name-only 8ffc40bfa63ec184112836255deb7e38f75c76f0..<tip>` touching only `docs/`)
- Verdict for the Mac gate: MAC_CERTIFIED_FOR_WINDOWS_TESTING (NOT Windows certified)
- Full matrix, environment, defects and open items: `docs/MAC_CERTIFICATION.md` and `docs/mac-validation.json` in `phoenix-rust` (same branch family)
- Results for this repo: 189 passed, 1 skipped; 12 FrozenStrategyTests BLOCKED (need Windows-only strategy/ files); book fail-closed, signing and recovery tests included; pip-audit reports cryptography 47.0.0 advisories (open, pin unchanged pending approval).
- Evidence classes: synthetic fixtures only; fees hypothetical; settlement venue-reported; no profitability claim.
- Windows steps: `docs/WINDOWS_HANDOFF.md` in `phoenix-rust`.
