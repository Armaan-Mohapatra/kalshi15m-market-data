# Python collector: read-only capture incident triage

This GitHub patch is not deployed to the Windows laptop. It is based on the sanitized collector/reliability-v2 development snapshot, not a verified copy of the active Python checkout.

## Verified source-level weaknesses

1. The previous supervisor treated a process with service_state RUNNING as HEALTHY after PID and PAPER_ONLY safeguards, without independent recent durable-event evidence. Process liveness is not capture health.
2. supervise.ps1 -CheckOnly previously wrote a log entry and created runtime directories before exiting, so it was not strictly observational.
3. The Python worker opens SQLite, attempts spool replay, and restores PAPER state before starting the HTTP status server. A stale status.json showing NOT_OPEN can reflect an older run, different runtime root, or blocked startup.

## New diagnostic

scripts/windows/diagnose-capture-readonly.ps1 accepts the explicitly selected runtime root and status port, observes the local read-only HTTP status endpoint twice, and records only process identity, PID metadata, file metadata, and narrowly bounded recent archive-part filenames. It neither opens active SQLite/WAL nor writes any file. No API key or raw command line is printed.

Run from a separate isolated source checkout only after confirming the actual worker account and runtime home:

```powershell
& .\scripts\windows\diagnose-capture-readonly.ps1 -RuntimeHome 'C:\Users\<account>\AppData\Local\Kalshi15mNative' -StatusPort 8768 -IntervalSeconds 10
```

Do not assume the current shell LOCALAPPDATA equals that of the scheduled-task owner. Reconcile status_runtime_home, checked_runtime_home, resolved_localappdata, status_pid, pid_record_pid, listener_pids, and the worker executable. The script does not read any production database contents. The two observations do not prove continuous market capture or prove that no files exist outside the sampled paths.

## Limits and approval

The supervisor adds capture_evidence reporting but deliberately does not trigger automatic restarts based on stale heartbeats or missing writes. There is no production cutover, restart, data migration, or service configuration approval. A GitHub patch cannot explain the current active worker without comparing the actual on-laptop source SHA, process, owner, logs and active runtime root.

Potential explanations that need laptop-only read-only investigation include account-specific runtime path redirection, startup blocking before status-server readiness, writer storage failure, stale status file replacement, or stuck network collection. Treat each as a hypothesis until confirmed.
