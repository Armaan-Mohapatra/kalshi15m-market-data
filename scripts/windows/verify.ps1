$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..')).TrimEnd('\')
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$checks = [System.Collections.Generic.List[object]]::new()
$python = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Missing isolated project Python environment.' }

function Add-Check([string]$Name, [string]$Result, [string]$Detail) {
    $checks.Add([pscustomobject]@{ name = $Name; result = $Result; detail = $Detail })
}

& $python -m compileall -q (Join-Path $root 'src')
Add-Check 'python_compile' 'PASS' 'all source modules compile'
$env:PYTHONPATH = "$root;$(Join-Path $root 'src')"
$testOutput = & $python -m unittest discover -s (Join-Path $root 'tests') -v 2>&1
$testExit = $LASTEXITCODE
$testSummary = ($testOutput | Select-String -Pattern '^Ran \d+ tests? in ').Line | Select-Object -Last 1
if (-not $testSummary) { $testSummary = 'unittest summary unavailable' }
Add-Check 'unit_tests' $(if ($testExit -eq 0) { 'PASS' } else { 'FAIL' }) "$testSummary; exit=$testExit"

$status = $null
try {
    $status = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 4
    if ($status.service -eq 'kalshi-15m-native' -and $status.service_state -eq 'RUNNING') {
        Add-Check 'status_endpoint' 'PASS' "read-only HTTP 200; port $statusPort; pid $($status.pid)"
    } else {
        Add-Check 'status_endpoint' 'FAIL' "unexpected service/state: $($status.service)/$($status.service_state)"
    }
} catch {
    Add-Check 'status_endpoint' 'UNKNOWN' 'worker status unavailable'
}

if ($status) {
    $workerProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$status.pid)" -ErrorAction SilentlyContinue
    $processMatches = $workerProcess -and (
        $workerProcess.CommandLine -match [regex]::Escape($root) -or
        $workerProcess.ExecutablePath -like "$root\\.venv\\Scripts\\python.exe"
    )
    Add-Check 'worker_identity' $(if ($processMatches) { 'PASS' } else { 'FAIL' }) $(if ($processMatches) { "PID $($status.pid) command belongs to this standalone project" } else { "PID $($status.pid) does not resolve to this project" })

    $writes = [long]$status.exchange_write_calls + [long]$status.orders_submitted_to_exchange
    $paperOnly = $status.mode -eq 'PAPER_ONLY' -and $writes -eq 0
    Add-Check 'exchange_write_guard' $(if ($paperOnly) { 'PASS' } else { 'FAIL' }) "mode=$($status.mode); exchange writes/orders=$writes"

    try {
        $heartbeat = [DateTimeOffset]::Parse([string]$status.heartbeat_utc)
        $heartbeatAge = ([DateTimeOffset]::UtcNow - $heartbeat.ToUniversalTime()).TotalSeconds
        $heartbeatFresh = $heartbeatAge -ge -2 -and $heartbeatAge -le 30
        Add-Check 'worker_heartbeat' $(if ($heartbeatFresh) { 'PASS' } else { 'FAIL' }) ("age_seconds={0:N1}" -f $heartbeatAge)
    } catch {
        Add-Check 'worker_heartbeat' 'UNKNOWN' 'heartbeat timestamp missing or invalid'
    }

    $orderAge = $status.feed_ages_seconds.rest_orderbook
    $tradeAge = $status.feed_ages_seconds.rest_public_trades
    $orderLimit = $status.rest_poll_stale_thresholds_seconds.rest_orderbook
    $tradeLimit = $status.rest_poll_stale_thresholds_seconds.rest_public_trades
    if ($null -ne $orderAge -and $null -ne $tradeAge -and $null -ne $orderLimit -and $null -ne $tradeLimit) {
        $restFresh = [double]$orderAge -le [double]$orderLimit -and [double]$tradeAge -le [double]$tradeLimit
        Add-Check 'rest_feed_freshness' $(if ($restFresh) { 'PASS' } else { 'FAIL' }) ("orderbook={0:N1}/{1:N0}s; public_trades={2:N1}/{3:N0}s" -f [double]$orderAge,[double]$orderLimit,[double]$tradeAge,[double]$tradeLimit)
    } else {
        Add-Check 'rest_feed_freshness' 'UNKNOWN' 'freshness telemetry missing'
    }

    $wsConnected = [string]$status.websocket_state -match 'CONNECTED|READY|SUBSCRIBED'
    if ($wsConnected -and $null -ne $status.sequence_gap_count -and [int]$status.sequence_gap_count -eq 0) {
        Add-Check 'websocket_sequence_integrity' 'UNKNOWN' 'transport reports connected, but snapshot-to-delta sequence proof is not exposed'
    } else {
        Add-Check 'websocket_sequence_integrity' 'UNKNOWN' "state=$($status.websocket_state); REST snapshots do not establish WebSocket sequence continuity"
    }
    Add-Check 'websocket_access' $(if ($wsConnected) { 'PASS' } else { 'FAIL' }) "read-only feed state=$($status.websocket_state); no credentials are reported or read by this verifier"

    $allEvaluators = [int]$status.strategy_all_profiles_evaluators_active
    $crossEvaluators = [int]$status.crossmarket_evaluators_active
    $decisions = [long]$status.crossmarket_decisions_persisted
    $evaluatorsActive = $allEvaluators -gt 0 -and $crossEvaluators -gt 0 -and $decisions -gt 0
    Add-Check 'paper_evaluators' $(if ($evaluatorsActive) { 'PASS' } else { 'FAIL' }) "status-reported evaluators=$allEvaluators; crossmarket=$crossEvaluators; decisions=$decisions"

    $rolloverCount = [int]$status.actual_rollovers_observed
    $rolloverSamples = [int]$status.rollover_detection_sample_count
    $rolloverLatency = [double]$status.last_rollover_detection_latency_seconds
    $rolloverObserved = $rolloverCount -gt 0 -and $rolloverSamples -gt 0 -and $rolloverLatency -ge 0 -and $rolloverLatency -lt 5
    Add-Check 'rollover_forward_evidence' $(if ($rolloverObserved) { 'PASS' } else { 'UNKNOWN' }) "status-reported transitions=$rolloverCount; measured_samples=$rolloverSamples; latest_detection_seconds=$rolloverLatency; preflight_lead_seconds=$($status.rollover_preflight_lead_seconds)"

    $requiredPathFields = @('runtime_home', 'data_dir', 'paper_ledger_path', 'parquet_dir')
    $pathsPresent = $true
    foreach ($field in $requiredPathFields) {
        if ([string]::IsNullOrWhiteSpace([string]$status.$field)) { $pathsPresent = $false }
    }
    $pathIdentity = $false
    if ($pathsPresent) {
        try {
            $homePath = [IO.Path]::GetFullPath([string]$status.runtime_home).TrimEnd('\')
            $dataPath = [IO.Path]::GetFullPath([string]$status.data_dir).TrimEnd('\')
            $ledgerPath = [IO.Path]::GetFullPath([string]$status.paper_ledger_path)
            $parquetPath = [IO.Path]::GetFullPath([string]$status.parquet_dir).TrimEnd('\')
            $pathIdentity = [string]::Equals($dataPath, [IO.Path]::Combine($homePath, 'data').TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase) -and
                [string]::Equals($ledgerPath, [IO.Path]::Combine($dataPath, 'paper_ledger.sqlite3'), [StringComparison]::OrdinalIgnoreCase) -and
                [string]::Equals($parquetPath, [IO.Path]::Combine($dataPath, 'market_data').TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase)
            if ($pathIdentity) {
                $pidPath = Join-Path $homePath 'runtime\worker.pid.json'
                if (Test-Path -LiteralPath $pidPath) {
                    $pidRecord = Get-Content -LiteralPath $pidPath -Raw | ConvertFrom-Json
                    if ([int]$pidRecord.pid -ne [int]$status.pid -or
                        -not [string]::Equals([IO.Path]::GetFullPath([string]$pidRecord.project).TrimEnd('\'), $root, [StringComparison]::OrdinalIgnoreCase)) {
                        $pathIdentity = $false
                    }
                }
            }
            $pathResult = if ($pathIdentity) { 'PASS' } else { 'FAIL' }
            Add-Check 'storage_path_identity' $pathResult "runtime_home=$homePath; ledger=$ledgerPath; parquet=$parquetPath"
        } catch {
            Add-Check 'storage_path_identity' 'FAIL' 'storage path telemetry is malformed'
        }
    } else {
        Add-Check 'storage_path_identity' 'UNKNOWN' 'running worker does not expose its runtime, ledger, and Parquet paths'
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$status.storage_open_database_path) -and
        -not [string]::IsNullOrWhiteSpace([string]$status.paper_ledger_path)) {
        try {
            $openedDatabasePath = [IO.Path]::GetFullPath([string]$status.storage_open_database_path)
            $configuredDatabasePath = [IO.Path]::GetFullPath([string]$status.paper_ledger_path)
            $openedPathMatches = [string]::Equals(
                $openedDatabasePath,
                $configuredDatabasePath,
                [StringComparison]::OrdinalIgnoreCase
            ) -and [bool]$status.storage_database_path_matches_config
            $fileIdentityStable = [string]$status.storage_path_file_identity_state -eq 'UNCHANGED_SINCE_OPEN'
            $connectionIdentityPass = $openedPathMatches -and $fileIdentityStable
            Add-Check 'sqlite_live_connection_identity' $(if ($connectionIdentityPass) { 'PASS' } else { 'FAIL' }) "opened_path=$openedDatabasePath; configured_path=$configuredDatabasePath; path_matches=$openedPathMatches; file_identity_state=$($status.storage_path_file_identity_state)"
        } catch {
            Add-Check 'sqlite_live_connection_identity' 'UNKNOWN' 'SQLite connection path telemetry is malformed'
        }
    } else {
        Add-Check 'sqlite_live_connection_identity' 'UNKNOWN' 'running worker does not expose SQLite PRAGMA database_list and path-file identity telemetry'
    }

    # The older running worker omits storage paths from its status API. A
    # matching supervisor manifest identifies the configured runtime home, but
    # does not by itself prove which open database handle the process writes.
    $auditHome = $null
    $manifestPath = $null
    $homeCandidates = [System.Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($env:KALSHI15M_HOME)) {
        $homeCandidates.Add([IO.Path]::GetFullPath($env:KALSHI15M_HOME).TrimEnd('\'))
    }
    if (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        $defaultHome = [IO.Path]::Combine($env:LOCALAPPDATA, 'Kalshi15mNative')
        if (-not $homeCandidates.Contains($defaultHome)) { $homeCandidates.Add($defaultHome) }
    }
    foreach ($candidateHome in $homeCandidates) {
        $candidateManifest = Join-Path $candidateHome 'runtime\worker.pid.json'
        if (-not (Test-Path -LiteralPath $candidateManifest)) { continue }
        try {
            $candidateRecord = Get-Content -LiteralPath $candidateManifest -Raw | ConvertFrom-Json -DateKind String
            $projectMatches = [string]::Equals(
                [IO.Path]::GetFullPath([string]$candidateRecord.project).TrimEnd('\'),
                $root,
                [StringComparison]::OrdinalIgnoreCase
            )
            $portMatches = [int]$candidateRecord.port -eq [int]$status.port
            $startMatches = $false
            try {
                $statusStart = [DateTimeOffset]::Parse([string]$status.started_utc)
                $manifestStart = [DateTimeOffset]::Parse([string]$candidateRecord.started_utc)
                $startMatches = [Math]::Abs(($manifestStart - $statusStart).TotalSeconds) -le 10
            } catch { }
            if ([int]$candidateRecord.pid -eq [int]$status.pid -and $projectMatches -and $portMatches -and $startMatches) {
                $auditHome = [IO.Path]::GetFullPath($candidateHome).TrimEnd('\')
                $manifestPath = $candidateManifest
                break
            }
        } catch { }
    }
    if ($auditHome) {
        Add-Check 'supervisor_manifest_identity' 'PASS' "PID, project, port, and start time match; manifest=$manifestPath; home=$auditHome"
    } else {
        Add-Check 'supervisor_manifest_identity' 'UNKNOWN' 'no supervisor manifest matched worker PID, project, port, and start time'
    }

    if ($pathIdentity) {
        try {
            $sqliteBytes = [long]0
            foreach ($path in @([string]$status.paper_ledger_path, ([string]$status.paper_ledger_path + '-wal'), ([string]$status.paper_ledger_path + '-shm'))) {
                if (Test-Path -LiteralPath $path) { $sqliteBytes += (Get-Item -LiteralPath $path).Length }
            }
            $parquetBytes = [long](Get-ChildItem -LiteralPath $status.parquet_dir -Filter '*.parquet' -File -Recurse -ErrorAction Stop | Measure-Object -Property Length -Sum).Sum
            $sqliteMetric = [long]$status.sqlite_bytes
            $parquetMetric = [long]$status.parquet_bytes
            $sqliteTolerance = [Math]::Max(1MB, $sqliteMetric * 0.01)
            $parquetTolerance = [Math]::Max(1MB, $parquetMetric * 0.01)
            $sizesMatch = [Math]::Abs($sqliteBytes - $sqliteMetric) -le $sqliteTolerance -and [Math]::Abs($parquetBytes - $parquetMetric) -le $parquetTolerance
            Add-Check 'storage_byte_reconciliation' $(if ($sizesMatch) { 'PASS' } else { 'FAIL' }) "SQLite actual=$sqliteBytes metric=$sqliteMetric; Parquet actual=$parquetBytes metric=$parquetMetric; tolerance=1% or 1MiB"
        } catch {
            Add-Check 'storage_byte_reconciliation' 'UNKNOWN' 'could not scan the verified storage paths'
        }
    } else {
        Add-Check 'storage_byte_reconciliation' 'UNKNOWN' 'worker storage paths are not verified'
    }

    if ($auditHome) {
        try {
            $auditDataDir = Join-Path $auditHome 'data'
            $auditLedgerPath = Join-Path $auditDataDir 'paper_ledger.sqlite3'
            $auditParquetDir = Join-Path $auditDataDir 'market_data'
            if (-not (Test-Path -LiteralPath $auditLedgerPath) -or -not (Test-Path -LiteralPath $auditParquetDir)) {
                Add-Check 'manifest_home_storage_metrics' 'UNKNOWN' 'manifest-matched data paths are incomplete'
            } else {
                $physicalSqliteBytes = [long]0
                foreach ($path in @($auditLedgerPath, ($auditLedgerPath + '-wal'), ($auditLedgerPath + '-shm'))) {
                    if (Test-Path -LiteralPath $path) { $physicalSqliteBytes += (Get-Item -LiteralPath $path).Length }
                }
                $parquetFiles = @(Get-ChildItem -LiteralPath $auditParquetDir -Filter '*.parquet' -File -Recurse -ErrorAction Stop)
                $physicalParquetBytes = [long](($parquetFiles | Measure-Object -Property Length -Sum).Sum)
                $sqliteMetricBytes = [long]$status.sqlite_bytes
                $parquetMetricBytes = [long]$status.parquet_bytes
                $sqliteToleranceBytes = [Math]::Max(1MB, $sqliteMetricBytes * 0.01)
                $parquetToleranceBytes = [Math]::Max(1MB, $parquetMetricBytes * 0.01)
                $manifestSizesMatch = [Math]::Abs($physicalSqliteBytes - $sqliteMetricBytes) -le $sqliteToleranceBytes -and
                    [Math]::Abs($physicalParquetBytes - $parquetMetricBytes) -le $parquetToleranceBytes
                Add-Check 'manifest_home_storage_metrics' $(if ($manifestSizesMatch) { 'PASS' } else { 'FAIL' }) "configured-home SQLite actual=$physicalSqliteBytes metric=$sqliteMetricBytes; Parquet actual=$physicalParquetBytes metric=$parquetMetricBytes; this does not prove active file-handle identity"

                $partialFiles = @(Get-ChildItem -LiteralPath $auditParquetDir -Filter '*.partial' -File -Recurse -ErrorAction Stop)
                $statusStartForFiles = [DateTimeOffset]::Parse([string]$status.started_utc).ToUniversalTime()
                $newestParquet = $parquetFiles | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
                $parquetAdvanced = $newestParquet -and ([DateTimeOffset]$newestParquet.LastWriteTimeUtc -ge $statusStartForFiles)
                Add-Check 'manifest_home_parquet_activity' $(if ($parquetAdvanced -and $partialFiles.Count -eq 0) { 'PASS' } else { 'UNKNOWN' }) "newest_after_worker_start=$([bool]$parquetAdvanced); partial_files=$($partialFiles.Count); parquet_files=$($parquetFiles.Count)"

                $sqliteAuditScript = @'
import json, os, sqlite3, sys, urllib.parse
path = os.path.abspath(sys.argv[1]).replace("\\", "/")
wal_path = sys.argv[1] + "-wal"
wal_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
if wal_before:
    print(json.dumps({"state": "UNKNOWN_WAL_PRESENT", "wal_bytes": wal_before}))
    raise SystemExit(0)
uri = "file:" + urllib.parse.quote(path) + "?mode=ro&immutable=1"
connection = sqlite3.connect(uri, uri=True, timeout=3)
try:
    count, latest = connection.execute("SELECT count(*), max(created_utc) FROM paper_ledger").fetchone()
    wal_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    state = "UNKNOWN_WAL_PRESENT" if wal_after else "OK"
    print(json.dumps({"state": state, "wal_bytes": wal_after, "paper_ledger_rows": count, "latest_created_utc": latest}))
finally:
    connection.close()
'@
                $sqliteAuditOutput = $sqliteAuditScript | & $python - $auditLedgerPath 2>&1
                $sqliteAuditExit = $LASTEXITCODE
                if ($sqliteAuditExit -eq 0) {
                    $ledgerAudit = ($sqliteAuditOutput | Select-Object -Last 1) | ConvertFrom-Json
                    if ($ledgerAudit.state -ne 'OK') {
                        Add-Check 'manifest_home_paper_ledger_freshness' 'UNKNOWN' "read-only immutable snapshot skipped or raced with WAL; state=$($ledgerAudit.state); WAL bytes=$($ledgerAudit.wal_bytes)"
                    } else {
                        $ledgerStart = [DateTimeOffset]::Parse([string]$status.started_utc).ToUniversalTime()
                        $ledgerLatest = [DateTimeOffset]::Parse([string]$ledgerAudit.latest_created_utc).ToUniversalTime()
                        $ledgerFresh = $ledgerLatest -ge $ledgerStart
                        Add-Check 'manifest_home_paper_ledger_freshness' $(if ($ledgerFresh) { 'PASS' } else { 'FAIL' }) "paper_rows=$($ledgerAudit.paper_ledger_rows); latest_record_utc=$($ledgerLatest.ToString('o')); worker_started_utc=$($ledgerStart.ToString('o')); read_only_immutable_snapshot=true"
                        if ($null -ne $status.storage_paper_ledger_record_count -and $null -ne $status.storage_latest_paper_record_utc) {
                            $runtimeLedgerCountMatches = [long]$ledgerAudit.paper_ledger_rows -eq [long]$status.storage_paper_ledger_record_count
                            $runtimeLedgerTimestampMatches = [string]::Equals(
                                $ledgerLatest.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'),
                                [DateTimeOffset]::Parse([string]$status.storage_latest_paper_record_utc).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'),
                                [StringComparison]::Ordinal
                            )
                            $runtimeLedgerMatches = $runtimeLedgerCountMatches -and $runtimeLedgerTimestampMatches
                            Add-Check 'live_paper_ledger_runtime_match' $(if ($runtimeLedgerMatches) { 'PASS' } else { 'FAIL' }) "disk_rows=$($ledgerAudit.paper_ledger_rows); status_rows=$($status.storage_paper_ledger_record_count); disk_latest_utc=$($ledgerLatest.ToString('o')); status_latest_utc=$($status.storage_latest_paper_record_utc)"
                        } else {
                            Add-Check 'live_paper_ledger_runtime_match' 'UNKNOWN' 'worker does not expose its durable paper-row count and latest committed timestamp'
                        }
                    }
                } else {
                    Add-Check 'manifest_home_paper_ledger_freshness' 'UNKNOWN' 'read-only paper-ledger query failed'
                }

                $latestJournalAuditScript = @'
import datetime, glob, json, os, sqlite3, sys, urllib.parse
import pyarrow.parquet as pq
parquet_dir, ledger_path, worker_start_text = sys.argv[1:4]
worker_start = datetime.datetime.fromisoformat(worker_start_text.replace("Z", "+00:00")).timestamp()
wal_path = ledger_path + "-wal"
wal_bytes = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
files = [path for path in glob.glob(os.path.join(parquet_dir, "**", "*.parquet"), recursive=True) if os.path.getmtime(path) >= worker_start]
files = sorted(files, key=os.path.getmtime, reverse=True)[:3]
result = {"file_count": len(files), "parquet_rows": 0, "distinct_keys": 0, "journal_matches": None, "wal_bytes": wal_bytes}
try:
    keys = []
    for path in files:
        keys.extend(pq.read_table(path, columns=["dedupe_key"])["dedupe_key"].to_pylist())
    unique = sorted(set(keys))
    result["parquet_rows"] = len(keys)
    result["distinct_keys"] = len(unique)
    if wal_bytes or not unique:
        result["state"] = "UNKNOWN_WAL_PRESENT" if wal_bytes else "UNKNOWN_NO_RECENT_KEYS"
    else:
        uri = "file:" + urllib.parse.quote(os.path.abspath(ledger_path).replace("\\", "/")) + "?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=3)
        try:
            matches = 0
            for offset in range(0, len(unique), 400):
                batch = unique[offset:offset+400]
                placeholders = ",".join("?" for _ in batch)
                matches += connection.execute(f"SELECT count(*) FROM market_events WHERE dedupe_key IN ({placeholders})", batch).fetchone()[0]
        finally:
            connection.close()
        result["journal_matches"] = matches
        result["state"] = "CHECKED"
except Exception as exc:
    result["state"] = "UNKNOWN_AUDIT_ERROR"
    result["error_type"] = type(exc).__name__
print(json.dumps(result))
'@
                $workerStartForJournal = [DateTimeOffset]::Parse([string]$status.started_utc).ToUniversalTime().ToString('o')
                $journalAuditOutput = $latestJournalAuditScript | & $python - $auditParquetDir $auditLedgerPath $workerStartForJournal 2>&1
                $journalAuditExit = $LASTEXITCODE
                if ($journalAuditExit -eq 0) {
                    $journalAudit = ($journalAuditOutput | Select-Object -Last 1) | ConvertFrom-Json
                    if ($journalAudit.state -ne 'CHECKED') {
                        Add-Check 'manifest_home_latest_journal_consistency' 'UNKNOWN' "read-only audit state=$($journalAudit.state); files=$($journalAudit.file_count); WAL bytes=$($journalAudit.wal_bytes)"
                    } else {
                        $journalMissing = [int]$journalAudit.distinct_keys - [int]$journalAudit.journal_matches
                        Add-Check 'manifest_home_latest_journal_consistency' $(if ($journalMissing -eq 0) { 'PASS' } else { 'FAIL' }) "latest_parts=$($journalAudit.file_count); Parquet_rows=$($journalAudit.parquet_rows); distinct_keys=$($journalAudit.distinct_keys); matching_configured_SQLite_rows=$($journalAudit.journal_matches); missing=$journalMissing; read_only_immutable_snapshot=true"
                    }
                } else {
                    Add-Check 'manifest_home_latest_journal_consistency' 'UNKNOWN' 'read-only Parquet-to-SQLite key audit failed'
                }
            }
        } catch {
            Add-Check 'manifest_home_storage_metrics' 'UNKNOWN' 'could not inspect manifest-matched storage paths read-only'
        }
    }

    $pendingRows = if ($null -ne $status.parquet_buffered_rows) { [int]$status.parquet_buffered_rows } else { -1 }
    if ($pendingRows -gt 0) {
        Add-Check 'pending_parquet_buffer' 'FAIL' "status reports $pendingRows rows still in the live buffer; they are not yet confirmed durable in Parquet"
    } elseif ($pendingRows -eq 0) {
        $rawCaptureEnabled = [string]$status.raw_capture_state -eq 'ENABLED'
        $freshParquetPublished = @($checks | Where-Object { $_.name -eq 'manifest_home_parquet_activity' -and $_.result -eq 'PASS' }).Count -gt 0
        if ($rawCaptureEnabled -and $freshParquetPublished) {
            Add-Check 'pending_parquet_buffer' 'PASS' 'live buffer is empty with raw capture enabled; fresh Parquet parts exist and no partial files were observed'
        } else {
            Add-Check 'pending_parquet_buffer' 'UNKNOWN' "live buffer is empty, but raw_capture_state=$($status.raw_capture_state) and/or fresh durable Parquet parts are unverified"
        }
    } else {
        Add-Check 'pending_parquet_buffer' 'UNKNOWN' 'worker does not expose its pending Parquet row count'
    }
}

$reportStamp = [DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$reportPath = Join-Path $root "reports\runtime-verification-$reportStamp.json"
$report = [pscustomobject]@{
    schema = 'kalshi15m.runtime_verification.v2'
    observed_utc = [DateTimeOffset]::UtcNow.ToString('o')
    worker_pid = if ($status) { [int]$status.pid } else { $null }
    worker_started_utc = if ($status) { [string]$status.started_utc } else { $null }
    worker_snapshot = if ($status) {
        [pscustomobject]@{
            service_state = $status.service_state
            mode = $status.mode
            heartbeat_utc = $status.heartbeat_utc
            exchange_write_calls = $status.exchange_write_calls
            orders_submitted_to_exchange = $status.orders_submitted_to_exchange
            websocket_state = $status.websocket_state
            sequence_gap_count = $status.sequence_gap_count
            feed_ages_seconds = $status.feed_ages_seconds
            series_count = $status.series_count
            series_category_counts = $status.series_category_counts
            active_market_rows = $status.active_market_rows
            market_tickers = @($status.market_tickers)
            events_inserted_since_process_start = $status.events_inserted
            events_duplicate_since_process_start = $status.events_duplicate
            events_dropped = $status.events_dropped
            raw_capture_state = $status.raw_capture_state
            sqlite_bytes_metric = $status.sqlite_bytes
            parquet_bytes_metric = $status.parquet_bytes
            parquet_buffered_rows = $status.parquet_buffered_rows
            actual_rollovers_observed = $status.actual_rollovers_observed
            rollover_detection_sample_count = $status.rollover_detection_sample_count
            last_rollover_detection_latency_seconds = $status.last_rollover_detection_latency_seconds
            rollover_preflight_lead_seconds = $status.rollover_preflight_lead_seconds
            btc_evaluators_active = $status.strategy_in_scope_evaluators_active
            total_profile_evaluators_active = $status.strategy_all_profiles_evaluators_active
            crossmarket_evaluators_active = $status.crossmarket_evaluators_active
            crossmarket_decisions_status_count = $status.crossmarket_decisions_persisted
            crossmarket_entry_attempts_status_count = $status.crossmarket_paper_entry_attempts
            crossmarket_fills_status_count = $status.crossmarket_paper_fill_records
            crossmarket_settlements_status_count = $status.crossmarket_paper_settlement_records
            crossmarket_open_positions_status_count = $status.crossmarket_paper_open_positions
            crossmarket_net_pnl_status_dollars = $status.crossmarket_paper_realized_net_pnl_dollars
            crossmarket_pnl_state = $status.crossmarket_paper_net_pnl_state
            crossmarket_eligible_market_tickers = @($status.crossmarket_eligible_market_tickers)
            crossmarket_excluded_market_tickers = @($status.crossmarket_excluded_market_tickers)
            crossmarket_excluded_reasons = $status.crossmarket_excluded_market_reasons
            crossmarket_blocked_reasons = $status.crossmarket_blocked_market_reasons
            btc_config_sha256 = $status.strategy_config_sha256
            crossmarket_config_sha256 = $status.crossmarket_strategy_config_sha256
            crossmarket_parent_btc_config_sha256 = $status.crossmarket_parent_btc_config_sha256
            storage_configured_ledger_path = $status.storage_configured_ledger_path
            storage_open_database_path = $status.storage_open_database_path
            storage_database_path_matches_config = $status.storage_database_path_matches_config
            storage_path_file_identity_state = $status.storage_path_file_identity_state
            storage_paper_ledger_record_count = $status.storage_paper_ledger_record_count
            storage_latest_paper_record_utc = $status.storage_latest_paper_record_utc
            storage_latest_market_event_received_utc = $status.storage_latest_market_event_received_utc
        }
    } else { $null }
    checks = @($checks)
}
$reportJson = $report | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($reportPath, $reportJson, [Text.UTF8Encoding]::new($false))
$checks | ConvertTo-Json -Depth 6
if ($testExit -ne 0) { exit $testExit }
if (@($checks | Where-Object { $_.result -eq 'FAIL' }).Count -gt 0) { exit 2 }
