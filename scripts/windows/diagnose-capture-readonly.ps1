# Strictly observational Phoenix capture diagnostics.
# No database connection, WAL access, file creation, service action, or network
# request beyond the existing loopback read-only /api/v1/status endpoint.
param(
    [Parameter(Mandatory = $true)][string]$RuntimeHome,
    [ValidateRange(1, 65535)][int]$StatusPort = 8768,
    [ValidateRange(0, 30)][int]$IntervalSeconds = 5
)

$ErrorActionPreference = 'Stop'
$homePath = [IO.Path]::GetFullPath($RuntimeHome)
$statusUrl = "http://127.0.0.1:$StatusPort/api/v1/status"
$dataPath = Join-Path $homePath 'data'
$runtimePath = Join-Path $homePath 'runtime'
$logPath = Join-Path $homePath 'logs'
$pidPath = Join-Path $runtimePath 'worker.pid.json'

function Get-MetadataOnly {
    param([Parameter(Mandatory = $true)][string]$Path)
    try {
        $item = Get-Item -LiteralPath $Path -ErrorAction Stop
        return [ordered]@{
            exists = $true
            path = [IO.Path]::GetFullPath($Path)
            is_directory = [bool]$item.PSIsContainer
            bytes = if ($item.PSIsContainer) { $null } else { [long]$item.Length }
            modified_utc = $item.LastWriteTimeUtc.ToString('o')
        }
    } catch {
        return [ordered]@{
            exists = $false
            path = [IO.Path]::GetFullPath($Path)
            is_directory = $null
            bytes = $null
            modified_utc = $null
        }
    }
}

function Get-CaptureObservation {
    $now = [DateTimeOffset]::UtcNow
    $status = $null
    $statusError = $null
    try {
        $status = Invoke-RestMethod -Uri $statusUrl -Method Get -TimeoutSec 3
    } catch {
        $statusError = $_.Exception.GetType().Name
    }
    $pidRecord = $null
    $pidReadError = $null
    if (Test-Path -LiteralPath $pidPath -PathType Leaf) {
        try {
            $pidRecord = Get-Content -LiteralPath $pidPath -Raw -ErrorAction Stop | ConvertFrom-Json
        } catch {
            $pidReadError = $_.Exception.GetType().Name
        }
    }
    $pidFromStatus = if ($status -and $status.pid) { [int]$status.pid } else { 0 }
    $pidFromRecord = if ($pidRecord -and $pidRecord.pid) { [int]$pidRecord.pid } else { 0 }
    $allPids = @($pidFromStatus, $pidFromRecord | Where-Object { $_ -gt 0 } | Select-Object -Unique)
    $processes = @()
    foreach ($processId in $allPids) {
        if ([int]$processId -le 0) { continue }
        $process = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
        if ($null -eq $process) { continue }
        $processes += [ordered]@{
            pid = [int]$process.ProcessId
            parent_pid = [int]$process.ParentProcessId
            executable_path = [string]$process.ExecutablePath
            created_utc = if ($process.CreationDate) {
                ([DateTime]$process.CreationDate).ToUniversalTime().ToString('o')
            } else { $null }
            command_contains_worker_module = [bool]($process.CommandLine -match 'kalshi15m[.]worker')
        }
    }
    $listenerPids = @(
        Get-NetTCPConnection -LocalPort $StatusPort -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )
    $heartbeatAge = $null
    if ($status -and $status.heartbeat_utc) {
        try {
            $hb = [DateTimeOffset]::Parse([string]$status.heartbeat_utc)
            $heartbeatAge = [math]::Round(($now - $hb.ToUniversalTime()).TotalSeconds, 2)
        } catch {}
    }
    $metadata = [ordered]@{}
    foreach ($item in @(
        (Join-Path $runtimePath 'status.json'),
        $pidPath,
        (Join-Path $dataPath 'paper_ledger.sqlite3'),
        (Join-Path $dataPath 'paper_ledger.sqlite3-wal'),
        (Join-Path $dataPath 'paper_ledger.sqlite3-shm'),
        (Join-Path $logPath 'worker.log'),
        (Join-Path $logPath 'worker.stderr.log'),
        (Join-Path $logPath 'supervisor.log')
    )) {
        $metadata[[IO.Path]::GetFileName($item)] = Get-MetadataOnly -Path $item
    }
    # Only investigate current/day-1/day-2 date partitions; no recursive walk.
    # Absence here is limited evidence, NOT proof that no other archive exists.
    $recentParts = @()
    $archiveRoot = Join-Path $dataPath 'market_data'
    for ($day = 0; $day -lt 3; $day++) {
        $date = $now.UtcDateTime.AddDays(-$day).ToString('yyyy-MM-dd')
        $partition = Join-Path $archiveRoot "date=$date"
        if (-not (Test-Path -LiteralPath $partition -PathType Container)) { continue }
        $kinds = @(Get-ChildItem -LiteralPath $partition -Directory -ErrorAction SilentlyContinue | Select-Object -First 16)
        foreach ($kind in $kinds) {
            $parts = @(Get-ChildItem -LiteralPath $kind.FullName -Filter '*.parquet' -File -ErrorAction SilentlyContinue | Select-Object -First 10)
            foreach ($part in $parts) {
                $recentParts += [ordered]@{
                    date = $date
                    kind = $kind.Name
                    path = $part.FullName
                    size_bytes = [long]$part.Length
                    modified_utc = $part.LastWriteTimeUtc.ToString('o')
                }
            }
        }
    }
    return [ordered]@{
        observed_utc = $now.ToString('o')
        checked_runtime_home = $homePath
        resolved_localappdata = $env:LOCALAPPDATA
        process_account = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        status_http = if ($status) {'RESPONDED'} else {'UNREACHABLE'}
        status_error_type = $statusError
        status_pid = if ($pidFromStatus -gt 0) {$pidFromStatus} else {$null}
        pid_record_pid = if ($pidFromRecord -gt 0) {$pidFromRecord} else {$null}
        pid_record_error_type = $pidReadError
        listener_pids = @($listenerPids)
        status_service = if ($status) {[string]$status.service} else {$null}
        status_service_state = if ($status) {[string]$status.service_state} else {$null}
        status_runtime_home = if ($status) {[string]$status.runtime_home} else {$null}
        status_storage_path = if ($status) {[string]$status.storage_open_database_path} else {$null}
        status_storage_write_state = if ($status) {[string]$status.storage_write_state} else {$null}
        status_storage_latest_disk_event_utc = if ($status) {$status.storage_latest_disk_market_event_received_utc} else {$null}
        status_storage_latest_event_utc = if ($status) {$status.storage_latest_market_event_received_utc} else {$null}
        status_events_inserted = if ($status) {$status.events_inserted} else {$null}
        status_heartbeat_utc = if ($status) {$status.heartbeat_utc} else {$null}
        status_heartbeat_age_seconds = $heartbeatAge
        status_capture_quality = if ($status) {$status.collection_quality_state} else {$null}
        processes = $processes
        file_metadata = $metadata
        bounded_recent_archive_samples = $recentParts
    }
}

$first = Get-CaptureObservation
if ($IntervalSeconds -gt 0) {
    Start-Sleep -Seconds $IntervalSeconds
}
$second = Get-CaptureObservation
$heartbeatAdvanced = [bool](
    $first.status_heartbeat_utc -and
    $second.status_heartbeat_utc -and
    $first.status_heartbeat_utc -ne $second.status_heartbeat_utc
)
$eventCounterAdvanced = $false
if ($null -ne $first.status_events_inserted -and $null -ne $second.status_events_inserted) {
    try {
        $eventCounterAdvanced = [long]$second.status_events_inserted -gt [long]$first.status_events_inserted
    } catch {}
}
$dbFileAdvanced = $false
foreach ($name in @('paper_ledger.sqlite3', 'paper_ledger.sqlite3-wal')) {
    $left = $first.file_metadata[$name]
    $right = $second.file_metadata[$name]
    if ($left.exists -and $right.exists -and (
        $left.bytes -ne $right.bytes -or $left.modified_utc -ne $right.modified_utc
    )) { $dbFileAdvanced = $true }
}
$classification = 'UNKNOWN_CAPTURE_NOT_INDEPENDENTLY_CONFIRMED'
if ($second.status_http -eq 'UNREACHABLE') {
    $classification = 'UNKNOWN_STATUS_UNREACHABLE'
} elseif ($second.status_storage_write_state -in @('FATAL_NOT_DURABLE','DEGRADED_RETRYING','RECOVERY_SPOOLED_STOPPING')) {
    $classification = 'DEGRADED_STORAGE_REPORTED'
} elseif ($second.status_storage_write_state -eq 'NOT_OPEN') {
    $classification = 'UNVERIFIED_STORAGE_NOT_OPEN'
} elseif ($null -ne $second.status_heartbeat_age_seconds -and $second.status_heartbeat_age_seconds -gt 30) {
    $classification = 'UNKNOWN_STALE_HEARTBEAT'
} elseif ($dbFileAdvanced -and $eventCounterAdvanced) {
    $classification = 'PROGRESS_SUGGESTED_BY_COUNTER_AND_FILE_METADATA_NOT_FULL_AUDIT'
} elseif ($heartbeatAdvanced) {
    $classification = 'PROCESS_RESPONSIVE_DURABLE_PROGRESS_UNVERIFIED'
}
[ordered]@{
    schema = 'phoenix.capture_readonly_triage.v1'
    classification = $classification
    interpretation = 'Two bounded observations cannot prove continuous exchange capture, source completeness, or absence of writes elsewhere.'
    interval_seconds = $IntervalSeconds
    heartbeat_advanced = $heartbeatAdvanced
    event_counter_advanced = $eventCounterAdvanced
    database_file_metadata_changed = $dbFileAdvanced
    first = $first
    second = $second
} | ConvertTo-Json -Depth 8
