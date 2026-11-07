param(
    [ValidateRange(1,30)][int]$GracefulWaitSeconds = 30,
    [switch]$KeepSupervisorPausedOnTimeout
)
$ErrorActionPreference = 'Stop'
$root = 'C:\Users\dakin\Projects\kalshi-15m-native'
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$pidPath = Join-Path $homePath 'runtime\worker.pid.json'
$manualStopPath = Join-Path $homePath 'runtime\worker.manual-stop'
$listenerPids = @(
    Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
)
if ($listenerPids.Count -gt 0) {
    $liveStatus = $null
    try {
        $liveStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 3
    } catch {}
    if (
        -not $liveStatus -or
        [string]$liveStatus.service -ne 'kalshi-15m-native' -or
        $listenerPids -notcontains [int]$liveStatus.pid
    ) {
        throw "Port $statusPort has a listener whose standalone identity could not be verified; refusing to write stop markers."
    }
    if (-not (Test-Path -LiteralPath $pidPath)) {
        throw "Standalone worker PID $($liveStatus.pid) is healthy, but this shell cannot see its runtime PID record; refusing to write stop markers from a different runtime view."
    }
    $visibleRecord = Get-Content -LiteralPath $pidPath -Raw | ConvertFrom-Json
    if ([int]$visibleRecord.pid -ne [int]$liveStatus.pid) {
        throw "Runtime PID record does not match the live standalone listener PID $($liveStatus.pid); refusing to write stop markers."
    }
}
'manual_stop' | Set-Content -LiteralPath $manualStopPath -Encoding ascii
if (-not (Test-Path $pidPath)) { 'NOT_RUNNING_SUPERVISOR_PAUSED'; exit 0 }
$record = Get-Content $pidPath -Raw | ConvertFrom-Json
$process = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue
if (-not $process -or $process.CommandLine -notmatch [regex]::Escape($root)) {
    Remove-Item -LiteralPath $pidPath -Force
    'NOT_RUNNING'
    exit 0
}
$stopPath = Join-Path $homePath 'runtime\stop.request'
'stop' | Set-Content -LiteralPath $stopPath -Encoding ascii
# This script never force-kills the worker. A caller may retain the manual-stop
# marker after timeout so a running supervisor cannot race a second attempt.
$deadline = [DateTime]::UtcNow.AddSeconds($GracefulWaitSeconds)
do {
    Start-Sleep -Seconds 1
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue
} while ($process -and [DateTime]::UtcNow -lt $deadline)
if ($process) {
    $runtimeStatus = $null
    try {
        $runtimeStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 5
    } catch {
        $runtimeStatus = $null
    }
    $listenerPids = @(
        Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )
    $workerIdentityVerified = [bool](
        $process.CommandLine -match [regex]::Escape($root) -and
        $runtimeStatus -and
        [int]$runtimeStatus.pid -eq [int]$record.pid -and
        $listenerPids -contains [int]$record.pid
    )
    $paperOnlyGuardVerified = [bool](
        $workerIdentityVerified -and
        $runtimeStatus.mode -eq 'PAPER_ONLY' -and
        [int]$runtimeStatus.exchange_write_calls -eq 0 -and
        ($null -eq $runtimeStatus.orders_submitted_to_exchange -or
            [int]$runtimeStatus.orders_submitted_to_exchange -eq 0)
    )
    $heartbeatAgeSeconds = $null
    $heartbeatFresh = $false
    if ($runtimeStatus -and $runtimeStatus.heartbeat_utc) {
        try {
            $heartbeat = [DateTimeOffset]::Parse([string]$runtimeStatus.heartbeat_utc)
            $heartbeatAgeSeconds = ([DateTimeOffset]::UtcNow - $heartbeat.ToUniversalTime()).TotalSeconds
            $heartbeatFresh = $heartbeatAgeSeconds -ge -2 -and $heartbeatAgeSeconds -le 30
        } catch {
            $heartbeatAgeSeconds = $null
        }
    }
    $ownedPaperWorkerStillHealthy = [bool]($paperOnlyGuardVerified -and $heartbeatFresh)
    $stopRequestCleared = $false
    $manualStopCleared = $false
    if ($paperOnlyGuardVerified) {
        $runtimePath = [IO.Path]::GetFullPath((Join-Path $homePath 'runtime')) + [IO.Path]::DirectorySeparatorChar
        $markersToClear = @($stopPath)
        if (-not $KeepSupervisorPausedOnTimeout) {
            $markersToClear += $manualStopPath
        }
        foreach ($markerPath in $markersToClear) {
            $markerFullPath = [IO.Path]::GetFullPath($markerPath)
            if (-not $markerFullPath.StartsWith($runtimePath, [StringComparison]::OrdinalIgnoreCase)) {
                throw 'Refusing to clear stop markers outside the standalone runtime directory.'
            }
            Remove-Item -LiteralPath $markerFullPath -Force -ErrorAction SilentlyContinue
        }
        $stopRequestCleared = -not (Test-Path -LiteralPath $stopPath)
        $manualStopCleared = -not (Test-Path -LiteralPath $manualStopPath)
    }
    [pscustomobject]@{
        stopped = $false
        pid = $record.pid
        state = if ($KeepSupervisorPausedOnTimeout -and $paperOnlyGuardVerified) { 'WORKER_STILL_RUNNING_SUPERVISOR_PAUSED' } elseif ($ownedPaperWorkerStillHealthy) { 'WORKER_STILL_HEALTHY_SUPERVISOR_RESTORED' } elseif ($paperOnlyGuardVerified) { 'WORKER_STILL_RUNNING_STALE_HEARTBEAT_SUPERVISOR_RESTORED' } else { 'STOP_REQUESTED_WORKER_STILL_RUNNING' }
        worker_identity_verified = $workerIdentityVerified
        paper_only_guard_verified = $paperOnlyGuardVerified
        heartbeat_fresh = $heartbeatFresh
        heartbeat_age_seconds = $heartbeatAgeSeconds
        stop_request_cleared = $stopRequestCleared
        manual_stop_marker_cleared = $manualStopCleared
        stop_file = $stopPath
    } | ConvertTo-Json -Compress
    exit 1
}
Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue
[pscustomobject]@{stopped=$true; pid=$record.pid; graceful=$true} | ConvertTo-Json -Compress
