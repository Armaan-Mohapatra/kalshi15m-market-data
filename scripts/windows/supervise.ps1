param([switch]$CheckOnly,[string]$RuntimeHome,[int]$RuntimePort = 0)

$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($RuntimeHome) { [IO.Path]::GetFullPath($RuntimeHome) } elseif ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$statusPort = if ($RuntimePort -gt 0) { $RuntimePort } elseif ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$statusUrl = "http://127.0.0.1:$statusPort/api/v1/status"
$pidPath = Join-Path $homePath 'runtime\worker.pid.json'
$manualStopPath = Join-Path $homePath 'runtime\worker.manual-stop'
$supervisorStopPath = Join-Path $homePath 'runtime\supervisor.stop'
$logPath = Join-Path $homePath 'logs\supervisor.log'
$startPath = Join-Path $root 'scripts\windows\start.ps1'
$lastLoggedState = $null
$lastHealthyPid = $null

New-Item -ItemType Directory -Path (Split-Path -Parent $logPath),(Split-Path -Parent $pidPath) -Force | Out-Null

function Write-SupervisorLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    if ((Test-Path -LiteralPath $logPath) -and (Get-Item -LiteralPath $logPath).Length -gt 1048576) {
        Move-Item -LiteralPath $logPath -Destination ($logPath + '.old') -Force
    }
    $line = "{0} {1}{2}" -f [DateTime]::UtcNow.ToString('o'),$Message,[Environment]::NewLine
    [IO.File]::AppendAllText($logPath,$line,[Text.UTF8Encoding]::new($false))
}

Write-SupervisorLog "SUPERVISOR_CONTEXT root=$root runtime_home=$homePath port=$statusPort account=$([Security.Principal.WindowsIdentity]::GetCurrent().Name) pid_record=$pidPath pid_record_exists=$(Test-Path -LiteralPath $pidPath) runtime_exists=$(Test-Path -LiteralPath (Split-Path -Parent $pidPath))"

function Get-StatusValue {
    param([AllowNull()][object]$Object,[Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Test-StandaloneWorker {
    $status = $null
    try { $status = Invoke-RestMethod -Uri $statusUrl -Method Get -TimeoutSec 3 } catch {}

    $record = $null
    $recordReadError = ''
    if (Test-Path -LiteralPath $pidPath) {
        try { $record = Get-Content -LiteralPath $pidPath -Raw | ConvertFrom-Json } catch { $recordReadError = $_.Exception.GetType().Name }
    }
    $recordPid = if ($record -and $record.pid) { [int]$record.pid } else { 0 }
    $recordProjectVerified = $false
    if ($record -and $record.project) {
        try {
            $recordProjectVerified = [IO.Path]::GetFullPath([string]$record.project).Equals(
                $root,[StringComparison]::OrdinalIgnoreCase
            )
        } catch {}
    }
    $listeners = @(
        Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )

    if ($status) {
        $statusPid = if ($status.pid) { [int]$status.pid } else { 0 }
        $listenerVerified = $statusPid -in $listeners
        $serviceVerified = [string]$status.service -eq 'kalshi-15m-native'
        $identityVerified = [bool]($serviceVerified -and $listenerVerified -and $statusPid -gt 0)
        if (-not $identityVerified) {
            $identityReason = "service=$serviceVerified;listener=$listenerVerified;status_pid=$statusPid;pid_record_match=$($recordPid -eq $statusPid);pid_record_error=$recordReadError;root=$root;runtime_home=$homePath"
            return [pscustomobject]@{
                healthy = $false; can_start = $false; reason = "STATUS_IDENTITY_UNVERIFIED_$identityReason"
                pid = $statusPid; state = [string]$status.service_state; identity_verified = $false
            }
        }

        $mode = [string](Get-StatusValue $status 'mode')
        $writes = [int](Get-StatusValue $status 'exchange_write_calls')
        $submitted = Get-StatusValue $status 'orders_submitted_to_exchange'
        if ($mode -ne 'PAPER_ONLY' -or $writes -ne 0 -or ($null -ne $submitted -and [int]$submitted -ne 0)) {
            return [pscustomobject]@{
                healthy = $false; can_start = $false; reason = 'PAPER_ONLY_GUARD_FAILED'
                pid = $statusPid; state = [string]$status.service_state; identity_verified = $true
            }
        }
        if ([string]$status.service_state -eq 'RUNNING') {
            return [pscustomobject]@{
                healthy = $true; can_start = $false; reason = 'HEALTHY'
                pid = $statusPid; state = [string]$status.service_state; identity_verified = $true
            }
        }
        return [pscustomobject]@{
            healthy = $false; can_start = $false; reason = 'OWNED_WORKER_NOT_READY'
            pid = $statusPid; state = [string]$status.service_state; identity_verified = $true
        }
    }

    if ($listeners.Count -gt 0) {
        return [pscustomobject]@{
            healthy = $false; can_start = $false; reason = 'UNRESPONSIVE_OR_FOREIGN_LISTENER'
            pid = [int]$listeners[0]; state = 'UNREACHABLE'
        }
    }
    if ($recordPid -gt 0) {
        $recordProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $recordPid" -ErrorAction SilentlyContinue
        $recordPathMatches = $true
        if ($recordProcess -and $recordProcess.ExecutablePath -and $record.executable) {
            try {
                $recordPathMatches = [IO.Path]::GetFullPath([string]$recordProcess.ExecutablePath).Equals(
                    [IO.Path]::GetFullPath([string]$record.executable),[StringComparison]::OrdinalIgnoreCase
                )
            } catch { $recordPathMatches = $false }
        }
        if ($recordProcess -and $recordProjectVerified -and $recordPathMatches) {
            return [pscustomobject]@{
                healthy = $false; can_start = $false; reason = 'OWNED_PROCESS_WITHOUT_STATUS_LISTENER'
                pid = $recordPid; state = 'UNREACHABLE'; identity_verified = $true
            }
        }
    }
    return [pscustomobject]@{
        healthy = $false; can_start = $true; reason = 'STANDALONE_WORKER_ABSENT'
        pid = $recordPid; state = 'UNREACHABLE'; identity_verified = $false
    }
}

if ($CheckOnly) {
    $check = Test-StandaloneWorker
    [pscustomobject]@{ healthy = $check.healthy; reason = $check.reason; pid = $check.pid; state = $check.state; identity_verified = $check.identity_verified; exchange_writes = 0 } | ConvertTo-Json -Compress
    if ($check.healthy) { exit 0 }
    exit 1
}

$env:KALSHI15M_HOME = $homePath
$env:KALSHI15M_STATUS_PORT = [string]$statusPort

while ($true) {
    $supervisorStopRequested = $false
    if (Test-Path -LiteralPath $supervisorStopPath) {
        try { $supervisorStopRequested = (Get-Content -LiteralPath $supervisorStopPath -Raw).Trim() -eq 'stop' } catch {}
    }
    if ($supervisorStopRequested) {
        Remove-Item -LiteralPath $supervisorStopPath -Force -ErrorAction SilentlyContinue
        Write-SupervisorLog 'SUPERVISOR_STOP_REQUESTED'
        exit 0
    }
    if (Test-Path -LiteralPath $manualStopPath) {
        if ($lastLoggedState -ne 'MANUAL_STOP') {
            Write-SupervisorLog 'MANUAL_WORKER_STOP_ACTIVE; no restart will be attempted'
            $lastLoggedState = 'MANUAL_STOP'
            $lastHealthyPid = $null
        }
        Start-Sleep -Seconds 5
        continue
    }

    $check = Test-StandaloneWorker
    if ($check.healthy) {
        if ($lastHealthyPid -ne $check.pid) {
            Write-SupervisorLog "HEALTHY worker_pid=$($check.pid) mode=PAPER_ONLY exchange_write_calls=0"
        }
        $lastHealthyPid = $check.pid
        $lastLoggedState = 'HEALTHY'
        Start-Sleep -Seconds 5
        continue
    }

    $lastHealthyPid = $null
    if ($check.can_start) {
        if ($lastLoggedState -ne 'STARTING') {
            Write-SupervisorLog 'STANDALONE_WORKER_ABSENT; invoking the project start script'
            $lastLoggedState = 'STARTING'
        }
        try {
            & $startPath | Out-Null
            $afterStart = Test-StandaloneWorker
            if ($afterStart.healthy) {
                Write-SupervisorLog "RESTART_VERIFIED worker_pid=$($afterStart.pid) mode=PAPER_ONLY exchange_write_calls=0"
                $lastHealthyPid = $afterStart.pid
                $lastLoggedState = 'HEALTHY'
            } else {
                Write-SupervisorLog "START_NOT_VERIFIED reason=$($afterStart.reason)"
                $lastLoggedState = 'START_NOT_VERIFIED'
            }
        } catch {
            Write-SupervisorLog "START_FAILED exception_type=$($_.Exception.GetType().Name)"
            $lastLoggedState = 'START_FAILED'
        }
        Start-Sleep -Seconds 10
        continue
    }

    if ($lastLoggedState -ne $check.reason) {
        Write-SupervisorLog "FAIL_CLOSED reason=$($check.reason) pid=$($check.pid) state=$($check.state)"
        $lastLoggedState = $check.reason
    }
    Start-Sleep -Seconds 5
}
