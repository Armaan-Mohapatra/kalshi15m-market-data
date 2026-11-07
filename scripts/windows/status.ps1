$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$pidPath = Join-Path $homePath 'runtime\worker.pid.json'
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$processInfo = $null
if (Test-Path $pidPath) {
    $record = Get-Content $pidPath -Raw | ConvertFrom-Json
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue
    if ($proc -and $proc.CommandLine -match [regex]::Escape($root)) { $processInfo = @{pid=$record.pid; command=$proc.CommandLine} }
}
try {
    $runtimeStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 5
    $record = if (Test-Path $pidPath) { Get-Content $pidPath -Raw | ConvertFrom-Json } else { $null }
    $managedProcess = if ($record) { Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue } else { $null }
    $listenerPids = @(Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique)
    $identityVerified = [bool]($managedProcess -and $managedProcess.CommandLine -match [regex]::Escape($root) -and [int]$runtimeStatus.pid -eq [int]$record.pid -and $listenerPids -contains [int]$runtimeStatus.pid)
    $runtimeStatus | Add-Member -NotePropertyName managed_pid -NotePropertyValue $(if ($record) { [int]$record.pid } else { $null }) -Force
    $runtimeStatus | Add-Member -NotePropertyName listener_pids -NotePropertyValue $listenerPids -Force
    $runtimeStatus | Add-Member -NotePropertyName worker_identity_verified -NotePropertyValue $identityVerified -Force
    $runtimeStatus | ConvertTo-Json -Depth 8
} catch {
    [pscustomobject]@{service='UNREACHABLE'; status_port=$statusPort; managed_process=$processInfo; error=$_.Exception.Message} | ConvertTo-Json -Compress
    exit 1
}
