param([string]$OutputPath)

$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$ledgerPath = Join-Path $homePath 'data\paper_ledger.sqlite3'
$pythonPath = Join-Path $root '.venv\Scripts\python.exe'
$pythonProbePath = Join-Path $root 'scripts\windows\sqlite_readonly_audit.py'

$status = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 5
$listeners = @(
    Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
)
$worker = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$status.pid)" -ErrorAction SilentlyContinue
$workerOwner = if ($worker) { Invoke-CimMethod -InputObject $worker -MethodName GetOwner } else { $null }
$keyId = [Environment]::GetEnvironmentVariable('KALSHI15M_MARKETDATA_API_KEY_ID', 'User')
$keyPath = [Environment]::GetEnvironmentVariable('KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH', 'User')

$database = $null
$databaseError = $null
$databaseErrorMessage = $null
$databaseProbeExitCode = $null
$databaseProbeOutput = $null
try {
    if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) { throw 'Project Python environment is missing.' }
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    if (-not (Test-Path -LiteralPath $pythonProbePath -PathType Leaf)) { throw 'Read-only SQLite probe script is missing.' }
    $probeOutput = @(& $pythonPath $pythonProbePath $ledgerPath 2>&1 | ForEach-Object { [string]$_ })
    $ErrorActionPreference = $previousErrorActionPreference
    $databaseProbeExitCode = $LASTEXITCODE
    $databaseProbeOutput = ($probeOutput -join [Environment]::NewLine).Trim()
    if ($databaseProbeExitCode -ne 0) {
        $probeText = ($probeOutput | Out-String).Trim()
        throw "SQLite probe failed with exit code ${databaseProbeExitCode}: $probeText"
    }
    $database = ($probeOutput -join [Environment]::NewLine) | ConvertFrom-Json
} catch {
    if ($null -ne $previousErrorActionPreference) {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $databaseError = $_.Exception.GetType().Name
    $databaseErrorMessage = ($_.Exception.Message -replace '[\r\n]+', ' ').Trim()
}

$report = [ordered]@{
    schema = 'kalshi15m.runtime_context_audit.v1'
    checked_utc = [DateTime]::UtcNow.ToString('o')
    runtime_home = [IO.Path]::GetFullPath($homePath)
    ledger_path = [IO.Path]::GetFullPath($ledgerPath)
    service = $status.service
    service_state = $status.service_state
    worker_pid = [int]$status.pid
    worker_owner = if ($workerOwner -and $workerOwner.ReturnValue -eq 0) { $workerOwner.User } else { 'UNKNOWN' }
    worker_command_matches_project = [bool]($worker -and $worker.CommandLine -match [regex]::Escape($root))
    listener_matches_worker = $listeners -contains [int]$status.pid
    mode = $status.mode
    exchange_write_calls = $status.exchange_write_calls
    orders_submitted_to_exchange = $status.orders_submitted_to_exchange
    websocket_state = $status.websocket_state
    latest_disk_event_utc = $status.storage_latest_disk_market_event_received_utc
    latest_disk_paper_utc = $status.storage_latest_disk_paper_record_utc
    storage_path_identity_state = $status.storage_path_file_identity_state
    user_key_id_configured = [bool]$keyId
    user_private_key_reference_configured = [bool]$keyPath
    user_private_key_file_exists = [bool]($keyPath -and (Test-Path -LiteralPath $keyPath -PathType Leaf))
    independent_database_read = $database
    independent_database_error = $databaseError
    independent_database_error_message = $databaseErrorMessage
    independent_database_probe_exit_code = $databaseProbeExitCode
    independent_database_probe_output = if ($databaseProbeOutput) { $databaseProbeOutput.Substring(0, [Math]::Min(2000, $databaseProbeOutput.Length)) } else { $null }
}
$json = $report | ConvertTo-Json -Depth 6
if ($OutputPath) {
    $destination = [IO.Path]::GetFullPath($OutputPath)
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($destination)) | Out-Null
    [IO.File]::WriteAllText($destination, $json, [Text.UTF8Encoding]::new($false))
}
$json
