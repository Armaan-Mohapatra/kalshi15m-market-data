param([switch]$InstallDependencies,[switch]$KeepSupervisorPaused)
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$venvPython = Join-Path $root '.venv\Scripts\python.exe'
# Check the live listener before touching this shell's runtime directory. A
# packaged/AppContainer PowerShell can resolve the same LocalAppData path to a
# private package cache and otherwise start a second worker against a different
# SQLite ledger while the normal-user worker is still active.
$existingListeners = @(
    Get-NetTCPConnection -LocalPort $statusPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
)
if ($existingListeners.Count -gt 0) {
    $existingStatus = $null
    try {
        $existingStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 3
    } catch {}
    if (
        $existingStatus -and
        [string]$existingStatus.service -eq 'kalshi-15m-native' -and
        $existingListeners -contains [int]$existingStatus.pid
    ) {
        throw "Standalone worker already owns port $statusPort with PID $($existingStatus.pid); refusing a duplicate start from runtime home $homePath."
    }
    throw "Port $statusPort is occupied by an unverified listener; refusing to start another worker."
}
New-Item -ItemType Directory -Path (Join-Path $homePath 'runtime'),(Join-Path $homePath 'logs'),(Join-Path $homePath 'data') -Force | Out-Null
$startLockPath = Join-Path $homePath 'runtime\worker.start.lock'
$startLock = $null
try {
    $startLock = [System.IO.File]::Open(
        $startLockPath,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
} catch [System.IO.IOException] {
    throw 'Another standalone worker start attempt is already in progress.'
}
try {
if (-not (Test-Path $venvPython)) { py -3.14 -m venv (Join-Path $root '.venv') }
if ($InstallDependencies) { & $venvPython -m pip install -r (Join-Path $root 'requirements.txt') }
if (-not (Test-Path $venvPython)) { throw 'Project venv is unavailable. Run start.ps1 -InstallDependencies.' }
$pidPath = Join-Path $homePath 'runtime\worker.pid.json'
if (Test-Path $pidPath) {
    $existing = Get-Content $pidPath -Raw | ConvertFrom-Json
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($existing.pid)" -ErrorAction SilentlyContinue
    if ($process -and $process.CommandLine -match [regex]::Escape($root)) { throw "Worker already running with PID $($existing.pid)." }
    Remove-Item -LiteralPath $pidPath -Force
}
$manualStopPath = Join-Path $homePath 'runtime\worker.manual-stop'
if (-not $KeepSupervisorPaused -and (Test-Path -LiteralPath $manualStopPath)) {
    Remove-Item -LiteralPath $manualStopPath -Force
}
$env:KALSHI15M_HOME = $homePath
$env:KALSHI15M_STATUS_PORT = [string]$statusPort
# A long-lived Task Scheduler supervisor can keep the environment block from
# before the user configured the standalone feed key. Refresh only these
# standalone market-data references from this task user's HKCU environment.
# Never read the main REFLEX credential store or log either value.
$userMarketDataKeyId = [Environment]::GetEnvironmentVariable('KALSHI15M_MARKETDATA_API_KEY_ID','User')
$userMarketDataKeyPath = [Environment]::GetEnvironmentVariable('KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH','User')
if ($userMarketDataKeyId -and $userMarketDataKeyPath -and (Test-Path -LiteralPath $userMarketDataKeyPath -PathType Leaf)) {
    $env:KALSHI15M_MARKETDATA_API_KEY_ID = $userMarketDataKeyId
    $env:KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH = $userMarketDataKeyPath
} else {
    Remove-Item Env:KALSHI15M_MARKETDATA_API_KEY_ID -ErrorAction SilentlyContinue
    Remove-Item Env:KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH -ErrorAction SilentlyContinue
}
$env:PYTHONPATH = Join-Path $root 'src'
$stdoutPath = Join-Path $homePath 'logs\worker.stdout.log'
$stderrPath = Join-Path $homePath 'logs\worker.stderr.log'
foreach ($logPath in @($stdoutPath,$stderrPath)) {
    if ((Test-Path $logPath) -and (Get-Item $logPath).Length -gt 8388608) {
        Move-Item -LiteralPath $logPath -Destination "$logPath.$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')).old"
    }
}
$proc = Start-Process -FilePath $venvPython -ArgumentList '-m','kalshi15m.worker' -WorkingDirectory $root -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
$launchStartedUtc = [DateTime]::UtcNow.ToString('o')
# Publish ownership as soon as the process exists. The worker can take longer
# than the status-server startup window while SQLite validates a large WAL.
# Without a provisional PID record, the supervisor may start a second worker
# after start.ps1 times out even though the first Python process is still alive.
$pidRecord = [ordered]@{
    pid=[int]$proc.Id
    executable=[IO.Path]::GetFullPath($venvPython)
    launcher_pid=[int]$proc.Id
    project=$root
    runtime_home=[IO.Path]::GetFullPath($homePath)
    data_dir=[IO.Path]::GetFullPath((Join-Path $homePath 'data'))
    paper_ledger_path=[IO.Path]::GetFullPath((Join-Path $homePath 'data\paper_ledger.sqlite3'))
    parquet_dir=[IO.Path]::GetFullPath((Join-Path $homePath 'data\market_data'))
    port=$statusPort
    started_utc=$launchStartedUtc
    startup_state='STARTING'
}
$pidRecord | ConvertTo-Json | Set-Content -LiteralPath $pidPath -Encoding utf8
$runtimeStatus = $null
 $workerProcess = $null
 $deadline = [DateTime]::UtcNow.AddSeconds(45)
 do {
     $workerCandidates = @(
         Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
             Where-Object {
                 [int]$_.ParentProcessId -eq [int]$proc.Id -and
                 $_.CommandLine -match [regex]::Escape($root) -and
                 $_.CommandLine -match '-m kalshi15m\.worker'
             }
     )
     if ($workerCandidates.Count -gt 0) {
         $candidate = $workerCandidates[0]
         if ([int]$pidRecord.pid -ne [int]$candidate.ProcessId) {
             $pidRecord.pid = [int]$candidate.ProcessId
             $pidRecord.executable = $candidate.ExecutablePath
             $pidRecord | ConvertTo-Json | Set-Content -LiteralPath $pidPath -Encoding utf8
         }
     }
     if ($proc.HasExited -and $workerCandidates.Count -eq 0) {
         throw "Worker launcher exited during startup. See $stderrPath."
     }
     try { $runtimeStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$statusPort/api/v1/status" -Method Get -TimeoutSec 2 } catch { $runtimeStatus = $null }
     if ($runtimeStatus -and $runtimeStatus.pid) {
         $workerProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $($runtimeStatus.pid)" -ErrorAction SilentlyContinue
         if (-not $workerProcess -or $workerProcess.CommandLine -notmatch [regex]::Escape($root)) { $workerProcess = $null }
     }
     if (-not $workerProcess) { Start-Sleep -Milliseconds 250 }
 } while (-not $workerProcess -and [DateTime]::UtcNow -lt $deadline)
if (-not $workerProcess) { throw "Could not verify the running worker PID from its read-only status endpoint. See $stderrPath." }
$pidRecord = [ordered]@{ pid=[int]$runtimeStatus.pid; executable=$workerProcess.ExecutablePath; launcher_pid=$proc.Id; project=$root; runtime_home=[IO.Path]::GetFullPath($homePath); data_dir=[IO.Path]::GetFullPath((Join-Path $homePath 'data')); paper_ledger_path=[IO.Path]::GetFullPath((Join-Path $homePath 'data\paper_ledger.sqlite3')); parquet_dir=[IO.Path]::GetFullPath((Join-Path $homePath 'data\market_data')); port=$statusPort; started_utc=$launchStartedUtc; startup_state='READY' }
$pidRecord | ConvertTo-Json | Set-Content -LiteralPath $pidPath -Encoding utf8
$strategyState = if ($runtimeStatus) { $runtimeStatus.strategy_state } else { 'STARTING' }
$strategyRevision = if ($runtimeStatus) { $runtimeStatus.strategy_config_revision } else { $null }
[pscustomobject]@{pid=[int]$runtimeStatus.pid; launcher_pid=$proc.Id; port=$statusPort; status_url="http://127.0.0.1:$statusPort/api/v1/status"; runtime_home=$homePath; strategy_state=$strategyState; strategy_config_revision=$strategyRevision; exchange_writes=0} | ConvertTo-Json -Compress
} finally {
    if ($startLock) { $startLock.Dispose() }
}
