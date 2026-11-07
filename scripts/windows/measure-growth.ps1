param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPath,
    [int]$DurationSeconds = 3600,
    [int]$IntervalSeconds = 300
)

$ErrorActionPreference = 'Stop'
$root = 'C:\Users\dakin\Projects\kalshi-15m-native'
$statusUrl = 'http://127.0.0.1:8768/api/v1/status'

if ($DurationSeconds -lt 60) { throw 'DurationSeconds must be at least 60.' }
if ($IntervalSeconds -lt 10) { throw 'IntervalSeconds must be at least 10.' }
$reportsRoot = [IO.Path]::GetFullPath((Join-Path $root 'reports'))
$outputFullPath = [IO.Path]::GetFullPath($OutputPath)
if (-not $outputFullPath.StartsWith($reportsRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'OutputPath must stay inside this standalone project reports directory.'
}
if (Test-Path -LiteralPath $outputFullPath) { throw "Output already exists: $outputFullPath" }

function Get-Snapshot {
    $sampledAt = [DateTime]::UtcNow
    $status = Invoke-RestMethod -Uri $statusUrl -Method Get -TimeoutSec 8
    $workerPid = [int]$status.pid
    $process = Get-Process -Id $workerPid -ErrorAction Stop
    $nativeProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $workerPid" -ErrorAction Stop
    $listeners = @(
        Get-NetTCPConnection -LocalPort 8768 -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )
    if (
        $status.storage_database_path_matches_config -ne $true -or
        [string]$status.storage_path_file_identity_state -ne 'UNCHANGED_SINCE_OPEN'
    ) {
        throw 'Worker storage path identity is not verified; refusing to measure a shell-local data directory.'
    }
    # Read byte counters from the worker that owns the canonical data path.
    # A packaged PowerShell process can resolve LocalAppData to its own cache,
    # so scanning that shell-visible path can silently measure the wrong DB.
    $sqliteBytes = [long]$status.sqlite_bytes
    $parquetBytes = [long]$status.parquet_bytes
    $dataBytes = $sqliteBytes + $parquetBytes

    [pscustomobject]@{
        sampled_utc = $sampledAt.ToString('o')
        service_state = [string]$status.service_state
        worker_pid = $workerPid
        process_identity_verified = ($nativeProcess.CommandLine -like '*kalshi-15m-native*')
        listener_pids = $listeners
        data_bytes = [long]$dataBytes
        sqlite_family_bytes = [long]$sqliteBytes
        parquet_bytes = [long]$parquetBytes
        storage_path_identity_state = [string]$status.storage_path_file_identity_state
        storage_database_path_matches_config = [bool]$status.storage_database_path_matches_config
        latest_market_event_utc = $status.storage_latest_disk_market_event_received_utc
        latest_paper_record_utc = $status.storage_latest_disk_paper_record_utc
        events_dropped = [long]$status.events_dropped
        sequence_gap_count = [int]$status.sequence_gap_count
        parquet_buffered_rows = [int]$status.parquet_buffered_rows
        queue_depth = [int]$status.queue_depth
        working_set_bytes = [long]$process.WorkingSet64
        private_bytes = [long]$process.PrivateMemorySize64
        cpu_seconds = [double]$process.CPU
        disk_free_bytes = [long](Get-PSDrive C).Free
        rest_state = [string]$status.rest_state
        websocket_state = [string]$status.websocket_state
        rest_orderbook_age_seconds = $status.feed_ages_seconds.rest_orderbook
        rest_public_trades_age_seconds = $status.feed_ages_seconds.rest_public_trades
        evaluator_count = [int]$status.strategy_all_profiles_evaluators_active
        exchange_write_calls = [int]$status.exchange_write_calls
        error_count = [int]$status.error_count
    }
}

$startedAt = [DateTime]::UtcNow
$deadline = $startedAt.AddSeconds($DurationSeconds)
$samples = [Collections.Generic.List[object]]::new()
$sampleErrors = [Collections.Generic.List[object]]::new()

while ($true) {
    try {
        $samples.Add((Get-Snapshot))
    } catch {
        $sampleErrors.Add([pscustomobject]@{
            sampled_utc = [DateTime]::UtcNow.ToString('o')
            error_type = $_.Exception.GetType().Name
        })
    }

    $remaining = ($deadline - [DateTime]::UtcNow).TotalSeconds
    if ($remaining -le 0) { break }
    Start-Sleep -Seconds ([Math]::Max(1, [Math]::Min($IntervalSeconds, [Math]::Ceiling($remaining))))
}

$first = if ($samples.Count -gt 0) { $samples[0] } else { $null }
$last = if ($samples.Count -gt 1) { $samples[$samples.Count - 1] } else { $null }
$elapsed = if ($first -and $last) {
    ([DateTime]::Parse($last.sampled_utc).ToUniversalTime() - [DateTime]::Parse($first.sampled_utc).ToUniversalTime()).TotalSeconds
} else { 0.0 }
$stablePid = $samples.Count -gt 1 -and @($samples | Where-Object {
    $_.worker_pid -ne $first.worker_pid -or
    $_.service_state -ne 'RUNNING' -or
    -not $_.process_identity_verified -or
    $first.worker_pid -notin $_.listener_pids
}).Count -eq 0
$rate = {
    param([long]$StartBytes, [long]$EndBytes)
    if ($elapsed -le 0) { return $null }
    [Math]::Round(($EndBytes - $StartBytes) / $elapsed * 3600 / 1000000, 3)
}
$complete = [DateTime]::UtcNow -ge $deadline
$report = [pscustomobject]@{
    measurement = 'standalone native Windows data growth and worker resources'
    started_utc = $startedAt.ToString('o')
    completed_utc = [DateTime]::UtcNow.ToString('o')
    requested_duration_seconds = $DurationSeconds
    actual_sample_span_seconds = [Math]::Round($elapsed, 3)
    interval_seconds = $IntervalSeconds
    complete = $complete
    sample_count = $samples.Count
    sample_errors = @($sampleErrors)
    stable_worker_identity_across_samples = $stablePid
    measurement_source = 'worker status counters after canonical storage path identity check'
    data_growth_bytes = if ($first -and $last) { $last.data_bytes - $first.data_bytes } else { $null }
    data_growth_mb_per_hour = if ($first -and $last) { & $rate $first.data_bytes $last.data_bytes } else { $null }
    sqlite_family_growth_bytes = if ($first -and $last) { $last.sqlite_family_bytes - $first.sqlite_family_bytes } else { $null }
    sqlite_family_growth_mb_per_hour = if ($first -and $last) { & $rate $first.sqlite_family_bytes $last.sqlite_family_bytes } else { $null }
    parquet_growth_bytes = if ($first -and $last) { $last.parquet_bytes - $first.parquet_bytes } else { $null }
    parquet_growth_mb_per_hour = if ($first -and $last) { & $rate $first.parquet_bytes $last.parquet_bytes } else { $null }
    samples = @($samples)
}

$temporaryPath = $outputFullPath + '.partial'
$json = $report | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($temporaryPath, $json, [Text.UTF8Encoding]::new($false))
Move-Item -LiteralPath $temporaryPath -Destination $outputFullPath
Write-Output $outputFullPath
