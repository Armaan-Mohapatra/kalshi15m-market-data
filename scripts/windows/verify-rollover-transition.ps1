param(
    [Parameter(Mandatory = $true)]
    [string]$TargetOpenUtc,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath,
    [int]$BoundaryPollSeconds = 1,
    [int]$EarlyPollSeconds = 5
)

$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$statusUrl = 'http://127.0.0.1:8768/api/v1/status'
$target = [DateTimeOffset]::Parse($TargetOpenUtc).ToUniversalTime()
$reportsRoot = [IO.Path]::GetFullPath((Join-Path $root 'reports'))
$outputFullPath = [IO.Path]::GetFullPath($OutputPath)
if (-not $outputFullPath.StartsWith($reportsRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'OutputPath must stay inside this standalone project reports directory.'
}
if (Test-Path -LiteralPath $outputFullPath) { throw "Output already exists: $outputFullPath" }
$temporaryPath = $outputFullPath + '.partial'
if (Test-Path -LiteralPath $temporaryPath) { throw "Partial output already exists and requires owner verification: $temporaryPath" }
if ($BoundaryPollSeconds -lt 1 -or $EarlyPollSeconds -lt 1) { throw 'Polling intervals must be positive.' }
$claimPath = $outputFullPath + '.claim'
$claimStream = $null
try {
    $claimStream = [IO.File]::Open($claimPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    $claimBytes = [Text.UTF8Encoding]::new($false).GetBytes((@{ pid = $PID; claimed_utc = [DateTimeOffset]::UtcNow.ToString('o') } | ConvertTo-Json -Compress))
    $claimStream.Write($claimBytes, 0, $claimBytes.Length)
    $claimStream.Flush($true)
}
catch [IO.IOException] {
    if ($claimStream) { $claimStream.Dispose(); $claimStream = $null }
    throw "Output is already claimed (or has a stale claim requiring owner verification): $claimPath"
}

try {
function Test-TargetOpen {
    param([string]$Value)
    if (-not $Value) { return $false }
    try { return ([DateTimeOffset]::Parse($Value).ToUniversalTime() -eq $target) }
    catch { return $false }
}

function Get-CompactSnapshot {
    $sampled = [DateTimeOffset]::UtcNow
    $status = Invoke-RestMethod -Uri $statusUrl -Method Get -TimeoutSec 5
    $workerProcessId = [int]$status.pid
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $workerProcessId" -ErrorAction SilentlyContinue
    $listeners = @(
        Get-NetTCPConnection -LocalPort 8768 -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )
    [pscustomobject]@{
        sampled_utc = $sampled.ToString('o')
        service = [string]$status.service
        service_state = [string]$status.service_state
        mode = [string]$status.mode
        worker_pid = $workerProcessId
        process_identity_verified = [bool]($process -and $process.CommandLine -like "*$root*" -and $listeners -contains $workerProcessId)
        listener_pids = $listeners
        heartbeat_utc = [string]$status.heartbeat_utc
        exchange_write_calls = [int]$status.exchange_write_calls
        websocket_state = [string]$status.websocket_state
        sequence_gap_count = [int]$status.sequence_gap_count
        series_count = [int]$status.series_count
        active_market_rows = [int]$status.active_market_rows
        active_evaluators = [int]$status.strategy_all_profiles_evaluators_active
        rollover_preopen_seconds = [int]$status.rollover_preopen_seconds
        rollover_preflight_lead_seconds = [int]$status.rollover_preflight_lead_seconds
        open_or_upcoming_count = [int]$status.open_or_upcoming_count
        prepared_market_tickers = @($status.rollover_prepared_market_tickers | ForEach-Object { [string]$_ })
        preflight_group_open_utc = [string]$status.rollover_preflight_group_open_utc
        preflight_started_utc = [string]$status.rollover_preflight_started_utc
        priority_reconcile_state = [string]$status.rollover_priority_reconcile_state
        priority_reconcile_ticker_count = [int]$status.rollover_priority_reconcile_ticker_count
        priority_reconcile_request_count = [int]$status.rollover_priority_reconcile_request_count
        last_rollover_open_utc = [string]$status.last_rollover_scheduled_open_utc
        last_rollover_detected_utc = [string]$status.last_rollover_detected_utc
        last_rollover_latency_seconds = $status.last_rollover_detection_latency_seconds
        last_rollover_new_market_tickers = @($status.last_rollover_new_market_tickers | ForEach-Object { [string]$_ })
        rest_state = [string]$status.rest_state
        rest_orderbook_age_seconds = $status.rest_poll_ages_seconds.rest_orderbook
        rest_orderbook_poll = $status.last_rest_poll_by_channel.rest_orderbook
        api_errors = [int]$status.market_api_errors
        api_rate_limit_responses = [int]$status.market_api_rate_limit_responses
    }
}

$initial = Get-CompactSnapshot
if ($initial.service -ne 'kalshi-15m-native' -or $initial.service_state -ne 'RUNNING' -or $initial.mode -ne 'PAPER_ONLY' -or -not $initial.process_identity_verified -or $initial.rollover_preopen_seconds -ne 300) {
    throw 'Standalone worker identity/health precondition failed.'
}
$workerPid = $initial.worker_pid
$preopen = $null
$preflight = $null
$detection = $null
$sampleCount = 1
$pidStable = $true
$deadline = $target.AddSeconds(120)

while ([DateTimeOffset]::UtcNow -lt $deadline) {
    $sample = Get-CompactSnapshot
    $sampleCount++
    if ($sample.worker_pid -ne $workerPid -or -not $sample.process_identity_verified) { $pidStable = $false }
    $secondsUntilOpen = ($target - [DateTimeOffset]::Parse($sample.sampled_utc)).TotalSeconds
    if ($secondsUntilOpen -le 300 -and $secondsUntilOpen -gt 45 -and $sample.prepared_market_tickers.Count -gt 0) {
        $preopen = $sample
    }
    if ($null -eq $preflight -and (Test-TargetOpen $sample.preflight_group_open_utc)) { $preflight = $sample }
    if (Test-TargetOpen $sample.last_rollover_open_utc) { $detection = $sample; break }
    $waitSeconds = if ($secondsUntilOpen -le 60) { $BoundaryPollSeconds } else { $EarlyPollSeconds }
    Start-Sleep -Seconds $waitSeconds
}

$final = Get-CompactSnapshot
$expectedTickers = if ($detection) { @($detection.last_rollover_new_market_tickers) } else { @() }
$preparedTickers = if ($preopen) { @($preopen.prepared_market_tickers) } else { @() }
$preopenOverlap = @($preparedTickers | Where-Object { $expectedTickers -contains $_ } | Sort-Object -Unique).Count
$preopenBook = if ($preopen) { $preopen.rest_orderbook_poll } else { $null }
$preopenBookFresh = [bool](
    $preopen -and
    $preopen.rest_orderbook_age_seconds -ne $null -and
    [double]$preopen.rest_orderbook_age_seconds -le 30 -and
    $preopenBook.status -eq 'READY' -and
    [int]$preopenBook.markets_with_data -eq [int]$preopenBook.requested_market_count
)
$latency = if ($detection -and $detection.last_rollover_latency_seconds -ne $null) { [double]$detection.last_rollover_latency_seconds } else { $null }
$preflightLeadSeconds = if ($preflight) {
    ($target - [DateTimeOffset]::Parse($preflight.sampled_utc)).TotalSeconds
} else { $null }
$preflightConfiguredLeadSeconds = if ($preflight) {
    [int]$preflight.rollover_preflight_lead_seconds
} else { $null }
$passed = [bool](
    $pidStable -and
    $final.service_state -eq 'RUNNING' -and
    $final.mode -eq 'PAPER_ONLY' -and
    $final.series_count -eq 34 -and
    $final.active_evaluators -eq 21 -and
    $final.exchange_write_calls -eq 0 -and
    $final.api_rate_limit_responses -eq 0 -and
    $detection -and
    $expectedTickers.Count -gt 0 -and
    $preopenOverlap -eq $expectedTickers.Count -and
    $preopenBookFresh -and
    $preflightLeadSeconds -gt 0 -and
    $preflightConfiguredLeadSeconds -ge 180 -and
    $latency -ne $null -and
    $latency -lt 2.641
)
$report = [pscustomobject]@{
    schema_version = 1
    target_open_utc = $target.ToString('o')
    completed_utc = [DateTimeOffset]::UtcNow.ToString('o')
    passed = $passed
    sample_count = $sampleCount
    worker_pid = $workerPid
    worker_pid_stable = $pidStable
    initial = $initial
    preopen_sample = $preopen
    preflight_sample = $preflight
    detected_transition = $detection
    final = $final
    checks = [pscustomobject]@{
        preopen_ticker_overlap = $preopenOverlap
        detected_new_ticker_count = $expectedTickers.Count
        preopen_rest_book_fresh_and_complete = $preopenBookFresh
        preflight_sample_lead_seconds = $preflightLeadSeconds
        preflight_configured_lead_seconds = $preflightConfiguredLeadSeconds
        preflight_before_open = ($preflightLeadSeconds -gt 0)
        detection_latency_seconds = $latency
        prior_latest_latency_seconds = 2.641
        sequence_continuity = if ($initial.websocket_state -eq 'CONNECTED') { 'REQUIRES_LIVE_GAP_AUDIT' } else { 'UNKNOWN_NOT_CONNECTED' }
        no_exchange_writes = ($final.exchange_write_calls -eq 0)
    }
}
[IO.File]::WriteAllText($temporaryPath, ($report | ConvertTo-Json -Depth 10), [Text.UTF8Encoding]::new($false))
Move-Item -LiteralPath $temporaryPath -Destination $outputFullPath
[pscustomobject]@{ report_path = $outputFullPath; passed = $passed; target_open_utc = $target.ToString('o'); detected = [bool]$detection; latency_seconds = $latency; preopen_overlap = $preopenOverlap; new_ticker_count = $expectedTickers.Count; sample_count = $sampleCount } | ConvertTo-Json -Compress
if (-not $passed) { exit 1 }
}
finally {
    if ($claimStream) { $claimStream.Dispose(); $claimStream = $null }
    if (Test-Path -LiteralPath $claimPath) { Remove-Item -LiteralPath $claimPath }
}
