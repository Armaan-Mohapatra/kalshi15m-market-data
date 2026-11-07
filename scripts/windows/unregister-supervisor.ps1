$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$taskName = 'Kalshi15mNativePaperSupervisor'
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if (-not $task) { 'SUPERVISOR_NOT_REGISTERED'; exit 0 }
$supervisorPath = Join-Path $root 'scripts\windows\supervise.ps1'
$actionMatches = @($task.Actions | Where-Object { $_.Arguments -like "*`"$supervisorPath`"*" }).Count -gt 0
if (-not $actionMatches) { throw "Task name $taskName does not point to this standalone project; it was left unchanged." }
$stopPath = Join-Path $homePath 'runtime\supervisor.stop'
'stop' | Set-Content -LiteralPath $stopPath -Encoding ascii
$deadline = [DateTime]::UtcNow.AddSeconds(15)
do {
    Start-Sleep -Milliseconds 500
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if (-not $task -or $task.State -ne 'Running') { break }
} while ([DateTime]::UtcNow -lt $deadline)
if ($task -and $task.State -eq 'Running') { Stop-ScheduledTask -TaskName $taskName }
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
Remove-Item -LiteralPath $stopPath -Force -ErrorAction SilentlyContinue
[pscustomobject]@{task_name=$taskName;removed=$true;worker_stopped=$false} | ConvertTo-Json -Compress
