$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$taskName = 'Kalshi15mNativePaperSupervisor'
$supervisorPath = Join-Path $root 'scripts\windows\supervise.ps1'
$powerShellExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
if (-not (Test-Path -LiteralPath $powerShellExe -PathType Leaf)) { throw 'Windows PowerShell executable is unavailable.' }
$statusPort = if ($env:KALSHI15M_STATUS_PORT) { [int]$env:KALSHI15M_STATUS_PORT } else { 8768 }
$arguments = '-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $supervisorPath + '" -RuntimeHome "' + $homePath + '" -RuntimePort ' + $statusPort
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($task) {
    $scriptMatches = @($task.Actions | Where-Object { $_.Arguments -like "*`"$supervisorPath`"*" }).Count -gt 0
    if (-not $scriptMatches) { throw "Task name $taskName already belongs to a different script; it was left unchanged." }
    $actionMatches = @($task.Actions | Where-Object {
        $_.Execute -eq $powerShellExe -and $_.Arguments -eq $arguments -and $_.WorkingDirectory -eq $root
    }).Count -gt 0
    if (-not $actionMatches) {
        if ($task.State -eq 'Running') { Stop-ScheduledTask -TaskName $taskName }
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        $task = $null
    }
}
if ($task) {
    Remove-Item -LiteralPath (Join-Path $homePath 'runtime\supervisor.stop') -Force -ErrorAction SilentlyContinue
    if ($task.State -ne 'Running') { Start-ScheduledTask -TaskName $taskName }
    [pscustomobject]@{task_name=$taskName;created=$false;state=(Get-ScheduledTask -TaskName $taskName).State;action=$supervisorPath} | ConvertTo-Json -Compress
    exit 0
}

New-Item -ItemType Directory -Path (Join-Path $homePath 'runtime'),(Join-Path $homePath 'logs') -Force | Out-Null
$account = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $powerShellExe -Argument $arguments -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $account
$principal = New-ScheduledTaskPrincipal -UserId $account -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$definition = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description 'Maintain the isolated Kalshi15mNative PAPER_ONLY collector after an unexpected worker exit.'
Register-ScheduledTask -TaskName $taskName -InputObject $definition -Force | Out-Null
Remove-Item -LiteralPath (Join-Path $homePath 'runtime\supervisor.stop') -Force -ErrorAction SilentlyContinue
Start-ScheduledTask -TaskName $taskName
$deadline = [DateTime]::UtcNow.AddSeconds(15)
do {
    $task = Get-ScheduledTask -TaskName $taskName
    if ($task.State -eq 'Running') { break }
    Start-Sleep -Milliseconds 250
} while ([DateTime]::UtcNow -lt $deadline)
if ($task.State -ne 'Running') { throw "Scheduled supervisor task did not enter Running state. Current state: $($task.State)." }
[pscustomobject]@{task_name=$taskName;created=$true;state=$task.State;run_as=$account;trigger='AtLogOn';action=$supervisorPath;worker_port=if($env:KALSHI15M_STATUS_PORT){[int]$env:KALSHI15M_STATUS_PORT}else{8768}} | ConvertTo-Json -Compress
