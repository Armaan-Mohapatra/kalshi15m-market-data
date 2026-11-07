# Phoenix isolated third-party source checkout. Clone only: does NOT execute installers.
# This is for Codex running locally on Windows. It never touches collector
# processes, settings, production DB/WAL, event archives, or strategy execution.
param(
    [string]$Root = 'E:\PhoenixTooling',
    [switch]$CloneAll,
    [ValidateRange(0,200)][int]$StartIndex = 0,
    [ValidateRange(1,10)][int]$MaxItems = 4,
    [ValidateRange(1,1024)][int]$MinimumFreeGB = 40
)
$ErrorActionPreference = 'Stop'
$manifestPath = Join-Path $PSScriptRoot 'repos.json'
$manifest = Get-Content -LiteralPath $manifestPath -Raw -ErrorAction Stop | ConvertFrom-Json
$destRoot = [IO.Path]::GetFullPath($Root)
# Intentional hard fail on any other drive/path. Keep E: data roots read-only.
if ($destRoot -notmatch '(?i)^E:\\PhoenixTooling(?:\\|$)') {
    throw 'Refusing non-isolated tooling root: only E:\PhoenixTooling is authorized.'
}
$items = @($manifest.repos)
if (-not $CloneAll) {
    [ordered]@{
        mode = 'INVENTORY_ONLY'
        source_manifest = $manifestPath
        total_repository_sources = $items.Count
        target_root = $destRoot
        next_command = '.\bootstrap.ps1 -CloneAll -StartIndex 0 -MaxItems 4'
        policy = 'No third-party code is executed or installed by this bootstrap.'
    } | ConvertTo-Json -Depth 5
    return
}
if (-not (Test-Path -LiteralPath 'E:\' -PathType Container)) {
    throw 'E: drive is unavailable. Do not fall back onto C: automatically.'
}
$volume = Get-PSDrive -Name E -ErrorAction Stop
if ($volume.Free -lt ($MinimumFreeGB * 1GB)) {
    throw "Not enough E: free space for isolated tool checkout; minimum $MinimumFreeGB GiB required."
}
if ($StartIndex -ge $items.Count) { throw 'StartIndex exceeds repository inventory.' }
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw 'Git is required; do not auto-install unreviewed dependencies.'
}
$cloneDir = Join-Path $destRoot 'src'
$reportDir = Join-Path $destRoot 'manifests'
New-Item -ItemType Directory -Force -Path $cloneDir,$reportDir | Out-Null
$previousLfs = $env:GIT_LFS_SKIP_SMUDGE
$previousPrompt = $env:GIT_TERMINAL_PROMPT
$env:GIT_LFS_SKIP_SMUDGE = '1'
$env:GIT_TERMINAL_PROMPT = '0'
$results = @()
try {
    $stop = [math]::Min($items.Count, ($StartIndex + $MaxItems))
    for ($i = $StartIndex; $i -lt $stop; $i++) {
        $item = $items[$i]
        $repoName = [string]$item.repo
        $url = [string]$item.url
        if ($repoName -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$' -or
            $url -ne "https://github.com/$repoName.git") {
            throw "Unexpected repository URL or path in manifest at index $i."
        }
        $folder = $repoName.Replace('/','--')
        $dest = Join-Path $cloneDir $folder
        $state = 'NOT_FETCHED'
        $sha = $null
        $errorKind = $null
        try {
            if ((Get-PSDrive -Name E).Free -lt ($MinimumFreeGB * 1GB)) {
                throw 'E: free space dropped below configured safety floor'
            }
            if (Test-Path -LiteralPath $dest) {
                if (-not (Test-Path -LiteralPath (Join-Path $dest '.git'))) {
                    throw 'Target exists but is not a Git checkout; do not overwrite'
                }
                $state = 'EXISTING_CLONE_UNTOUCHED'
            } else {
                # Depth=1 and no submodules, no Git-LFS assets, no build/postinstall,
                # no install hooks. One source checkout at a time.
                & git clone --depth 1 --filter=blob:none --single-branch --no-recurse-submodules -- $url $dest | Out-Null
                if ($LASTEXITCODE -ne 0) { throw "git clone failed: exit $LASTEXITCODE" }
                $state = 'SOURCE_CLONED_NOT_INSTALLED'
            }
            $sha = (& git -C $dest rev-parse HEAD | Out-String).Trim()
            if ($LASTEXITCODE -ne 0 -or $sha -notmatch '^[a-fA-F0-9]{40}$') {
                throw 'Cannot verify source commit'
            }
        } catch {
            $errorKind = $_.Exception.GetType().Name
            if ($state -eq 'NOT_FETCHED') { $state = 'BLOCKED_REVIEW_REQUIRED' }
        }
        $results += [ordered]@{
            index = $i
            repository = $repoName
            checkout = $dest
            state = $state
            git_sha = $sha
            activation_policy = [string]$item.activation
            error_type = $errorKind
        }
        # No auto-activation, pip/npm install, background server, scheduled task,
        # GPU driver changes, browser extension, or trading service.
    }
} finally {
    $env:GIT_LFS_SKIP_SMUDGE = $previousLfs
    $env:GIT_TERMINAL_PROMPT = $previousPrompt
}
$report = [ordered]@{
    schema = 'phoenix.third_party_checkout_receipt.v1'
    generated_utc = [DateTime]::UtcNow.ToString('o')
    root = $destRoot
    requested_range = "$StartIndex..$($stop - 1)"
    next_start_index = if ($stop -lt $items.Count) { $stop } else { $null }
    source_count = $items.Count
    clones_only = $true
    results = $results
}
$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$reportPath = Join-Path $reportDir "checkout-$timestamp-$StartIndex.json"
# Disposable research reports only under E:\PhoenixTooling, never production roots.
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $reportPath -Encoding utf8
$report | ConvertTo-Json -Depth 8
