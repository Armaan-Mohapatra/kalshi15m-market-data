param(
    [Parameter(Mandatory = $true)]
    [string]$Binary,
    [string]$StageHome = (Join-Path $env:LOCALAPPDATA 'Kalshi15mRustStage'),
    [string]$Fixture = (Join-Path $PSScriptRoot '..\..\rust\collector\fixtures\smoke.ndjson')
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$collector = Join-Path $repoRoot 'rust\collector'
$binaryPath = (Resolve-Path -LiteralPath $Binary).Path
$fixturePath = (Resolve-Path -LiteralPath $Fixture).Path
$homePath = [System.IO.Path]::GetFullPath($StageHome)
foreach ($protectedName in @('TradingNextData', 'Kalshi15mNative', 'KalshiRuntime')) {
    if ($homePath -match [regex]::Escape($protectedName)) {
        throw "Refusing a protected or production StageHome path: $protectedName"
    }
}

& $binaryPath --home $homePath replay --input $fixturePath
if ($LASTEXITCODE -ne 0) { throw 'Offline replay failed' }
& $binaryPath --home $homePath audit
if ($LASTEXITCODE -ne 0) { throw 'Staging audit failed' }
$python = Join-Path $repoRoot '.venv-rust-verification\Scripts\python.exe'
& $python (Join-Path $collector 'tools\check_python_parity.py') $homePath
if ($LASTEXITCODE -ne 0) { throw 'Python SQLite/Parquet compatibility check failed' }
