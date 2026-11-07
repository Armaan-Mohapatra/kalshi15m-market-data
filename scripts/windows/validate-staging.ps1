param(
    [string]$StageHome = (Join-Path $env:LOCALAPPDATA 'Kalshi15mRustStage'),
    [string]$Fixture = (Join-Path $PSScriptRoot '..\..\rust\collector\fixtures\smoke.ndjson')
)

$ErrorActionPreference = 'Stop'
$collector = (Resolve-Path (Join-Path $PSScriptRoot '..\..\rust\collector')).Path
$fixturePath = (Resolve-Path -LiteralPath $Fixture).Path
$homePath = [System.IO.Path]::GetFullPath($StageHome)
$blocked = @('TradingNextData', 'Kalshi15mNative', 'KalshiRuntime')
foreach ($item in $blocked) {
    if ($homePath -match [regex]::Escape($item)) {
        throw "Refusing protected or production path in StageHome: $item"
    }
}

Push-Location $collector
try {
    cargo fmt --all -- --check
    if ($LASTEXITCODE -ne 0) { throw 'cargo fmt failed' }
    cargo check --all-targets --locked
    if ($LASTEXITCODE -ne 0) { throw 'cargo check failed' }
    cargo build --all-targets --locked
    if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
    cargo test --all-targets --locked
    if ($LASTEXITCODE -ne 0) { throw 'cargo test failed' }
    cargo clippy --all-targets --locked -- -D warnings
    if ($LASTEXITCODE -ne 0) { throw 'cargo clippy failed' }
    $binary = Join-Path $collector 'target\debug\kalshi15m-rust.exe'
    & $binary --home $homePath replay --input $fixturePath
    if ($LASTEXITCODE -ne 0) { throw 'Offline replay failed' }
    & $binary --home $homePath audit
    if ($LASTEXITCODE -ne 0) { throw 'Staging audit failed' }
    $repoRoot = Split-Path -Parent (Split-Path -Parent $collector)
    $python = Join-Path $repoRoot '.venv-rust-verification\Scripts\python.exe'
    & $python (Join-Path $collector 'tools\check_python_parity.py') $homePath
    if ($LASTEXITCODE -ne 0) { throw 'Python SQLite/Parquet compatibility check failed' }
} finally {
    Pop-Location
}
