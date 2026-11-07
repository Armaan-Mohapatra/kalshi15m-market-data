$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$homePath = if ($env:KALSHI15M_HOME) { $env:KALSHI15M_HOME } else { Join-Path $env:LOCALAPPDATA 'Kalshi15mNative' }
$sourceDb = Join-Path $homePath 'data\paper_ledger.sqlite3'
$outputRoot = Join-Path $homePath 'benchmarks'
$pgBin = 'C:\Program Files\PostgreSQL\17\bin'
$python = Join-Path $root '.venv\Scripts\python.exe'
$pgData = Join-Path $outputRoot ("postgres-cluster-" + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ'))
$pgLog = Join-Path $pgData 'postgres.log'
$port = $null
$started = $false

if (-not (Test-Path -LiteralPath $sourceDb)) { throw "No captured project ledger exists at $sourceDb." }
if (-not (Test-Path -LiteralPath $python)) { throw 'Project Python environment is unavailable.' }
foreach ($candidate in 55438..55448) {
    if (-not (Get-NetTCPConnection -LocalPort $candidate -State Listen -ErrorAction SilentlyContinue)) {
        $port = $candidate
        break
    }
}
if (-not $port) { throw 'No free loopback benchmark port in 55438..55448.' }

$null = New-Item -ItemType Directory -Path $pgData -Force
try {
    & (Join-Path $pgBin 'initdb.exe') -D $pgData -U postgres --auth-local=trust --auth-host=trust --encoding=UTF8 --no-instructions
    if ($LASTEXITCODE -ne 0) { throw "initdb failed with exit code $LASTEXITCODE." }

    $options = "-h 127.0.0.1 -p $port -c listen_addresses=127.0.0.1 -c max_connections=10"
    & (Join-Path $pgBin 'pg_ctl.exe') -D $pgData -l $pgLog -o $options -w start
    if ($LASTEXITCODE -ne 0) { throw "pg_ctl start failed with exit code $LASTEXITCODE." }
    $started = $true

    $env:PYTHONPATH = Join-Path $root 'src'
    & $python (Join-Path $root 'scripts\benchmark_storage.py') --source-db $sourceDb --output-root $outputRoot --psql (Join-Path $pgBin 'psql.exe') --postgres-port $port
    if ($LASTEXITCODE -ne 0) { throw "Storage benchmark failed with exit code $LASTEXITCODE." }
}
finally {
    if ($started) {
        & (Join-Path $pgBin 'pg_ctl.exe') -D $pgData -m fast -w stop
        if ($LASTEXITCODE -ne 0) { Write-Warning 'The isolated benchmark PostgreSQL cluster did not confirm a clean stop.' }
    }
}
