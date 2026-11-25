# Stop the native 3-node TC cluster.
$ErrorActionPreference = "SilentlyContinue"
$Log = Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path "logs"
$file = Join-Path $Log "tc-cluster.pids.json"
if (-not (Test-Path $file)) { Write-Host "no tc pid file"; exit 0 }
$pids = Get-Content $file | ConvertFrom-Json
foreach ($id in $pids) {
    Write-Host "stopping tc pid=$id"
    Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
}
Remove-Item $file -Force -ErrorAction SilentlyContinue
