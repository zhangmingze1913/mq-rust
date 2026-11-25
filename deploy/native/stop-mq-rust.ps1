# Stop MQ-rust native processes started by start-mq-rust.ps1.
$ErrorActionPreference = "SilentlyContinue"
$Log = Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path "logs"
$file = Join-Path $Log "mq-rust.pids.json"
if (-not (Test-Path $file)) { Write-Host "no pid file"; exit 0 }
$pids = Get-Content $file | ConvertFrom-Json
foreach ($name in $pids.PSObject.Properties.Name) {
    $id = $pids.$name
    Write-Host "stopping $name pid=$id"
    Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
}
Remove-Item $file -Force -ErrorAction SilentlyContinue
