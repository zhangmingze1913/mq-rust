# Start a 3-node TC Raft cluster on Windows (no Docker).
# Ports: raft 19301-19303, API 19401-19403.
$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Bin = Join-Path $Root "target\release\mq-tc.exe"
$Data = "G:\MQ_soft\mq-rust-tc-cluster"
$Log = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $Data, $Log | Out-Null
if (-not (Test-Path $Bin)) { throw "missing $Bin; run cargo build --release -p mq-tc" }

$peers = "1@127.0.0.1:19301,2@127.0.0.1:19302,3@127.0.0.1:19303"
$pids = @()
1..3 | ForEach-Object {
    $pids += (Start-Process -FilePath $Bin `
        -ArgumentList "--id", $_, "--raft-listen", "127.0.0.1:1930$_", `
            "--api-listen", "127.0.0.1:1940$_", "--peers", $peers, `
            "--data-dir", (Join-Path $Data "node-$_") `
        -RedirectStandardOutput (Join-Path $Log "tc-cluster-$_.log") `
        -RedirectStandardError (Join-Path $Log "tc-cluster-$_.err.log") `
        -PassThru -WindowStyle Hidden).Id
}
$pids | ConvertTo-Json | Set-Content (Join-Path $Log "tc-cluster.pids.json")
Start-Sleep -Seconds 4
1..3 | ForEach-Object {
    try {
        $body = (Invoke-WebRequest -Uri "http://127.0.0.1:1940$($_)/leader" -UseBasicParsing).Content
        Write-Host "node$_ $body"
    } catch { Write-Host "node$_ DOWN" }
}
