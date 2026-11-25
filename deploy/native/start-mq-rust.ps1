# Start MQ-rust on Windows without Docker.
# Ports: namesrv 19877, broker 20921, metrics 19090, admin 19091, TC 19191.
$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Bin = Join-Path $Root "target\release"
$Data = "G:\MQ_soft\mq-rust-native"
$Log = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $Data, $Log | Out-Null

foreach ($exe in @("mq-namesrv.exe", "mq-broker.exe", "mq-tc.exe")) {
    if (-not (Test-Path (Join-Path $Bin $exe))) {
        throw "missing $exe; run: cargo build --release -p mq-namesrv -p mq-broker -p mq-tc"
    }
}

$pids = @{}

$pids["namesrv"] = (Start-Process -FilePath (Join-Path $Bin "mq-namesrv.exe") `
    -ArgumentList "--listen", "127.0.0.1:19877", "--cluster", "DefaultCluster", `
        "--broker-name", "broker-a", "--broker-addr", "127.0.0.1:20921", "--queues", "8" `
    -RedirectStandardOutput (Join-Path $Log "native-namesrv.log") `
    -RedirectStandardError (Join-Path $Log "native-namesrv.err.log") `
    -PassThru -WindowStyle Hidden).Id

Start-Sleep -Seconds 1
$pids["tc"] = (Start-Process -FilePath (Join-Path $Bin "mq-tc.exe") `
    -ArgumentList "--api-listen", "127.0.0.1:19191" `
    -RedirectStandardOutput (Join-Path $Log "native-tc.log") `
    -RedirectStandardError (Join-Path $Log "native-tc.err.log") `
    -PassThru -WindowStyle Hidden).Id

$pids["broker"] = (Start-Process -FilePath (Join-Path $Bin "mq-broker.exe") `
    -ArgumentList "--listen", "127.0.0.1:20921", "--namesrv", "127.0.0.1:19877", `
        "--queues", "8", "--commitlog-shards", "1", `
        "--store-root", (Join-Path $Data "broker"), `
        "--metrics-listen", "127.0.0.1:19090", "--admin-listen", "127.0.0.1:19091", `
        "--tx-timeout-ms", "10000", "--tc-url", "http://127.0.0.1:19191", `
        "--tc-outbox-path", (Join-Path $Data "tc-outbox.log"), `
        "--audit-path", (Join-Path $Data "audit.log") `
    -RedirectStandardOutput (Join-Path $Log "native-broker.log") `
    -RedirectStandardError (Join-Path $Log "native-broker.err.log") `
    -PassThru -WindowStyle Hidden).Id

$pids | ConvertTo-Json | Set-Content (Join-Path $Log "mq-rust.pids.json")
Start-Sleep -Seconds 3

foreach ($port in 19877, 20921, 19191, 19090, 19091) {
    $listening = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    Write-Host ("port {0}: {1}" -f $port, ($(if ($listening) { "UP" } else { "DOWN" })))
}
