# MQ-rust 原生进程启动脚本（Windows）
# 用法：powershell -ExecutionPolicy Bypass -File deploy/native/start-rocketmq.ps1
# 说明：仅用于本机兼容性基线；生产请用容器/K8s。

$ErrorActionPreference = "Stop"

$JavaHome = "G:\MQ_soft\jdk-17.0.2"
$RocketMqHome = "G:\MQ_soft\rocketmq-all-4.9.8-bin-release"
$DataRoot = "G:\MQ_soft\rocketmq-data"
$LogDir = Join-Path $PSScriptRoot "..\..\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if (-not (Test-Path "$JavaHome\bin\java.exe")) { throw "JAVA_HOME not found: $JavaHome" }
if (-not (Test-Path "$RocketMqHome\bin\mqnamesrv.cmd")) { throw "RocketMQ not found: $RocketMqHome" }

$env:JAVA_HOME = $JavaHome
$env:ROCKETMQ_HOME = $RocketMqHome
$env:Path = "$JavaHome\bin;$env:Path"

# 1. NameServer
$ns = Get-Process java -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "$JavaHome*" }
# 端口检查比进程名更可靠
$nsListening = Get-NetTCPConnection -LocalPort 9876 -State Listen -ErrorAction SilentlyContinue
if (-not $nsListening) {
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "`"$RocketMqHome\bin\mqnamesrv.cmd`"" `
        -WorkingDirectory $RocketMqHome `
        -RedirectStandardOutput (Join-Path $LogDir "namesrv.out.log") `
        -RedirectStandardError (Join-Path $LogDir "namesrv.err.log") `
        -WindowStyle Hidden
    Write-Host "starting namesrv..."
    Start-Sleep -Seconds 6
} else {
    Write-Host "namesrv already listening on 9876"
}

# 2. Broker
$brokerListening = Get-NetTCPConnection -LocalPort 10911 -State Listen -ErrorAction SilentlyContinue
if (-not $brokerListening) {
    $conf = Join-Path $RocketMqHome "conf\broker-dev.conf"
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "`"$RocketMqHome\bin\mqbroker.cmd`" -n 127.0.0.1:9876 -c `"$conf`"" `
        -WorkingDirectory $RocketMqHome `
        -RedirectStandardOutput (Join-Path $LogDir "broker.out.log") `
        -RedirectStandardError (Join-Path $LogDir "broker.err.log") `
        -WindowStyle Hidden
    Write-Host "starting broker..."
    Start-Sleep -Seconds 8
} else {
    Write-Host "broker already listening on 10911"
}

& "$RocketMqHome\bin\mqadmin.cmd" clusterList -n 127.0.0.1:9876
