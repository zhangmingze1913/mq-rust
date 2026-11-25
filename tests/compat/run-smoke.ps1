# MQ-rust R1/R2 Java 兼容性 smoke
#
# 前置：mq-namesrv 与 mq-broker 已启动（默认 127.0.0.1:9877 / 10921）。
# 用法：
#   powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1
#   powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 -Push -Topic MQ_RUST_PUSH
#   powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 -PullOnly -Expected 15
param(
    [string]$Namesrv = "127.0.0.1:9877",
    [string]$Topic = "MQ_RUST_SMOKE",
    [switch]$PullOnly,
    [switch]$Push,
    [switch]$Tag,
    [switch]$Batch,
    [switch]$Retry,
    [switch]$Rebalance,
    [switch]$Tx,
    [switch]$TxTimeout,
    [switch]$TxCheck,
    [switch]$Idem,
    [switch]$Dlq,
    [switch]$Delay,
    [switch]$StartDeliver,
    [switch]$Order,
    [switch]$Inbox,
    [int]$Expected = 15
)

$ErrorActionPreference = "Stop"
$JavaHome = "G:\MQ_soft\jdk-17.0.2"
$RocketMqHome = "G:\MQ_soft\rocketmq-all-4.9.8-bin-release"
$Classpath = "$RocketMqHome\lib\*"
$Out = Join-Path $PSScriptRoot "out"
New-Item -ItemType Directory -Force -Path $Out | Out-Null

if (-not (Test-Path "$JavaHome\bin\javac.exe")) { throw "JDK not found: $JavaHome" }
if (-not (Test-Path "$RocketMqHome\lib\rocketmq-client-4.9.8.jar")) { throw "RocketMQ 4.9.8 not found" }

& "$JavaHome\bin\javac.exe" -encoding UTF-8 -cp $Classpath -d $Out `
    (Join-Path $PSScriptRoot "src\MqRustCompatSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustPullOnly.java") `
    (Join-Path $PSScriptRoot "src\MqRustPushSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustTagSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustBatchSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustRetrySmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustRebalanceSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustTxSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustTxTimeoutSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustTxCheckBackSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustIdempotentSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustDlqSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustDelaySmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustStartDeliverSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustOrderSmoke.java") `
    (Join-Path $PSScriptRoot "src\MqRustInboxSmoke.java")
if ($LASTEXITCODE -ne 0) { throw "javac failed" }

if ($Order) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustOrderSmoke $Namesrv $Topic
} elseif ($Inbox) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustInboxSmoke $Namesrv 127.0.0.1:9091 $Topic
} elseif ($StartDeliver) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustStartDeliverSmoke $Namesrv $Topic
} elseif ($Delay) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustDelaySmoke $Namesrv $Topic
} elseif ($Dlq) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustDlqSmoke $Namesrv $Topic
} elseif ($Idem) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustIdempotentSmoke $Namesrv $Topic
} elseif ($TxCheck) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustTxCheckBackSmoke $Namesrv $Topic
} elseif ($TxTimeout) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustTxTimeoutSmoke $Namesrv $Topic
} elseif ($Tx) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustTxSmoke $Namesrv `
        "$Topic`_COMMIT" "$Topic`_ROLLBACK"
} elseif ($Rebalance) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustRebalanceSmoke $Namesrv $Topic
} elseif ($Batch) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustBatchSmoke $Namesrv $Topic
} elseif ($Retry) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustRetrySmoke $Namesrv $Topic
} elseif ($Tag) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustTagSmoke $Namesrv $Topic
} elseif ($Push) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustPushSmoke $Namesrv $Topic 5
} elseif ($PullOnly) {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustPullOnly $Namesrv $Topic $Expected
} else {
    & "$JavaHome\bin\java.exe" -cp "$Out;$Classpath" MqRustCompatSmoke $Namesrv $Topic
}
if ($LASTEXITCODE -ne 0) { throw "compat smoke failed" }
