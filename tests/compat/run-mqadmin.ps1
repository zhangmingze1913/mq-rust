# MQ-rust RocketMQ mqadmin smoke
# Preconditions: mq-namesrv/mq-broker running on 127.0.0.1:9877 / 10921.
$ErrorActionPreference = "Stop"
$Namesrv = if ($args.Length -gt 0) { $args[0] } else { "127.0.0.1:9877" }
$Command = if ($args.Length -gt 1) { $args[1] } else { "clusterList" }
$Extra = if ($args.Length -gt 2) { $args[2..($args.Length - 1)] } else { @() }
$JavaHome = "G:\MQ_soft\jdk-17.0.2"
$RocketMqHome = "G:\MQ_soft\rocketmq-all-4.9.8-bin-release"
$env:JAVA_HOME = $JavaHome
$env:ROCKETMQ_HOME = $RocketMqHome
$env:Path = "$JavaHome\bin;$env:Path"
& "$RocketMqHome\bin\mqadmin.cmd" $Command -n $Namesrv @Extra
if ($LASTEXITCODE -ne 0) { throw "mqadmin $Command failed" }
