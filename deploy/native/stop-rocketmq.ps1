# 停止由 start-rocketmq.ps1 启动的 NameServer/Broker（仅按端口 + 命令行匹配）
$ErrorActionPreference = "Stop"

foreach ($port in 10911, 9876) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        $p = Get-CimInstance Win32_Process -Filter "ProcessId=$($c.OwningProcess)" -ErrorAction SilentlyContinue
        if ($p -and $p.CommandLine -match "rocketmq") {
            Write-Host "stopping pid=$($p.ProcessId) port=$port"
            Stop-Process -Id $p.ProcessId -Force
        } else {
            Write-Host "skip pid=$($c.OwningProcess) port=$port (not rocketmq)"
        }
    }
}
