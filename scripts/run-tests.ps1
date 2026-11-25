# MQ-rust 本地质量门禁（R0/R1）
# 用法：powershell -ExecutionPolicy Bypass -File scripts/run-tests.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    Write-Host "== cargo fmt =="
    cargo fmt --all -- --check
    if ($LASTEXITCODE -ne 0) { throw "cargo fmt failed" }

    Write-Host "== cargo clippy =="
    cargo clippy --workspace --all-targets -- -D warnings
    if ($LASTEXITCODE -ne 0) { throw "cargo clippy failed" }

    Write-Host "== cargo test =="
    cargo test --workspace
    if ($LASTEXITCODE -ne 0) { throw "cargo test failed" }

    Write-Host "== smoke: binaries =="
    cargo run -q -p mq-namesrv
    cargo run -q -p mq-broker
} finally {
    Pop-Location
}
