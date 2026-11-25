param(
    [string]$Version = "0.9.0-rc.1"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$DistRoot = Join-Path $Root "dist"
$Name = "mq-rust-$Version-win64"
$Out = Join-Path $DistRoot $Name

if (Test-Path $Out) { Remove-Item -Recurse -Force $Out }
New-Item -ItemType Directory -Force -Path $Out | Out-Null

Push-Location $Root
try {
    & cargo build --release --workspace
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
} finally {
    Pop-Location
}

foreach ($exe in @("mq-broker.exe", "mq-namesrv.exe", "mq-tc.exe")) {
    Copy-Item (Join-Path $Root "target\release\$exe") (Join-Path $Out $exe)
}
Copy-Item (Join-Path $Root "README.md") $Out
Copy-Item (Join-Path $Root "CHANGELOG.md") $Out
Get-ChildItem (Join-Path $Root "docs") -Filter "*.md" | Where-Object {
    $_.Name -match "^(1[5-9]|2[0-1])-"
} | ForEach-Object {
    Copy-Item $_.FullName $Out
}
Copy-Item (Join-Path $Root "deploy") (Join-Path $Out "deploy") -Recurse

$VersionText = @"
MQ-rust $Version (win64)
Built: $(Get-Date -Format o)
Protocol: RocketMQ 4.9.8 remoting
Status: release candidate, not production 1.0
"@
Set-Content -Path (Join-Path $Out "VERSION.txt") -Value $VersionText -Encoding UTF8

$hashes = Get-ChildItem $Out -File | Where-Object { $_.Name -ne "SHA256SUMS" } | ForEach-Object {
    $h = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower()
    "$h  $($_.Name)"
}
Set-Content -Path (Join-Path $Out "SHA256SUMS") -Value $hashes -Encoding UTF8

$Zip = Join-Path $DistRoot "$Name.zip"
if (Test-Path $Zip) { Remove-Item -Force $Zip }
Compress-Archive -Path (Join-Path $Out "*") -DestinationPath $Zip
Write-Host "dist: $Out"
Write-Host "zip:  $Zip"
