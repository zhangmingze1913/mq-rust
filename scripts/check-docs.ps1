# Release consistency checks for MQ-rust docs and distribution.
param(
    [string]$Root = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
$errors = New-Object System.Collections.Generic.List[string]

function Read-Doc([string]$pattern) {
    $file = Get-ChildItem (Join-Path $Root "docs") -Filter $pattern | Select-Object -First 1
    if (-not $file) { throw "missing doc $pattern" }
    return Get-Content $file.FullName -Raw -Encoding UTF8
}

$readme = Get-Content (Join-Path $Root "README.md") -Raw -Encoding UTF8
$doc15 = Read-Doc "15-*.md"
$doc16 = Read-Doc "16-*.md"
$doc20 = Read-Doc "20-*.md"
$doc21 = Read-Doc "21-*.md"
$changelog = Get-Content (Join-Path $Root "CHANGELOG.md") -Raw -Encoding UTF8

foreach ($pair in @(
    @("README", $readme),
    @("15", $doc15),
    @("16", $doc16),
    @("20", $doc20)
)) {
    if ($pair[1] -notmatch "83 ") {
        $errors.Add("$($pair[0]) does not mention 83 tests")
    }
}
if ($changelog -notmatch "83 ") { $errors.Add("CHANGELOG does not mention 83 tests") }

$zip = Join-Path $Root "dist\mq-rust-0.9.0-rc.1-win64.zip"
if (-not (Test-Path $zip)) {
    $errors.Add("distribution zip missing")
} else {
    $hash = (Get-FileHash $zip -Algorithm SHA256).Hash
    foreach ($pair in @(@("20", $doc20), @("16", $doc16), @("21", $doc21))) {
        if ($pair[1] -notmatch $hash) {
            $errors.Add("$($pair[0]) SHA256 does not match $hash")
        }
    }
}

if ($readme -notmatch "R166") { $errors.Add("README does not mention R166") }
if ($doc20 -notmatch "R166") { $errors.Add("20 does not mention R166") }

$mdFiles = @(Get-ChildItem (Join-Path $Root "docs") -Recurse -Filter *.md)
$mdFiles += Get-Item (Join-Path $Root "README.md")
$mdFiles += Get-Item (Join-Path $Root "CHANGELOG.md")
foreach ($file in $mdFiles) {
    $text = Get-Content $file.FullName -Raw -Encoding UTF8
    foreach ($match in [regex]::Matches($text, '\]\(([^)]+)\)')) {
        $target = $match.Groups[1].Value.Trim()
        if ($target -match '^(https?://|#|mailto:)') { continue }
        $target = $target.Split('#')[0]
        if (-not $target) { continue }
        $resolved = Join-Path $file.DirectoryName $target
        if (-not (Test-Path $resolved)) {
            $errors.Add("broken link in $($file.Name): $target")
        }
    }
}

if ($errors.Count -gt 0) {
    $errors | ForEach-Object { Write-Host "FAIL: $_" }
    exit 1
}
Write-Host "docs consistency: OK"
Write-Host "sha256: $hash"
