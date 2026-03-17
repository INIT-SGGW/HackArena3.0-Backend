param(
    [string]$Version = "",
    [string]$ReleaseDir = "deploy/releases"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-GameServerVersion {
    $cargoToml = Join-Path $PSScriptRoot "..\game-server\Cargo.toml"
    $content = Get-Content $cargoToml -Raw
    $match = [regex]::Match($content, '(?m)^version\s*=\s*"([^"]+)"')
    if (-not $match.Success) {
        throw "Cannot read game-server version from $cargoToml"
    }
    return $match.Groups[1].Value
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$resolvedVersion = if ([string]::IsNullOrWhiteSpace($Version)) { Get-GameServerVersion } else { $Version.Trim() }
$releaseVersionDir = Join-Path $repoRoot (Join-Path $ReleaseDir "v$resolvedVersion")

if (-not (Test-Path $releaseVersionDir)) {
    throw "Release directory not found: $releaseVersionDir"
}

$zipFiles = Get-ChildItem -Path $releaseVersionDir -Filter "*.zip" -File | Sort-Object Name
if ($zipFiles.Count -eq 0) {
    throw "No zip files found in: $releaseVersionDir"
}

$lines = New-Object System.Collections.Generic.List[string]
foreach ($zip in $zipFiles) {
    $hash = (Get-FileHash -Path $zip.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $lines.Add("$hash  $($zip.Name)")
}

$outputFile = Join-Path $releaseVersionDir "SHA256SUMS.txt"
[System.IO.File]::WriteAllLines($outputFile, $lines)

Write-Host "SHA256 sums written to: $outputFile" -ForegroundColor Green
