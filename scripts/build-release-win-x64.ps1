param(
    [string]$Version = "",
    [string]$Target = "x86_64-pc-windows-msvc",
    [string]$OutputDir = "deploy/releases",
    [ValidateSet("both", "local", "official")]
    [string]$Mode = "both",
    [switch]$SkipBuild
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

function Build-Binary {
    param(
        [Parameter(Mandatory = $true)][string]$BinName,
        [Parameter(Mandatory = $true)][string]$Features,
        [Parameter(Mandatory = $true)][string]$TargetTriple
    )

    $cmd = "cargo build -p game-server --bin $BinName --features $Features --release --target $TargetTriple"
    Write-Host "==> $cmd" -ForegroundColor Cyan
    Invoke-Expression $cmd
}

function Write-PackagedEnv {
    param(
        [Parameter(Mandatory = $true)][string]$PackageRoot
    )

    $envContent = @'
# Application environment: development | preprod | production
APP_ENV=production

# gRPC server listen address
LISTEN_ADDR=0.0.0.0:50052

# Logging
RUST_LOG=warn,boink=info,tonic_web=info,game_server=info,game_engine=info,game_server::config=debug

# Allowed CORS origins
CORS_ALLOWED_ORIGINS=https://ha3-game.hackarena.pl

API_URL=https://ha3-api.hackarena.pl

# Game-token JWT settings used by `ha3-backend-local`
HPS_ENDPOINT=https://platform-grpc.hackarena.pl
GAME_JWT_LOCAL_AUDIENCE=ha3-local
GAME_JWT_LOCAL_ISSUERS=ha3-dev-auth
'@

    Set-Content -Path (Join-Path $PackageRoot ".env") -Value $envContent -NoNewline
}

function Copy-RequiredFiles {
    param(
        [Parameter(Mandatory = $true)][string]$PackageRoot,
        [Parameter(Mandatory = $true)][string]$ExePath,
        [Parameter(Mandatory = $true)][string]$TargetReleaseDir,
        [Parameter(Mandatory = $true)][ValidateSet("local", "official")][string]$RuntimeKind,
        [Parameter(Mandatory = $false)][string]$NativeRuntimeDir = ""
    )

    New-Item -ItemType Directory -Force -Path $PackageRoot | Out-Null

    Copy-Item $ExePath -Destination $PackageRoot -Force

    Get-ChildItem $TargetReleaseDir -Filter "*.dll" -File | ForEach-Object {
        Copy-Item $_.FullName -Destination $PackageRoot -Force
    }

    if (-not [string]::IsNullOrWhiteSpace($NativeRuntimeDir) -and (Test-Path $NativeRuntimeDir)) {
        Get-ChildItem $NativeRuntimeDir -Filter "*.dll" -File | ForEach-Object {
            Copy-Item $_.FullName -Destination $PackageRoot -Force
        }
    }

    $bolidsSource = Join-Path $PSScriptRoot "..\game-server\assets\bolids"
    if (Test-Path $bolidsSource) {
        $assetsTarget = Join-Path $PackageRoot "assets"
        New-Item -ItemType Directory -Force -Path $assetsTarget | Out-Null
        Copy-Item $bolidsSource -Destination (Join-Path $assetsTarget "bolids") -Recurse -Force
    }

    Write-PackagedEnv -PackageRoot $PackageRoot
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$resolvedVersion = if ([string]::IsNullOrWhiteSpace($Version)) { Get-GameServerVersion } else { $Version.Trim() }
$archLabel = $Target

$targetReleaseDir = Join-Path $repoRoot "target\$Target\release"
$releaseRoot = Join-Path $repoRoot $OutputDir
$stagingRoot = Join-Path $releaseRoot "_staging\v$resolvedVersion"
$zipRoot = Join-Path $releaseRoot "v$resolvedVersion"
$nativeRuntimeDir = Join-Path $repoRoot "game-engine\boink-sys\native\windows\x86_64\release"

New-Item -ItemType Directory -Force -Path $stagingRoot | Out-Null
New-Item -ItemType Directory -Force -Path $zipRoot | Out-Null

if (-not $SkipBuild) {
    if ($Mode -eq "both" -or $Mode -eq "local") {
        Build-Binary -BinName "ha3-backend-local" -Features "local" -TargetTriple $Target
    }
    if ($Mode -eq "both" -or $Mode -eq "official") {
        Build-Binary -BinName "ha3-backend-official" -Features "official" -TargetTriple $Target
    }
}

$createdZips = New-Object System.Collections.Generic.List[string]

if ($Mode -eq "both" -or $Mode -eq "local") {
    $localPackageName = "ha3-backend-local-$archLabel-v$resolvedVersion"
    $localPackageDir = Join-Path $stagingRoot $localPackageName
    if (Test-Path $localPackageDir) { Remove-Item $localPackageDir -Recurse -Force }

    Copy-RequiredFiles `
        -PackageRoot $localPackageDir `
        -ExePath (Join-Path $targetReleaseDir "ha3-backend-local.exe") `
        -TargetReleaseDir $targetReleaseDir `
        -RuntimeKind "local" `
        -NativeRuntimeDir $nativeRuntimeDir

    $localZip = Join-Path $zipRoot "$localPackageName.zip"
    if (Test-Path $localZip) { Remove-Item $localZip -Force }
    Compress-Archive -Path (Join-Path $localPackageDir "*") -DestinationPath $localZip -CompressionLevel Optimal
    $createdZips.Add($localZip)
}

if ($Mode -eq "both" -or $Mode -eq "official") {
    $officialPackageName = "ha3-backend-official-$archLabel-v$resolvedVersion"
    $officialPackageDir = Join-Path $stagingRoot $officialPackageName
    if (Test-Path $officialPackageDir) { Remove-Item $officialPackageDir -Recurse -Force }

    Copy-RequiredFiles `
        -PackageRoot $officialPackageDir `
        -ExePath (Join-Path $targetReleaseDir "ha3-backend-official.exe") `
        -TargetReleaseDir $targetReleaseDir `
        -RuntimeKind "official" `
        -NativeRuntimeDir $nativeRuntimeDir

    $officialZip = Join-Path $zipRoot "$officialPackageName.zip"
    if (Test-Path $officialZip) { Remove-Item $officialZip -Force }
    Compress-Archive -Path (Join-Path $officialPackageDir "*") -DestinationPath $officialZip -CompressionLevel Optimal
    $createdZips.Add($officialZip)
}

Write-Host ""
Write-Host "Release packages created:" -ForegroundColor Green
foreach ($zip in $createdZips) {
    Write-Host "  $zip"
}
