param(
    [string]$Version = "",
    [string]$Target = "x86_64-pc-windows-msvc",
    [string]$OutputDir = "deploy/releases",
    [switch]$SkipBuild,
    [switch]$SkipFrontendBuild,
    [switch]$FrontendDocker,
    [string]$FrontendDockerImage = "node:22-alpine",
    [string]$FrontendPnpmVersion = "10.19.0",
    [string]$FrontendNodeModulesVolume = "ha3_fe_nm",
    [string]$FrontendPnpmStoreVolume = "ha3_fe_store"
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

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $false)][string]$WorkingDir = ""
    )

    Write-Host "==> $Command" -ForegroundColor Cyan
    if ([string]::IsNullOrWhiteSpace($WorkingDir)) {
        Invoke-Expression $Command
    }
    else {
        Push-Location $WorkingDir
        try {
            Invoke-Expression $Command
        }
        finally {
            Pop-Location
        }
    }
}

function Assert-CommandAvailable {
    param(
        [Parameter(Mandatory = $true)][string]$CommandName,
        [Parameter(Mandatory = $false)][string]$Hint = ""
    )

    if (-not (Get-Command -Name $CommandName -ErrorAction SilentlyContinue)) {
        if ([string]::IsNullOrWhiteSpace($Hint)) {
            throw "Required command '$CommandName' is not available in PATH."
        }
        throw "Required command '$CommandName' is not available in PATH. $Hint"
    }
}

function Invoke-FrontendDockerBuild {
    param(
        [Parameter(Mandatory = $true)][string]$FrontendRoot,
        [Parameter(Mandatory = $true)][string]$DockerImage,
        [Parameter(Mandatory = $true)][string]$PnpmVersion,
        [Parameter(Mandatory = $true)][string]$NodeModulesVolume,
        [Parameter(Mandatory = $true)][string]$PnpmStoreVolume
    )

    $resolvedFrontendRoot = (Resolve-Path $FrontendRoot).Path
    $dockerShellCommand = "corepack enable && corepack prepare pnpm@$PnpmVersion --activate && pnpm config set store-dir /pnpm-store && pnpm install --frozen-lockfile && pnpm build"

    $dockerArgs = @(
        "run",
        "--rm",
        "-v", "$resolvedFrontendRoot`:/app",
        "-v", "$NodeModulesVolume`:/app/node_modules",
        "-v", "$PnpmStoreVolume`:/pnpm-store",
        "-w", "/app",
        $DockerImage,
        "sh",
        "-lc",
        $dockerShellCommand
    )

    Write-Host "==> docker $($dockerArgs -join ' ')" -ForegroundColor Cyan
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Docker frontend build failed with exit code $LASTEXITCODE."
    }
}

function Write-PackagedStandaloneEnv {
    param(
        [Parameter(Mandatory = $true)][string]$PackageRoot
    )

    $envContent = @'
# Application environment: development | preprod | production
APP_ENV=development

# gRPC server listen address
LISTEN_ADDR=0.0.0.0:50051

# Standalone frontend HTTP server
FRONTEND_ENABLE=true
FRONTEND_LISTEN_ADDR=0.0.0.0:8080
FRONTEND_DIR=frontend

# Logging
RUST_LOG=warn,boink=info,tonic_web=info,game_server=info,game_engine=info

# Standalone bundled assets
TRACKS_DIR=assets/tracks
BOLIDS_DIR=assets/bolids

# Optional simulation tickrate
# SIMULATION_HZ=60
'@

    Set-Content -Path (Join-Path $PackageRoot ".env.standalone") -Value $envContent -NoNewline
}

function Copy-StandalonePackageFiles {
    param(
        [Parameter(Mandatory = $true)][string]$PackageRoot,
        [Parameter(Mandatory = $true)][string]$ExePath,
        [Parameter(Mandatory = $true)][string]$TargetReleaseDir,
        [Parameter(Mandatory = $true)][string]$NativeRuntimeDir,
        [Parameter(Mandatory = $true)][string]$TracksSource,
        [Parameter(Mandatory = $true)][string]$BolidsSource,
        [Parameter(Mandatory = $true)][string]$FrontendDistSource
    )

    if (Test-Path $PackageRoot) {
        Remove-Item $PackageRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $PackageRoot | Out-Null

    Copy-Item $ExePath -Destination $PackageRoot -Force

    Get-ChildItem $TargetReleaseDir -Filter "*.dll" -File | ForEach-Object {
        Copy-Item $_.FullName -Destination $PackageRoot -Force
    }

    if (Test-Path $NativeRuntimeDir) {
        Get-ChildItem $NativeRuntimeDir -Filter "*.dll" -File | ForEach-Object {
            Copy-Item $_.FullName -Destination $PackageRoot -Force
        }
    }

    $assetsTarget = Join-Path $PackageRoot "assets"
    New-Item -ItemType Directory -Force -Path $assetsTarget | Out-Null
    Copy-Item $TracksSource -Destination (Join-Path $assetsTarget "tracks") -Recurse -Force
    Copy-Item $BolidsSource -Destination (Join-Path $assetsTarget "bolids") -Recurse -Force

    Copy-Item $FrontendDistSource -Destination (Join-Path $PackageRoot "frontend") -Recurse -Force

    Write-PackagedStandaloneEnv -PackageRoot $PackageRoot
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$resolvedVersion = if ([string]::IsNullOrWhiteSpace($Version)) { Get-GameServerVersion } else { $Version.Trim() }
$archLabel = $Target

$frontendRoot = Join-Path $repoRoot "third_party\HackArena3.0-Frontend"
$frontendPackageJson = Join-Path $frontendRoot "package.json"
$frontendDistDir = Join-Path $frontendRoot "dist"

if (-not (Test-Path $frontendRoot)) {
    throw "Frontend source missing: $frontendRoot. Ensure submodule/content is already present (script does not run git submodule commands)."
}
if (-not (Test-Path $frontendPackageJson)) {
    throw "Frontend package.json missing: $frontendPackageJson"
}

$tracksSource = Join-Path $repoRoot "game-server\assets\tracks"
$bolidsSource = Join-Path $repoRoot "game-server\assets\bolids"
if (-not (Test-Path $tracksSource)) {
    throw "Tracks source missing: $tracksSource"
}
if (-not (Test-Path $bolidsSource)) {
    throw "Bolids source missing: $bolidsSource"
}

$targetReleaseDir = Join-Path $repoRoot "target\$Target\release"
$releaseRoot = Join-Path $repoRoot $OutputDir
$stagingRoot = Join-Path $releaseRoot "_staging\v$resolvedVersion"
$zipRoot = Join-Path $releaseRoot "v$resolvedVersion"
$nativeRuntimeDir = Join-Path $repoRoot "game-engine\boink-sys\native\windows\x86_64\release"

New-Item -ItemType Directory -Force -Path $stagingRoot | Out-Null
New-Item -ItemType Directory -Force -Path $zipRoot | Out-Null

if (-not $SkipBuild) {
    Assert-CommandAvailable -CommandName "cargo" -Hint "Install Rust toolchain and retry."

    if ($SkipFrontendBuild) {
        Write-Host "==> Skipping frontend build; using existing dist: $frontendDistDir" -ForegroundColor Cyan
    } else {
        if ($FrontendDocker) {
            Assert-CommandAvailable -CommandName "docker" -Hint "Install Docker and retry, or run script without -FrontendDocker."
            Invoke-FrontendDockerBuild `
                -FrontendRoot $frontendRoot `
                -DockerImage $FrontendDockerImage `
                -PnpmVersion $FrontendPnpmVersion `
                -NodeModulesVolume $FrontendNodeModulesVolume `
                -PnpmStoreVolume $FrontendPnpmStoreVolume
        }
        else {
            Assert-CommandAvailable -CommandName "pnpm" -Hint "Install pnpm and retry, or run script with -FrontendDocker."
            Invoke-Step -Command "pnpm install --frozen-lockfile" -WorkingDir $frontendRoot
            Invoke-Step -Command "pnpm build" -WorkingDir $frontendRoot
        }
    }

    Invoke-Step -Command "cargo build -p game-server --bin ha3-standalone --features standalone --release --target $Target"
}

if (-not (Test-Path $frontendDistDir)) {
    throw "Frontend dist missing: $frontendDistDir. Run frontend build first, omit -SkipFrontendBuild, or use -FrontendDocker."
}

$standaloneExePath = Join-Path $targetReleaseDir "ha3-standalone.exe"
if (-not (Test-Path $standaloneExePath)) {
    throw "Standalone executable missing: $standaloneExePath. Run cargo build first or execute this script without -SkipBuild."
}

$packageName = "ha3-standalone-$archLabel-v$resolvedVersion"
$packageDir = Join-Path $stagingRoot $packageName

Copy-StandalonePackageFiles `
    -PackageRoot $packageDir `
    -ExePath $standaloneExePath `
    -TargetReleaseDir $targetReleaseDir `
    -NativeRuntimeDir $nativeRuntimeDir `
    -TracksSource $tracksSource `
    -BolidsSource $bolidsSource `
    -FrontendDistSource $frontendDistDir

$zipPath = Join-Path $zipRoot "$packageName.zip"
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}
Compress-Archive -Path (Join-Path $packageDir "*") -DestinationPath $zipPath -CompressionLevel Optimal

Write-Host ""
Write-Host "Standalone release package created:" -ForegroundColor Green
Write-Host "  $zipPath"
