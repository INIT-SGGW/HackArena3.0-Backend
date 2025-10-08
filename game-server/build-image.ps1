$env:DOCKER_BUILDKIT = "1"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$repoContext = Resolve-Path (Join-Path $scriptDir "..")

docker build `
    --build-context repo_context=$repoContext `
    -t game-server `
    "$scriptDir"