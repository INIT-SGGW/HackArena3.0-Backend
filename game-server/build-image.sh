#!/usr/bin/env bash
set -euo pipefail

export DOCKER_BUILDKIT=1 

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_CONTEXT="$(realpath "${SCRIPT_DIR}/..")"

docker build \
  --build-context "repo_context=${REPO_CONTEXT}" \
  -t game-server \
  "${SCRIPT_DIR}"