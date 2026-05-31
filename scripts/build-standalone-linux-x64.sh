#!/usr/bin/env bash

set -euo pipefail

VERSION=""
TARGET="x86_64-unknown-linux-gnu"
OUTPUT_DIR="deploy/releases"
SKIP_BUILD="false"
SKIP_FRONTEND_BUILD="false"
FRONTEND_DOCKER="false"
FRONTEND_DOCKER_IMAGE="node:22-alpine"
FRONTEND_PNPM_VERSION="10.19.0"

print_usage() {
  cat <<'EOF'
Usage: build-standalone-linux-x64.sh [options]

Options:
  --version <ver>             Package version (default: read from game-server/Cargo.toml)
  --target <triple>           Rust target triple (default: x86_64-unknown-linux-gnu)
  --output-dir <path>         Output root for releases (default: deploy/releases)
  --skip-build                Do not run frontend/cargo build, only package existing artifacts
  --skip-frontend-build       Do not rebuild frontend; use existing dist
  --frontend-docker           Build frontend in Docker instead of local pnpm
  --frontend-docker-image <i> Docker image for frontend build (default: node:22-alpine)
  --frontend-pnpm-version <v> pnpm version for Docker frontend build (default: 10.19.0)
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD="true"
      shift
      ;;
    --skip-frontend-build)
      SKIP_FRONTEND_BUILD="true"
      shift
      ;;
    --frontend-docker)
      FRONTEND_DOCKER="true"
      shift
      ;;
    --frontend-docker-image)
      FRONTEND_DOCKER_IMAGE="${2:-}"
      shift 2
      ;;
    --frontend-pnpm-version)
      FRONTEND_PNPM_VERSION="${2:-}"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

get_game_server_version() {
  local cargo_toml="$REPO_ROOT/game-server/Cargo.toml"
  local line
  line="$(grep -m1 -E '^version[[:space:]]*=' "$cargo_toml" || true)"
  if [[ -z "$line" ]]; then
    echo "Cannot read game-server version from $cargo_toml" >&2
    exit 1
  fi
  echo "$line" | sed -E 's/^version[[:space:]]*=[[:space:]]*"([^"]+)".*$/\1/'
}

assert_command_available() {
  local command_name="$1"
  local hint="${2:-}"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    if [[ -n "$hint" ]]; then
      echo "Required command '$command_name' is not available in PATH. $hint" >&2
    else
      echo "Required command '$command_name' is not available in PATH." >&2
    fi
    exit 1
  fi
}

run_step() {
  local workdir="$1"
  shift
  echo "==> $*"
  (
    cd "$workdir"
    "$@"
  )
}

build_frontend_locally() {
  local frontend_root="$1"
  local pnpm_version="$2"

  if command -v pnpm >/dev/null 2>&1; then
    run_step "$frontend_root" pnpm install --frozen-lockfile
    run_step "$frontend_root" pnpm build
    return
  fi

  if command -v corepack >/dev/null 2>&1; then
    run_step "$frontend_root" corepack enable
    run_step "$frontend_root" corepack prepare "pnpm@$pnpm_version" --activate
    run_step "$frontend_root" pnpm install --frozen-lockfile
    run_step "$frontend_root" pnpm build
    return
  fi

  echo "Required command 'pnpm' is not available in PATH. Install pnpm, ensure Node ships corepack, or run script with --frontend-docker." >&2
  exit 1
}

build_frontend_in_docker() {
  local frontend_root="$1"
  local docker_image="$2"
  local pnpm_version="$3"
  local docker_shell_command
  docker_shell_command="corepack enable && corepack prepare pnpm@$pnpm_version --activate && pnpm install --frozen-lockfile && pnpm build"

  echo "==> docker run --rm -v $frontend_root:/app -w /app $docker_image sh -lc \"$docker_shell_command\""
  docker run --rm \
    -v "$frontend_root:/app" \
    -w /app \
    "$docker_image" \
    sh -lc "$docker_shell_command"
}

copy_versioned_standalone_config() {
  local package_root="$1"
  local template_path="$REPO_ROOT/standalone.toml.default"
  if [[ ! -f "$template_path" ]]; then
    echo "Standalone config template missing: $template_path" >&2
    exit 1
  fi
  cp -f "$template_path" "$package_root/standalone.toml"
}

write_launcher() {
  local package_root="$1"
  local launcher_path="$package_root/run-ha3-standalone.sh"
  cat > "$launcher_path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export LD_LIBRARY_PATH="$DIR:${LD_LIBRARY_PATH:-}"
export BOINK_NATIVE_LIB_DIR="$DIR"
exec "$DIR/ha3-standalone" "$@"
EOF
  chmod +x "$launcher_path"
}

normalize_linux_boink_runtime_aliases() {
  local package_root="$1"
  local versioned_name
  versioned_name="$(
    find "$package_root" -maxdepth 1 -type f -name 'libboink.so.*.*' -printf '%f\n' \
      | sort -V \
      | tail -n 1
  )"

  if [[ -z "$versioned_name" ]]; then
    echo "Missing versioned Linux boink runtime in $package_root (expected libboink.so.<major>.<minor>...)" >&2
    exit 1
  fi

  if [[ ! "$versioned_name" =~ ^libboink\.so\.([0-9]+)\.[0-9].*$ ]]; then
    echo "Invalid Linux boink runtime filename: $versioned_name" >&2
    exit 1
  fi

  local major="${BASH_REMATCH[1]}"
  local versioned_path="$package_root/$versioned_name"

  cp -f "$versioned_path" "$package_root/libboink.so.$major"
  cp -f "$versioned_path" "$package_root/libboink.so"
}

copy_standalone_package_files() {
  local package_root="$1"
  local exe_path="$2"
  local target_release_dir="$3"
  local native_runtime_dir="$4"
  local tracks_source="$5"
  local bolids_source="$6"
  local frontend_dist_source="$7"

  rm -rf "$package_root"
  mkdir -p "$package_root"

  cp -f "$exe_path" "$package_root/"

  if [[ -d "$target_release_dir" ]]; then
    find "$target_release_dir" -maxdepth 1 \
      \( -type f -o -type l \) \
      \( -name "*.so" -o -name "*.so.*" \) \
      -exec cp -a {} "$package_root/" \;
  fi

  if [[ -d "$native_runtime_dir" ]]; then
    find "$native_runtime_dir" -maxdepth 1 \
      \( -type f -o -type l \) \
      \( -name "*.so" -o -name "*.so.*" \) \
      -exec cp -a {} "$package_root/" \;
  fi

  normalize_linux_boink_runtime_aliases "$package_root"

  mkdir -p "$package_root/assets"
  cp -R "$tracks_source" "$package_root/assets/tracks"
  cp -R "$bolids_source" "$package_root/assets/bolids"
  cp -R "$frontend_dist_source" "$package_root/frontend"

  copy_versioned_standalone_config "$package_root"
  write_launcher "$package_root"
}

RESOLVED_VERSION="$VERSION"
if [[ -z "$RESOLVED_VERSION" ]]; then
  RESOLVED_VERSION="$(get_game_server_version)"
fi

ARCH_LABEL="$TARGET"
FRONTEND_ROOT="$REPO_ROOT/third_party/HackArena3.0-Frontend"
FRONTEND_PACKAGE_JSON="$FRONTEND_ROOT/package.json"
FRONTEND_DIST_DIR="$FRONTEND_ROOT/dist"
TRACKS_SOURCE="$REPO_ROOT/game-server/assets/tracks"
BOLIDS_SOURCE="$REPO_ROOT/game-server/assets/bolids"
TARGET_RELEASE_DIR="$REPO_ROOT/target/$TARGET/release"
RELEASE_ROOT="$REPO_ROOT/$OUTPUT_DIR"
STAGING_ROOT="$RELEASE_ROOT/_staging/v$RESOLVED_VERSION"
ARCHIVE_ROOT="$RELEASE_ROOT/v$RESOLVED_VERSION"
NATIVE_RUNTIME_DIR="$REPO_ROOT/game-engine/boink-sys/native/linux/x86_64/release"

if [[ ! -d "$FRONTEND_ROOT" ]]; then
  echo "Frontend source missing: $FRONTEND_ROOT. Ensure submodule/content is already present." >&2
  exit 1
fi
if [[ ! -f "$FRONTEND_PACKAGE_JSON" ]]; then
  echo "Frontend package.json missing: $FRONTEND_PACKAGE_JSON" >&2
  exit 1
fi
if [[ ! -d "$TRACKS_SOURCE" ]]; then
  echo "Tracks source missing: $TRACKS_SOURCE" >&2
  exit 1
fi
if [[ ! -d "$BOLIDS_SOURCE" ]]; then
  echo "Bolids source missing: $BOLIDS_SOURCE" >&2
  exit 1
fi

mkdir -p "$STAGING_ROOT"
mkdir -p "$ARCHIVE_ROOT"

if [[ "$SKIP_BUILD" != "true" ]]; then
  assert_command_available "cargo" "Install Rust toolchain and retry."

  if [[ "$SKIP_FRONTEND_BUILD" == "true" ]]; then
    echo "==> Skipping frontend build; using existing dist: $FRONTEND_DIST_DIR"
  else
    if [[ "$FRONTEND_DOCKER" == "true" ]]; then
      assert_command_available "docker" "Install Docker and retry, or run script without --frontend-docker."
      build_frontend_in_docker "$FRONTEND_ROOT" "$FRONTEND_DOCKER_IMAGE" "$FRONTEND_PNPM_VERSION"
    else
      build_frontend_locally "$FRONTEND_ROOT" "$FRONTEND_PNPM_VERSION"
    fi
  fi

  run_step "$REPO_ROOT" cargo build -p game-server --bin ha3-standalone --features standalone --release --target "$TARGET"
fi

if [[ ! -d "$FRONTEND_DIST_DIR" ]]; then
  echo "Frontend dist missing: $FRONTEND_DIST_DIR. Run frontend build first or omit --skip-build." >&2
  exit 1
fi

STANDALONE_EXE_PATH="$TARGET_RELEASE_DIR/ha3-standalone"
if [[ ! -f "$STANDALONE_EXE_PATH" ]]; then
  echo "Standalone executable missing: $STANDALONE_EXE_PATH. Run cargo build first or execute this script without --skip-build." >&2
  exit 1
fi

PACKAGE_NAME="ha3-standalone-$ARCH_LABEL-v$RESOLVED_VERSION"
PACKAGE_DIR="$STAGING_ROOT/$PACKAGE_NAME"

copy_standalone_package_files \
  "$PACKAGE_DIR" \
  "$STANDALONE_EXE_PATH" \
  "$TARGET_RELEASE_DIR" \
  "$NATIVE_RUNTIME_DIR" \
  "$TRACKS_SOURCE" \
  "$BOLIDS_SOURCE" \
  "$FRONTEND_DIST_DIR"

ARCHIVE_PATH="$ARCHIVE_ROOT/$PACKAGE_NAME.tar.gz"
rm -f "$ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$PACKAGE_DIR" .

echo ""
echo "Standalone release package created:"
echo "  $ARCHIVE_PATH"
