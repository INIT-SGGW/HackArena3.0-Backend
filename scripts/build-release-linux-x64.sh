#!/usr/bin/env bash

set -euo pipefail

VERSION=""
TARGET="x86_64-unknown-linux-gnu"
OUTPUT_DIR="deploy/releases"
MODE="both"
SKIP_BUILD="false"

print_usage() {
  cat <<'EOF'
Usage: build-release-linux-x64.sh [options]

Options:
  --version <ver>       Package version (default: read from game-server/Cargo.toml)
  --target <triple>     Rust target triple (default: x86_64-unknown-linux-gnu)
  --output-dir <path>   Output root for releases (default: deploy/releases)
  --mode <mode>         One of: both, local, official (default: both)
  --skip-build          Do not run cargo build, only package existing binaries
  -h, --help            Show this help
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
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD="true"
      shift
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

if [[ "$MODE" != "both" && "$MODE" != "local" && "$MODE" != "official" ]]; then
  echo "Invalid --mode: $MODE (expected both|local|official)" >&2
  exit 1
fi

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

build_binary() {
  local bin_name="$1"
  local features="$2"
  echo "==> cargo build -p game-server --bin $bin_name --features $features --release --target $TARGET"
  cargo build -p game-server --bin "$bin_name" --features "$features" --release --target "$TARGET"
}

write_packaged_env() {
  local package_root="$1"
  cat > "$package_root/.env" <<'EOF'
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
EOF
}

copy_required_files() {
  local package_root="$1"
  local bin_path="$2"
  local target_release_dir="$3"
  local native_runtime_dir="$4"

  mkdir -p "$package_root"
  cp -f "$bin_path" "$package_root/"

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

  local bolids_source="$REPO_ROOT/game-server/assets/bolids"
  if [[ -d "$bolids_source" ]]; then
    local assets_target="$package_root/assets"
    mkdir -p "$assets_target"
    rm -rf "$assets_target/bolids"
    cp -R "$bolids_source" "$assets_target/bolids"
  fi

  write_packaged_env "$package_root"
}

RESOLVED_VERSION="$VERSION"
if [[ -z "$RESOLVED_VERSION" ]]; then
  RESOLVED_VERSION="$(get_game_server_version)"
fi

ARCH_LABEL="$TARGET"
TARGET_RELEASE_DIR="$REPO_ROOT/target/$TARGET/release"
RELEASE_ROOT="$REPO_ROOT/$OUTPUT_DIR"
STAGING_ROOT="$RELEASE_ROOT/_staging/v$RESOLVED_VERSION"
ARCHIVE_ROOT="$RELEASE_ROOT/v$RESOLVED_VERSION"
NATIVE_RUNTIME_DIR="$REPO_ROOT/game-engine/boink-sys/native/linux/x86_64/release"

mkdir -p "$STAGING_ROOT"
mkdir -p "$ARCHIVE_ROOT"

if [[ "$SKIP_BUILD" != "true" ]]; then
  if [[ "$MODE" == "both" || "$MODE" == "local" ]]; then
    build_binary "ha3-backend-local" "local"
  fi
  if [[ "$MODE" == "both" || "$MODE" == "official" ]]; then
    build_binary "ha3-backend-official" "official"
  fi
fi

declare -a CREATED_ARCHIVES=()

if [[ "$MODE" == "both" || "$MODE" == "local" ]]; then
  local_package_name="ha3-backend-local-$ARCH_LABEL-v$RESOLVED_VERSION"
  local_package_dir="$STAGING_ROOT/$local_package_name"
  rm -rf "$local_package_dir"

  copy_required_files \
    "$local_package_dir" \
    "$TARGET_RELEASE_DIR/ha3-backend-local" \
    "$TARGET_RELEASE_DIR" \
    "$NATIVE_RUNTIME_DIR"

  local_archive="$ARCHIVE_ROOT/$local_package_name.tar.gz"
  rm -f "$local_archive"
  tar -czf "$local_archive" -C "$local_package_dir" .
  CREATED_ARCHIVES+=("$local_archive")
fi

if [[ "$MODE" == "both" || "$MODE" == "official" ]]; then
  official_package_name="ha3-backend-official-$ARCH_LABEL-v$RESOLVED_VERSION"
  official_package_dir="$STAGING_ROOT/$official_package_name"
  rm -rf "$official_package_dir"

  copy_required_files \
    "$official_package_dir" \
    "$TARGET_RELEASE_DIR/ha3-backend-official" \
    "$TARGET_RELEASE_DIR" \
    "$NATIVE_RUNTIME_DIR"

  official_archive="$ARCHIVE_ROOT/$official_package_name.tar.gz"
  rm -f "$official_archive"
  tar -czf "$official_archive" -C "$official_package_dir" .
  CREATED_ARCHIVES+=("$official_archive")
fi

echo ""
echo "Release packages created:"
for archive in "${CREATED_ARCHIVES[@]}"; do
  echo "  $archive"
done
