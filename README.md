# HackArena 3.0 Platform

### Generating local protos

Proto definitions are sourced from git submodule at `third_party/HackArean3.0-Proto` (files under `third_party/HackArean3.0-Proto/proto`).
Initialize or update submodules after clone:

```powershell
git submodule update --init --recursive
```

Then run backend:

```bash
cargo run --bin ha3-backend-local --features local
cargo run --bin ha3-backend-official --features official
```

The build script in `proto/build.rs` reads `.proto` files from `third_party/HackArean3.0-Proto/proto`,
generates Rust code with vendored `protoc`, and writes outputs to `proto/gen/` (ignored by Git).

### Standalone release package (Windows x64) - manual build

This section describes building the standalone package manually on your machine.

Script: `scripts/build-standalone-win-x64.ps1`

What it does:
- builds frontend from `third_party/HackArena3.0-Frontend` (`pnpm install --frozen-lockfile` + `pnpm build`)
- builds backend binary `ha3-standalone.exe` (`--features standalone`)
- packages one zip containing:
  - `ha3-standalone.exe`
  - `ha3-standalone-update.exe`
  - runtime DLLs
  - `assets/tracks` and `assets/bolids`
  - `frontend` (from frontend `dist`)
  - `standalone.toml` with bundled asset/frontend paths

Important:
- the script does not run any git/submodule commands
- frontend sources must already exist in `third_party/HackArena3.0-Frontend`
- `standalone.toml` is the main user config file for packaged standalone releases
- `log_level` in `standalone.toml` controls user-facing log verbosity: `minimal`, `verbose`, `debug`, `info`, `warn`, `error`
- `minimal` keeps warnings/errors plus the key startup messages such as config path and browser URL
- `update_check_cache_ttl_minutes` in `standalone.toml` controls GitHub update-check metadata caching (default: `30`)
- advanced overrides can still be provided through real process environment variables

Standalone updater:
- on startup, `ha3-standalone.exe` can check GitHub Releases for a newer standalone package before booting the current version
- `.\ha3-standalone.exe --update-to v0.2.0-beta.9` updates to an exact release tag before startup
- `.\ha3-standalone.exe --ignore-update-cache` forces a fresh GitHub metadata check for that run
- updater cache/state lives under `%LOCALAPPDATA%\HackArena\3_0\standalone\update-cache`
- automatic update-check failures never block the current standalone from starting

Example:

```powershell
.\scripts\build-standalone-win-x64.ps1
```

If you want frontend build in Docker (and backend build locally in Rust), use:

```powershell
.\scripts\build-standalone-win-x64.ps1 -FrontendDocker
```

If frontend `dist/` already exists and you only want to rebuild backend and package, use:

```powershell
.\scripts\build-standalone-win-x64.ps1 -SkipFrontendBuild
```

Skip all build steps and only package existing artifacts:

```powershell
.\scripts\build-standalone-win-x64.ps1 -SkipBuild
```

After unpacking the release zip, run:

```powershell
.\ha3-standalone.exe
```

If you want to change ports or asset/frontend paths, edit:

```
standalone.toml
```

Default standalone endpoints:
- gRPC/gRPC-web: `0.0.0.0:50051` (`LISTEN_ADDR`)
- frontend HTTP: `0.0.0.0:8080` (`FRONTEND_LISTEN_ADDR`)

Then open browser:
- `http://localhost:8080`

Standalone GitHub release requirement:
- each published standalone release must include `ha3-standalone-x86_64-pc-windows-msvc-v<version>.zip`
- the same GitHub release must also include `SHA256SUMS.txt`
