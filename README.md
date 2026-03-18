# HackArena3.0-Server

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
