# HackArena3.0-Server

### Generating local protos

To use local `.proto` files from a separate repository, set the `PROTO_PATH` environment variable to the **root folder** containing your proto sources (e.g. `race/v1`) and enable the `proto-local` feature.
Example (Windows PowerShell):

```powershell
$env:PROTO_PATH = "..\..\HackArena3.0-Proto\proto"
cargo run --bin game-server --features proto-local
```

On Linux/macOS:

```bash
export PROTO_PATH="../../HackArena3.0-Proto/proto"
cargo run --bin game-server --features proto-local
```

The build script in `proto/build.rs` will automatically detect `.proto` files under `$PROTO_PATH`, generate Rust code with a vendored `protoc`, and place them in `proto/gen/` (ignored by Git).
