use std::{
    env, fs,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(proto_mode_local)");
    println!("cargo:rustc-check-cfg=cfg(proto_mode_published)");

    let use_local = env::var("CARGO_FEATURE_PROTO_LOCAL").is_ok()
        || env::var("PROTO_LOCAL").map(|v| v == "1").unwrap_or(false);

    println!("cargo:rerun-if-env-changed=PROTO_LOCAL");
    println!("cargo:rerun-if-env-changed=PROTO_PATH");
    println!("cargo:rerun-if-changed=build.rs");

    if !use_local {
        println!("cargo:rustc-cfg=proto_mode_published");
        println!("cargo:warning=mode=PUBLISHED");
        return Ok(());
    }

    println!("cargo:rustc-cfg=proto_mode_local");
    println!("cargo:warning=mode=LOCAL");

    let proto_root = PathBuf::from(env::var("PROTO_PATH").map_err(
        |_| "PROTO_PATH is required in proto-local mode. Set env to the root of your proto files",
    )?)
    .canonicalize()?;

    println!("cargo:rerun-if-changed={}", proto_root.display());

    let subdirs = ["race/v1"];
    let protos = collect_proto_files(&proto_root, &subdirs)?;
    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe { env::set_var("PROTOC", protoc_path.as_os_str()) };

    let include_dir = normalize_path(&proto_root);
    let files: Vec<String> = protos.iter().map(normalize_path).collect();

    tonic_prost_build::configure()
        .build_server(true)
        .bytes(".")
        .compile_protos(&files, &[include_dir])?;

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let gen_dir = Path::new("gen");
    let _ = fs::remove_dir_all(gen_dir);
    fs::create_dir_all(gen_dir)?;
    for entry in fs::read_dir(&out_dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().ends_with(".rs") {
            fs::copy(entry.path(), gen_dir.join(entry.file_name()))?;
        }
    }

    println!("cargo:warning=generated {} files", files.len());

    Ok(())
}

fn collect_proto_files(
    root: &Path,
    subdirs: &[&str],
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for sd in subdirs {
        for e in WalkDir::new(root.join(sd)).into_iter().flatten() {
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("proto") {
                out.push(p.to_path_buf());
            }
        }
    }

    if out.is_empty() {
        Err("No .proto files found".into())
    } else {
        Ok(out)
    }
}

fn normalize_path<P: AsRef<Path>>(p: P) -> String {
    let s = p.as_ref().to_string_lossy();
    let s = s.strip_prefix(r"\\?\").unwrap_or(&s);
    s.replace('\\', "/")
}
