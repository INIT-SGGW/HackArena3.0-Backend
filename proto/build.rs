use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let (ha3_proto_root, hackarena_proto_root) = resolve_proto_roots()?;
    println!("cargo:rerun-if-changed={}", ha3_proto_root.display());
    println!("cargo:rerun-if-changed={}", hackarena_proto_root.display());

    let race_v1_dir = ha3_proto_root.join("race").join("v1");
    let weather_v1_dir = ha3_proto_root.join("weather").join("v1");
    let achievement_v1_dir = ha3_proto_root.join("achievement").join("v1");
    let build_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("build")
        .join("v1");
    let submission_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("submission")
        .join("v1");
    let platform_common_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("platform")
        .join("common")
        .join("v1");
    let platform_teams_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("platform")
        .join("teams")
        .join("v1");
    ensure_proto_dir(&race_v1_dir, "race/v1")?;
    ensure_proto_dir(&weather_v1_dir, "weather/v1")?;
    ensure_proto_dir(&achievement_v1_dir, "achievement/v1")?;
    ensure_proto_dir(&build_v1_dir, "hackarena/build/v1")?;
    ensure_proto_dir(&submission_v1_dir, "hackarena/submission/v1")?;
    ensure_proto_dir(&platform_common_v1_dir, "hackarena/platform/common/v1")?;
    ensure_proto_dir(&platform_teams_v1_dir, "hackarena/platform/teams/v1")?;

    let mut protos = Vec::new();
    protos.extend(collect_proto_files_in_dir(&race_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&weather_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&achievement_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&build_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&submission_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&platform_common_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&platform_teams_v1_dir)?);
    if protos.is_empty() {
        return Err(
            "No .proto files found in configured proto roots (HA3 + HackArena APIs)".into(),
        );
    }
    for proto_file in &protos {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe { env::set_var("PROTOC", protoc_path.as_os_str()) };

    tonic_prost_build::configure()
        .build_server(true)
        .bytes(".")
        .compile_protos(
            &protos,
            &[ha3_proto_root.clone(), hackarena_proto_root.clone()],
        )?;

    copy_generated_rs_to_gen_dir()?;

    eprintln!("[proto build] generated {} files", protos.len());

    Ok(())
}

fn resolve_proto_roots() -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let ha3_proto_root = manifest_dir
        .join("..")
        .join("third_party")
        .join("HackArean3.0-Proto")
        .join("proto");

    let hackarena_proto_root = manifest_dir
        .join("..")
        .join("third_party")
        .join("HackArena-Proto")
        .join("proto");

    ensure_proto_root(&ha3_proto_root, "third_party/HackArean3.0-Proto/proto")?;
    ensure_proto_root(&hackarena_proto_root, "third_party/HackArena-Proto/proto")?;

    Ok((ha3_proto_root, hackarena_proto_root))
}

fn ensure_proto_root(path: &Path, logical_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!(
            "Proto submodule missing at `{logical_path}`. Run: git submodule update --init --recursive"
        )
        .into());
    }
    if !path.is_dir() {
        return Err(format!("Expected `{logical_path}` to be a directory").into());
    }
    Ok(())
}

fn ensure_proto_dir(path: &Path, logical_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_dir() {
        return Err(format!(
            "Missing proto directory `{}` (expected at `{}`)",
            logical_name,
            path.display()
        )
        .into());
    }
    Ok(())
}

fn collect_proto_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn copy_generated_rs_to_gen_dir() -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
