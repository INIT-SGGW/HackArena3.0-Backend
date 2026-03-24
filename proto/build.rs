use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let game_proto_root = resolve_game_proto_root()?;
    let hackarena_proto_root = resolve_hackarena_proto_root()?;
    println!("cargo:rerun-if-changed={}", game_proto_root.display());
    println!("cargo:rerun-if-changed={}", hackarena_proto_root.display());

    let race_v1_dir = game_proto_root.join("race").join("v1");
    let weather_v1_dir = game_proto_root.join("weather").join("v1");
    let achievement_v1_dir = game_proto_root.join("achievement").join("v1");
    let auth_v1_dir = game_proto_root.join("auth").join("v1");
    let broker_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("broker")
        .join("v1");
    let connect_v1_dir = hackarena_proto_root
        .join("hackarena")
        .join("connect")
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
    ensure_proto_dir(&auth_v1_dir, "auth/v1")?;
    ensure_proto_dir(&broker_v1_dir, "hackarena/broker/v1")?;
    ensure_proto_dir(&connect_v1_dir, "hackarena/connect/v1")?;
    ensure_proto_dir(&submission_v1_dir, "hackarena/submission/v1")?;
    ensure_proto_dir(&platform_common_v1_dir, "hackarena/platform/common/v1")?;
    ensure_proto_dir(&platform_teams_v1_dir, "hackarena/platform/teams/v1")?;

    let mut protos = Vec::new();
    protos.extend(collect_proto_files_in_dir(&race_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&weather_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&achievement_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&auth_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&broker_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&connect_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&submission_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&platform_common_v1_dir)?);
    protos.extend(collect_proto_files_in_dir(&platform_teams_v1_dir)?);
    if protos.is_empty() {
        return Err(
            "No .proto files found in third_party/HackArean3.0-Proto/proto/{race,weather,achievement,auth}/v1 or third_party/HackArena-Proto/proto/hackarena/{broker,connect}/v1".into(),
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
        .compile_protos(&protos, &[game_proto_root, hackarena_proto_root])?;

    copy_generated_rs_to_gen_dir()?;

    eprintln!("[proto build] generated {} files", protos.len());

    Ok(())
}

fn resolve_game_proto_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir
        .join("..")
        .join("third_party")
        .join("HackArean3.0-Proto")
        .join("proto");

    if !proto_root.exists() {
        return Err(
            "Proto submodule missing at `third_party/HackArean3.0-Proto/proto`. Run: git submodule update --init --recursive"
                .into(),
        );
    }
    if !proto_root.is_dir() {
        return Err("Expected `third_party/HackArean3.0-Proto/proto` to be a directory".into());
    }

    Ok(proto_root)
}

fn resolve_hackarena_proto_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir
        .join("..")
        .join("third_party")
        .join("HackArena-Proto")
        .join("proto");

    if !proto_root.exists() {
        return Err(
            "Proto submodule missing at `third_party/HackArena-Proto/proto`. Run: git submodule update --init --recursive"
                .into(),
        );
    }
    if !proto_root.is_dir() {
        return Err("Expected `third_party/HackArena-Proto/proto` to be a directory".into());
    }

    Ok(proto_root)
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
