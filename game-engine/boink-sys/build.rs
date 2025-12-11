use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not set");

    match target_os.as_str() {
        "windows" => setup_windows(&manifest_dir),
        "linux" => setup_linux(&manifest_dir),
        "macos" => setup_macos(&manifest_dir),
        other => {
            println!(
                "cargo:warning=boink-sys: no native library configuration for target_os={}",
                other
            );
        }
    }
}

fn emit_rerun_for_dir(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}

fn setup_windows(manifest_dir: &PathBuf) {
    let lib_dir = manifest_dir.join("native").join("windows");

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    if cfg!(feature = "static") {
        println!("cargo:rustc-link-lib=static=boink");
    } else {
        println!("cargo:rustc-link-lib=dylib=boink");
    }
}

fn setup_linux(manifest_dir: &PathBuf) {
    let lib_dir = manifest_dir.join("native").join("linux");

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
}

fn setup_macos(manifest_dir: &PathBuf) {
    let lib_dir = manifest_dir.join("native").join("macos");

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
}
