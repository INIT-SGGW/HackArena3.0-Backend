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
    let lib_dir = determine_lib_dir(manifest_dir, "windows");
    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    if cfg!(feature = "static") {
        println!("cargo:rustc-link-lib=static=boink");
    } else {
        println!("cargo:rustc-link-lib=dylib=boink");
        copy_runtime_lib(&lib_dir, "boink.dll");
    }
}

fn setup_linux(manifest_dir: &PathBuf) {
    let lib_dir = determine_lib_dir(manifest_dir, "linux");

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    copy_runtime_lib(&lib_dir, "libboink.so");
}

fn setup_macos(manifest_dir: &PathBuf) {
    let lib_dir = determine_lib_dir(manifest_dir, "macos");

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    copy_runtime_lib(&lib_dir, "libboink.dylib");
}

fn determine_lib_dir(manifest_dir: &Path, platform: &str) -> PathBuf {
    println!("cargo:rerun-if-env-changed=BOINK_NATIVE_LIB_DIR");

    env::var_os("BOINK_NATIVE_LIB_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| manifest_dir.join("native").join(platform))
}

fn copy_runtime_lib(lib_dir: &Path, file_name: &str) {
    let source = lib_dir.join(file_name);

    if !source.exists() {
        println!(
            "cargo:warning=boink-sys: runtime artifact {} missing at {}",
            file_name,
            source.display()
        );
        return;
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("Unexpected OUT_DIR layout (target/<profile>/build/..)");

    if let Err(err) = fs::copy(&source, profile_dir.join(file_name)) {
        panic!(
            "Failed to copy {} to {}: {}",
            source.display(),
            profile_dir.display(),
            err
        );
    }
}
