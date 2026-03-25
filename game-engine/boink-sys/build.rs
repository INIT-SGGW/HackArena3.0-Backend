use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not set");
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH not set");
    let profile = env::var("PROFILE").expect("PROFILE not set");

    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ARCH");

    match target_os.as_str() {
        "windows" => setup_windows(&manifest_dir, &target_arch, &profile),
        "linux" => setup_linux(&manifest_dir, &target_arch, &profile),
        "macos" => setup_macos(&manifest_dir, &target_arch, &profile),
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

fn setup_windows(manifest_dir: &PathBuf, target_arch: &str, profile: &str) {
    let lib_dir = determine_lib_dir(manifest_dir, "windows", target_arch, profile);
    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    if cfg!(feature = "static") {
        println!("cargo:rustc-link-lib=static=boink");
    } else {
        println!("cargo:rustc-link-lib=dylib=boink");
        copy_runtime_lib(&lib_dir, "boink.dll");
        // copy_runtime_lib(&lib_dir, "glfw3.dll");
        if profile == "debug" {
            // copy_runtime_lib(&lib_dir, "fmtd.dll");
            // copy_runtime_lib(&lib_dir, "spdlogd.dll");
        } else {
            // copy_runtime_lib(&lib_dir, "fmt.dll");
            // copy_runtime_lib(&lib_dir, "spdlog.dll");
        }
    }
}

fn setup_linux(manifest_dir: &PathBuf, target_arch: &str, profile: &str) {
    let lib_dir = determine_lib_dir(manifest_dir, "linux", target_arch, profile);

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    copy_runtime_lib(&lib_dir, "libboink.so");
}

fn setup_macos(manifest_dir: &PathBuf, target_arch: &str, profile: &str) {
    let lib_dir = determine_lib_dir(manifest_dir, "macos", target_arch, profile);

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    copy_runtime_lib(&lib_dir, "libboink.dylib");
}

fn determine_lib_dir(
    manifest_dir: &Path,
    platform: &str,
    target_arch: &str,
    profile: &str,
) -> PathBuf {
    println!("cargo:rerun-if-env-changed=BOINK_NATIVE_LIB_DIR");

    env::var_os("BOINK_NATIVE_LIB_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| {
            let arch_dir = match target_arch {
                "x86_64" => "x86_64",
                "aarch64" => "aarch64",
                other => {
                    panic!("boink-sys: unsupported arch {}", other);
                }
            };
            manifest_dir
                .join("native")
                .join(platform)
                .join(arch_dir)
                .join(profile)
        })
}

fn copy_runtime_lib(lib_dir: &Path, file_name: &str) {
    let source = lib_dir.join(file_name);

    if !source.exists() {
        if env::var("PROFILE").as_deref() == Ok("release") {
            panic!(
                "boink-sys: required runtime artifact {} missing at {}",
                file_name,
                source.display()
            );
        } else {
            println!(
                "cargo:warning=boink-sys: runtime artifact {} missing at {}",
                file_name,
                source.display()
            );
            return;
        }
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
