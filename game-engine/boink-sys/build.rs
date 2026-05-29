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

    let Some(versioned_runtime) = select_linux_versioned_runtime(&lib_dir) else {
        if profile == "release" {
            panic!(
                "boink-sys: no versioned Linux runtime artifact matching libboink.so.<major>.<minor>... in {}",
                lib_dir.display()
            );
        } else {
            println!(
                "cargo:warning=boink-sys: no versioned Linux runtime artifact matching libboink.so.<major>.<minor>... in {}",
                lib_dir.display()
            );
            return;
        }
    };
    let link_dir = materialize_linux_runtime_aliases(&versioned_runtime);

    println!("cargo:rustc-link-search=native={}", link_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    // Make runtime loader search next to the executable in release artifacts.
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
}

fn setup_macos(manifest_dir: &PathBuf, target_arch: &str, profile: &str) {
    let lib_dir = determine_lib_dir(manifest_dir, "macos", target_arch, profile);

    emit_rerun_for_dir(&lib_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=boink");
    // Make runtime loader search next to the executable in release artifacts.
    println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path");
    copy_runtime_lib(&lib_dir, "libboink.dylib");
    copy_optional_runtime_lib(&lib_dir, "libboink.1.dylib");
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

fn copy_optional_runtime_lib(lib_dir: &Path, file_name: &str) {
    let source = lib_dir.join(file_name);
    if !source.exists() {
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

fn select_linux_versioned_runtime(lib_dir: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<(Vec<u64>, PathBuf)> = fs::read_dir(lib_dir)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_file() {
                return None;
            }
            let name = path.file_name()?.to_str()?;
            let version = parse_linux_runtime_version(name)?;
            Some((version, path))
        })
        .collect();

    candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
    candidates.pop().map(|(_, path)| path)
}

fn parse_linux_runtime_version(file_name: &str) -> Option<Vec<u64>> {
    let suffix = file_name.strip_prefix("libboink.so.")?;
    let segments: Vec<u64> = suffix
        .split('.')
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if segments.len() < 2 {
        return None;
    }
    Some(segments)
}

fn materialize_linux_runtime_aliases(source: &Path) -> PathBuf {
    let versioned_name = source
        .file_name()
        .and_then(|name| name.to_str())
        .expect("boink-sys: Linux runtime source file name is not valid UTF-8");
    let major = parse_linux_runtime_version(versioned_name)
        .and_then(|segments| segments.first().copied())
        .expect("boink-sys: failed to derive Linux runtime major version");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let link_dir = out_dir.join("boink-linux-runtime");
    if link_dir.exists() {
        fs::remove_dir_all(&link_dir).unwrap_or_else(|err| {
            panic!(
                "Failed to clean Linux boink staging dir {}: {}",
                link_dir.display(),
                err
            )
        });
    }
    fs::create_dir_all(&link_dir).unwrap_or_else(|err| {
        panic!(
            "Failed to create Linux boink staging dir {}: {}",
            link_dir.display(),
            err
        )
    });

    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("Unexpected OUT_DIR layout (target/<profile>/build/..)");

    for file_name in [
        versioned_name.to_string(),
        format!("libboink.so.{major}"),
        "libboink.so".to_string(),
    ] {
        copy_file(source, &link_dir.join(&file_name));
        copy_file(source, &profile_dir.join(&file_name));
    }

    link_dir
}

fn copy_file(source: &Path, destination: &Path) {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!("Failed to create parent dir {}: {}", parent.display(), err)
        });
    }
    if let Err(err) = fs::copy(source, destination) {
        panic!(
            "Failed to copy {} to {}: {}",
            source.display(),
            destination.display(),
            err
        );
    }
}
