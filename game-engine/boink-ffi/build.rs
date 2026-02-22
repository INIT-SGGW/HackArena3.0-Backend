fn main() {
    use std::{env, fs, io::Write, path::PathBuf};

    const HEADER_FILE_NAME: &str = "boink_c_api.h";

    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = crate_dir.join("include");
    let out_path = out_dir.join(HEADER_FILE_NAME);
    let cfg_path = crate_dir.join("cbindgen.toml");

    fs::create_dir_all(&out_dir).expect("create include/ failed");

    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed={}", cfg_path.display());

    let config = cbindgen::Config::from_file(&cfg_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", cfg_path.display(), e));

    let bindings = cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .expect("cbindgen failed");

    let mut bytes = Vec::<u8>::new();
    bindings.write(&mut bytes);

    let header = String::from_utf8(bytes).expect("header not UTF-8");

    // Replace `extern <ret> name(...)` with `BOINK_API <ret> name(...)`
    // while leaving `extern "C"` linkage blocks untouched.
    let mut fixed = String::with_capacity(header.len());
    for line in header.lines() {
        let trimmed = line.trim_start();

        let looks_like_foreign_fn = trimmed.starts_with("extern ")
            && !trimmed.starts_with("extern \"C\"")
            && trimmed.contains('(')
            && !trimmed.ends_with('{');

        if looks_like_foreign_fn {
            let indent_len = line.len() - trimmed.len();
            let indent = &line[..indent_len];
            let after = &trimmed["extern ".len()..];
            fixed.push_str(indent);
            fixed.push_str("BOINK_API ");
            fixed.push_str(after);
            fixed.push('\n');
        } else {
            fixed.push_str(line);
            fixed.push('\n');
        }
    }

    let mut file = fs::File::create(&out_path).expect("create header file failed");
    file.write_all(fixed.as_bytes())
        .expect("write header file failed");

    eprintln!("[boink-ffi build] generated header: {}", out_path.display());
}
