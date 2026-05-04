//! Standalone updater helper binary for Windows package self-updates.
//!
//! Build with `--features standalone`.

#[cfg(all(not(feature = "ide"), not(feature = "standalone")))]
compile_error!("ha3-standalone-update requires --features standalone");
#[cfg(all(not(feature = "ide"), feature = "official"))]
compile_error!("ha3-standalone-update cannot be built with --features official");
#[cfg(all(feature = "ide", not(debug_assertions)))]
compile_error!("feature `ide` is for editor use only; do not enable in release builds");

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        game_server::standalone_updater::print_apply_update_help("ha3-standalone-update.exe");
        return Ok(());
    }

    let apply_args = game_server::standalone_updater::parse_apply_update_args_from_iter(args)?;
    std::env::set_current_dir(&apply_args.install_dir)?;
    let _tracing_guard = game_server::init_tracing("ha3-standalone-update")?;
    game_server::standalone_updater::run_apply_update(&apply_args)?;
    Ok(())
}
