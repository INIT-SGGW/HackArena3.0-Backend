//! Standalone backend binary for local self-hosted runs.
//!
//! Build with `--features standalone`.

#[cfg(all(not(feature = "ide"), not(feature = "standalone")))]
compile_error!("ha3-standalone requires --features standalone");
#[cfg(all(not(feature = "ide"), feature = "official"))]
compile_error!("ha3-standalone cannot be built with --features official");
#[cfg(all(feature = "ide", not(debug_assertions)))]
compile_error!("feature `ide` is for editor use only; do not enable in release builds");

use std::error::Error;
use std::sync::Arc;

use dotenv::{dotenv, from_filename};
use game_server::config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut env_standalone_fallback: Option<dotenv::Error> = None;
    let mut env_default_fallback_error: Option<dotenv::Error> = None;
    if let Err(err) = from_filename(".env.standalone") {
        env_standalone_fallback = Some(err);
        if let Err(default_err) = dotenv() {
            env_default_fallback_error = Some(default_err);
        }
    }

    let _tracing_guard = game_server::init_tracing("ha3-standalone")?;

    if let Some(err) = env_standalone_fallback {
        tracing::warn!(
            error = %err,
            "failed to load .env.standalone; falling back to .env"
        );
        if let Some(default_err) = env_default_fallback_error {
            tracing::warn!(
                error = %default_err,
                "failed to load fallback .env"
            );
        }
    }

    let cfg = Arc::new(Config::load_or_exit());

    tracing::info!("ha3-standalone starting");

    game_server::run(cfg).await
}
