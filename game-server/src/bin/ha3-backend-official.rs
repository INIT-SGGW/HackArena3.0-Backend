//! Official backend binary for multi-team competition.
//!
//! Build with `--features official`. Uses official JWT defaults and the shared runtime.

#[cfg(all(not(feature = "ide"), not(feature = "official")))]
compile_error!("ha3-backend-official requires --features official");
#[cfg(all(not(feature = "ide"), feature = "local"))]
compile_error!("ha3-backend-official cannot be built with --features local");
#[cfg(all(feature = "ide", not(debug_assertions)))]
compile_error!("feature `ide` is for editor use only; do not enable in release builds");

use std::error::Error;
use std::sync::Arc;

use dotenv;
use game_server::config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut env_official_fallback: Option<dotenv::Error> = None;
    let mut env_default_fallback_error: Option<dotenv::Error> = None;
    if let Err(err) = dotenv::from_filename(".env.official") {
        env_official_fallback = Some(err);
        if let Err(default_err) = dotenv::dotenv() {
            env_default_fallback_error = Some(default_err);
        }
    }

    let _tracing_guard = game_server::init_tracing("ha3-backend-official")?;

    if let Some(err) = env_official_fallback {
        tracing::warn!(
            error = %err,
            "failed to load .env.official; falling back to .env"
        );
        if let Some(default_err) = env_default_fallback_error {
            tracing::warn!(
                error = %default_err,
                "failed to load fallback .env"
            );
        }
    }

    let cfg = Arc::new(Config::load_or_exit());

    tracing::info!("ha3-backend-official starting");

    game_server::run(cfg).await
}
