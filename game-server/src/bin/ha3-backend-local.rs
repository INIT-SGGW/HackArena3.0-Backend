//! Local backend binary for team-level testing.
//!
//! Build with `--features local`. Uses local JWT defaults and the shared runtime.

#[cfg(all(not(feature = "ide"), not(feature = "local")))]
compile_error!("ha3-backend-local requires --features local");
#[cfg(all(not(feature = "ide"), feature = "official"))]
compile_error!("ha3-backend-local cannot be built with --features official");
#[cfg(all(feature = "ide", not(debug_assertions)))]
compile_error!("feature `ide` is for editor use only; do not enable in release builds");

use std::error::Error;
use std::sync::Arc;

use dotenv::{dotenv, from_filename};
use game_server::config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut env_local_fallback: Option<dotenv::Error> = None;
    let mut env_default_fallback_error: Option<dotenv::Error> = None;
    if let Err(err) = from_filename(".env.local") {
        env_local_fallback = Some(err);
        if let Err(default_err) = dotenv() {
            env_default_fallback_error = Some(default_err);
        }
    }

    let _tracing_guard = game_server::init_tracing("ha3-backend-local")?;

    if let Some(err) = env_local_fallback {
        tracing::warn!(
            error = %err,
            "failed to load .env.local; falling back to .env"
        );
        if let Some(default_err) = env_default_fallback_error {
            tracing::warn!(
                error = %default_err,
                "failed to load fallback .env"
            );
        }
    }

    let cfg = Arc::new(Config::load_or_exit());

    tracing::info!("ha3-backend-local starting");

    game_server::run(cfg).await
}
