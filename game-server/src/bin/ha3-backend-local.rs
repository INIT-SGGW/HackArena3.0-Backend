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

use game_server::config::{Config, JwtDefaults};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    game_server::init_tracing();

    let cfg = Arc::new(Config::load_or_exit_with_defaults(JwtDefaults::local()));

    tracing::info!("ha3-backend-local starting");

    game_server::run(cfg).await
}
