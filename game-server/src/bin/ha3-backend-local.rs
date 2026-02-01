use std::error::Error;
use std::sync::Arc;

use game_server::config::{Config, JwtDefaults};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    game_server::init_tracing();

    let cfg = Arc::new(Config::load_or_exit_with_defaults(
        JwtDefaults::local(),
    ));

    tracing::info!("ha3-backend-local starting");

    game_server::run(cfg).await
}
