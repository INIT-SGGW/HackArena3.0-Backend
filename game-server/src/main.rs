mod config;
mod server;

use dotenv::dotenv;
use tracing_subscriber::filter::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let cfg = config::Config::load_or_exit();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(cfg.env.is_production())
        .with_file(!cfg.env.is_production())
        .with_line_number(!cfg.env.is_production())
        .compact()
        .init();

    server::run(cfg).await
}
