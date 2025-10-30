mod config;
mod server;
mod services;

use dotenv::dotenv;
use tracing_subscriber::filter::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let is_prod = matches!(
        std::env::var("APP_ENV")
            .unwrap_or_else(|_| "development".to_string())
            .to_ascii_lowercase()
            .as_str(),
        "prod" | "production"
    );

    let env_filter: EnvFilter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(is_prod)
        .with_file(!is_prod)
        .with_line_number(!is_prod)
        .compact()
        .init();

    let cfg = config::Config::load_or_exit();

    server::run(cfg).await
}
