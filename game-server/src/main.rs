mod config;
mod server;

use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let cfg = config::Config::load_or_exit();
    server::run(cfg).await
}
