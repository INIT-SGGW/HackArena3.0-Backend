mod config;
mod server;

use game_engine::start_engine;

use dotenv::dotenv;
use tracing_subscriber::filter::EnvFilter;
use std::thread;

fn main(){
    dotenv().ok();
    
    let cfg = config::Config::load_or_exit();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into());

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_file(!cfg.env.is_production())
        .with_line_number(!cfg.env.is_production())
        .compact()
        .init();

    let handle =thread::spawn(||{
        tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let _=server::run(cfg).await;
                });
    });

    start_engine();
    handle.join().unwrap();
}

