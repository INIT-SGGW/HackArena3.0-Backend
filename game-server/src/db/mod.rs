//! Database setup for the official backend (Postgres).

use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::{Level, debug, info};

pub async fn connect_and_migrate(
    database_url: &str,
    max_connections: u32,
) -> anyhow::Result<PgPool> {
    info!(max_connections, "connecting to database");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;

    info!("running migrations");
    sqlx::migrate!().run(&pool).await?;
    info!("migrations complete");

    if tracing::enabled!(Level::DEBUG) {
        let rows = sqlx::query(
            "SELECT version, description, installed_on::text AS installed_on FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&pool)
        .await?;
        for row in rows {
            let version: i64 = row.try_get("version")?;
            let description: String = row.try_get("description")?;
            let installed_on: String = row.try_get("installed_on")?;
            debug!(
                version,
                description,
                installed_on = %installed_on,
                "migration applied"
            );
        }
    }

    Ok(pool)
}
