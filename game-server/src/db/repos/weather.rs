//! Weather repository for persisted global schedule.

use proto::weather::v1::WeatherType;
use sqlx::PgPool;

use crate::domain::weather::ScheduleEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "weather_type", rename_all = "snake_case")]
enum DbWeatherType {
    Unspecified,
    Clear,
    PartlyCloudy,
    Overcast,
    LightRain,
    MediumRain,
    HeavyRain,
}

impl From<DbWeatherType> for WeatherType {
    fn from(value: DbWeatherType) -> Self {
        match value {
            DbWeatherType::Unspecified => WeatherType::Unspecified,
            DbWeatherType::Clear => WeatherType::Clear,
            DbWeatherType::PartlyCloudy => WeatherType::PartlyCloudy,
            DbWeatherType::Overcast => WeatherType::Overcast,
            DbWeatherType::LightRain => WeatherType::LightRain,
            DbWeatherType::MediumRain => WeatherType::MediumRain,
            DbWeatherType::HeavyRain => WeatherType::HeavyRain,
        }
    }
}

impl From<WeatherType> for DbWeatherType {
    fn from(value: WeatherType) -> Self {
        match value {
            WeatherType::Unspecified => DbWeatherType::Unspecified,
            WeatherType::Clear => DbWeatherType::Clear,
            WeatherType::PartlyCloudy => DbWeatherType::PartlyCloudy,
            WeatherType::Overcast => DbWeatherType::Overcast,
            WeatherType::LightRain => DbWeatherType::LightRain,
            WeatherType::MediumRain => DbWeatherType::MediumRain,
            WeatherType::HeavyRain => DbWeatherType::HeavyRain,
        }
    }
}

/// Repository for reading and replacing global weather schedule.
#[derive(Clone)]
pub struct WeatherRepo {
    pool: PgPool,
}

impl WeatherRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fetches full global schedule ordered by start timestamp.
    pub async fn get_schedule(&self) -> anyhow::Result<Vec<ScheduleEntry>> {
        let rows = sqlx::query!(
            r#"SELECT starts_at_ms, weather_type AS "weather_type: DbWeatherType", temperature_c FROM weather_schedule ORDER BY starts_at_ms ASC"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(ScheduleEntry {
                starts_at_ms: row.starts_at_ms,
                weather_type: row.weather_type.into(),
                temperature_c: row.temperature_c,
            });
        }

        Ok(entries)
    }

    /// Replaces whole global schedule in a single transaction.
    pub async fn replace_schedule(&self, entries: &[ScheduleEntry]) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!("DELETE FROM weather_schedule")
            .execute(&mut *tx)
            .await?;

        for entry in entries {
            let weather_type = DbWeatherType::from(entry.weather_type);
            sqlx::query!(
                "INSERT INTO weather_schedule (starts_at_ms, weather_type, temperature_c) VALUES ($1, $2, $3)",
                entry.starts_at_ms,
                weather_type as _,
                entry.temperature_c
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
