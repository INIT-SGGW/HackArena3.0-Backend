CREATE TYPE weather_type AS ENUM (
    'unspecified',
    'clear',
    'partly_cloudy',
    'overcast',
    'light_rain',
    'medium_rain',
    'heavy_rain'
);

CREATE TABLE weather_schedule (
    starts_at_ms BIGINT PRIMARY KEY,
    weather_type weather_type NOT NULL
);
