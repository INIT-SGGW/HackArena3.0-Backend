CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE weather_type AS ENUM (
    'clear',
    'partly_cloudy',
    'overcast',
    'light_rain',
    'medium_rain',
    'heavy_rain'
);

CREATE TABLE race (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_index INTEGER NOT NULL,
    name TEXT NOT NULL,
    scorable BOOLEAN NOT NULL DEFAULT TRUE,
    planned_start TIMESTAMPTZ NOT NULL,
    length INTERVAL NOT NULL
);

CREATE TABLE weather (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    race_id UUID NOT NULL REFERENCES race(id) ON DELETE CASCADE,
    starts_at TIMESTAMPTZ NOT NULL,
    weather_type weather_type NOT NULL
);

CREATE INDEX weather_race_starts_at_idx ON weather (race_id, starts_at);

CREATE TABLE race_result_team_position (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    race_id UUID NOT NULL REFERENCES race(id) ON DELETE CASCADE,
    team_id UUID NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 1)
);

CREATE UNIQUE INDEX race_result_team_position_race_team_idx
    ON race_result_team_position (race_id, team_id);

CREATE UNIQUE INDEX race_result_team_position_race_position_idx
    ON race_result_team_position (race_id, position);
