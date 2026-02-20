CREATE TYPE start_placement_mode AS ENUM (
    'random',
    'scoreboard',
    'reversed_scoreboard'
);

CREATE TYPE time_of_day_preset AS ENUM (
    'morning',
    'noon',
    'evening',
    'night'
);

CREATE TABLE race_config_schedule (
    race_id TEXT PRIMARY KEY,
    race_name TEXT NOT NULL,
    starts_at_ms BIGINT NOT NULL,
    ends_at_ms BIGINT NOT NULL,
    map_id TEXT NOT NULL,
    map_version INTEGER,
    start_placement_mode start_placement_mode NOT NULL,
    points_multiplier_fixed REAL NOT NULL,
    time_of_day_preset time_of_day_preset NOT NULL,
    CHECK (starts_at_ms < ends_at_ms),
    CHECK (char_length(trim(race_name)) > 0),
    CHECK (char_length(trim(map_id)) > 0),
    CHECK (map_version IS NULL OR map_version > 0),
    CHECK (points_multiplier_fixed > 0),
    CHECK (points_multiplier_fixed < 'Infinity'::real)
);

CREATE INDEX race_config_schedule_starts_at_idx
    ON race_config_schedule (starts_at_ms ASC);
