CREATE TYPE ghost_mode_condition_logic AS ENUM (
    'and',
    'or'
);

CREATE TABLE sandbox_config_state (
    singleton_key BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton_key),
    revision BIGINT NOT NULL CHECK (revision >= 0)
);

INSERT INTO sandbox_config_state (singleton_key, revision)
VALUES (TRUE, 0);

CREATE TABLE sandbox_configs (
    sandbox_id TEXT PRIMARY KEY,
    sandbox_name TEXT NOT NULL,
    map_id TEXT NOT NULL,
    time_of_day_preset time_of_day_preset NOT NULL,

    ghost_mode_enabled BOOLEAN,
    ghost_min_speed_enter_mps REAL,
    ghost_min_speed_exit_mps REAL,
    ghost_enter_delay_ms BIGINT,
    ghost_exit_delay_ms BIGINT,
    ghost_min_completed_laps BIGINT,
    ghost_condition_logic ghost_mode_condition_logic,
    ghost_overlap_exit_delay_ms BIGINT,

    CHECK (char_length(trim(sandbox_id)) > 0),
    CHECK (char_length(trim(sandbox_name)) > 0),
    CHECK (char_length(trim(map_id)) > 0),
    CHECK (ghost_min_speed_enter_mps IS NULL OR ghost_min_speed_enter_mps >= 0),
    CHECK (ghost_min_speed_exit_mps IS NULL OR ghost_min_speed_exit_mps >= 0),
    CHECK (
        ghost_enter_delay_ms IS NULL
        OR (ghost_enter_delay_ms >= 0 AND ghost_enter_delay_ms <= 4294967295)
    ),
    CHECK (
        ghost_exit_delay_ms IS NULL
        OR (ghost_exit_delay_ms >= 0 AND ghost_exit_delay_ms <= 4294967295)
    ),
    CHECK (
        ghost_min_completed_laps IS NULL
        OR (ghost_min_completed_laps >= 0 AND ghost_min_completed_laps <= 4294967295)
    ),
    CHECK (
        ghost_overlap_exit_delay_ms IS NULL
        OR (ghost_overlap_exit_delay_ms >= 0 AND ghost_overlap_exit_delay_ms <= 4294967295)
    ),
    CHECK (
        (
            ghost_mode_enabled IS NULL
            AND ghost_min_speed_enter_mps IS NULL
            AND ghost_min_speed_exit_mps IS NULL
            AND ghost_enter_delay_ms IS NULL
            AND ghost_exit_delay_ms IS NULL
            AND ghost_min_completed_laps IS NULL
            AND ghost_condition_logic IS NULL
            AND ghost_overlap_exit_delay_ms IS NULL
        )
        OR (
            ghost_mode_enabled IS NOT NULL
            AND ghost_min_speed_enter_mps IS NOT NULL
            AND ghost_min_speed_exit_mps IS NOT NULL
            AND ghost_enter_delay_ms IS NOT NULL
            AND ghost_exit_delay_ms IS NOT NULL
            AND ghost_min_completed_laps IS NOT NULL
            AND ghost_condition_logic IS NOT NULL
            AND ghost_overlap_exit_delay_ms IS NOT NULL
        )
    )
);
