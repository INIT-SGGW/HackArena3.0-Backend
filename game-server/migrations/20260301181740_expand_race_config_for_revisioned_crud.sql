ALTER TABLE race_config_schedule
    ADD COLUMN race_duration_sec INTEGER;

UPDATE race_config_schedule
SET race_duration_sec = ((ends_at_ms - starts_at_ms + 999) / 1000);

ALTER TABLE race_config_schedule
    ALTER COLUMN race_duration_sec SET NOT NULL,
    ADD CONSTRAINT race_config_schedule_race_duration_sec_chk
        CHECK (race_duration_sec > 0);

CREATE TABLE race_config_state (
    singleton_key BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton_key = TRUE),
    revision BIGINT NOT NULL CHECK (revision >= 0)
);

INSERT INTO race_config_state (singleton_key, revision)
VALUES (TRUE, 0);
