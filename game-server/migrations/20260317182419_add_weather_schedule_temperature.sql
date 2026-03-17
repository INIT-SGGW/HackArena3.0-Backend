ALTER TABLE weather_schedule
    ADD COLUMN temperature_c INT NOT NULL DEFAULT 16,
    ADD CONSTRAINT weather_schedule_temperature_c_range
        CHECK (temperature_c BETWEEN 1 AND 30);
