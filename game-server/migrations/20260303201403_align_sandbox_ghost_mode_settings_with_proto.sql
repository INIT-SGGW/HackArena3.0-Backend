DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sandbox_configs'
          AND column_name = 'ghost_max_speed_enter_mps'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_max_speed_enter_mps TO ghost_enter_speed_max_mps';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sandbox_configs'
          AND column_name = 'ghost_max_speed_exit_mps'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_max_speed_exit_mps TO ghost_exit_speed_min_mps';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sandbox_configs'
          AND column_name = 'ghost_min_completed_laps'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_min_completed_laps TO ghost_until_completed_laps';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sandbox_configs'
          AND column_name = 'ghost_overlap_exit_delay_ms'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_overlap_exit_delay_ms TO ghost_vehicle_overlap_exit_delay_ms';
    END IF;
END
$$;

DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        WHERE c.conrelid = 'sandbox_configs'::regclass
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%ghost_condition_logic%'
    LOOP
        EXECUTE format('ALTER TABLE sandbox_configs DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;

ALTER TABLE sandbox_configs
DROP COLUMN IF EXISTS ghost_condition_logic;

DROP TYPE IF EXISTS ghost_mode_condition_logic;

ALTER TABLE sandbox_configs
DROP CONSTRAINT IF EXISTS sandbox_configs_ghost_speed_threshold_order_chk;

ALTER TABLE sandbox_configs
DROP CONSTRAINT IF EXISTS sandbox_configs_ghost_max_speed_threshold_order_chk;

ALTER TABLE sandbox_configs
ADD CONSTRAINT sandbox_configs_ghost_speed_threshold_order_chk
CHECK (
    ghost_enter_speed_max_mps IS NULL
    OR ghost_exit_speed_min_mps IS NULL
    OR ghost_enter_speed_max_mps <= ghost_exit_speed_min_mps
);

ALTER TABLE sandbox_configs
DROP CONSTRAINT IF EXISTS sandbox_configs_ghost_mode_completeness_chk;

ALTER TABLE sandbox_configs
ADD CONSTRAINT sandbox_configs_ghost_mode_completeness_chk
CHECK (
    (
        ghost_mode_enabled IS NULL
        AND ghost_enter_speed_max_mps IS NULL
        AND ghost_exit_speed_min_mps IS NULL
        AND ghost_enter_delay_ms IS NULL
        AND ghost_exit_delay_ms IS NULL
        AND ghost_until_completed_laps IS NULL
        AND ghost_vehicle_overlap_exit_delay_ms IS NULL
    )
    OR (
        ghost_mode_enabled IS NOT NULL
        AND ghost_enter_speed_max_mps IS NOT NULL
        AND ghost_exit_speed_min_mps IS NOT NULL
        AND ghost_enter_delay_ms IS NOT NULL
        AND ghost_exit_delay_ms IS NOT NULL
        AND ghost_until_completed_laps IS NOT NULL
        AND ghost_vehicle_overlap_exit_delay_ms IS NOT NULL
    )
);
