DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sandbox_configs'
          AND column_name = 'ghost_min_speed_enter_mps'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_min_speed_enter_mps TO ghost_max_speed_enter_mps';
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
          AND column_name = 'ghost_min_speed_exit_mps'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME COLUMN ghost_min_speed_exit_mps TO ghost_max_speed_exit_mps';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'sandbox_configs_ghost_speed_threshold_order_chk'
    ) THEN
        EXECUTE 'ALTER TABLE sandbox_configs RENAME CONSTRAINT sandbox_configs_ghost_speed_threshold_order_chk TO sandbox_configs_ghost_max_speed_threshold_order_chk';
    END IF;
END
$$;
