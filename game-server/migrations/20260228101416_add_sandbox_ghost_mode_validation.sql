-- Normalize legacy rows authored before max-speed semantics.
UPDATE sandbox_configs
SET
    ghost_min_speed_enter_mps = LEAST(ghost_min_speed_enter_mps, ghost_min_speed_exit_mps),
    ghost_min_speed_exit_mps = GREATEST(ghost_min_speed_enter_mps, ghost_min_speed_exit_mps)
WHERE
    ghost_min_speed_enter_mps IS NOT NULL
    AND ghost_min_speed_exit_mps IS NOT NULL
    AND ghost_min_speed_enter_mps > ghost_min_speed_exit_mps;

ALTER TABLE sandbox_configs
ADD CONSTRAINT sandbox_configs_ghost_speed_threshold_order_chk
CHECK (
    ghost_min_speed_enter_mps IS NULL
    OR ghost_min_speed_exit_mps IS NULL
    OR ghost_min_speed_enter_mps <= ghost_min_speed_exit_mps
);
