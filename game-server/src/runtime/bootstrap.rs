//! Runtime bootstrap helpers for official backend startup.

use crate::config::Config;
use crate::db::repos::sandbox_config::SandboxConfigRepo;
use crate::services::sandbox_mappers::{
    engine_ghost_mode_settings_from_record, runtime_time_of_day_preset_from_proto,
};

use super::engine_worker::{EngineActivityKind, EngineClient};

/// In official debug-development startup, activate first configured sandbox if present.
pub async fn bootstrap_first_configured_sandbox_for_official_dev(
    cfg: &Config,
    engine: &EngineClient,
    sandbox_repo: &SandboxConfigRepo,
) {
    if !(cfg!(debug_assertions) && cfg.env.is_development()) {
        return;
    }

    let snapshot = match sandbox_repo.get_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "official dev profile: failed to read sandbox config snapshot for startup bootstrap"
            );
            return;
        }
    };

    let Some(first_sandbox) = snapshot.sandboxes.first() else {
        return;
    };

    let sandbox_id = first_sandbox.sandbox_id.clone();
    let map_id = first_sandbox.config.map_id.clone();
    let time_of_day =
        runtime_time_of_day_preset_from_proto(first_sandbox.config.time_of_day_preset);
    let ghost_mode =
        engine_ghost_mode_settings_from_record(first_sandbox.config.ghost_mode.as_ref());

    let runtime_before = match engine.runtime_state().await {
        Ok(runtime_before) => runtime_before,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "official dev profile: failed to read runtime state before startup bootstrap"
            );
            return;
        }
    };

    match engine
        .switch_runtime(
            runtime_before.revision,
            EngineActivityKind::Sandbox,
            map_id.clone(),
            Some(sandbox_id.clone()),
            Some(time_of_day),
            Some(ghost_mode),
        )
        .await
    {
        Ok(_) => tracing::info!(
            sandbox_id = %sandbox_id,
            map_id = %map_id,
            "bootstrapped first configured sandbox for official dev profile"
        ),
        Err(err) => tracing::warn!(
            sandbox_id = %sandbox_id,
            map_id = %map_id,
            error = %err,
            "failed to bootstrap first configured sandbox for official dev profile"
        ),
    }
}
