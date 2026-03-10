use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::error::LocalSandboxConfigStoreError;
use super::model::{
    LocalSandboxConfigRecord, LocalSandboxConfigSnapshot, LocalSandboxSpawnModeRecord,
    LocalTimeOfDaySettingsRecord, LocalWeatherSettingsRecord, validate_local_sandbox_config_input,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LocalSandboxConfigState {
    #[serde(default)]
    revision: u64,
    #[serde(default)]
    sandboxes: Vec<LocalSandboxConfigRecord>,
}

/// JSON-backed, in-process store for local sandbox configs.
#[derive(Clone)]
pub struct LocalSandboxConfigStore {
    path: PathBuf,
    state: Arc<RwLock<LocalSandboxConfigState>>,
}

impl LocalSandboxConfigStore {
    /// Loads store from disk or creates an empty one if file does not exist.
    pub async fn load_or_create(path: PathBuf) -> Result<Self, LocalSandboxConfigStoreError> {
        let exists = tokio::fs::try_exists(&path).await?;
        let state = if exists {
            let raw = tokio::fs::read(&path).await?;
            if raw.is_empty() {
                LocalSandboxConfigState::default()
            } else {
                serde_json::from_slice::<LocalSandboxConfigState>(&raw)?
            }
        } else {
            LocalSandboxConfigState::default()
        };

        let store = Self {
            path,
            state: Arc::new(RwLock::new(state)),
        };

        if !exists {
            store.persist().await?;
        }

        Ok(store)
    }

    /// Returns configured JSON store file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads full local sandbox config snapshot.
    pub async fn get_snapshot(&self) -> LocalSandboxConfigSnapshot {
        let guard = self.state.read().await;
        LocalSandboxConfigSnapshot {
            revision: guard.revision,
            sandboxes: guard.sandboxes.clone(),
        }
    }

    /// Creates a local sandbox config and bumps revision.
    pub async fn create_config(
        &self,
        expected_revision: u64,
        sandbox: LocalSandboxConfigRecord,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        validate_local_sandbox_config_input(&sandbox.config)?;
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        if guard
            .sandboxes
            .iter()
            .any(|entry| entry.sandbox_id == sandbox.sandbox_id)
        {
            return Err(LocalSandboxConfigStoreError::AlreadyExists {
                sandbox_id: sandbox.sandbox_id,
            });
        }

        guard.sandboxes.push(sandbox);
        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    /// Updates a local sandbox config and bumps revision.
    pub async fn update_config(
        &self,
        expected_revision: u64,
        sandbox: LocalSandboxConfigRecord,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        validate_local_sandbox_config_input(&sandbox.config)?;
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        let Some(current) = guard
            .sandboxes
            .iter_mut()
            .find(|entry| entry.sandbox_id == sandbox.sandbox_id)
        else {
            return Err(LocalSandboxConfigStoreError::NotFound {
                sandbox_id: sandbox.sandbox_id,
            });
        };

        *current = sandbox;
        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    /// Deletes a local sandbox config and bumps revision.
    pub async fn delete_config(
        &self,
        expected_revision: u64,
        sandbox_id: &str,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        let before = guard.sandboxes.len();
        guard
            .sandboxes
            .retain(|entry| entry.sandbox_id != sandbox_id);
        if guard.sandboxes.len() == before {
            return Err(LocalSandboxConfigStoreError::NotFound {
                sandbox_id: sandbox_id.to_string(),
            });
        }

        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    /// Updates local sandbox time-of-day settings and bumps revision.
    pub async fn update_time_of_day(
        &self,
        expected_revision: u64,
        sandbox_id: &str,
        time_of_day: LocalTimeOfDaySettingsRecord,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        let Some(current) = guard
            .sandboxes
            .iter_mut()
            .find(|entry| entry.sandbox_id == sandbox_id)
        else {
            return Err(LocalSandboxConfigStoreError::NotFound {
                sandbox_id: sandbox_id.to_string(),
            });
        };
        current.config.time_of_day = time_of_day;
        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    /// Updates local sandbox weather settings and bumps revision.
    pub async fn update_weather(
        &self,
        expected_revision: u64,
        sandbox_id: &str,
        weather: LocalWeatherSettingsRecord,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        let Some(current) = guard
            .sandboxes
            .iter_mut()
            .find(|entry| entry.sandbox_id == sandbox_id)
        else {
            return Err(LocalSandboxConfigStoreError::NotFound {
                sandbox_id: sandbox_id.to_string(),
            });
        };
        current.config.weather = weather;
        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    /// Updates local sandbox spawn mode and bumps revision.
    pub async fn update_spawn_mode(
        &self,
        expected_revision: u64,
        sandbox_id: &str,
        spawn_mode: LocalSandboxSpawnModeRecord,
    ) -> Result<u64, LocalSandboxConfigStoreError> {
        let mut guard = self.state.write().await;
        ensure_expected_revision(expected_revision, guard.revision)?;
        let Some(current) = guard
            .sandboxes
            .iter_mut()
            .find(|entry| entry.sandbox_id == sandbox_id)
        else {
            return Err(LocalSandboxConfigStoreError::NotFound {
                sandbox_id: sandbox_id.to_string(),
            });
        };
        current.config.spawn_mode = spawn_mode;
        guard.revision = guard.revision.saturating_add(1);
        let revision = guard.revision;
        drop(guard);
        self.persist().await?;
        Ok(revision)
    }

    async fn persist(&self) -> Result<(), LocalSandboxConfigStoreError> {
        let guard = self.state.read().await;
        let raw = serde_json::to_vec_pretty(&*guard)?;
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&self.path, raw).await?;
        Ok(())
    }
}

fn ensure_expected_revision(
    expected_revision: u64,
    current_revision: u64,
) -> Result<(), LocalSandboxConfigStoreError> {
    if expected_revision == current_revision {
        return Ok(());
    }
    Err(LocalSandboxConfigStoreError::RevisionMismatch {
        expected: expected_revision,
        actual: current_revision,
    })
}
