//! Local backend map sync client and filesystem cache.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use proto::race::v1::asset_service_client::AssetServiceClient;
use proto::race::v1::{
    GetMapAssetSyncMetaRequest, ListMapsRequest, MapCatalogEntry, MapGlbKind,
    StreamMapAssetGlbRequest,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

use crate::local::broker::fetch_auth_token;

const REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REMOTE_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_CHUNK_LIMIT: u64 = 2 * 1024 * 1024;
const CATALOG_CACHE_FILE: &str = ".maps-catalog-cache.json";

#[derive(Clone)]
pub struct LocalMapAssetsSync {
    endpoint: String,
    cache_dir: PathBuf,
    channel: Channel,
    origin: http::Uri,
    sync_lock: Arc<Mutex<()>>,
    auth_token: Arc<RwLock<Option<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedMapCatalogEntry {
    map_id: String,
    display_name: String,
}

impl LocalMapAssetsSync {
    pub fn new(endpoint: String, cache_dir: PathBuf) -> Result<Self, String> {
        let origin: http::Uri = endpoint
            .parse()
            .map_err(|e| format!("invalid backend endpoint `{endpoint}`: {e}"))?;
        let endpoint_builder = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| format!("invalid backend endpoint `{endpoint}`: {e}"))?;
        let endpoint_builder = if endpoint.starts_with("https://") {
            endpoint_builder
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(|e| format!("invalid TLS config for backend endpoint `{endpoint}`: {e}"))?
        } else {
            endpoint_builder
        };
        std::fs::create_dir_all(&cache_dir).map_err(|e| {
            format!(
                "failed to create local tracks cache directory {}: {e}",
                cache_dir.display()
            )
        })?;
        Ok(Self {
            endpoint,
            cache_dir,
            channel: endpoint_builder
                .connect_timeout(REMOTE_CONNECT_TIMEOUT)
                .connect_lazy(),
            origin,
            sync_lock: Arc::new(Mutex::new(())),
            auth_token: Arc::new(RwLock::new(None)),
        })
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    async fn token_or_fetch(&self) -> Result<String, Status> {
        if let Some(token) = self.auth_token.read().await.clone() {
            return Ok(token);
        }
        let token = fetch_auth_token().await.map_err(map_token_fetch_error)?;
        *self.auth_token.write().await = Some(token.clone());
        Ok(token)
    }

    async fn refresh_token(&self) -> Result<String, Status> {
        let token = fetch_auth_token().await.map_err(map_token_fetch_error)?;
        *self.auth_token.write().await = Some(token.clone());
        Ok(token)
    }

    fn attach_auth_cookie<T>(request: &mut Request<T>, token: &str) -> Result<(), Status> {
        let cookie = format!("auth_token={token}");
        let metadata = MetadataValue::try_from(cookie.as_str())
            .map_err(|_| Status::unauthenticated("invalid auth token cookie"))?;
        request.metadata_mut().insert("cookie", metadata);
        Ok(())
    }

    pub async fn list_maps_remote_or_cached(&self) -> Result<Vec<MapCatalogEntry>, Status> {
        let mut client = AssetServiceClient::with_origin(self.channel.clone(), self.origin.clone());
        let token = self.token_or_fetch().await?;
        let mut request = Request::new(ListMapsRequest {});
        Self::attach_auth_cookie(&mut request, &token)?;
        let first = tokio::time::timeout(REMOTE_RPC_TIMEOUT, client.list_maps(request)).await;
        let response = match first {
            Ok(Ok(response)) => Some(response),
            Ok(Err(status)) if is_auth_error(&status) => {
                let token = self.refresh_token().await?;
                let mut retry_request = Request::new(ListMapsRequest {});
                Self::attach_auth_cookie(&mut retry_request, &token)?;
                match tokio::time::timeout(REMOTE_RPC_TIMEOUT, client.list_maps(retry_request))
                    .await
                {
                    Ok(Ok(response)) => Some(response),
                    Ok(Err(status)) => return self.read_catalog_cache_or(status).await,
                    Err(_) => {
                        return self
                            .read_catalog_cache_or(Status::deadline_exceeded(
                                "map catalog request timed out",
                            ))
                            .await;
                    }
                }
            }
            Ok(Err(status)) => return self.read_catalog_cache_or(status).await,
            Err(_) => {
                return self
                    .read_catalog_cache_or(Status::deadline_exceeded(
                        "map catalog request timed out",
                    ))
                    .await;
            }
        };
        if let Some(response) = response {
            let mut maps = response.into_inner().maps;
            maps.sort_by(|a, b| a.map_id.cmp(&b.map_id));
            self.write_catalog_cache(&maps).await;
            Ok(maps)
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn ensure_map_cached(&self, map_id: &str) -> Result<(), Status> {
        let map_id = sanitize_map_id(map_id)?;
        if self.required_files_exist(&map_id).await {
            return Ok(());
        }

        let _guard = self.sync_lock.lock().await;
        if self.required_files_exist(&map_id).await {
            return Ok(());
        }

        let mut client = AssetServiceClient::with_origin(self.channel.clone(), self.origin.clone());
        let token = self.token_or_fetch().await?;
        let mut request = Request::new(GetMapAssetSyncMetaRequest {
            map_id: map_id.clone(),
        });
        Self::attach_auth_cookie(&mut request, &token)?;
        let first =
            tokio::time::timeout(REMOTE_RPC_TIMEOUT, client.get_map_asset_sync_meta(request))
                .await
                .map_err(|_| Status::deadline_exceeded("map sync metadata request timed out"))?;
        let sync_meta_response = match first {
            Ok(response) => response,
            Err(status) if is_auth_error(&status) => {
                let token = self.refresh_token().await?;
                let mut retry_request = Request::new(GetMapAssetSyncMetaRequest {
                    map_id: map_id.clone(),
                });
                Self::attach_auth_cookie(&mut retry_request, &token)?;
                tokio::time::timeout(
                    REMOTE_RPC_TIMEOUT,
                    client.get_map_asset_sync_meta(retry_request),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("map sync metadata request timed out"))?
                .map_err(|status| {
                    tracing::warn!(
                        endpoint = %self.endpoint,
                        map_id = %map_id,
                        code = ?status.code(),
                        status = %status,
                        "map sync metadata request failed after token refresh"
                    );
                    status
                })?
            }
            Err(status) => {
                tracing::warn!(
                    endpoint = %self.endpoint,
                    map_id = %map_id,
                    code = ?status.code(),
                    status = %status,
                    "map sync metadata request failed"
                );
                return Err(status);
            }
        };
        let sync_meta = sync_meta_response.into_inner();

        if sync_meta.map_id.trim() != map_id {
            return Err(Status::failed_precondition(
                "map sync metadata map_id mismatch",
            ));
        }

        write_bytes_atomically(
            &self.cache_dir.join(format!("{map_id}.minimap.svg")),
            sync_meta.minimap_svg.as_ref(),
        )
        .await?;
        write_bytes_atomically(
            &self.cache_dir.join(format!("{map_id}.json")),
            sync_meta.main_metadata_json.as_ref(),
        )
        .await?;
        write_bytes_atomically(
            &self.cache_dir.join(format!("{map_id}.minimap.json")),
            sync_meta.minimap_metadata_json.as_ref(),
        )
        .await?;

        self.stream_glb_to_cache(&map_id, MapGlbKind::Main, true)
            .await?;
        self.stream_glb_to_cache(&map_id, MapGlbKind::Animation, false)
            .await?;
        Ok(())
    }

    async fn stream_glb_to_cache(
        &self,
        map_id: &str,
        kind: MapGlbKind,
        required: bool,
    ) -> Result<(), Status> {
        let target_path = match kind {
            MapGlbKind::Main => self.cache_dir.join(format!("{map_id}.glb")),
            MapGlbKind::Animation => self.cache_dir.join(format!("{map_id}.animation.glb")),
            MapGlbKind::Unspecified => {
                return Err(Status::invalid_argument("glb kind is required"));
            }
        };
        let mut client = AssetServiceClient::with_origin(self.channel.clone(), self.origin.clone());
        let token = self.token_or_fetch().await?;
        let mut request = Request::new(StreamMapAssetGlbRequest {
            map_id: map_id.to_string(),
            kind: kind as i32,
            offset: 0,
            limit: REMOTE_CHUNK_LIMIT,
        });
        Self::attach_auth_cookie(&mut request, &token)?;
        let first = tokio::time::timeout(REMOTE_RPC_TIMEOUT, client.stream_map_asset_glb(request))
            .await
            .map_err(|_| Status::deadline_exceeded("map glb stream request timed out"))?;

        let response = match first {
            Ok(value) => value,
            Err(status) if is_auth_error(&status) => {
                let token = self.refresh_token().await?;
                let mut retry_request = Request::new(StreamMapAssetGlbRequest {
                    map_id: map_id.to_string(),
                    kind: kind as i32,
                    offset: 0,
                    limit: REMOTE_CHUNK_LIMIT,
                });
                Self::attach_auth_cookie(&mut retry_request, &token)?;
                let retry = tokio::time::timeout(
                    REMOTE_RPC_TIMEOUT,
                    client.stream_map_asset_glb(retry_request),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("map glb stream request timed out"))?;
                match retry {
                    Ok(value) => value,
                    Err(status) => {
                        if !required && status.code() == Code::NotFound {
                            return Ok(());
                        }
                        tracing::warn!(
                            endpoint = %self.endpoint,
                            map_id = %map_id,
                            kind = ?kind,
                            code = ?status.code(),
                            status = %status,
                            "map glb stream request failed after token refresh"
                        );
                        return Err(status);
                    }
                }
            }
            Err(status) => {
                if !required && status.code() == Code::NotFound {
                    return Ok(());
                }
                tracing::warn!(
                    endpoint = %self.endpoint,
                    map_id = %map_id,
                    kind = ?kind,
                    code = ?status.code(),
                    status = %status,
                    "map glb stream request failed"
                );
                return Err(status);
            }
        };

        let tmp_path = tmp_path_for(&target_path);
        let mut file = tokio::fs::File::create(&tmp_path)
            .await
            .map_err(|e| Status::internal(format!("failed to create temp GLB file: {e}")))?;

        let mut stream = response.into_inner();
        let mut expected_offset = 0u64;
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::unavailable(format!("failed to receive GLB stream chunk: {e}")))?
        {
            if chunk.offset != expected_offset {
                return Err(Status::failed_precondition(
                    "invalid GLB stream chunk offset",
                ));
            }
            file.write_all(&chunk.data)
                .await
                .map_err(|e| Status::internal(format!("failed to write GLB chunk: {e}")))?;
            expected_offset = expected_offset.saturating_add(chunk.data.len() as u64);
            if chunk.eof {
                break;
            }
        }
        file.flush()
            .await
            .map_err(|e| Status::internal(format!("failed to flush GLB file: {e}")))?;
        drop(file);
        replace_file_atomically(&tmp_path, &target_path).await
    }

    async fn read_catalog_cache_or(&self, status: Status) -> Result<Vec<MapCatalogEntry>, Status> {
        match self.read_catalog_cache().await {
            Ok(Some(cached)) => {
                tracing::warn!(
                    endpoint = %self.endpoint,
                    code = ?status.code(),
                    status = %status,
                    cached_maps = cached.len(),
                    "map catalog request failed; using cached catalog"
                );
                Ok(cached)
            }
            Ok(None) => Err(status),
            Err(err) => {
                tracing::warn!(
                    endpoint = %self.endpoint,
                    error = %err,
                    code = ?status.code(),
                    status = %status,
                    "map catalog request failed and cache read failed"
                );
                Err(status)
            }
        }
    }

    async fn write_catalog_cache(&self, maps: &[MapCatalogEntry]) {
        let cached = maps
            .iter()
            .map(|entry| CachedMapCatalogEntry {
                map_id: entry.map_id.clone(),
                display_name: entry.display_name.clone(),
            })
            .collect::<Vec<_>>();
        let bytes = match serde_json::to_vec(&cached) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = ?err, "failed to encode map catalog cache");
                return;
            }
        };
        let path = self.cache_dir.join(CATALOG_CACHE_FILE);
        if let Err(err) = write_bytes_atomically(&path, &bytes).await {
            tracing::warn!(error = %err, path = %path.display(), "failed to update map catalog cache");
        }
    }

    async fn read_catalog_cache(&self) -> Result<Option<Vec<MapCatalogEntry>>, Status> {
        let path = self.cache_dir.join(CATALOG_CACHE_FILE);
        let raw = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(Status::internal(format!(
                    "failed to read map catalog cache {}: {err}",
                    path.display()
                )));
            }
        };
        let parsed: Vec<CachedMapCatalogEntry> = serde_json::from_slice(&raw).map_err(|err| {
            Status::failed_precondition(format!(
                "invalid map catalog cache {}: {err}",
                path.display()
            ))
        })?;
        Ok(Some(
            parsed
                .into_iter()
                .map(|entry| MapCatalogEntry {
                    map_id: entry.map_id,
                    display_name: entry.display_name,
                })
                .collect(),
        ))
    }

    async fn required_files_exist(&self, map_id: &str) -> bool {
        let required = [
            self.cache_dir.join(format!("{map_id}.glb")),
            self.cache_dir.join(format!("{map_id}.json")),
            self.cache_dir.join(format!("{map_id}.minimap.json")),
            self.cache_dir.join(format!("{map_id}.minimap.svg")),
        ];
        for path in &required {
            match tokio::fs::try_exists(path).await {
                Ok(true) => {}
                _ => return false,
            }
        }
        true
    }
}

fn sanitize_map_id(map_id: &str) -> Result<String, Status> {
    let map_id = map_id.trim();
    if map_id.is_empty() {
        return Err(Status::invalid_argument("map_id is required"));
    }
    if !map_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Status::invalid_argument(
            "map_id contains invalid characters",
        ));
    }
    Ok(map_id.to_string())
}

fn map_token_fetch_error(err: anyhow::Error) -> Status {
    let message = err.to_string();
    if message.contains("login required") {
        return Status::unauthenticated(message);
    }
    Status::unavailable(format!("failed to get auth token for map sync: {message}"))
}

fn is_auth_error(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::PermissionDenied
    )
}

async fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<(), Status> {
    let tmp_path = tmp_path_for(path);
    tokio::fs::write(&tmp_path, bytes).await.map_err(|e| {
        Status::internal(format!(
            "failed to write temp file {}: {e}",
            tmp_path.display()
        ))
    })?;
    replace_file_atomically(&tmp_path, path).await
}

async fn replace_file_atomically(tmp_path: &Path, final_path: &Path) -> Result<(), Status> {
    let _ = tokio::fs::remove_file(final_path).await;
    tokio::fs::rename(tmp_path, final_path).await.map_err(|e| {
        Status::internal(format!(
            "failed to replace file {}: {e}",
            final_path.display()
        ))
    })?;
    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("tmp");
    path.with_file_name(format!("{file_name}.tmp"))
}
