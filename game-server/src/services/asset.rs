//! gRPC AssetService implementation for map assets and minimap metadata.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(any(
    feature = "official",
    all(feature = "local", not(feature = "standalone"))
))]
use std::sync::Arc;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use hash_cache::HashCache;
use proto::race::v1::asset_service_server::AssetService;
use proto::race::v1::{
    GetMapAssetBundleMetaRequest, GetMapAssetBundleMetaResponse, GetMapAssetRequest,
    GetMapAssetResponse, GetMapAssetSyncMetaRequest, GetMapAssetSyncMetaResponse,
    HelicopterViewMetadata, ListAllMapsRequest, ListAllMapsResponse, ListMapsRequest,
    ListMapsResponse, MapAssetBundleMeta, MapAssetKind, MapAssetMeta, MapCatalogEntry, MapGlbKind,
    MapMetadata, MimeType, MinimapMetadata, StreamMapAssetGlbRequest, StreamMapAssetGlbResponse,
    Vector3,
};
use serde::Deserialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tonic::{Request, Response, Status};

#[cfg(feature = "official")]
use crate::auth::auth_claims::TokenValidator;
#[cfg(all(feature = "local", not(feature = "standalone")))]
use crate::local::map_assets::LocalMapAssetsSync;
#[cfg(any(feature = "official", feature = "standalone"))]
use std::collections::HashMap;
#[cfg(any(feature = "official", feature = "standalone"))]
use std::time::{Duration, Instant};
#[cfg(any(feature = "official", feature = "standalone"))]
use tokio::sync::RwLock;

type BoxStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64KB
const MAX_CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2MB
#[cfg(any(feature = "official", feature = "standalone"))]
const MAP_BUNDLE_META_CACHE_TTL: Duration = Duration::from_secs(120);
#[cfg(any(feature = "official", feature = "standalone"))]
const LOCAL_MARKER_FILE: &str = ".local";

#[derive(Debug, Deserialize)]
struct MinimapMetadataJson {
    #[serde(alias = "viewBoxMinX")]
    view_box_min_x: f64,
    #[serde(alias = "viewBoxMinY")]
    view_box_min_y: f64,
    #[serde(alias = "viewBoxWidth")]
    view_box_width: f64,
    #[serde(alias = "viewBoxHeight")]
    view_box_height: f64,
    #[serde(alias = "originWorldX")]
    origin_world_x: f64,
    #[serde(alias = "originWorldZ")]
    origin_world_z: f64,
    #[serde(alias = "rotationRad")]
    rotation_rad: f64,
    #[serde(alias = "scaleSvgUnitsPerMeter")]
    scale_svg_units_per_meter: f64,
}

#[derive(Debug, Deserialize)]
struct MapMetadataJson {
    name: String,
    #[serde(alias = "lapLengthMeters")]
    lap_length_meters: f32,
    #[serde(default, alias = "followCameraAnchorPositions")]
    follow_camera_anchor_positions: Vec<Vector3Json>,
    #[serde(default, alias = "helicopterView")]
    helicopter_view: Option<HelicopterViewMetadataJson>,
}

#[derive(Debug, Deserialize)]
struct Vector3Json {
    x: f32,
    y: f32,
    z: f32,
}

#[derive(Debug, Deserialize)]
struct HelicopterViewMetadataJson {
    #[serde(alias = "ellipseCenterWorld")]
    ellipse_center_world: Option<Vector3Json>,
    #[serde(alias = "ellipseSemiAxisXM", alias = "ellipseSemiAxisXm")]
    ellipse_semi_axis_x_m: f32,
    #[serde(alias = "ellipseSemiAxisZM", alias = "ellipseSemiAxisZm")]
    ellipse_semi_axis_z_m: f32,
    #[serde(alias = "ellipseRotationRad")]
    ellipse_rotation_rad: f32,
    #[serde(alias = "desiredArcOffsetTowardsM")]
    desired_arc_offset_towards_m: f32,
    #[serde(alias = "desiredArcOffsetAwayM")]
    desired_arc_offset_away_m: f32,
}

impl From<Vector3Json> for Vector3 {
    fn from(value: Vector3Json) -> Self {
        Self {
            x: value.x,
            y: value.y,
            z: value.z,
        }
    }
}

impl From<HelicopterViewMetadataJson> for HelicopterViewMetadata {
    fn from(value: HelicopterViewMetadataJson) -> Self {
        Self {
            ellipse_center_world: value.ellipse_center_world.map(Into::into),
            ellipse_semi_axis_x_m: value.ellipse_semi_axis_x_m,
            ellipse_semi_axis_z_m: value.ellipse_semi_axis_z_m,
            ellipse_rotation_rad: value.ellipse_rotation_rad,
            desired_arc_offset_towards_m: value.desired_arc_offset_towards_m,
            desired_arc_offset_away_m: value.desired_arc_offset_away_m,
        }
    }
}

impl From<MinimapMetadataJson> for MinimapMetadata {
    fn from(value: MinimapMetadataJson) -> Self {
        Self {
            view_box_min_x: value.view_box_min_x,
            view_box_min_y: value.view_box_min_y,
            view_box_width: value.view_box_width,
            view_box_height: value.view_box_height,
            origin_world_x: value.origin_world_x,
            origin_world_z: value.origin_world_z,
            rotation_rad: value.rotation_rad,
            scale_svg_units_per_meter: value.scale_svg_units_per_meter,
        }
    }
}

impl From<MapMetadataJson> for MapMetadata {
    fn from(value: MapMetadataJson) -> Self {
        Self {
            map_id: String::new(),
            map_name: value.name,
            total_length_m: value.lap_length_meters,
            follow_camera_anchor_positions: value
                .follow_camera_anchor_positions
                .into_iter()
                .map(Into::into)
                .collect(),
            helicopter_view: value.helicopter_view.map(Into::into),
        }
    }
}

/// gRPC AssetService implementation.
pub struct AssetServiceImpl {
    serving_enabled: bool,
    tracks_dir: PathBuf,
    hash_cache: HashCache,
    #[cfg(any(feature = "official", feature = "standalone"))]
    map_bundle_meta_cache: RwLock<HashMap<String, CachedMapBundleMeta>>,
    #[cfg(feature = "official")]
    admin_token_validator: Option<Arc<TokenValidator>>,
    #[cfg(all(feature = "local", not(feature = "standalone")))]
    local_sync: Option<Arc<LocalMapAssetsSync>>,
}

#[cfg(any(feature = "official", feature = "standalone"))]
#[derive(Debug, Clone)]
struct MapBundlePaths {
    storage_key: String,
    internal_map_id: String,
    bundle_dir: PathBuf,
    main_glb_path: PathBuf,
    animation_glb_path: PathBuf,
    minimap_svg_path: PathBuf,
    minimap_metadata_path: PathBuf,
    map_metadata_path: PathBuf,
    local_marker_path: PathBuf,
}

#[cfg(any(feature = "official", feature = "standalone"))]
#[derive(Debug, Clone)]
struct CachedMapBundleMeta {
    bundle: MapAssetBundleMeta,
    cached_at: Instant,
}

impl AssetServiceImpl {
    fn new_internal(
        tracks_dir: PathBuf,
        serving_enabled: bool,
        #[cfg(feature = "official")] admin_token_validator: Option<Arc<TokenValidator>>,
        use_tracks_hash_sidecar_dir: bool,
        #[cfg(all(feature = "local", not(feature = "standalone")))] local_sync: Option<
            Arc<LocalMapAssetsSync>,
        >,
    ) -> Self {
        let hash_cache = if use_tracks_hash_sidecar_dir {
            let sidecars = tracks_dir.join(".hashes");
            HashCache::new(Some(sidecars))
        } else {
            // Nested bundle layout can reuse internal map ids, so flat sidecar dir would collide.
            HashCache::new(None)
        };
        Self {
            serving_enabled,
            tracks_dir,
            hash_cache,
            #[cfg(any(feature = "official", feature = "standalone"))]
            map_bundle_meta_cache: RwLock::new(HashMap::new()),
            #[cfg(feature = "official")]
            admin_token_validator,
            #[cfg(all(feature = "local", not(feature = "standalone")))]
            local_sync,
        }
    }

    #[cfg(feature = "official")]
    pub fn for_official(tracks_dir: PathBuf, admin_token_validator: Arc<TokenValidator>) -> Self {
        Self::new_internal(tracks_dir, true, Some(admin_token_validator), false)
    }

    #[cfg(all(feature = "local", not(feature = "standalone")))]
    pub fn for_local(tracks_cache_dir: PathBuf, local_sync: Arc<LocalMapAssetsSync>) -> Self {
        Self::new_internal(tracks_cache_dir, true, true, Some(local_sync))
    }

    #[cfg(feature = "standalone")]
    pub fn for_standalone(tracks_dir: PathBuf) -> Self {
        Self::new_internal(tracks_dir, true, false)
    }

    fn sanitize_internal_map_id(id: &str) -> Result<(), Status> {
        if !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            Err(Status::invalid_argument("invalid internal map id"))
        } else {
            Ok(())
        }
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    fn is_valid_storage_key(storage_key: &str) -> bool {
        storage_key.chars().all(|c| c.is_ascii_alphanumeric())
    }

    fn validate_requested_map_id(raw_map_id: &str) -> Result<String, Status> {
        let map_id = raw_map_id.trim();
        if map_id.is_empty() {
            return Err(Status::invalid_argument("map_id is required"));
        }
        #[cfg(any(feature = "official", feature = "standalone"))]
        if !Self::is_valid_storage_key(map_id) {
            return Err(Status::invalid_argument(
                "map_id(storage_key) must be alphanumeric",
            ));
        }
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        Self::sanitize_internal_map_id(map_id)?;
        Ok(map_id.to_string())
    }

    fn resolve_main_glb_path(root: &Path, internal_map_id: &str) -> PathBuf {
        root.join(format!("{internal_map_id}.glb"))
    }

    fn resolve_animation_glb_path(root: &Path, internal_map_id: &str) -> PathBuf {
        root.join(format!("{internal_map_id}.animation.glb"))
    }

    fn resolve_minimap_svg_path(root: &Path, internal_map_id: &str) -> PathBuf {
        root.join(format!("{internal_map_id}.minimap.svg"))
    }

    fn resolve_minimap_metadata_path(root: &Path, internal_map_id: &str) -> PathBuf {
        root.join(format!("{internal_map_id}.minimap.json"))
    }

    fn resolve_map_metadata_path(root: &Path, internal_map_id: &str) -> PathBuf {
        root.join(format!("{internal_map_id}.json"))
    }

    fn resolve_path_for_glb_kind(
        root: &Path,
        internal_map_id: &str,
        kind: MapGlbKind,
    ) -> Result<PathBuf, Status> {
        match kind {
            MapGlbKind::Main => Ok(Self::resolve_main_glb_path(root, internal_map_id)),
            MapGlbKind::Animation => Ok(Self::resolve_animation_glb_path(root, internal_map_id)),
            MapGlbKind::Unspecified => Err(Status::invalid_argument("glb kind is required")),
        }
    }

    fn internal_map_id_from_main_glb_name(file_name: &str) -> Option<String> {
        if !file_name.ends_with(".glb") || file_name.ends_with(".animation.glb") {
            return None;
        }
        let map_id = file_name.strip_suffix(".glb")?;
        Some(map_id.to_string())
    }

    #[cfg(all(feature = "local", not(feature = "standalone")))]
    fn has_required_bundle_assets(root: &Path, internal_map_id: &str) -> bool {
        let main = Self::resolve_main_glb_path(root, internal_map_id);
        let minimap_svg = Self::resolve_minimap_svg_path(root, internal_map_id);
        let minimap_metadata = Self::resolve_minimap_metadata_path(root, internal_map_id);
        let map_metadata = Self::resolve_map_metadata_path(root, internal_map_id);
        main.is_file()
            && minimap_svg.is_file()
            && minimap_metadata.is_file()
            && map_metadata.is_file()
    }

    fn choose_chunk_size(requested: u64) -> Result<usize, Status> {
        if requested == 0 {
            Ok(DEFAULT_CHUNK_SIZE)
        } else {
            let size = requested as usize;
            if size > MAX_CHUNK_SIZE {
                Err(Status::invalid_argument("limit too large"))
            } else {
                Ok(size)
            }
        }
    }

    fn content_type_for_kind(kind: MapAssetKind) -> Result<MimeType, Status> {
        match kind {
            MapAssetKind::MainGlb | MapAssetKind::AnimationGlb => Ok(MimeType::GltfBinary),
            MapAssetKind::MinimapSvg => Ok(MimeType::ImageSvgXml),
            MapAssetKind::Unspecified => {
                Err(Status::invalid_argument("map asset kind is required"))
            }
        }
    }

    fn resolve_path_for_kind(
        root: &Path,
        internal_map_id: &str,
        kind: MapAssetKind,
    ) -> Result<PathBuf, Status> {
        match kind {
            MapAssetKind::MainGlb => Ok(Self::resolve_main_glb_path(root, internal_map_id)),
            MapAssetKind::AnimationGlb => {
                Ok(Self::resolve_animation_glb_path(root, internal_map_id))
            }
            MapAssetKind::MinimapSvg => Ok(Self::resolve_minimap_svg_path(root, internal_map_id)),
            MapAssetKind::Unspecified => {
                Err(Status::invalid_argument("map asset kind is required"))
            }
        }
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    fn bundle_paths(
        storage_key: &str,
        internal_map_id: &str,
        bundle_dir: PathBuf,
    ) -> MapBundlePaths {
        MapBundlePaths {
            storage_key: storage_key.to_string(),
            internal_map_id: internal_map_id.to_string(),
            main_glb_path: Self::resolve_main_glb_path(&bundle_dir, internal_map_id),
            animation_glb_path: Self::resolve_animation_glb_path(&bundle_dir, internal_map_id),
            minimap_svg_path: Self::resolve_minimap_svg_path(&bundle_dir, internal_map_id),
            minimap_metadata_path: Self::resolve_minimap_metadata_path(
                &bundle_dir,
                internal_map_id,
            ),
            map_metadata_path: Self::resolve_map_metadata_path(&bundle_dir, internal_map_id),
            local_marker_path: bundle_dir.join(LOCAL_MARKER_FILE),
            bundle_dir,
        }
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    fn bundle_is_complete(bundle: &MapBundlePaths) -> bool {
        bundle.main_glb_path.is_file()
            && bundle.minimap_svg_path.is_file()
            && bundle.minimap_metadata_path.is_file()
            && bundle.map_metadata_path.is_file()
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    async fn resolve_official_bundle_paths(
        &self,
        storage_key: &str,
    ) -> Result<MapBundlePaths, Status> {
        let bundle_dir = self.tracks_dir.join(storage_key);
        let bundle_meta = fs::metadata(&bundle_dir)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => Status::not_found("map asset bundle not found"),
                _ => {
                    tracing::error!(
                        error = ?e,
                        storage_key = %storage_key,
                        path = %bundle_dir.display(),
                        "failed to read map bundle directory metadata"
                    );
                    Status::internal("failed to inspect map asset bundle")
                }
            })?;
        if !bundle_meta.is_dir() {
            return Err(Status::not_found("map asset bundle not found"));
        }

        let mut entries = fs::read_dir(&bundle_dir).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                storage_key = %storage_key,
                path = %bundle_dir.display(),
                "failed to list map bundle directory"
            );
            Status::internal("failed to inspect map asset bundle")
        })?;

        let mut internal_map_id: Option<String> = None;
        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            tracing::error!(
                error = ?e,
                storage_key = %storage_key,
                path = %bundle_dir.display(),
                "failed to read map bundle directory entry"
            );
            Status::internal("failed to inspect map asset bundle")
        })? {
            let file_type = entry.file_type().await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    storage_key = %storage_key,
                    path = %bundle_dir.display(),
                    "failed to inspect map bundle entry file type"
                );
                Status::internal("failed to inspect map asset bundle")
            })?;
            if !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(value) => value,
                None => continue,
            };
            let Some(candidate) = Self::internal_map_id_from_main_glb_name(file_name) else {
                continue;
            };

            if Self::sanitize_internal_map_id(&candidate).is_err() {
                tracing::warn!(storage_key = %storage_key, internal_map_id = %candidate, "skipping invalid internal map id candidate");
                continue;
            }

            if let Some(existing) = &internal_map_id {
                tracing::warn!(
                    storage_key = %storage_key,
                    existing_internal_map_id = %existing,
                    conflicting_internal_map_id = %candidate,
                    "map bundle has multiple main glb files"
                );
                return Err(Status::failed_precondition(
                    "map bundle contains multiple main glb files",
                ));
            }
            internal_map_id = Some(candidate);
        }

        let internal_map_id = internal_map_id.ok_or_else(|| {
            tracing::warn!(storage_key = %storage_key, path = %bundle_dir.display(), "map bundle has no main glb file");
            Status::failed_precondition("map bundle does not contain main glb")
        })?;
        let bundle = Self::bundle_paths(storage_key, &internal_map_id, bundle_dir);
        if !Self::bundle_is_complete(&bundle) {
            tracing::warn!(storage_key = %storage_key, internal_map_id = %bundle.internal_map_id, path = %bundle.bundle_dir.display(), "map bundle is incomplete");
            return Err(Status::failed_precondition("map bundle is incomplete"));
        }
        Ok(bundle)
    }

    async fn build_asset_meta(
        &self,
        map_id: &str,
        kind: MapAssetKind,
        path: &Path,
    ) -> Result<MapAssetMeta, Status> {
        let content_type = Self::content_type_for_kind(kind)?;
        let fs_meta = fs::metadata(path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => {
                tracing::warn!(%map_id, kind = ?kind, file = %path.display(), "asset file missing");
                Status::not_found("map asset not found")
            }
            _ => {
                tracing::error!(
                    error = ?e,
                    %map_id,
                    kind = ?kind,
                    file = %path.display(),
                    "asset metadata failed"
                );
                Status::internal("failed to read map asset metadata")
            }
        })?;

        let content_hash = self.hash_cache.get_or_compute(path).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                %map_id,
                kind = ?kind,
                file = %path.display(),
                "asset hash computation failed"
            );
            Status::internal("failed to hash map asset")
        })?;

        Ok(MapAssetMeta {
            kind: kind as i32,
            content_type: content_type as i32,
            size_bytes: fs_meta.len(),
            content_hash,
        })
    }

    async fn read_minimap_metadata(
        &self,
        map_id: &str,
        path: &Path,
    ) -> Result<MinimapMetadata, Status> {
        let raw = fs::read_to_string(path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => {
                tracing::warn!(%map_id, file = %path.display(), "minimap metadata file missing");
                Status::not_found("map minimap metadata not found")
            }
            _ => {
                tracing::error!(
                    error = ?e,
                    %map_id,
                    file = %path.display(),
                    "failed to read minimap metadata file"
                );
                Status::internal("failed to read minimap metadata")
            }
        })?;

        let parsed: MinimapMetadataJson = serde_json::from_str(&raw).map_err(|e| {
            tracing::warn!(
                error = ?e,
                %map_id,
                file = %path.display(),
                "invalid minimap metadata json"
            );
            Status::failed_precondition("invalid minimap metadata json")
        })?;

        Ok(parsed.into())
    }

    async fn read_map_metadata(
        &self,
        public_map_id: &str,
        path: &Path,
    ) -> Result<MapMetadata, Status> {
        let raw = fs::read_to_string(path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => {
                tracing::warn!(map_id = %public_map_id, file = %path.display(), "map metadata file missing");
                Status::not_found("map metadata not found")
            }
            _ => {
                tracing::error!(
                    error = ?e,
                    map_id = %public_map_id,
                    file = %path.display(),
                    "failed to read map metadata file"
                );
                Status::internal("failed to read map metadata")
            }
        })?;

        let parsed: MapMetadataJson = serde_json::from_str(&raw).map_err(|e| {
            tracing::warn!(
                error = ?e,
                map_id = %public_map_id,
                file = %path.display(),
                "invalid map metadata json"
            );
            Status::failed_precondition("invalid map metadata json")
        })?;

        if parsed.name.trim().is_empty() {
            tracing::warn!(map_id = %public_map_id, file = %path.display(), "map metadata map_name is empty");
            return Err(Status::failed_precondition(
                "map metadata map_name must be non-empty",
            ));
        }

        let mut map_metadata: MapMetadata = parsed.into();
        map_metadata.map_id = public_map_id.to_string();
        Ok(map_metadata)
    }

    #[cfg(all(feature = "local", not(feature = "standalone")))]
    async fn ensure_local_map_cached_if_needed(&self, map_id: &str) -> Result<(), Status> {
        if let Some(sync) = &self.local_sync {
            if !Self::has_required_bundle_assets(&self.tracks_dir, map_id) {
                sync.ensure_map_cached(map_id).await?;
            }
        }
        Ok(())
    }

    #[cfg(any(not(feature = "local"), feature = "standalone"))]
    async fn ensure_local_map_cached_if_needed(&self, _map_id: &str) -> Result<(), Status> {
        Ok(())
    }

    #[cfg(all(feature = "local", not(feature = "standalone")))]
    async fn list_maps_from_dir(&self, root: &Path) -> Result<Vec<MapCatalogEntry>, Status> {
        let mut maps = Vec::new();
        let mut entries = fs::read_dir(root).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                path = %root.display(),
                "failed to list tracks directory"
            );
            Status::internal("failed to list maps")
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            tracing::error!(
                error = ?e,
                path = %root.display(),
                "failed to read tracks directory entry"
            );
            Status::internal("failed to list maps")
        })? {
            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(value) => value,
                None => continue,
            };
            let Some(map_id) = Self::internal_map_id_from_main_glb_name(file_name) else {
                continue;
            };

            if Self::sanitize_internal_map_id(&map_id).is_err() {
                tracing::warn!(%map_id, "skipping invalid map id");
                continue;
            }

            if !Self::has_required_bundle_assets(root, &map_id) {
                tracing::warn!(%map_id, "skipping incomplete map asset bundle");
                continue;
            }

            maps.push(MapCatalogEntry {
                map_id: map_id.clone(),
                display_name: map_id,
            });
        }

        maps.sort_by(|a, b| a.map_id.cmp(&b.map_id));
        Ok(maps)
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    async fn list_official_maps(
        &self,
        only_local_marked: bool,
    ) -> Result<Vec<MapCatalogEntry>, Status> {
        let mut maps = Vec::new();
        let mut entries = fs::read_dir(&self.tracks_dir).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                path = %self.tracks_dir.display(),
                "failed to list tracks directory"
            );
            Status::internal("failed to list maps")
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            tracing::error!(
                error = ?e,
                path = %self.tracks_dir.display(),
                "failed to read tracks directory entry"
            );
            Status::internal("failed to list maps")
        })? {
            let file_type = entry.file_type().await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    path = %self.tracks_dir.display(),
                    "failed to inspect tracks directory entry type"
                );
                Status::internal("failed to list maps")
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let storage_key = entry.file_name();
            let storage_key = match storage_key.to_str() {
                Some(value) => value.trim(),
                None => continue,
            };

            if !Self::is_valid_storage_key(storage_key) {
                tracing::warn!(storage_key = %storage_key, "skipping invalid storage key");
                continue;
            }

            let bundle = match self.resolve_official_bundle_paths(storage_key).await {
                Ok(bundle) => bundle,
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::NotFound | tonic::Code::FailedPrecondition
                    ) =>
                {
                    tracing::warn!(
                        storage_key = %storage_key,
                        error = %status.message(),
                        "skipping incomplete or invalid map bundle"
                    );
                    continue;
                }
                Err(status) => return Err(status),
            };

            if only_local_marked && !bundle.local_marker_path.is_file() {
                continue;
            }

            let map_metadata = match self
                .read_map_metadata(&bundle.storage_key, &bundle.map_metadata_path)
                .await
            {
                Ok(metadata) => metadata,
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::NotFound | tonic::Code::FailedPrecondition
                    ) =>
                {
                    tracing::warn!(
                        storage_key = %bundle.storage_key,
                        error = %status.message(),
                        "skipping map bundle with invalid metadata"
                    );
                    continue;
                }
                Err(status) => return Err(status),
            };

            maps.push(MapCatalogEntry {
                map_id: bundle.storage_key.clone(),
                display_name: map_metadata.map_name,
            });
        }

        maps.sort_by(|a, b| a.map_id.cmp(&b.map_id));
        Ok(maps)
    }

    #[cfg(feature = "official")]
    async fn require_admin(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        let validator = self
            .admin_token_validator
            .as_ref()
            .ok_or_else(|| Status::internal("asset admin validator is not configured"))?;
        let is_admin = validator.is_admin(metadata).await?;
        if !is_admin {
            return Err(Status::permission_denied("admin role required"));
        }
        Ok(())
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    async fn try_get_cached_bundle_meta(&self, map_id: &str) -> Option<MapAssetBundleMeta> {
        let now = Instant::now();
        {
            let cache = self.map_bundle_meta_cache.read().await;
            if let Some(entry) = cache.get(map_id) {
                if now.duration_since(entry.cached_at) < MAP_BUNDLE_META_CACHE_TTL {
                    return Some(entry.bundle.clone());
                }
            }
        }

        let mut cache = self.map_bundle_meta_cache.write().await;
        if let Some(entry) = cache.get(map_id) {
            if now.duration_since(entry.cached_at) < MAP_BUNDLE_META_CACHE_TTL {
                return Some(entry.bundle.clone());
            }
            cache.remove(map_id);
        }
        None
    }

    #[cfg(any(feature = "official", feature = "standalone"))]
    async fn store_cached_bundle_meta(&self, map_id: String, bundle: MapAssetBundleMeta) {
        let mut cache = self.map_bundle_meta_cache.write().await;
        cache.insert(
            map_id,
            CachedMapBundleMeta {
                bundle,
                cached_at: Instant::now(),
            },
        );
    }
}

#[tonic::async_trait]
impl AssetService for AssetServiceImpl {
    async fn list_maps(
        &self,
        _request: Request<ListMapsRequest>,
    ) -> Result<Response<ListMapsResponse>, Status> {
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        if let Some(sync) = &self.local_sync {
            let maps = sync.list_maps_remote_or_cached().await?;
            return Ok(Response::new(ListMapsResponse { maps }));
        }

        if !self.serving_enabled {
            return Ok(Response::new(ListMapsResponse { maps: Vec::new() }));
        }

        #[cfg(feature = "official")]
        let maps = self.list_official_maps(true).await?;
        #[cfg(feature = "standalone")]
        let maps = self.list_official_maps(false).await?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let maps = self.list_maps_from_dir(&self.tracks_dir).await?;
        Ok(Response::new(ListMapsResponse { maps }))
    }

    async fn list_all_maps(
        &self,
        request: Request<ListAllMapsRequest>,
    ) -> Result<Response<ListAllMapsResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "ListAllMaps is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            self.require_admin(request.metadata()).await?;
            let _ = request.into_inner();
            let maps = self.list_official_maps(false).await?;
            Ok(Response::new(ListAllMapsResponse { maps }))
        }
    }

    async fn get_map_asset_bundle_meta(
        &self,
        request: Request<GetMapAssetBundleMetaRequest>,
    ) -> Result<Response<GetMapAssetBundleMetaResponse>, Status> {
        let GetMapAssetBundleMetaRequest { map_id } = request.into_inner();
        let map_id = Self::validate_requested_map_id(&map_id)?;
        self.ensure_local_map_cached_if_needed(&map_id).await?;
        if !self.serving_enabled {
            return Err(Status::not_found("map asset bundle not found"));
        }
        #[cfg(any(feature = "official", feature = "standalone"))]
        if let Some(bundle) = self.try_get_cached_bundle_meta(&map_id).await {
            tracing::debug!(%map_id, "map asset bundle meta cache hit");
            return Ok(Response::new(GetMapAssetBundleMetaResponse {
                bundle: Some(bundle),
            }));
        }

        #[cfg(any(feature = "official", feature = "standalone"))]
        let bundle_paths = self.resolve_official_bundle_paths(&map_id).await?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let main_glb_path = Self::resolve_main_glb_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let main_glb_path = bundle_paths.main_glb_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let animation_glb_path = Self::resolve_animation_glb_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let animation_glb_path = bundle_paths.animation_glb_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let minimap_svg_path = Self::resolve_minimap_svg_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let minimap_svg_path = bundle_paths.minimap_svg_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let minimap_metadata_path = Self::resolve_minimap_metadata_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let minimap_metadata_path = bundle_paths.minimap_metadata_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let map_metadata_path = Self::resolve_map_metadata_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let map_metadata_path = bundle_paths.map_metadata_path.clone();

        let main_glb = self
            .build_asset_meta(&map_id, MapAssetKind::MainGlb, &main_glb_path)
            .await?;
        let minimap_svg = self
            .build_asset_meta(&map_id, MapAssetKind::MinimapSvg, &minimap_svg_path)
            .await?;
        let minimap_metadata = self
            .read_minimap_metadata(&map_id, &minimap_metadata_path)
            .await?;
        let map_metadata = self.read_map_metadata(&map_id, &map_metadata_path).await?;

        let animation_glb = match fs::metadata(&animation_glb_path).await {
            Ok(_) => Some(
                self.build_asset_meta(&map_id, MapAssetKind::AnimationGlb, &animation_glb_path)
                    .await?,
            ),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    %map_id,
                    file = %animation_glb_path.display(),
                    "failed to read animation asset metadata"
                );
                return Err(Status::internal("failed to read map asset metadata"));
            }
        };

        let bundle = MapAssetBundleMeta {
            map_id: map_id.clone(),
            main_glb: Some(main_glb),
            animation_glb,
            minimap_svg: Some(minimap_svg),
            minimap_metadata: Some(minimap_metadata),
            map_metadata: Some(map_metadata),
        };

        #[cfg(any(feature = "official", feature = "standalone"))]
        self.store_cached_bundle_meta(map_id.clone(), bundle.clone())
            .await;
        tracing::debug!(%map_id, "map asset bundle meta resolved");
        Ok(Response::new(GetMapAssetBundleMetaResponse {
            bundle: Some(bundle),
        }))
    }

    type GetMapAssetStream = BoxStream<GetMapAssetResponse>;

    async fn get_map_asset(
        &self,
        request: Request<GetMapAssetRequest>,
    ) -> Result<Response<Self::GetMapAssetStream>, Status> {
        let GetMapAssetRequest {
            map_id,
            kind,
            offset,
            limit,
            if_match_hash,
        } = request.into_inner();

        let map_id = Self::validate_requested_map_id(&map_id)?;
        self.ensure_local_map_cached_if_needed(&map_id).await?;
        if !self.serving_enabled {
            return Err(Status::not_found("map asset not found"));
        }
        let kind = MapAssetKind::try_from(kind).unwrap_or(MapAssetKind::Unspecified);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let bundle_paths = self.resolve_official_bundle_paths(&map_id).await?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let path = Self::resolve_path_for_kind(&self.tracks_dir, &map_id, kind)?;
        #[cfg(any(feature = "official", feature = "standalone"))]
        let path = Self::resolve_path_for_kind(
            &bundle_paths.bundle_dir,
            &bundle_paths.internal_map_id,
            kind,
        )?;

        tracing::debug!(%map_id, kind = ?kind, file = %path.display(), "asset requested");

        let meta = fs::metadata(&path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => {
                tracing::warn!(error = ?e, %map_id, kind = ?kind, file = %path.display(), "asset metadata failed");
                Status::not_found("map asset not found")
            }
            _ => {
                tracing::error!(error = ?e, %map_id, kind = ?kind, file = %path.display(), "asset metadata failed");
                Status::internal("file IO error")
            }
        })?;
        let total_len = meta.len();

        if !if_match_hash.is_empty() {
            let hash = self.hash_cache.get_or_compute(&path).await.map_err(|e| {
                tracing::error!(error=?e, %map_id, kind = ?kind, file=%path.display(), "hash compute failed");
                Status::internal("hash compute failed")
            })?;

            if if_match_hash != hash {
                tracing::warn!(%map_id, kind = ?kind, "if_match_hash mismatch");
                return Err(Status::failed_precondition("hash mismatch"));
            }
        }

        if offset >= total_len {
            tracing::debug!(%map_id, kind = ?kind, offset, total_len, "offset beyond EOF -> immediate EOF");
            let s = try_stream! {
                yield GetMapAssetResponse {
                    offset,
                    data: Bytes::new(),
                    eof: true,
                };
            };
            return Ok(Response::new(Box::pin(s) as Self::GetMapAssetStream));
        }

        let chunk_size = Self::choose_chunk_size(limit)?;
        let stream_map_id = map_id.clone();
        let stream_path = path.clone();

        let s = try_stream! {
            let mut file = File::open(&stream_path).await.map_err(|e| {
                tracing::warn!(error = ?e, %stream_map_id, kind = ?kind, file = %stream_path.display(), "open failed");
                Status::not_found("map asset not found")
            })?;

            file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|e| {
                tracing::error!(error = ?e, %stream_map_id, kind = ?kind, file = %stream_path.display(), "seek failed");
                Status::internal("file IO error")
            })?;

            let mut reader = BufReader::with_capacity(chunk_size, file);
            let mut pos = offset;
            let mut buf = BytesMut::with_capacity(chunk_size);

            loop {
                buf.clear();
                buf.reserve(chunk_size);

                let n = reader.read_buf(&mut buf).await.map_err(|e| {
                    tracing::error!(error = ?e, %stream_map_id, kind = ?kind, file = %stream_path.display(), "read failed");
                    Status::internal("file IO error")
                })?;

                if n == 0 {
                    yield GetMapAssetResponse {
                        offset: pos,
                        data: Bytes::new(),
                        eof: true,
                    };
                    break;
                }

                let end = pos + n as u64;
                let eof = end >= total_len;
                let chunk_bytes = buf.split().freeze();

                yield GetMapAssetResponse {
                    offset: pos,
                    data: chunk_bytes,
                    eof,
                };

                pos = end;
                if eof {
                    break;
                }
            }
        };
        tracing::info!(%map_id, kind = ?kind, "asset streaming started");

        Ok(Response::new(Box::pin(s)))
    }

    async fn get_map_asset_sync_meta(
        &self,
        request: Request<GetMapAssetSyncMetaRequest>,
    ) -> Result<Response<GetMapAssetSyncMetaResponse>, Status> {
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        if self.local_sync.is_some() {
            return Err(Status::unimplemented(
                "GetMapAssetSyncMeta is not supported on local backend",
            ));
        }

        let GetMapAssetSyncMetaRequest { map_id } = request.into_inner();
        let map_id = Self::validate_requested_map_id(&map_id)?;
        if !self.serving_enabled {
            return Err(Status::not_found("map sync metadata not found"));
        }

        #[cfg(any(feature = "official", feature = "standalone"))]
        let bundle_paths = self.resolve_official_bundle_paths(&map_id).await?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let minimap_svg_path = Self::resolve_minimap_svg_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let minimap_svg_path = bundle_paths.minimap_svg_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let map_metadata_path = Self::resolve_map_metadata_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let map_metadata_path = bundle_paths.map_metadata_path.clone();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let minimap_metadata_path = Self::resolve_minimap_metadata_path(&self.tracks_dir, &map_id);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let minimap_metadata_path = bundle_paths.minimap_metadata_path.clone();

        let minimap_svg = fs::read(&minimap_svg_path)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => Status::not_found("map minimap svg not found"),
                _ => Status::internal("failed to read minimap svg"),
            })?;
        let main_metadata_json =
            fs::read(&map_metadata_path)
                .await
                .map_err(|e| match e.kind() {
                    ErrorKind::NotFound => Status::not_found("map metadata json not found"),
                    _ => Status::internal("failed to read map metadata json"),
                })?;
        let minimap_metadata_json = fs::read(&minimap_metadata_path).await.map_err(|e| match e
            .kind()
        {
            ErrorKind::NotFound => Status::not_found("map minimap metadata json not found"),
            _ => Status::internal("failed to read map minimap metadata json"),
        })?;

        Ok(Response::new(GetMapAssetSyncMetaResponse {
            map_id,
            minimap_svg: Bytes::from(minimap_svg),
            main_metadata_json: Bytes::from(main_metadata_json),
            minimap_metadata_json: Bytes::from(minimap_metadata_json),
        }))
    }

    type StreamMapAssetGlbStream = BoxStream<StreamMapAssetGlbResponse>;

    async fn stream_map_asset_glb(
        &self,
        request: Request<StreamMapAssetGlbRequest>,
    ) -> Result<Response<Self::StreamMapAssetGlbStream>, Status> {
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        if self.local_sync.is_some() {
            return Err(Status::unimplemented(
                "StreamMapAssetGlb is not supported on local backend",
            ));
        }

        let StreamMapAssetGlbRequest {
            map_id,
            kind,
            offset,
            limit,
        } = request.into_inner();
        let map_id = Self::validate_requested_map_id(&map_id)?;
        if !self.serving_enabled {
            return Err(Status::not_found("map glb not found"));
        }
        let kind = MapGlbKind::try_from(kind).unwrap_or(MapGlbKind::Unspecified);
        #[cfg(any(feature = "official", feature = "standalone"))]
        let bundle_paths = self.resolve_official_bundle_paths(&map_id).await?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let path = Self::resolve_path_for_glb_kind(&self.tracks_dir, &map_id, kind)?;
        #[cfg(any(feature = "official", feature = "standalone"))]
        let path = Self::resolve_path_for_glb_kind(
            &bundle_paths.bundle_dir,
            &bundle_paths.internal_map_id,
            kind,
        )?;

        let meta = fs::metadata(&path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => Status::not_found("map glb not found"),
            _ => Status::internal("file IO error"),
        })?;
        let total_len = meta.len();

        if offset >= total_len {
            let s = try_stream! {
                yield StreamMapAssetGlbResponse {
                    offset,
                    data: Bytes::new(),
                    eof: true,
                };
            };
            return Ok(Response::new(Box::pin(s) as Self::StreamMapAssetGlbStream));
        }

        let chunk_size = Self::choose_chunk_size(limit)?;
        let stream_path = path.clone();
        let s = try_stream! {
            let mut file = File::open(&stream_path).await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => Status::not_found("map glb not found"),
                _ => Status::internal("file IO error"),
            })?;

            file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|_| Status::internal("file IO error"))?;

            let mut reader = BufReader::with_capacity(chunk_size, file);
            let mut pos = offset;
            let mut buf = BytesMut::with_capacity(chunk_size);

            loop {
                buf.clear();
                buf.reserve(chunk_size);
                let n = reader
                    .read_buf(&mut buf)
                    .await
                    .map_err(|_| Status::internal("file IO error"))?;

                if n == 0 {
                    yield StreamMapAssetGlbResponse {
                        offset: pos,
                        data: Bytes::new(),
                        eof: true,
                    };
                    break;
                }

                let end = pos + n as u64;
                let eof = end >= total_len;
                let chunk_bytes = buf.split().freeze();
                yield StreamMapAssetGlbResponse {
                    offset: pos,
                    data: chunk_bytes,
                    eof,
                };
                pos = end;
                if eof {
                    break;
                }
            }
        };
        Ok(Response::new(Box::pin(s)))
    }
}
