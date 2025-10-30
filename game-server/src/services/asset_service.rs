use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tonic::{Request, Response, Status};

use hash_cache::HashCache;
use proto::race::v1::asset_service_server::AssetService;
use proto::race::v1::{
    GetTrackMetaRequest, GetTrackMetaResponse, GetTrackRequest, GetTrackResponse, MimeType,
    TrackMeta,
};

type BoxStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64KB
const MAX_CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2MB

pub struct AssetServiceImpl {
    tracks_dir: PathBuf,
    hash_cache: HashCache,
}
impl AssetServiceImpl {
    pub fn new(tracks_dir: PathBuf) -> Self {
        let sidecars = tracks_dir.join(".hashes");
        let hash_cache = HashCache::new(Some(sidecars));
        Self {
            tracks_dir,
            hash_cache,
        }
    }

    fn sanitize_id(id: &str) -> Result<(), Status> {
        if !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            Err(Status::invalid_argument("Invalid ID"))
        } else {
            Ok(())
        }
    }

    fn resolve_track_path(root: &Path, id: &str) -> PathBuf {
        root.join(format!("{id}.glb"))
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
}

#[tonic::async_trait]
impl AssetService for AssetServiceImpl {
    async fn get_track_meta(
        &self,
        request: Request<GetTrackMetaRequest>,
    ) -> Result<Response<GetTrackMetaResponse>, Status> {
        let GetTrackMetaRequest { id } = request.into_inner();
        Self::sanitize_id(&id)?;

        let path = Self::resolve_track_path(&self.tracks_dir, &id);
        tracing::debug!(%id, file = %path.display(), "requested");

        let meta = fs::metadata(&path).await.map_err(|e| {
            tracing::warn!(error = ?e, %id, file = %path.display(), "metadata failed");
            Status::not_found("track not found")
        })?;
        let size = meta.len();

        let hash = self.hash_cache.get_or_compute(&path).await.map_err(|e| {
            tracing::error!(error = ?e, %id, file = %path.display(), "hash computation failed");
            Status::internal("hash computation failed")
        })?;

        let track_meta = TrackMeta {
            id: id.clone(),
            content_type: MimeType::MimeGltfBinary as i32,
            size_bytes: size,
            content_hash: hash,
            version: 1, // TODO : Versioning
        };

        tracing::info!(%id, "meta ok");
        Ok(Response::new(GetTrackMetaResponse {
            meta: Some(track_meta),
        }))
    }

    type GetTrackStream = BoxStream<GetTrackResponse>;

    async fn get_track(
        &self,
        request: Request<GetTrackRequest>,
    ) -> Result<Response<Self::GetTrackStream>, Status> {
        let GetTrackRequest {
            id,
            offset,
            limit,
            if_match_hash,
        } = request.into_inner();
        Self::sanitize_id(&id)?;

        let path = Self::resolve_track_path(&self.tracks_dir, &id);
        tracing::debug!(%id, file = %path.display(), "requested");

        let meta = fs::metadata(&path).await.map_err(|e| {
            tracing::warn!(error = ?e, %id, file = %path.display(), "metadata failed");
            Status::not_found("track not found")
        })?;
        let total_len = meta.len();

        if !if_match_hash.is_empty() {
            let hash = self.hash_cache.get_or_compute(&path).await.map_err(|e| {
                tracing::error!(error=?e, %id, file=%path.display(), "hash compute failed");
                Status::internal("hash compute failed")
            })?;

            if if_match_hash != hash {
                tracing::warn!(%id, "if_match_hash mismatch");
                return Err(Status::failed_precondition("hash mismatch"));
            }
        }

        if offset >= total_len {
            tracing::debug!(%id, offset, total_len, "offset beyond EOF -> immediate EOF");
            let s = try_stream! {
                yield GetTrackResponse {
                    offset,
                    data: Bytes::new(),
                    eof: true,
                };
            };
            return Ok(Response::new(Box::pin(s) as Self::GetTrackStream));
        }

        let chunk_size = Self::choose_chunk_size(limit)?;
        let stream_id = id.clone();
        let stream_path = path.clone();

        let s = try_stream! {
            let mut file = File::open(&stream_path).await.map_err(|e| {
                tracing::warn!(error = ?e, %stream_id, file = %stream_path.display(), "open failed");
                Status::not_found("track not found")
            })?;

            file.seek(std::io::SeekFrom::Start(offset)).await.map_err(|e| {
                tracing::error!(error = ?e, %stream_id, file = %stream_path.display(), "seek failed");
                Status::internal("file IO error")
            })?;

            let mut reader = BufReader::with_capacity(chunk_size, file);
            let mut pos = offset;
            let mut buf = BytesMut::with_capacity(chunk_size);

            loop {
                buf.clear();
                buf.reserve(chunk_size);

                let n = reader.read_buf(&mut buf).await.map_err(|e| {
                    tracing::error!(error = ?e, %stream_id, file = %stream_path.display(), "read failed");
                    Status::internal("file IO error")
                })?;

                if n == 0 {
                    if pos == offset {
                        yield GetTrackResponse {
                            offset: pos,
                            data: Bytes::new(),
                            eof: true,
                        };
                    }
                    break;
                }

                let end = pos + n as u64;
                let eof = end >= total_len;
                let chunk_bytes = buf.split().freeze();

                yield GetTrackResponse {
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
        tracing::info!(%id, "streaming started");

        Ok(Response::new(Box::pin(s)))
    }
}
