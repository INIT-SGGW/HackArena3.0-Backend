use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

const BUF_SIZE: usize = 128 * 1024; // 128 KB
const SHA256_LEN: usize = 32;

/// In-memory + on-disk (sidecar) cache for SHA-256 of files.
///
/// Sidecar format (binary + ASCII):
///   `[32B raw hash][\n][size: ASCII u64][\n][mtime_secs: ASCII u64]`
/// Newlines are `\n` (LF). Values are decimal ASCII. No CRLF.
///
/// Concurrency:
/// - Per-path async mutex prevents concurrent double-hashing of the same file.
/// - `DashMap` for lock-free reads/writes of in-memory entries.
///
/// Freshness:
/// - Entry is valid iff both file size and mtime (rounded to seconds) match.
#[derive(Clone)]
pub struct HashCache {
    mem: Arc<DashMap<PathBuf, (SystemTime, u64, Bytes)>>,
    sidecar_dir: Option<PathBuf>,
    locks: Arc<DashMap<PathBuf, Arc<Mutex<()>>>>,
}

impl HashCache {
    /// Creates a new cache. When `sidecar_dir` is `Some`, sidecars are written under that dir
    /// using file name + `.sha256`. When `None`, sidecar is `<asset>.sha256` next to the file.
    pub fn new(sidecar_dir: Option<PathBuf>) -> Self {
        Self {
            mem: Arc::new(DashMap::new()),
            sidecar_dir,
            locks: Arc::new(DashMap::new()),
        }
    }

    /// Removes a single in-memory entry (does not delete sidecar on disk).
    pub fn invalidate(&self, path: &Path) {
        self.mem.remove(path);
    }

    /// Returns SHA-256 of `path`, using:
    /// 1) in-memory cache (size + mtime-secs match),
    /// 2) sidecar (validated against size + mtime-secs),
    /// 3) compute & persist to sidecar (best-effort).
    pub async fn get_or_compute(&self, path: &Path) -> Result<Bytes, std::io::Error> {
        let lock = self
            .locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock.lock().await;

        let meta = fs::metadata(path).await?;
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let size = meta.len();

        if let Some(e) = self.mem.get(path) {
            if same_mtime(&e.0, &mtime) && e.1 == size {
                return Ok(e.2.clone());
            }
        }

        if let Some(bytes) = self.try_read_sidecar(path, mtime, size).await? {
            self.mem
                .insert(path.to_path_buf(), (mtime, size, bytes.clone()));
            return Ok(bytes);
        }

        let hash = Self::compute_sha256(path).await?;
        let meta2 = fs::metadata(path).await?;
        let mtime2 = meta2.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let size2 = meta2.len();
        if !same_mtime(&mtime, &mtime2) || size != size2 {
            return Ok(hash);
        }

        let _ = self.write_sidecar(path, mtime2, size2, &hash).await;
        self.mem
            .insert(path.to_path_buf(), (mtime2, size2, hash.clone()));

        Ok(hash)
    }

    async fn compute_sha256(path: &Path) -> Result<Bytes, std::io::Error> {
        let file = File::open(path).await?;
        let mut reader = BufReader::with_capacity(BUF_SIZE, file);
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; BUF_SIZE];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        // 32 bytes
        Ok(Bytes::copy_from_slice(&hasher.finalize()))
    }

    async fn try_read_sidecar(
        &self,
        path: &Path,
        mtime_now: SystemTime,
        size_now: u64,
    ) -> Result<Option<Bytes>, std::io::Error> {
        // Sidecar layout: [32B hash][\n][size][\n][mtime_secs] (ASCII decimals; LF only).
        // We parse leniently: empty/malformed lines cause a miss (return None).

        let Some(sidecar) = self.sidecar_path_for(path) else {
            return Ok(None);
        };

        let bytes = match fs::read(&sidecar).await {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(e = %e, path=%sidecar.display(), ?size_now, ?mtime_now, "sidecar read failed");
                return Ok(None);
            }
        };

        if bytes.len() < SHA256_LEN {
            return Ok(None);
        }

        let hash = Bytes::copy_from_slice(&bytes[..SHA256_LEN]);
        let rest = &bytes[SHA256_LEN..];

        let text = String::from_utf8_lossy(rest);
        let mut lines = text.split('\n').map(str::trim);

        let size_ok = lines
            .next()
            .and_then(|s| (!s.is_empty()).then_some(s))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|sz| sz == size_now)
            .unwrap_or(false);

        let mtime_ok = lines
            .next()
            .and_then(|s| (!s.is_empty()).then_some(s))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|secs| {
                mtime_secs(mtime_now)
                    .map(|now_secs| now_secs == secs)
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        if size_ok && mtime_ok {
            Ok(Some(hash))
        } else {
            tracing::debug!(path=%sidecar.display(), size_now, mtime_now=?mtime_now, "sidecar invalid or stale");
            Ok(None)
        }
    }

    async fn write_sidecar(
        &self,
        path: &Path,
        mtime: SystemTime,
        size: u64,
        hash: &Bytes,
    ) -> Result<(), std::io::Error> {
        // Atomic-ish write: write to `<file>.sha256.tmp` and rename to final path.
        // On Windows, rename-over may fail if target exists, so we remove first.

        debug_assert_eq!(hash.len(), SHA256_LEN);

        let Some(sidecar) = self.sidecar_path_for(path) else {
            return Ok(());
        };

        if let Some(parent) = sidecar.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let tmp = sidecar.with_extension("sha256.tmp");

        let mut f = File::create(&tmp).await?;
        f.write_all(hash).await?;

        let secs = mtime_secs(mtime).unwrap_or(0);
        f.write_all(b"\n").await?;
        f.write_all(size.to_string().as_bytes()).await?;
        f.write_all(b"\n").await?;
        f.write_all(secs.to_string().as_bytes()).await?;
        f.flush().await?;
        drop(f);

        let _ = fs::remove_file(&sidecar).await;
        fs::rename(tmp, sidecar).await?;

        Ok(())
    }

    fn sidecar_path_for(&self, asset: &Path) -> Option<PathBuf> {
        // TODO: if assets may live in subdirectories, consider deriving a relative path
        // and encoding separators (e.g., '/' -> '__') to avoid collisions in a flat `.hashes/`.

        if let Some(dir) = &self.sidecar_dir {
            let file_name = asset.file_name()?.to_string_lossy();
            return Some(dir.join(format!("{file_name}.sha256")));
        }

        Some(asset.with_extension("sha256"))
    }
}

fn mtime_secs(t: SystemTime) -> Option<u64> {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

fn same_mtime(a: &SystemTime, b: &SystemTime) -> bool {
    match (mtime_secs(*a), mtime_secs(*b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}
