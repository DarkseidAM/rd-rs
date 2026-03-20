//! `CacheItem` — one sparse file on disk per `(access_key, filename)` pair.
//!
//! Orchestrates background HTTP downloader(s) and wakes waiting FUSE readers
//! via `tokio::sync::Notify` once their requested byte range is on disk.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use tokio::sync::Notify;

use crate::cache::bitmap::ByteRanges;
use crate::config::Config;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::types::Download;

use crate::cache::download_session::DownloadSession;
use crate::cache::sparse::scan_sparse_file;

// ─── CacheItem ────────────────────────────────────────────────────────────────

pub struct CacheItem {
    /// Absolute path to the sparse file on disk.
    pub(crate) path: PathBuf,
    /// Total file size in bytes.
    pub(crate) file_size: u64,
    /// Ranges of bytes already on disk.
    pub(crate) ranges: RwLock<ByteRanges>,
    /// Number of currently open FUSE file handles.
    pub(crate) opens: AtomicU32,
    /// Last access time (unix seconds). Updated on every `open()`.
    pub(crate) atime: AtomicU64,
    /// Notified whenever new bytes are written to the sparse file.
    pub(crate) notify: Arc<Notify>,
    /// Bytes downloaded into this item (for global stats).
    pub(crate) downloaded_bytes: AtomicI64,
    /// Active download tasks (position + ref-count / abort handle).
    pub(crate) active_workers: std::sync::Mutex<Vec<Arc<DownloadSession>>>,
}

impl CacheItem {
    /// Create or reopen the sparse file for this cache entry.
    pub fn open_or_create(
        path: PathBuf,
        file_size: u64,
        recover_sparse_extents: bool,
    ) -> Result<Arc<Self>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create cache dir {:?}", parent))?;
        }

        // Open (or create) the sparse file and pre-allocate to file_size.
        // On Linux, a truncate() that extends the file creates a sparse
        // region — no actual disk blocks are allocated until data is written.
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open cache file {:?}", path))?;

        let current_len = file.metadata()?.len();
        if current_len != file_size {
            file.set_len(file_size)
                .with_context(|| format!("truncate cache file {:?}", path))?;
        }

        let ranges = if recover_sparse_extents {
            scan_sparse_file(&file, file_size)
        } else {
            ByteRanges::new()
        };
        if recover_sparse_extents && !ranges.is_empty() {
            let mb = ranges.total_bytes() as f64 / 1048576.0;
            tracing::info!(
                "Recovered {:.2} MB of cached bytes from existing sparse file for {:?}",
                mb,
                path.file_name().unwrap_or_default()
            );
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(Arc::new(Self {
            path,
            file_size,
            ranges: RwLock::new(ranges),
            opens: AtomicU32::new(0),
            atime: AtomicU64::new(now_secs),
            notify: Arc::new(Notify::new()),
            downloaded_bytes: AtomicI64::new(0),
            active_workers: std::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Increment open handle count and update access time.
    pub fn open(&self) {
        self.opens.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.atime.store(now, Ordering::Relaxed);
    }

    /// Decrement open handle count.
    pub fn release(&self) {
        self.opens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    pub fn is_open(&self) -> bool {
        self.opens.load(Ordering::Relaxed) > 0
    }

    pub fn atime_secs(&self) -> u64 {
        self.atime.load(Ordering::Relaxed)
    }

    /// Returns `true` if `[start, end)` is fully in the on-disk cache.
    pub fn has_range(&self, start: u64, end: u64) -> bool {
        self.ranges.read().unwrap().has_range(start, end)
    }

    /// Sum of lengths of all cached byte intervals (for stats / tests).
    pub fn total_cached_bytes(&self) -> u64 {
        self.ranges.read().unwrap().total_bytes()
    }

    /// Read directly from the sparse file. Caller must have already verified
    /// the range is present via `has_range`.
    pub fn read_from_file(&self, offset: u64, size: u32) -> Result<bytes::Bytes> {
        use std::io::Read;
        use std::io::Seek;

        let mut file = fs::File::open(&self.path)
            .with_context(|| format!("open cache file for read {:?}", self.path))?;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let to_read = (size as u64).min(self.file_size.saturating_sub(offset)) as usize;
        let mut buf = vec![0u8; to_read];
        file.read_exact(&mut buf)?;
        Ok(bytes::Bytes::from(buf))
    }

    /// Write `data` to the sparse file at `offset`, update the ranges bitmap,
    /// and notify any waiting readers.
    pub fn write_range(&self, offset: u64, data: &[u8]) -> Result<()> {
        use std::io::Seek;
        use std::io::Write as _;

        if data.is_empty() {
            return Ok(());
        }

        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .with_context(|| format!("open cache file for write {:?}", self.path))?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(data)?;

        let end = offset + data.len() as u64;
        self.ranges.write().unwrap().insert(offset, end);
        self.downloaded_bytes
            .fetch_add(data.len() as i64, Ordering::Relaxed);
        self.notify.notify_waiters();
        Ok(())
    }

    /// Serve `[offset, offset+size)` from cache, downloading if needed.
    ///
    /// Returns the bytes or an `RdError` that the FUSE layer uses to decide
    /// whether to retry or mark the file broken.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_at(
        self: &Arc<Self>,
        fuse_ctx: tokio_util::sync::CancellationToken,
        offset: u64,
        size: u32,
        download: &Download,
        rd: &Arc<RealDebrid>,
        unrestrict_cache: &UnrestrictCache,
        config: &Config,
    ) -> std::result::Result<bytes::Bytes, CacheReadError> {
        crate::cache::item_read_at::read_at(
            self,
            fuse_ctx,
            offset,
            size,
            download,
            rd,
            unrestrict_cache,
            config,
        )
        .await
    }
}

// ─── CacheReadError ───────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CacheReadError {
    #[error("IO error reading from cache: {0}")]
    Io(#[from] anyhow::Error),
    #[error("download failed or timed out")]
    DownloadFailed,
    #[error("FUSE request cancelled")]
    Cancelled,
}
