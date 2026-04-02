//! `CacheItem` — one sparse file on disk per `(access_key, filename)` pair.
//!
//! Orchestrates background HTTP downloader(s) and wakes waiting FUSE readers
//! via `tokio::sync::Notify` once their requested byte range is on disk.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};

use parking_lot::RwLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Notify;

use crate::cache::bitmap::ByteRanges;
use crate::config::Config;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::types::Download;

use crate::cache::download_session::DownloadSession;
use crate::cache::range_db::RangeDb;

const FLUSH_DEBOUNCE: Duration = Duration::from_secs(2);

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
    pub(crate) cache_key: String,
    pub(crate) range_db: Arc<RangeDb>,
    persist_dirty: AtomicBool,
    last_persist_unix: AtomicU64,
}

impl CacheItem {
    /// Create or reopen the sparse file for this cache entry.
    pub fn open_or_create(path: PathBuf, file_size: u64) -> Result<Arc<Self>> {
        let fallback_key = path.to_string_lossy().into_owned();
        let range_db = Arc::new(RangeDb::open_in_memory()?);
        Self::open_or_create_with_db(path, fallback_key, file_size, range_db)
    }

    /// Create or reopen with a shared persistent range database.
    pub fn open_or_create_with_db(
        path: PathBuf,
        cache_key: String,
        file_size: u64,
        range_db: Arc<RangeDb>,
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

        let mut ranges = ByteRanges::new();
        if let Some(row) = range_db.get(&cache_key)? {
            if row.file_size == file_size {
                ranges = row.ranges;
                tracing::info!(
                    key = %cache_key,
                    restored_intervals = ranges.len(),
                    restored_mb = (ranges.total_bytes() as f64 / 1_048_576.0),
                    updated_at = row.updated_at,
                    "cache open: restored ranges from cache_ranges.db"
                );
            } else {
                tracing::warn!(
                    key = %cache_key,
                    db_file_size = row.file_size,
                    expected = file_size,
                    "cache open: range db file_size mismatch; ignoring persisted ranges"
                );
            }
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
            cache_key,
            range_db,
            persist_dirty: AtomicBool::new(false),
            last_persist_unix: AtomicU64::new(now_secs),
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
        let prev = self
            .opens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .unwrap_or(0);
        if prev == 1 {
            self.flush_ranges(true);
        }
    }

    pub fn is_open(&self) -> bool {
        self.opens.load(Ordering::Relaxed) > 0
    }

    pub fn atime_secs(&self) -> u64 {
        self.atime.load(Ordering::Relaxed)
    }

    /// Returns `true` if `[start, end)` is fully in the on-disk cache.
    pub fn has_range(&self, start: u64, end: u64) -> bool {
        self.ranges.read().has_range(start, end)
    }

    /// Sum of lengths of all cached byte intervals (for stats / tests).
    pub fn total_cached_bytes(&self) -> u64 {
        self.ranges.read().total_bytes()
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
        self.ranges.write().insert(offset, end);
        self.persist_dirty.store(true, Ordering::Relaxed);
        self.downloaded_bytes
            .fetch_add(data.len() as i64, Ordering::Relaxed);
        self.flush_ranges(false);
        self.notify.notify_waiters();
        Ok(())
    }

    pub fn flush_ranges(&self, force: bool) {
        if !self.persist_dirty.load(Ordering::Relaxed) {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = self.last_persist_unix.load(Ordering::Relaxed);
        if !force && now.saturating_sub(last) < FLUSH_DEBOUNCE.as_secs() {
            return;
        }
        let snapshot = self.ranges.read().clone();
        if let Err(e) = self
            .range_db
            .upsert(&self.cache_key, self.file_size, now as i64, &snapshot)
        {
            tracing::warn!(
                key = %self.cache_key,
                "cache ranges persist failed: {e:#}"
            );
            return;
        }
        self.last_persist_unix.store(now, Ordering::Relaxed);
        self.persist_dirty.store(false, Ordering::Relaxed);
        tracing::trace!(
            key = %self.cache_key,
            intervals = snapshot.len(),
            "cache ranges persisted"
        );
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
        pause_rx: tokio::sync::watch::Receiver<bool>,
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
            pause_rx,
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
