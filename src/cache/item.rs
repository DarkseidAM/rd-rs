//! `CacheItem` — one sparse file on disk per `(access_key, filename)` pair.
//!
//! Orchestrates background HTTP downloader(s) and wakes waiting FUSE readers
//! via `tokio::sync::Notify` once their requested byte range is on disk.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use std::os::unix::io::AsRawFd;

use anyhow::{Context, Result};
use tokio::sync::Notify;
use tokio::time::{self, Duration};

use crate::cache::bitmap::ByteRanges;
use crate::config::{Config, parse_byte_size};
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::types::Download;

// ─── Constants (from decypharr afddc46) ──────────────────────────────────────

/// Adaptive chunk size cap: never grow beyond 16× the base chunk.
const MAX_CHUNK_MULTIPLIER: u64 = 16;
/// If no bytes are written to cache for this duration, cancel the in-flight
/// HTTP stream attempt and retry from the same offset.
const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(45);
/// How often the no-progress watchdog polls for stall detection.
const NO_PROGRESS_CHECK: Duration = Duration::from_secs(1);
/// Idle items are evictable after 1 minute with no open handles.
pub(crate) const ITEM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Max retries before giving up and returning an error to FUSE.
const MAX_DOWNLOAD_RETRIES: u32 = 3;

// ─── DownloaderArgs ──────────────────────────────────────────────────────────

/// Arguments for `run_downloader`, grouped to stay under clippy's arg-count limit.
pub(crate) struct DownloaderArgs {
    pub start: u64,
    pub end: u64,
    pub base_chunk: u64,
    pub read_ahead: u64,
    pub download_url: String,
    #[allow(dead_code)]
    pub unrestrict_url: String,
    pub rd: Arc<RealDebrid>,
    #[allow(dead_code)]
    pub unrestrict_cache: UnrestrictCache,
}

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
    /// Active download tasks (to prevent duplicate background streams).
    pub(crate) active_workers: std::sync::Mutex<Vec<std::ops::Range<u64>>>,
}

impl CacheItem {
    /// Create or reopen the sparse file for this cache entry.
    pub fn open_or_create(path: PathBuf, file_size: u64) -> Result<Arc<Self>> {
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

        let ranges = scan_sparse_file(&file, file_size);
        if !ranges.is_empty() {
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

    // ─── High-level read_at ───────────────────────────────────────────────────

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
        let end = (offset + size as u64).min(self.file_size);
        if offset >= self.file_size {
            return Ok(bytes::Bytes::new());
        }

        // Fast path: already on disk.
        if self.has_range(offset, end) {
            return self
                .read_from_file(offset, (end - offset) as u32)
                .map_err(CacheReadError::Io);
        }

        // Deduplicate: if an active worker already covers `offset`, just wait.
        let base_chunk = parse_byte_size(&config.vfs.chunk_size);
        let read_ahead = parse_byte_size(&config.vfs.read_ahead);
        let target_end = (end + read_ahead).min(self.file_size);

        let mut should_spawn = false;
        {
            let mut workers = self.active_workers.lock().unwrap();
            let covered = workers.iter().any(|r| r.start <= offset && r.end > offset);
            if !covered {
                workers.push(offset..target_end);
                should_spawn = true;
            }
        }

        let mut spawned_task = None;
        if should_spawn {
            let download_url = download.download.clone();
            let rd_clone = Arc::clone(rd);
            let item = Arc::clone(self);
            let unrestrict_url = download.link.clone();
            let unrestrict_cache = unrestrict_cache.clone();

            spawned_task = Some(tokio::spawn(async move {
                struct WorkerGuard {
                    item: Arc<CacheItem>,
                    range: std::ops::Range<u64>,
                }
                impl Drop for WorkerGuard {
                    fn drop(&mut self) {
                        if let Ok(mut w) = self.item.active_workers.lock() {
                            w.retain(|r| r != &self.range);
                        }
                        self.item.notify.notify_waiters();
                    }
                }
                let _guard = WorkerGuard {
                    item: Arc::clone(&item),
                    range: offset..target_end,
                };

                if let Err(e) = item
                    .run_downloader(DownloaderArgs {
                        start: offset,
                        end,
                        base_chunk,
                        read_ahead,
                        download_url,
                        unrestrict_url,
                        rd: rd_clone,
                        unrestrict_cache,
                    })
                    .await
                {
                    tracing::warn!("cache downloader error at offset {offset}: {e:#}");
                }
            }));
        }

        // Inline wait loop — polls Notify until range on disk.
        let wait_result: bool = tokio::select! {
            _ = fuse_ctx.cancelled() => {
                if let Some(task) = &spawned_task {
                    // Only abort if WE spawned it.
                    task.abort();
                }
                false
            }
            result = async {
                // Fast path.
                if self.has_range(offset, end) {
                    return true;
                }
                loop {
                    let notified = self.notify.notified();
                    if self.has_range(offset, end) {
                        return true;
                    }
                    if let Some(task) = &spawned_task {
                        if task.is_finished() {
                            return self.has_range(offset, end);
                        }
                    } else {
                        // We are waiting on another worker. If it failed/died, we wake up here.
                        let covered = {
                            let w = self.active_workers.lock().unwrap();
                            w.iter().any(|r| r.start <= offset && r.end > offset)
                        };
                        if !covered {
                            return self.has_range(offset, end);
                        }
                    }
                    notified.await;
                }
            } => result
        };

        if wait_result {
            self.read_from_file(offset, (end - offset) as u32)
                .map_err(CacheReadError::Io)
        } else if fuse_ctx.is_cancelled() {
            Err(CacheReadError::Cancelled)
        } else {
            Err(CacheReadError::DownloadFailed)
        }
    }

    // ─── Downloader ───────────────────────────────────────────────────────────

    /// HTTP Range GET loop for `[start, file_end)` with read-ahead and
    /// no-progress watchdog. Mirrors decypharr's downloader pattern.
    async fn run_downloader(self: &Arc<Self>, args: DownloaderArgs) -> Result<()> {
        let DownloaderArgs {
            start,
            end,
            base_chunk,
            read_ahead,
            download_url,
            rd,
            ..
        } = args;
        let mut current_chunk = base_chunk;
        let max_chunk = base_chunk * MAX_CHUNK_MULTIPLIER;

        // Apply read-ahead: download further than strictly needed.
        let target_end = (end + read_ahead).min(self.file_size);

        let mut pos = {
            // Start from the first missing byte in [start, target_end).
            let r = self.ranges.read().unwrap();
            r.find_missing(start, target_end)
                .map(|(s, _)| s)
                .unwrap_or(target_end)
        };

        let mut retries: u32 = 0;

        while pos < target_end {
            // Resolve the next missing sub-range.
            let (miss_start, _miss_end) = {
                let r = self.ranges.read().unwrap();
                match r.find_missing(pos, target_end) {
                    Some(m) => m,
                    None => break, // fully cached up to target_end
                }
            };
            pos = miss_start;

            let chunk_end = (pos + current_chunk).min(target_end);
            let range_header = format!("bytes={}-{}", pos, chunk_end - 1);

            // --- No-progress watchdog ---
            let last_progress = Arc::new(std::sync::atomic::AtomicI64::new(
                chrono::Utc::now().timestamp_millis(),
            ));
            let lp_clone = Arc::clone(&last_progress);
            let (watchdog_tx, mut watchdog_rx) = tokio::sync::oneshot::channel::<()>();

            let watchdog = tokio::spawn(async move {
                let mut interval = time::interval(NO_PROGRESS_CHECK);
                loop {
                    interval.tick().await;
                    if watchdog_tx.is_closed() {
                        return;
                    }
                    let last = lp_clone.load(Ordering::Relaxed);
                    let now = chrono::Utc::now().timestamp_millis();
                    if (now - last) as u64 >= NO_PROGRESS_TIMEOUT.as_millis() as u64 {
                        tracing::warn!("no-progress watchdog fired after 45s stall");
                        let _ = watchdog_tx;
                        return;
                    }
                }
            });

            // Start the request, bounded by watchdog
            last_progress.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
            let mut resp = tokio::select! {
                res = rd.http_range_get(&download_url, &range_header) => {
                    match res {
                        Ok(r) => r,
                        Err(e) => {
                            watchdog.abort();
                            tracing::warn!("HTTP range GET failed at byte {pos}: {e:#}");
                            current_chunk = base_chunk;
                            retries += 1;
                            if retries >= MAX_DOWNLOAD_RETRIES {
                                return Err(e);
                            }
                            continue;
                        }
                    }
                }
                _ = &mut watchdog_rx => {
                    watchdog.abort();
                    tracing::warn!("stream headers stalled for 45s");
                    current_chunk = base_chunk;
                    retries += 1;
                    if retries >= MAX_DOWNLOAD_RETRIES {
                        return Err(anyhow::anyhow!("stream stalled: no progress for 45s"));
                    }
                    continue;
                }
            };

            let mut chunk_bytes_downloaded = 0u64;
            let mut download_error = None;

            loop {
                let chunk_res = tokio::select! {
                    res = resp.chunk() => res,
                    _ = &mut watchdog_rx => {
                        download_error = Some(anyhow::anyhow!("stream body stalled for 45s"));
                        break;
                    }
                };

                match chunk_res {
                    Ok(Some(data)) => {
                        last_progress
                            .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                        if let Err(e) = self.write_range(pos, &data[..]) {
                            download_error = Some(e);
                            break;
                        }
                        pos += data.len() as u64;
                        chunk_bytes_downloaded += data.len() as u64;
                    }
                    Ok(None) => break, // Success, EOF for this chunk
                    Err(e) => {
                        download_error = Some(anyhow::anyhow!("HTTP chunk error: {e:#}"));
                        break;
                    }
                }
            }

            watchdog.abort();

            match download_error {
                None => {
                    let c_mb = format!("{:.2} MB", current_chunk as f64 / 1048576.0);
                    let b_mb = format!("{:.2} MB", chunk_bytes_downloaded as f64 / 1048576.0);
                    let o_bytes = pos.saturating_sub(chunk_bytes_downloaded);
                    let o_mb = format!("{:.2} MB", o_bytes as f64 / 1048576.0);

                    tracing::info!(
                        file = %self.path.file_name().unwrap_or_default().to_string_lossy(),
                        chunk = %format!("{} ({})", current_chunk, c_mb),
                        bytes = %format!("{} ({})", chunk_bytes_downloaded, b_mb),
                        offset = %format!("{} ({})", o_bytes, o_mb),
                        "chunk downloaded successfully"
                    );
                    current_chunk = (current_chunk * 2).min(max_chunk);

                    retries = 0;
                }
                Some(e) => {
                    tracing::warn!("HTTP streaming failed at byte {pos}: {e:#}");
                    current_chunk = base_chunk;
                    retries += 1;
                    if retries >= MAX_DOWNLOAD_RETRIES {
                        return Err(e);
                    }
                    time::sleep(Duration::from_secs(retries as u64 * 2)).await;
                }
            }
        }

        Ok(())
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

// ─── Native Sparse File Extent Scanner ────────────────────────────────────────

/// Scans mapped extents of an existing sparse file to reconstruct its ByteRanges.
fn scan_sparse_file(file: &std::fs::File, file_size: u64) -> ByteRanges {
    let mut ranges = ByteRanges::new();
    let fd = file.as_raw_fd();
    let mut offset: i64 = 0;
    let end = file_size as i64;

    while offset < end {
        // Find next data segment
        let data_start = unsafe { libc::lseek(fd, offset, libc::SEEK_DATA) };
        if data_start < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENXIO) {
                // No more data segments.
                break;
            }
            tracing::warn!("lseek(SEEK_DATA) failed: {err}");
            break;
        }

        // Find next hole after this data segment
        let hole_start = unsafe { libc::lseek(fd, data_start, libc::SEEK_HOLE) };
        if hole_start < 0 {
            tracing::warn!(
                "lseek(SEEK_HOLE) failed: {}",
                std::io::Error::last_os_error()
            );
            break;
        }

        let slice_end = hole_start.min(end);
        ranges.insert(data_start as u64, slice_end as u64);

        offset = slice_end;
    }

    ranges
}
