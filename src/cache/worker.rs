use crate::cache::item::CacheItem;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tokio::time::{self, Duration};

/// Adaptive chunk size cap: never grow beyond 16× the base chunk.
pub(crate) const MAX_CHUNK_MULTIPLIER: u64 = 16;
/// If no bytes are written to cache for this duration, cancel the in-flight
/// HTTP stream attempt and retry from the same offset.
pub(crate) const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(45);
/// How often the no-progress watchdog polls for stall detection.
pub(crate) const NO_PROGRESS_CHECK: Duration = Duration::from_secs(1);
/// Max retries before giving up and returning an error to FUSE.
pub(crate) const MAX_DOWNLOAD_RETRIES: u32 = 3;

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
    pub worker_pos: Arc<AtomicU64>,
}

/// HTTP Range GET loop for `[start, file_end)` with read-ahead and
/// no-progress watchdog. Mirrors decypharr's downloader pattern.
pub(crate) async fn run_downloader(item: &Arc<CacheItem>, args: DownloaderArgs) -> Result<()> {
    let DownloaderArgs {
        start,
        end,
        base_chunk,
        read_ahead,
        download_url,
        rd,
        worker_pos,
        ..
    } = args;
    let mut current_chunk = base_chunk;
    let max_chunk = base_chunk * MAX_CHUNK_MULTIPLIER;

    // Apply read-ahead: download further than strictly needed.
    let target_end = (end + read_ahead).min(item.file_size);

    let mut pos = {
        // Start from the first missing byte in [start, target_end).
        let r = item.ranges.read().unwrap();
        r.find_missing(start, target_end)
            .map(|(s, _)| s)
            .unwrap_or(target_end)
    };

    let mut retries: u32 = 0;

    while pos < target_end {
        // Resolve the next missing sub-range.
        let (miss_start, _miss_end) = {
            let r = item.ranges.read().unwrap();
            match r.find_missing(pos, target_end) {
                Some(m) => m,
                None => break, // fully cached up to target_end
            }
        };
        pos = miss_start;

        let chunk_end = (pos + current_chunk).min(target_end);
        let range_header = format!("bytes={}-{}", pos, chunk_end - 1);

        // --- No-progress watchdog ---
        let last_progress = Arc::new(AtomicI64::new(chrono::Utc::now().timestamp_millis()));
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
                    last_progress.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                    if let Err(e) = item.write_range(pos, &data[..]) {
                        download_error = Some(e);
                        break;
                    }
                    pos += data.len() as u64;
                    chunk_bytes_downloaded += data.len() as u64;

                    // Update our live tracker so other waiting reads know we've advanced.
                    worker_pos.store(pos, Ordering::Relaxed);
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
                    file = %item.path.file_name().unwrap_or_default().to_string_lossy(),
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
