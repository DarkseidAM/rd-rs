use crate::cache::item::CacheItem;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::time::{self, Duration};

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
    pub max_parallel_streams: u32,
    pub download_url: String,
    #[allow(dead_code)]
    pub unrestrict_url: String,
    pub rd: Arc<RealDebrid>,
    #[allow(dead_code)]
    pub unrestrict_cache: UnrestrictCache,
}

/// HTTP Range GET pool for `[start, file_end)` using concurrent connection chunking
/// and RD global semaphores.
pub(crate) async fn run_downloader(item: &Arc<CacheItem>, args: DownloaderArgs) -> Result<()> {
    let DownloaderArgs {
        start,
        end,
        base_chunk,
        read_ahead,
        max_parallel_streams,
        download_url,
        rd,
        ..
    } = args;

    // Apply read-ahead: download further than strictly needed.
    let target_end = (end + read_ahead).min(item.file_size);

    // Compute exactly which pieces of this slice we are missing.
    let mut chunks_to_fetch = Vec::new();
    {
        let r = item.ranges.read().unwrap();
        let mut pos = start;
        while pos < target_end {
            if let Some((miss_start, miss_end)) = r.find_missing(pos, target_end) {
                let mut slice_start = miss_start;
                while slice_start < miss_end {
                    let slice_end = (slice_start + base_chunk).min(miss_end);
                    chunks_to_fetch.push((slice_start, slice_end));
                    slice_start = slice_end;
                }
                pos = miss_end;
            } else {
                break;
            }
        }
    }

    if chunks_to_fetch.is_empty() {
        return Ok(());
    }

    let total_chunks = chunks_to_fetch.len();
    let queue = Arc::new(std::sync::Mutex::new(chunks_to_fetch.into_iter()));
    let num_workers = max_parallel_streams.min(total_chunks as u32).max(1);

    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..num_workers {
        let queue_clone = Arc::clone(&queue);
        let rd_clone = Arc::clone(&rd);
        let url_clone = download_url.clone();
        let item_clone = Arc::clone(item);
        join_set.spawn(async move {
            loop {
                let chunk_opt = { queue_clone.lock().unwrap().next() };
                let Some((chunk_start, chunk_end)) = chunk_opt else { break Ok::<(), anyhow::Error>(()) };

                let mut retries = 0;
                loop {
                    // Acquire global RD semaphore permit!
                    let _permit = rd_clone.connection_semaphore.acquire().await
                        .map_err(|e| anyhow::anyhow!("Semaphore closed: {}", e))?;

                    let range_header = format!("bytes={}-{}", chunk_start, chunk_end - 1);

                    // --- No-progress watchdog ---
                    let last_progress = Arc::new(AtomicI64::new(chrono::Utc::now().timestamp_millis()));
                    let lp_clone = Arc::clone(&last_progress);
                    let (watchdog_tx, mut watchdog_rx) = tokio::sync::oneshot::channel::<()>();

                    let watchdog = tokio::spawn(async move {
                        let mut interval = time::interval(NO_PROGRESS_CHECK);
                        loop {
                            interval.tick().await;
                            if watchdog_tx.is_closed() { return; }
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
                        res = rd_clone.http_range_get(&url_clone, &range_header) => {
                            match res {
                                Ok(r) => r,
                                Err(e) => {
                                    watchdog.abort();
                                    tracing::warn!(
                                        chunk_start,
                                        range = %range_header,
                                        "HTTP range GET failed: {e:#}"
                                    );
                                    retries += 1;
                                    if retries >= MAX_DOWNLOAD_RETRIES {
                                        return Err(anyhow::anyhow!(
                                            "HTTP range GET failed after {MAX_DOWNLOAD_RETRIES} retries at {chunk_start} ({range_header}): {e:#}"
                                        ));
                                    }
                                    continue;
                                }
                            }
                        }
                        _ = &mut watchdog_rx => {
                            watchdog.abort();
                            tracing::warn!(
                                chunk_start,
                                range = %range_header,
                                "stream headers stalled for 45s"
                            );
                            retries += 1;
                            if retries >= MAX_DOWNLOAD_RETRIES {
                                return Err(anyhow::anyhow!(
                                    "stream stalled (headers): no progress for 45s at {chunk_start} ({range_header})"
                                ));
                            }
                            continue;
                        }
                    };

                    let mut chunk_bytes_downloaded = 0u64;
                    let mut download_error = None;
                    let mut current_pos = chunk_start;

                    loop {
                        let chunk_res = tokio::select! {
                            res = resp.chunk() => res,
                            _ = &mut watchdog_rx => {
                                download_error =
                                    Some(anyhow::anyhow!("stream body stalled for 45s"));
                                break;
                            }
                        };

                        match chunk_res {
                            Ok(Some(data)) => {
                                last_progress.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                                if let Err(e) = item_clone.write_range(current_pos, &data[..]) {
                                    download_error = Some(e);
                                    break;
                                }
                                current_pos += data.len() as u64;
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
                            let chunk_len = chunk_end - chunk_start;
                            let c_mb = format!("{:.2} MB", chunk_len as f64 / 1048576.0);
                            let b_mb = format!("{:.2} MB", chunk_bytes_downloaded as f64 / 1048576.0);
                            let o_mb = format!("{:.2} MB", chunk_start as f64 / 1048576.0);

                            tracing::info!(
                                file = %item_clone.path.file_name().unwrap_or_default().to_string_lossy(),
                                range = %range_header,
                                chunk = %format!("{} ({})", chunk_len, c_mb),
                                bytes = %format!("{} ({})", chunk_bytes_downloaded, b_mb),
                                offset = %format!("{} ({})", chunk_start, o_mb),
                                "chunk downloaded successfully"
                            );
                            break; // Done with this retry loop
                        }
                        Some(e) => {
                            let expected = chunk_end - chunk_start;
                            tracing::warn!(
                                chunk_start,
                                range = %range_header,
                                downloaded = chunk_bytes_downloaded,
                                expected,
                                "HTTP streaming failed: {e:#}"
                            );
                            retries += 1;
                            if retries >= MAX_DOWNLOAD_RETRIES {
                                return Err(e);
                            }
                            time::sleep(Duration::from_secs(retries as u64 * 2)).await;
                        }
                    }
                } // End retry loop
            } // End chunk loop
        });
    }

    // Wait for all HTTP streams to complete or fail.
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(worker_res) => {
                if let Err(e) = worker_res {
                    tracing::error!("A chunk worker failed: {e:#}");
                    join_set.abort_all();
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("worker task panicked: {e:#}");
                join_set.abort_all();
                return Err(anyhow::anyhow!("task join error"));
            }
        }
    }

    Ok(())
}
