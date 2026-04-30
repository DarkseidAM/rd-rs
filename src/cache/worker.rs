use crate::cache::item::CacheItem;
use crate::cache::link_heal;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use anyhow::Result;
use parking_lot::Mutex as ParkingMutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

/// If no bytes are written to cache for this duration, cancel the in-flight
/// HTTP stream attempt and retry from the same offset.
pub(crate) const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(15);
/// Max retries before giving up and returning an error to FUSE.
pub(crate) const MAX_DOWNLOAD_RETRIES: u32 = 3;

#[inline]
fn no_progress_timeout_secs() -> u64 {
    NO_PROGRESS_TIMEOUT.as_secs()
}

/// Arguments for `run_downloader`, grouped to stay under clippy's arg-count limit.
pub(crate) struct DownloaderArgs {
    pub start: u64,
    pub end: u64,
    pub base_chunk: u64,
    pub read_ahead: u64,
    pub max_parallel_streams: u32,
    /// Current CDN URL; updated when link auto-heal succeeds.
    pub live_download_url: Arc<RwLock<String>>,
    /// Original RD link for `POST /unrestrict/link`.
    pub source_link: String,
    pub rd: Arc<RealDebrid>,
    pub unrestrict_cache: UnrestrictCache,
    pub link_refresh_lock: Arc<Mutex<()>>,
    pub heal_remaining: Arc<AtomicU32>,
    pub pause_rx: tokio::sync::watch::Receiver<bool>,
    pub cancel: CancellationToken,
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
        live_download_url,
        source_link,
        rd,
        unrestrict_cache,
        link_refresh_lock,
        heal_remaining,
        pause_rx,
        cancel,
    } = args;

    // Apply read-ahead: download further than strictly needed.
    let target_end = (end + read_ahead).min(item.file_size);

    // Compute exactly which slices of this range we are missing.
    let mut missing_slices = std::collections::VecDeque::new();
    {
        let r = item.ranges.read();
        let mut pos = start;
        while pos < target_end {
            if let Some((miss_start, miss_end)) = r.find_missing(pos, target_end) {
                missing_slices.push_back((miss_start, miss_end));
                pos = miss_end;
            } else {
                break;
            }
        }
    }

    if missing_slices.is_empty() {
        return Ok(());
    }

    let queue = Arc::new(ParkingMutex::new(missing_slices));
    let num_workers = max_parallel_streams.max(1);
    // Shared adaptive multiplier — all workers read/write the same value so a
    // CDN stall on any one worker immediately reduces chunk sizes for all.
    let shared_multiplier: Arc<AtomicU32> = Arc::new(AtomicU32::new(1));

    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..num_workers {
        let queue_clone = Arc::clone(&queue);
        let rd_clone = Arc::clone(&rd);
        let live_url = Arc::clone(&live_download_url);
        let source_link = source_link.clone();
        let unc = unrestrict_cache.clone();
        let refresh_lock = Arc::clone(&link_refresh_lock);
        let heal_rem = Arc::clone(&heal_remaining);
        let item_clone = Arc::clone(item);
        let mut p_rx = pause_rx.clone();
        let mult: Arc<AtomicU32> = Arc::clone(&shared_multiplier);
        let cancel = cancel.clone();

        join_set.spawn(async move {

            loop {
                if cancel.is_cancelled() {
                    break Ok::<(), anyhow::Error>(());
                }
                if *p_rx.borrow() {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break Ok::<(), anyhow::Error>(()),
                        _ = p_rx.changed() => {}
                    }
                }

                let chunk_opt = {
                    let mut q = queue_clone.lock();
                    if let Some((slice_start, slice_end)) = q.pop_front() {
                        let multiplier = mult.load(Ordering::Relaxed);
                        let chunk_size = (base_chunk * u64::from(multiplier)).min(slice_end - slice_start);
                        let chunk_end = slice_start + chunk_size;
                        if chunk_end < slice_end {
                            q.push_front((chunk_end, slice_end));
                        }
                        Some((slice_start, chunk_end))
                    } else {
                        None
                    }
                };
                let Some((chunk_start, mut chunk_end)) = chunk_opt else { break Ok::<(), anyhow::Error>(()) };

                    let mut retries = 0;

                    loop {
                        if cancel.is_cancelled() {
                            return Ok::<(), anyhow::Error>(());
                        }
                        // Acquire global RD semaphore permit!
                        let _permit = tokio::select! {
                            _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                            p = rd_clone.connection_semaphore.acquire() => {
                                p.map_err(|e| anyhow::anyhow!("Semaphore closed: {}", e))?
                            }
                        };

                        let range_header = format!("bytes={}-{}", chunk_start, chunk_end - 1);

                        let url_snapshot = {
                            let g = live_url.read().await;
                            g.clone()
                        };

                        // --- Headers phase with timeout ---
                        let mut resp = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                            res = tokio::time::timeout(NO_PROGRESS_TIMEOUT, rd_clone.http_range_get(&url_snapshot, &range_header)) => {
                                match res {
                                    Err(_elapsed) => {
                                        let secs = no_progress_timeout_secs();
                                        tracing::warn!(
                                            chunk_start,
                                            range = %range_header,
                                            stall_timeout_secs = secs,
                                            "stream headers stalled for {}s",
                                            secs
                                        );
                                        retries += 1;
                                        if retries >= MAX_DOWNLOAD_RETRIES {
                                            return Err(anyhow::anyhow!(
                                                "stream stalled (headers): no progress for {secs}s at {chunk_start} ({range_header})"
                                            ));
                                        }
                                        mult.store(1, Ordering::Relaxed);
                                        let chunk_size = chunk_end - chunk_start;
                                        if chunk_size > base_chunk {
                                            let new_chunk_end = chunk_start + base_chunk;
                                            queue_clone.lock().push_front((new_chunk_end, chunk_end));
                                            chunk_end = new_chunk_end;
                                        }
                                        continue;
                                    }
                                    Ok(Ok(r)) => r,
                                    Ok(Err(e)) => {
                                        if link_heal::attempt_cdn_link_refresh(
                                            &e,
                                            &rd_clone,
                                            &unc,
                                            &source_link,
                                            &live_url,
                                            &refresh_lock,
                                            &heal_rem,
                                        )
                                        .await
                                        {
                                            retries = 0;
                                            continue;
                                        }
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
                                        mult.store(1, Ordering::Relaxed);
                                        let chunk_size = chunk_end - chunk_start;
                                        if chunk_size > base_chunk {
                                            let new_chunk_end = chunk_start + base_chunk;
                                            queue_clone.lock().push_front((new_chunk_end, chunk_end));
                                            chunk_end = new_chunk_end;
                                        }
                                        continue;
                                    }
                                }
                            }
                        };

                        // --- Check for missing Content-Range on a Range request (stale CDN node) ---
                        // A 200 response without Content-Range means the server ignored our Range
                        // header and returned the full body from offset 0.  Writing that at
                        // chunk_start would corrupt the sparse file, so we discard and retry.
                        if resp.status() == reqwest::StatusCode::OK
                            && !resp.headers().contains_key(reqwest::header::CONTENT_RANGE)
                            && chunk_start > 0
                        {
                            tracing::warn!(
                                chunk_start,
                                range = %range_header,
                                "CDN returned 200 without Content-Range (stale node); retrying"
                            );
                            retries += 1;
                            if retries >= MAX_DOWNLOAD_RETRIES {
                                return Err(anyhow::anyhow!(
                                    "CDN kept returning 200 without Content-Range after {MAX_DOWNLOAD_RETRIES} retries at {chunk_start}"
                                ));
                            }
                            // brief back-off before retry
                            tokio::select! {
                                _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                                _ = time::sleep(Duration::from_secs(retries as u64 * 2)) => {}
                            }
                            continue;
                        }

                        let mut chunk_bytes_downloaded = 0u64;
                        let mut download_error = None;
                        let mut current_pos = chunk_start;

                        loop {
                            // Per-chunk body read with individual timeout to detect body stalls.
                            let chunk_res = tokio::select! {
                                biased;
                                _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                                res = tokio::time::timeout(NO_PROGRESS_TIMEOUT, resp.chunk()) => res,
                            };

                            match chunk_res {
                                Err(_elapsed) => {
                                    let secs = no_progress_timeout_secs();
                                    download_error = Some(anyhow::anyhow!(
                                        "stream body stalled for {secs}s"
                                    ));
                                    break;
                                }
                                Ok(Ok(Some(data))) => {
                                    if let Err(e) = item_clone.write_range(current_pos, &data[..]) {
                                        download_error = Some(e);
                                        break;
                                    }
                                    current_pos += data.len() as u64;
                                    chunk_bytes_downloaded += data.len() as u64;
                                }
                                Ok(Ok(None)) => break, // Success, EOF for this chunk
                                Ok(Err(e)) => {
                                    download_error = Some(anyhow::anyhow!("HTTP chunk error: {e:#}"));
                                    break;
                                }
                            }
                        }

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
                                mult.store(1, Ordering::Relaxed);
                                let chunk_size = chunk_end - chunk_start;
                                if chunk_size > base_chunk {
                                    let new_chunk_end = chunk_start + base_chunk;
                                    queue_clone.lock().push_front((new_chunk_end, chunk_end));
                                    chunk_end = new_chunk_end;
                                }
                                tokio::select! {
                                    _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                                    _ = time::sleep(Duration::from_secs(retries as u64 * 2)) => {}
                                }
                            }
                        }
                    } // End retry loop

                // Grow the shared multiplier on success (up to 16×).
                let _ = mult.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |m| {
                    Some((m * 2).min(16u32))
                });
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
