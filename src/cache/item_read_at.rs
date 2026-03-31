//! `CacheItem::read_at` — download + wait with ref-counted downloader lifetime.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use crate::cache::download_session::{DownloadSession, WaiterGuard};
use crate::cache::item::{CacheItem, CacheReadError};
use crate::cache::link_heal::MAX_SESSION_LINK_HEALS;
use crate::cache::worker::{DownloaderArgs, run_downloader};
use crate::config::{Config, parse_byte_size};
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::types::Download;
use tokio::sync::{Mutex, RwLock};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_at(
    item: &Arc<CacheItem>,
    fuse_ctx: tokio_util::sync::CancellationToken,
    offset: u64,
    size: u32,
    download: &Download,
    rd: &Arc<RealDebrid>,
    unrestrict_cache: &UnrestrictCache,
    config: &Config,
    pause_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<bytes::Bytes, CacheReadError> {
    let end = (offset + size as u64).min(item.file_size);
    if offset >= item.file_size {
        return Ok(bytes::Bytes::new());
    }

    if item.has_range(offset, end) {
        return item
            .read_from_file(offset, (end - offset) as u32)
            .map_err(CacheReadError::Io);
    }

    let base_chunk = parse_byte_size(&config.vfs.chunk_size);
    let read_ahead = parse_byte_size(&config.vfs.read_ahead);
    let max_parallel_streams = config.vfs.max_parallel_streams;
    let fetch_until = (end + read_ahead).min(item.file_size);

    let (spawned_task, session) = {
        let mut workers = item.active_workers.lock().unwrap();
        let existing = workers
            .iter()
            .find(|s| s.covers_fuse_offset(offset))
            .map(Arc::clone);
        if let Some(s) = existing {
            (None::<tokio::task::JoinHandle<()>>, s)
        } else {
            let (handle, session) = spawn_worker(
                item,
                offset,
                end,
                fetch_until,
                base_chunk,
                read_ahead,
                max_parallel_streams,
                download,
                rd,
                unrestrict_cache,
                pause_rx.clone(),
            );
            workers.push(Arc::clone(&session));
            (Some(handle), session)
        }
    };

    let mut _waiter = WaiterGuard::new(Arc::clone(&session));

    let wait_result: bool = tokio::select! {
        _ = fuse_ctx.cancelled() => false,
        result = async {
            if item.has_range(offset, end) {
                return true;
            }
            loop {
                let notified = item.notify.notified();
                if item.has_range(offset, end) {
                    return true;
                }

                if spawned_task.is_none() {
                    // We joined an existing worker session. Just wait for it to download the data.
                    match tokio::time::timeout(std::time::Duration::from_millis(5000), notified).await {
                        Ok(_) => {} // Progress was made
                        Err(_) => tracing::trace!("fuse read waiting > 5s at {offset} (worker is likely downloading a large chunk)"),
                    }
                } else {
                    notified.await;
                }

                if item.has_range(offset, end) {
                    return true;
                }

                if let Some(task) = &spawned_task {
                    if task.is_finished() {
                        return item.has_range(offset, end);
                    }
                } else {
                    let still_registered = {
                        let w = item.active_workers.lock().unwrap();
                        w.iter().any(|s| Arc::ptr_eq(s, &session))
                    };
                    if !still_registered {
                        return item.has_range(offset, end);
                    }
                }
            }
        } => result
    };

    if wait_result {
        item.read_from_file(offset, (end - offset) as u32)
            .map_err(CacheReadError::Io)
    } else if fuse_ctx.is_cancelled() {
        Err(CacheReadError::Cancelled)
    } else {
        Err(CacheReadError::DownloadFailed)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_worker(
    item: &Arc<CacheItem>,
    offset: u64,
    end: u64,
    fetch_until: u64,
    base_chunk: u64,
    read_ahead: u64,
    max_parallel_streams: u32,
    download: &Download,
    rd: &Arc<RealDebrid>,
    unrestrict_cache: &UnrestrictCache,
    pause_rx: tokio::sync::watch::Receiver<bool>,
) -> (tokio::task::JoinHandle<()>, Arc<DownloadSession>) {
    let session = Arc::new(DownloadSession::new(offset, fetch_until));

    let live_download_url = Arc::new(RwLock::new(download.download.clone()));
    let link_refresh_lock = Arc::new(Mutex::new(()));
    let heal_remaining = Arc::new(AtomicU32::new(MAX_SESSION_LINK_HEALS));
    let rd_clone = Arc::clone(rd);
    let item_clone = Arc::clone(item);
    let source_link = download.link.clone();
    let unc = unrestrict_cache.clone();
    let session_for_guard = Arc::clone(&session);

    let handle = tokio::spawn(async move {
        struct WorkerGuard {
            item: Arc<CacheItem>,
            session: Arc<DownloadSession>,
        }
        impl Drop for WorkerGuard {
            fn drop(&mut self) {
                if let Ok(mut w) = self.item.active_workers.lock() {
                    w.retain(|s| !Arc::ptr_eq(s, &self.session));
                }
                self.item.notify.notify_waiters();
            }
        }
        let _guard = WorkerGuard {
            item: Arc::clone(&item_clone),
            session: Arc::clone(&session_for_guard),
        };

        if let Err(e) = run_downloader(
            &item_clone,
            DownloaderArgs {
                start: offset,
                end,
                base_chunk,
                read_ahead,
                max_parallel_streams,
                live_download_url: Arc::clone(&live_download_url),
                source_link,
                rd: rd_clone,
                unrestrict_cache: unc,
                link_refresh_lock: Arc::clone(&link_refresh_lock),
                heal_remaining: Arc::clone(&heal_remaining),
                pause_rx,
            },
        )
        .await
        {
            tracing::warn!("cache downloader error at offset {offset}: {e:#}");
        }
    });

    session.set_abort_handle(handle.abort_handle());
    (handle, session)
}
