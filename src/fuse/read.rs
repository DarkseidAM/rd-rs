//! FUSE `read()` implementation.
//!
//! Resolves the inode to `(access_key, file_index)`, unrestricts the link,
//! serves from the disk cache (HTTP-filling missing ranges), and implements
//! transparent corrupted-link detection with retry.

use std::time::{Duration, Instant};

use fuse3::{Errno, Result as FuseResult, raw::prelude::ReplyData};

use bytes::Bytes;

use crate::cache::CacheReadError;
use crate::config::parse_byte_size;
use crate::rd::api::{UnrestrictCache, clear_unrestrict_cache};

use super::consts::INODE_FILE_BASE;
use super::fs::RdFs;
use super::vfs_read_buffer::{PrepareRead, VfsReadBuffer, clamp_buffer_size};

/// Max unrestrict+retry cycles per single `read()` call.
const MAX_RETRIES: usize = 3;

pub async fn read(
    fs: &RdFs,
    inode: u64,
    fh: u64,
    offset: u64,
    size: u32,
    unrestrict_cache: &UnrestrictCache,
    fuse_ctx: tokio_util::sync::CancellationToken,
) -> FuseResult<ReplyData> {
    // ── 1. Resolve inode → (torrent_index, file_index) ────────────────────────
    // file_inode(ti, fi) = INODE_FILE_BASE + ti * 10_000 + fi
    let offset_from_base = inode
        .checked_sub(INODE_FILE_BASE)
        .ok_or(Errno::from(libc::ENOENT))?;
    let torrent_idx = offset_from_base / 10_000;
    let file_idx = (offset_from_base % 10_000) as usize;

    let torrent_inode = crate::fuse::consts::INODE_TORRENT_BASE + torrent_idx;
    let access_key = fs
        .inode_to_access_key(torrent_inode)
        .ok_or(Errno::from(libc::ENOENT))?;

    // ── 2. Ensure torrent info is loaded ──────────────────────────────────────
    let mt = fs
        .ensure_torrent_info(&access_key)
        .await
        .ok_or(Errno::from(libc::ENOENT))?;

    let ti = mt.info.as_ref().ok_or(Errno::from(libc::ENOENT))?;
    let selected: Vec<_> = ti.files.iter().filter(|f| f.is_selected()).collect();
    let file = selected.get(file_idx).ok_or(Errno::from(libc::ENOENT))?;

    let file_size = file.bytes as u64;
    if file_size == 0 {
        return Ok(ReplyData {
            data: bytes::Bytes::new(),
        });
    }

    if mt
        .file_states
        .as_ref()
        .is_some_and(|fs| fs.get(&file.path).is_some_and(|s| s == "broken"))
    {
        let log_key = format!("{access_key}\x1f{}", file.path);
        let now = Instant::now();
        let emit = fs
            .broken_read_warn_ts
            .get(&log_key)
            .map(|t| now.duration_since(*t.value()) >= Duration::from_secs(60))
            .unwrap_or(true);
        if emit {
            fs.broken_read_warn_ts.insert(log_key, now);
            tracing::warn!(
                path = %file.path,
                key = %access_key,
                "fuse read: file marked broken in file_states (skipping unrestrict); clear with repair or edit DB if wrong"
            );
        } else {
            tracing::debug!(
                path = %file.path,
                key = %access_key,
                "fuse read: file marked broken, skip unrestrict"
            );
        }
        return Err(Errno::from(libc::ENOENT));
    }

    // Pick the right link: torrent links are 1:1 with selected files.
    let link_idx = file_idx.min(ti.links.len().saturating_sub(1));
    let torrent_link = ti
        .links
        .get(link_idx)
        .ok_or(Errno::from(libc::ENOENT))?
        .clone();

    // Build a stable cache filename from the file's path.
    let filename = std::path::Path::new(&file.path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("file_{file_idx}"));
    let sanitized = sanitize_filename(&filename);

    let config = fs.config.load();

    let cache_item = fs
        .cache
        .get_or_create(&access_key, &sanitized, file_size)
        .map_err(|e| {
            tracing::error!("cache get_or_create failed: {e:#}");
            Errno::from(libc::EIO)
        })?;

    cache_item.open();
    // Decrement on exit (regardless of success/failure path).
    struct ReleaseGuard<'a>(&'a crate::cache::CacheItem);
    impl Drop for ReleaseGuard<'_> {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let _guard = ReleaseGuard(&cache_item);

    let buffer_size = clamp_buffer_size(parse_byte_size(&config.vfs.buffer_size));

    // Per-fd read window vs direct `read_at(offset, size)`.
    enum FetchPlan {
        Direct,
        Buffered {
            buf: std::sync::Arc<tokio::sync::Mutex<VfsReadBuffer>>,
            take: u32,
        },
    }

    let (read_off, read_len, plan) = 'fetch: {
        if fh != 0
            && let Some(ent) = fs.open_files.get(&fh)
        {
            let buf = std::sync::Arc::clone(ent.value());
            drop(ent);
            let (fill_offset, fill_len, take) = {
                let mut g = buf.lock().await;
                match g.prepare_read(offset, size, file_size, buffer_size) {
                    PrepareRead::Hit(data) => {
                        tracing::trace!(
                            inode,
                            offset,
                            bytes = data.len(),
                            "fuse read: vfs buffer hit"
                        );
                        return Ok(ReplyData { data });
                    }
                    PrepareRead::Miss {
                        fill_offset,
                        fill_len,
                        take,
                    } => (fill_offset, fill_len, take),
                }
            };
            if fill_len == 0 {
                return Ok(ReplyData { data: Bytes::new() });
            }
            break 'fetch (fill_offset, fill_len, FetchPlan::Buffered { buf, take });
        }
        (offset, size, FetchPlan::Direct)
    };

    // ── 3. Retry loop : unrestrict → corrupted-link check → cache read ────────
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(attempt as u64 * 2)).await;
        }

        let download = match fs.rd.unrestrict_link(unrestrict_cache, &torrent_link).await {
            Ok(d) => d,
            Err(e) => {
                let is_fatal = match &e {
                    crate::rd::client::RdError::Api(api_err) => !api_err.should_retry(),
                    _ => false,
                };

                tracing::warn!(
                    attempt,
                    link = %torrent_link,
                    is_fatal,
                    "unrestrict failed: {e:#}"
                );

                if is_fatal {
                    tracing::error!(link = %torrent_link, "fatal unrestrict error, queuing repair");
                    clear_unrestrict_cache(
                        unrestrict_cache,
                        fs.rd.credentials.load().token.as_str(),
                        &torrent_link,
                    );
                    let tm = fs.torrent_manager.clone();
                    let ak = access_key.clone();
                    let file_path = file.path.clone();
                    if !tm.fuse_begin_fatal_read_repair(&ak, &file_path).await {
                        return Err(Errno::from(libc::ENOENT));
                    }
                    async {
                        // `unrepairable_reason` is for cascade “won’t fix” outcomes; if we set it here,
                        // [`RepairEngine::repair_one_torrent`] skips the torrent entirely (no repair run).
                        if let Err(err) = tm
                            .update_torrent_state(&ak, crate::db::TorrentState::Broken, None)
                            .await
                        {
                            tracing::error!(
                                "Failed to set Broken after fatal unrestrict for {}: {}",
                                ak,
                                err
                            );
                        }
                        let _ = tm.mark_file_broken(&ak, &file_path).await;
                        tm.enqueue_repair(ak.clone()).await;
                    }
                    .await;
                    tm.fuse_end_fatal_read_repair(&ak, &file_path).await;
                    return Err(Errno::from(libc::ENOENT));
                }

                continue;
            }
        };

        let path_ext = std::path::Path::new(&file.path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase());
        let dl_ext = download.extension().map(|s| s.to_ascii_lowercase());

        // Corrupted-link detection (commit 00ed4714):
        // filesize mismatch AND extension mismatch → bad link, not just a CDN redirect.
        if download.filesize != 0
            && download.filesize as u64 != file_size
            && dl_ext.as_deref() != path_ext.as_deref()
        {
            tracing::warn!(
                link = %torrent_link,
                rd_size = download.filesize,
                expected = file_size,
                "corrupted link detected — clearing unrestrict cache and VFS disk cache, queuing repair"
            );
            clear_unrestrict_cache(unrestrict_cache, &download.token, &torrent_link);
            fs.cache.invalidate(&access_key, &sanitized);

            let tm = fs.torrent_manager.clone();
            let ak = access_key.clone();
            let file_path = file.path.clone();
            if !tm.fuse_begin_fatal_read_repair(&ak, &file_path).await {
                return Err(Errno::from(libc::ENOENT));
            }
            async {
                if let Err(e) = tm
                    .update_torrent_state(&ak, crate::db::TorrentState::Broken, None)
                    .await
                {
                    tracing::error!(
                        "Failed to set Broken after corrupted link for {}: {}",
                        ak,
                        e
                    );
                }
                let _ = tm.mark_file_broken(&ak, &file_path).await;
                tm.enqueue_repair(ak.clone()).await;
            }
            .await;
            tm.fuse_end_fatal_read_repair(&ak, &file_path).await;

            return Err(Errno::from(libc::ENOENT));
        }

        match cache_item
            .read_at(
                fuse_ctx.clone(),
                read_off,
                read_len,
                &download,
                &fs.rd,
                unrestrict_cache,
                &config,
                fs.cache.pause_downloads.subscribe(),
            )
            .await
        {
            Ok(filled) => {
                let data = match &plan {
                    FetchPlan::Direct => filled,
                    FetchPlan::Buffered { buf, take } => {
                        let mut g = buf.lock().await;
                        let reply_len = (*take as usize).min(filled.len());
                        let reply = filled.slice(0..reply_len);
                        g.after_fetch(read_off, filled, *take);
                        reply
                    }
                };
                tracing::trace!(inode, offset, bytes = data.len(), "fuse read: served");
                return Ok(ReplyData { data });
            }
            Err(CacheReadError::Cancelled) => {
                return Err(Errno::from(libc::EINTR));
            }
            Err(CacheReadError::DownloadFailed) => {
                clear_unrestrict_cache(unrestrict_cache, &download.token, &torrent_link);
                tracing::warn!(attempt, inode, offset, "download failed — retrying");
                continue;
            }
            Err(CacheReadError::Io(e)) => {
                tracing::error!(inode, offset, "read_at IO error: {e:#}");
                return Err(Errno::from(libc::EIO));
            }
        }
    }

    tracing::error!(inode, "read: all {MAX_RETRIES} retries exhausted");
    Err(Errno::from(libc::EIO))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Strip characters unsafe in filesystem paths, keeping alphanumeric, `.`, `-`, `_`.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
