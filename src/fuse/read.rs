//! FUSE `read()` implementation.
//!
//! Resolves the inode to `(access_key, file_index)`, unrestricts the link,
//! serves from the disk cache (HTTP-filling missing ranges), and implements
//! transparent corrupted-link detection with retry.

use fuse3::{Errno, Result as FuseResult, raw::prelude::ReplyData};

use crate::cache::CacheReadError;
use crate::rd::api::UnrestrictCache;

use super::consts::INODE_FILE_BASE;
use super::fs::RdFs;

/// Max unrestrict+retry cycles per single `read()` call.
const MAX_RETRIES: usize = 3;

pub async fn read(
    fs: &RdFs,
    inode: u64,
    _fh: u64,
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

    // ── 3. Retry loop : unrestrict → corrupted-link check → cache read ────────
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(attempt as u64 * 2)).await;
        }

        let download = match fs.rd.unrestrict_link(unrestrict_cache, &torrent_link).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(
                    attempt,
                    link = %torrent_link,
                    "unrestrict failed: {e:#}"
                );
                continue;
            }
        };

        // Corrupted-link detection (commit 00ed4714):
        // filesize mismatch AND extension mismatch → bad link, not just a CDN redirect.
        if download.filesize != 0
            && download.filesize as u64 != file_size
            && (download.extension() != Some("mkv"))
        {
            tracing::warn!(
                link = %torrent_link,
                rd_size = download.filesize,
                expected = file_size,
                "corrupted link detected — clearing cache, queuing repair"
            );
            crate::rd::RealDebrid::clear_unrestrict_cache(unrestrict_cache, &torrent_link);
            // TODO: enqueue_repair (repair engine: Weeks 7–9)
            return Err(Errno::from(libc::ENOENT));
        }

        match cache_item
            .read_at(
                fuse_ctx.clone(),
                offset,
                size,
                &download,
                &fs.rd,
                unrestrict_cache,
                &config,
            )
            .await
        {
            Ok(data) => {
                tracing::trace!(inode, offset, bytes = data.len(), "fuse read: served");
                return Ok(ReplyData { data });
            }
            Err(CacheReadError::Cancelled) => {
                return Err(Errno::from(libc::EINTR));
            }
            Err(CacheReadError::DownloadFailed) => {
                crate::rd::RealDebrid::clear_unrestrict_cache(unrestrict_cache, &torrent_link);
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
