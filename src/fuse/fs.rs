//! `RdFs` — the fuse3 PathFilesystem implementation.
//!
//! **Phase 2 (complete):**
//!   - `lookup` + `getattr` → size from TorrentInfo.files; root, `__all__`, torrent dirs, files
//!   - `readdir` / `readdirplus` → root, `__all__` (by display name, dedup), torrent dirs (lazy TorrentInfo)
//!   - `opendir`, `access`, `listxattr`, `getxattr` → implemented so kernel does not EIO
//!   - `open` → unique `fh` per regular file open; `release` drops per-fd read buffer state
//!   - `read` → **Phase 3: HTTP range + disk cache (implemented)**
//!
//! ## Checking FUSE methods (service running)
//!
//! With the mount live (e.g. at `/mnt/test`), use shell commands to hit each path:
//!
//! - **readdir(root)** — `ls /mnt/test`
//! - **lookup(root, "__all__")** — `ls /mnt/test/__all__`
//! - **readdir(INODE_ALL)** — same as above
//! - **lookup(INODE_ALL, access_key)** — `stat /mnt/test/__all__/<some_access_key>`
//! - **readdir(torrent dir)** — `ls /mnt/test/__all__/<access_key>` (triggers ensure_torrent_info)
//! - **lookup(file)** — `stat /mnt/test/__all__/<access_key>/<filename>`
//! - **getattr(file)** — same as above, or after open
//! - **open** — `touch /mnt/test/__all__/x/y 2>/dev/null` (will fail: read-only) or open for read
//! - **read** — `cat /mnt/test/__all__/.../file` (cache + optional per-fd buffer)

use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result as FuseResult};

use crate::cache::Cache;
use crate::config::Config;
use crate::fuse::consts::{INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE};
use crate::fuse::vfs_read_buffer::VfsReadBuffer;
use crate::rd::RealDebrid;
use crate::rd::api::{UnrestrictCache, new_unrestrict_cache};
use crate::torrent::{ManagedTorrent, TorrentManager};
use arc_swap::ArcSwap;

// ─── RdFs struct ─────────────────────────────────────────────────────────────

type CachedAllDir = std::sync::RwLock<(Instant, Arc<Vec<(String, OsString)>>)>;

pub struct RdFs {
    pub torrents: Arc<DashMap<String, Arc<ManagedTorrent>>>,
    pub torrent_manager: Arc<TorrentManager>,
    pub rd: Arc<RealDebrid>,
    pub config: Arc<ArcSwap<Config>>,
    pub cache: Arc<Cache>,
    pub(crate) unrestrict_cache: UnrestrictCache,
    pub(crate) key_to_inode: DashMap<String, u64>,
    pub(crate) inode_to_key: DashMap<u64, String>,
    pub(crate) next_torrent_inode: AtomicU64,
    pub(crate) cached_all_dir: CachedAllDir,
    /// Throttle `warn!` when reads skip a file marked `broken` in `file_states` (otherwise silent at `info`).
    pub(crate) broken_read_warn_ts: DashMap<String, Instant>,
    /// Next FUSE file handle for regular files (`>= 1`; `0` = no per-fd buffer).
    pub(crate) next_fh: AtomicU64,
    /// Per-`fh` read-ahead buffer for file opens (see `vfs_read_buffer`).
    pub(crate) open_files: DashMap<u64, Arc<tokio::sync::Mutex<VfsReadBuffer>>>,
    /// Kernel attribute cache TTL (from `vfs.attr_timeout_secs`).
    pub(crate) attr_ttl: Duration,
    /// Kernel directory-entry cache TTL (from `vfs.entry_timeout_secs`).
    pub(crate) entry_ttl: Duration,
}

impl RdFs {
    pub fn new(
        torrent_manager: Arc<TorrentManager>,
        rd: Arc<RealDebrid>,
        config: Arc<ArcSwap<Config>>,
        cache: Arc<Cache>,
    ) -> Self {
        let torrents = torrent_manager.torrents.clone();
        let vfs = config.load().vfs.clone();
        Self {
            torrents,
            torrent_manager,
            rd,
            config,
            cache,
            unrestrict_cache: new_unrestrict_cache(),
            key_to_inode: DashMap::new(),
            inode_to_key: DashMap::new(),
            next_torrent_inode: AtomicU64::new(INODE_TORRENT_BASE),
            cached_all_dir: std::sync::RwLock::new((
                std::time::Instant::now() - Duration::from_secs(600),
                Arc::new(Vec::new()),
            )),
            broken_read_warn_ts: DashMap::new(),
            next_fh: AtomicU64::new(1),
            open_files: DashMap::new(),
            attr_ttl: Duration::from_secs(vfs.attr_timeout_secs),
            entry_ttl: Duration::from_secs(vfs.entry_timeout_secs),
        }
    }
}

// ─── PathFilesystem impl ──────────────────────────────────────────────────────

impl Filesystem for RdFs {
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: std::num::NonZeroU32::new(128 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        super::fs_lookup::lookup(self, req, parent, name).await
    }

    async fn getattr(
        &self,
        req: Request,
        inode: u64,
        fh: Option<u64>,
        flags: u32,
    ) -> FuseResult<ReplyAttr> {
        super::fs_lookup::getattr(self, req, inode, fh, flags).await
    }

    async fn readdir<'a>(
        &'a self,
        req: Request,
        inode: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<
        ReplyDirectory<
            impl futures_util::stream::Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a,
        >,
    > {
        super::fs_readdir::readdir(self, req, inode, fh, offset).await
    }

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: u64,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> FuseResult<
        ReplyDirectoryPlus<
            impl futures_util::stream::Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a,
        >,
    > {
        super::fs_readdirplus::readdirplus(self, req, parent, fh, offset, lock_owner).await
    }

    async fn opendir(&self, _req: Request, _inode: u64, _flags: u32) -> FuseResult<ReplyOpen> {
        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn listxattr(&self, _req: Request, _inode: u64, size: u32) -> FuseResult<ReplyXAttr> {
        if size == 0 {
            Ok(ReplyXAttr::Size(0))
        } else {
            Ok(ReplyXAttr::Data(Bytes::new()))
        }
    }

    async fn getxattr(
        &self,
        _req: Request,
        _inode: u64,
        _name: &OsStr,
        _size: u32,
    ) -> FuseResult<ReplyXAttr> {
        Err(Errno::from(libc::ENODATA))
    }

    async fn access(&self, _req: Request, inode: u64, mask: u32) -> FuseResult<()> {
        const W_OK: u32 = 2;
        if (mask & W_OK) != 0 {
            return Err(Errno::from(libc::EACCES));
        }
        match inode {
            INODE_ROOT | INODE_ALL => Ok(()),
            i if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&i) => {
                if self.inode_to_access_key(i).is_some() {
                    Ok(())
                } else {
                    Err(Errno::from(libc::ENOENT))
                }
            }
            i if i >= crate::fuse::consts::INODE_FILE_BASE => Ok(()),
            _ => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> FuseResult<ReplyOpen> {
        if (flags & (libc::O_WRONLY as u32 | libc::O_RDWR as u32)) != 0 {
            return Err(Errno::from(libc::EACCES));
        }
        if inode >= INODE_FILE_BASE {
            let fh = self
                .next_fh
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.open_files.insert(
                fh,
                Arc::new(tokio::sync::Mutex::new(VfsReadBuffer::default())),
            );
            return Ok(ReplyOpen { fh, flags: 0 });
        }
        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        self.open_files.remove(&fh);
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let ctx = tokio_util::sync::CancellationToken::new();
        super::read::read(self, inode, fh, offset, size, &self.unrestrict_cache, ctx).await
    }
}
