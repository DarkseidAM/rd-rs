//! `RdFs` — the fuse3 PathFilesystem implementation.
//!
//! Phase 2 scope:
//!   - `lookup` + `getattr` → size from TorrentInfo.files or total_size
//!   - `readdir` → list `__all__/` and each torrent directory
//!   - `open` → stateless (no per-fd handle allocated yet)
//!   - `read` → **stub, returns ENOSYS** (implemented in Phase 3)

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use dashmap::DashMap;
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result as FuseResult, Timestamp};
use futures_util::stream::{self, Iter};

use crate::config::Config;
use crate::rd::RealDebrid;
use crate::torrent::ManagedTorrent;

/// Top-level directory name served under the mount point.
const ALL_DIR: &str = "__all__";

/// Attribute cache TTL — matches Go's `attr_timeout = 1m`.
const ATTR_TTL: Duration = Duration::from_secs(60);

/// Directory entry cache TTL — matches Go's `dir_cache_time = 10m`.
const ENTRY_TTL: Duration = Duration::from_secs(600);

/// Inode ranges (stable for the FUSE layer).
const INODE_ROOT: u64 = 1;
const INODE_ALL: u64 = 2;
// Torrent dirs start at 3, files at INODE_FILE_BASE + offset.
const INODE_TORRENT_BASE: u64 = 3;
const INODE_FILE_BASE: u64 = 1 << 32; // high range to avoid collisions

// ─── RdFs struct ─────────────────────────────────────────────────────────────

pub struct RdFs {
    pub torrents: Arc<DashMap<String, Arc<ManagedTorrent>>>,
    pub rd: Arc<RealDebrid>,
    pub config: Arc<Config>,
}

impl RdFs {
    pub fn new(
        torrents: Arc<DashMap<String, Arc<ManagedTorrent>>>,
        rd: Arc<RealDebrid>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            torrents,
            rd,
            config,
        }
    }

    // ─── Attribute helpers ─────────────────────────────────────────────────

    fn dir_attr(&self, inode: u64) -> FileAttr {
        FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH.into(),
            mtime: UNIX_EPOCH.into(),
            ctime: UNIX_EPOCH.into(),
            kind: FileType::Directory,
            perm: 0o555,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
        }
    }

    fn file_attr(&self, inode: u64, size: u64, mtime: Timestamp) -> FileAttr {
        FileAttr {
            ino: inode,
            size,
            blocks: size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            kind: FileType::RegularFile,
            perm: 0o444,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
        }
    }

    fn torrent_mtime(mt: &ManagedTorrent) -> Timestamp {
        (UNIX_EPOCH + Duration::from_secs(mt.torrent.added.timestamp().max(0) as u64)).into()
    }

    /// Stable inode for a torrent directory indexed by DashMap insertion order.
    /// This is approximate — good enough for FUSE until we store inodes in SQLite.
    fn torrent_inode(index: u64) -> u64 {
        INODE_TORRENT_BASE + index
    }

    fn file_inode(torrent_index: u64, file_index: u64) -> u64 {
        INODE_FILE_BASE + torrent_index * 10_000 + file_index
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

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        let name = name.to_string_lossy();

        match parent {
            INODE_ROOT => {
                if name == ALL_DIR {
                    return Ok(ReplyEntry {
                        ttl: ENTRY_TTL,
                        attr: self.dir_attr(INODE_ALL),
                        generation: 0,
                    });
                }
                Err(Errno::from(libc::ENOENT))
            }
            INODE_ALL => {
                // Looking up an access_key directory
                for (index, entry) in self.torrents.iter().enumerate() {
                    if entry.key() == name.as_ref() {
                        let mt = entry.value();
                        let mtime = Self::torrent_mtime(mt);
                        return Ok(ReplyEntry {
                            ttl: ENTRY_TTL,
                            attr: {
                                let mut a = self.dir_attr(Self::torrent_inode(index as u64));
                                a.mtime = mtime;
                                a.atime = mtime;
                                a
                            },
                            generation: 0,
                        });
                    }
                }
                Err(Errno::from(libc::ENOENT))
            }
            inode if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&inode) => {
                // Looking up a file inside a torrent directory
                let torrent_index = inode - INODE_TORRENT_BASE;
                if let Some(mt_entry) = self.torrents.iter().nth(torrent_index as usize) {
                    let mt = mt_entry.value();
                    for (fi, file) in mt.files().iter().enumerate() {
                        // file.path is like "/movie.mkv" — strip leading slash
                        let fname = file.path.trim_start_matches('/');
                        if fname == name.as_ref() {
                            let mtime = Self::torrent_mtime(mt);
                            let size = file.bytes.max(0) as u64;
                            return Ok(ReplyEntry {
                                ttl: ENTRY_TTL,
                                attr: self.file_attr(
                                    Self::file_inode(torrent_index, fi as u64),
                                    size,
                                    mtime,
                                ),
                                generation: 0,
                            });
                        }
                    }
                }
                Err(Errno::from(libc::ENOENT))
            }
            _ => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let attr = match inode {
            INODE_ROOT => self.dir_attr(INODE_ROOT),
            INODE_ALL => self.dir_attr(INODE_ALL),

            i if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&i) => {
                let idx = (i - INODE_TORRENT_BASE) as usize;
                let mt_entry = self
                    .torrents
                    .iter()
                    .nth(idx)
                    .ok_or(Errno::from(libc::ENOENT))?;
                let mtime = Self::torrent_mtime(mt_entry.value());
                let mut a = self.dir_attr(i);
                a.mtime = mtime;
                a.atime = mtime;
                a
            }

            i if i >= INODE_FILE_BASE => {
                let torrent_idx = ((i - INODE_FILE_BASE) / 10_000) as usize;
                let file_idx = ((i - INODE_FILE_BASE) % 10_000) as usize;
                let mt_entry = self
                    .torrents
                    .iter()
                    .nth(torrent_idx)
                    .ok_or(Errno::from(libc::ENOENT))?;
                let mt = mt_entry.value();
                let file = mt.files().get(file_idx).ok_or(Errno::from(libc::ENOENT))?;
                self.file_attr(i, file.bytes.max(0) as u64, Self::torrent_mtime(mt))
            }

            _ => return Err(Errno::from(libc::ENOENT)),
        };

        Ok(ReplyAttr {
            ttl: ATTR_TTL,
            attr,
        })
    }

    type DirEntryStream<'a>
        = Iter<
        std::iter::Map<
            std::vec::IntoIter<DirectoryEntry>,
            fn(DirectoryEntry) -> FuseResult<DirectoryEntry>,
        >,
    >
    where
        Self: 'a;

    type DirEntryPlusStream<'a> = stream::Empty<FuseResult<DirectoryEntryPlus>>;

    async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'_>>> {
        let entries: Vec<DirectoryEntry> = match inode {
            INODE_ROOT => {
                vec![
                    DirectoryEntry {
                        inode: INODE_ROOT,
                        kind: FileType::Directory,
                        name: ".".into(),
                        offset: 1,
                    },
                    DirectoryEntry {
                        inode: INODE_ROOT,
                        kind: FileType::Directory,
                        name: "..".into(),
                        offset: 2,
                    },
                    DirectoryEntry {
                        inode: INODE_ALL,
                        kind: FileType::Directory,
                        name: ALL_DIR.into(),
                        offset: 3,
                    },
                ]
            }

            INODE_ALL => {
                let mut entries = vec![
                    DirectoryEntry {
                        inode: INODE_ALL,
                        kind: FileType::Directory,
                        name: ".".into(),
                        offset: 1,
                    },
                    DirectoryEntry {
                        inode: INODE_ROOT,
                        kind: FileType::Directory,
                        name: "..".into(),
                        offset: 2,
                    },
                ];
                for (index, entry) in self.torrents.iter().enumerate() {
                    entries.push(DirectoryEntry {
                        inode: Self::torrent_inode(index as u64),
                        kind: FileType::Directory,
                        name: entry.key().into(),
                        offset: (index + 3) as i64,
                    });
                }
                entries
            }

            i if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&i) => {
                let idx = (i - INODE_TORRENT_BASE) as usize;
                let mt_ref = self
                    .torrents
                    .iter()
                    .nth(idx)
                    .ok_or(Errno::from(libc::ENOENT))?;
                let mt = mt_ref.value();

                let mut entries = vec![
                    DirectoryEntry {
                        inode: i,
                        kind: FileType::Directory,
                        name: ".".into(),
                        offset: 1,
                    },
                    DirectoryEntry {
                        inode: INODE_ALL,
                        kind: FileType::Directory,
                        name: "..".into(),
                        offset: 2,
                    },
                ];
                for (fi, file) in mt.files().iter().enumerate() {
                    let fname = file.path.trim_start_matches('/').to_string();
                    entries.push(DirectoryEntry {
                        inode: Self::file_inode(idx as u64, fi as u64),
                        kind: FileType::RegularFile,
                        name: fname.into(),
                        offset: (fi + 3) as i64,
                    });
                }
                entries
            }

            _ => return Err(Errno::from(libc::ENOENT)),
        };

        // Apply offset (FUSE convention: skip entries already sent).
        let off = offset.max(0) as usize;
        let tail: Vec<_> = entries.into_iter().skip(off).collect();

        Ok(ReplyDirectory {
            entries: stream::iter(
                tail.into_iter()
                    .map(Ok as fn(DirectoryEntry) -> FuseResult<DirectoryEntry>),
            ),
        })
    }

    async fn open(&self, _req: Request, _inode: u64, flags: u32) -> FuseResult<ReplyOpen> {
        // Reject write requests
        if (flags & (libc::O_WRONLY as u32 | libc::O_RDWR as u32)) != 0 {
            return Err(Errno::from(libc::EACCES));
        }

        let reply = ReplyOpen {
            fh: 0,
            flags: libc::O_DIRECT as u32,
        };
        Ok(reply)
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _offset: u64,
        _size: u32,
    ) -> FuseResult<ReplyData> {
        // Phase 3 will implement HTTP range + disk cache.
        Err(Errno::from(libc::ENOSYS))
    }
}
