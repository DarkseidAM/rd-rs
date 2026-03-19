//! Lookup and getattr FUSE operations.

use std::ffi::OsStr;

use fuse3::raw::prelude::*;
use fuse3::{Errno, Result as FuseResult};

use crate::fuse::consts::{
    ALL_DIR, ATTR_TTL, ENTRY_TTL, INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE,
};
use crate::fuse::fs::RdFs;

pub(super) async fn lookup(
    fs: &RdFs,
    _req: Request,
    parent: u64,
    name: &OsStr,
) -> FuseResult<ReplyEntry> {
    let name = name.to_string_lossy();

    match parent {
        INODE_ROOT => {
            if name == ALL_DIR {
                return Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: fs.dir_attr(INODE_ALL),
                    generation: 0,
                });
            }
            Err(Errno::from(libc::ENOENT))
        }
        INODE_ALL => {
            let keys = fs.get_sorted_keys();
            for (key, unique_name) in keys.iter() {
                if unique_name.to_string_lossy() == name
                    && let Some(mt) = fs.torrents.get(key)
                {
                    let inode = fs.get_or_assign_torrent_inode(key);
                    let mtime = RdFs::torrent_mtime(&mt);
                    return Ok(ReplyEntry {
                        ttl: ENTRY_TTL,
                        attr: {
                            let mut a = fs.dir_attr(inode);
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
            let torrent_index = inode - INODE_TORRENT_BASE;
            let Some(key) = fs.inode_to_access_key(inode) else {
                return Err(Errno::from(libc::ENOENT));
            };
            let Some(mt) = fs.ensure_torrent_info(&key).await else {
                return Err(Errno::from(libc::ENOENT));
            };
            for (fi, file) in mt.selected_files().into_iter().enumerate() {
                let fname = RdFs::sanitize_dirent_name(file.path.trim_start_matches('/'));
                if fname.to_string_lossy() == name.as_ref() {
                    let mtime = RdFs::torrent_mtime(&mt);
                    let size = file.bytes.max(0) as u64;
                    return Ok(ReplyEntry {
                        ttl: ENTRY_TTL,
                        attr: fs.file_attr(RdFs::file_inode(torrent_index, fi as u64), size, mtime),
                        generation: 0,
                    });
                }
            }
            Err(Errno::from(libc::ENOENT))
        }
        _ => Err(Errno::from(libc::ENOENT)),
    }
}

pub(super) async fn getattr(
    fs: &RdFs,
    _req: Request,
    inode: u64,
    _fh: Option<u64>,
    _flags: u32,
) -> FuseResult<ReplyAttr> {
    let attr = match inode {
        INODE_ROOT => fs.dir_attr(INODE_ROOT),
        INODE_ALL => fs.dir_attr(INODE_ALL),

        i if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&i) => {
            let key = fs.inode_to_access_key(i).ok_or(Errno::from(libc::ENOENT))?;
            let mt = fs.torrents.get(&key).ok_or(Errno::from(libc::ENOENT))?;
            let mtime = RdFs::torrent_mtime(mt.value());
            let mut a = fs.dir_attr(i);
            a.mtime = mtime;
            a.atime = mtime;
            a
        }

        i if i >= INODE_FILE_BASE => {
            let torrent_idx = ((i - INODE_FILE_BASE) / 10_000) as usize;
            let file_idx = ((i - INODE_FILE_BASE) % 10_000) as usize;
            let torrent_inode = INODE_TORRENT_BASE + torrent_idx as u64;
            let access_key = fs
                .inode_to_access_key(torrent_inode)
                .ok_or(Errno::from(libc::ENOENT))?;
            let mt = fs
                .ensure_torrent_info(&access_key)
                .await
                .ok_or(Errno::from(libc::ENOENT))?;
            let file = mt
                .selected_files()
                .get(file_idx)
                .copied()
                .ok_or(Errno::from(libc::ENOENT))?;
            fs.file_attr(i, file.bytes.max(0) as u64, RdFs::torrent_mtime(&mt))
        }

        _ => return Err(Errno::from(libc::ENOENT)),
    };

    Ok(ReplyAttr {
        ttl: ATTR_TTL,
        attr,
    })
}
