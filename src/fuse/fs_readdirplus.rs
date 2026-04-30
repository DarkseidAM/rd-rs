//! readdirplus FUSE operation.

use fuse3::Result as FuseResult;
use fuse3::raw::prelude::*;
use futures_util::stream::{self};

use crate::fuse::consts::{ALL_DIR, INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE};
use crate::fuse::fs::RdFs;

pub(super) async fn readdirplus<'a>(
    fs: &'a RdFs,
    _req: Request,
    parent: u64,
    _fh: u64,
    offset: u64,
    _lock_owner: u64,
) -> FuseResult<
    ReplyDirectoryPlus<
        impl futures_util::stream::Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a,
    >,
> {
    let entries: Vec<DirectoryEntryPlus> = match parent {
        INODE_ROOT => {
            vec![
                DirectoryEntryPlus {
                    inode: INODE_ROOT,
                    generation: 0,
                    kind: FileType::Directory,
                    name: ".".into(),
                    offset: 1,
                    attr: fs.dir_attr(INODE_ROOT),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
                DirectoryEntryPlus {
                    inode: INODE_ROOT,
                    generation: 0,
                    kind: FileType::Directory,
                    name: "..".into(),
                    offset: 2,
                    attr: fs.dir_attr(INODE_ROOT),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
                DirectoryEntryPlus {
                    inode: INODE_ALL,
                    generation: 0,
                    kind: FileType::Directory,
                    name: ALL_DIR.into(),
                    offset: 3,
                    attr: fs.dir_attr(INODE_ALL),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
            ]
        }
        INODE_ALL => {
            let keys = fs.get_sorted_keys();
            let mut entries = vec![
                DirectoryEntryPlus {
                    inode: INODE_ALL,
                    generation: 0,
                    kind: FileType::Directory,
                    name: ".".into(),
                    offset: 1,
                    attr: fs.dir_attr(INODE_ALL),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
                DirectoryEntryPlus {
                    inode: INODE_ROOT,
                    generation: 0,
                    kind: FileType::Directory,
                    name: "..".into(),
                    offset: 2,
                    attr: fs.dir_attr(INODE_ROOT),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
            ];
            let start_idx = (offset.max(2) - 2) as usize;
            for (i, (key, unique_name)) in keys.iter().skip(start_idx).take(1000).enumerate() {
                let index = start_idx + i;
                if let Some(mt) = fs.torrents.get(key) {
                    let inode = fs.get_or_assign_torrent_inode(key);
                    let mtime = RdFs::torrent_mtime(&mt);
                    entries.push(DirectoryEntryPlus {
                        inode,
                        generation: 0,
                        kind: FileType::Directory,
                        name: unique_name.clone(),
                        offset: (index + 3) as i64,
                        attr: {
                            let mut a = fs.dir_attr(inode);
                            a.mtime = mtime;
                            a.atime = mtime;
                            a.ctime = mtime;
                            a
                        },
                        entry_ttl: fs.entry_ttl,
                        attr_ttl: fs.attr_ttl,
                    });
                }
            }
            entries
        }
        i if (INODE_TORRENT_BASE..INODE_FILE_BASE).contains(&i) => {
            let Some(access_key) = fs.inode_to_access_key(i) else {
                return Err(fuse3::Errno::from(libc::ENOENT));
            };
            let mt = fs
                .ensure_torrent_info(&access_key)
                .await
                .ok_or(fuse3::Errno::from(libc::ENOENT))?;
            let mtime = RdFs::torrent_mtime(&mt);
            let slot = i - INODE_TORRENT_BASE;
            let mut entries = vec![
                DirectoryEntryPlus {
                    inode: i,
                    generation: 0,
                    kind: FileType::Directory,
                    name: ".".into(),
                    offset: 1,
                    attr: {
                        let mut a = fs.dir_attr(i);
                        a.mtime = mtime;
                        a.atime = mtime;
                        a.ctime = mtime;
                        a
                    },
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
                DirectoryEntryPlus {
                    inode: INODE_ALL,
                    generation: 0,
                    kind: FileType::Directory,
                    name: "..".into(),
                    offset: 2,
                    attr: fs.dir_attr(INODE_ALL),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                },
            ];
            for (fi, file) in mt.selected_files().into_iter().enumerate() {
                let fname = RdFs::sanitize_dirent_name(file.path.trim_start_matches('/'));
                entries.push(DirectoryEntryPlus {
                    inode: RdFs::file_inode(slot, fi as u64),
                    generation: 0,
                    kind: FileType::RegularFile,
                    name: fname,
                    offset: (fi + 3) as i64,
                    attr: fs.file_attr(
                        RdFs::file_inode(slot, fi as u64),
                        file.bytes.max(0) as u64,
                        mtime,
                    ),
                    entry_ttl: fs.entry_ttl,
                    attr_ttl: fs.attr_ttl,
                });
            }
            entries
        }
        _ => return Err(fuse3::Errno::from(libc::ENOENT)),
    };

    let tail: Vec<_> = entries
        .into_iter()
        .filter(|e| e.offset > offset as i64)
        .collect();
    Ok(ReplyDirectoryPlus {
        entries: stream::iter(
            tail.into_iter()
                .map(Ok as fn(DirectoryEntryPlus) -> FuseResult<DirectoryEntryPlus>),
        ),
    })
}
