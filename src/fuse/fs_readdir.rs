//! readdir and readdirplus FUSE operations.

use fuse3::Result as FuseResult;
use fuse3::raw::prelude::*;
use futures_util::stream::{self};

use crate::fuse::consts::{ALL_DIR, INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE};
use crate::fuse::fs::RdFs;

pub(super) async fn readdir<'a>(
    fs: &'a RdFs,
    _req: Request,
    inode: u64,
    _fh: u64,
    offset: i64,
) -> FuseResult<
    ReplyDirectory<
        impl futures_util::stream::Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a,
    >,
> {
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
            let keys = fs.get_sorted_keys();
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
            let start_idx = (offset.max(2) - 2) as usize;
            for (i, (key, unique_name)) in keys.iter().skip(start_idx).take(1000).enumerate() {
                let index = start_idx + i;
                if fs.torrents.get(key).is_some() {
                    let inode = fs.get_or_assign_torrent_inode(key);
                    entries.push(DirectoryEntry {
                        inode,
                        kind: FileType::Directory,
                        name: unique_name.clone(),
                        offset: (index + 3) as i64,
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
            let slot = i - INODE_TORRENT_BASE;
            for (fi, file) in mt.selected_files().into_iter().enumerate() {
                let fname = RdFs::sanitize_dirent_name(file.path.trim_start_matches('/'));
                entries.push(DirectoryEntry {
                    inode: RdFs::file_inode(slot, fi as u64),
                    kind: FileType::RegularFile,
                    name: fname,
                    offset: (fi + 3) as i64,
                });
            }
            entries
        }

        _ => return Err(fuse3::Errno::from(libc::ENOENT)),
    };

    let tail: Vec<_> = entries.into_iter().filter(|e| e.offset > offset).collect();

    Ok(ReplyDirectory {
        entries: stream::iter(
            tail.into_iter()
                .map(Ok as fn(DirectoryEntry) -> FuseResult<DirectoryEntry>),
        ),
    })
}
