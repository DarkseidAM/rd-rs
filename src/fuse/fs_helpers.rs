//! RdFs helper methods (inode, attrs, sorted keys, ensure_torrent_info).

use std::ffi::OsString;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, UNIX_EPOCH};

use fuse3::Timestamp;
use fuse3::raw::prelude::*;

use crate::fuse::consts::INODE_FILE_BASE;
use crate::fuse::consts::INODE_TORRENT_BASE;
use crate::torrent::ManagedTorrent;

use super::fs::RdFs;

impl RdFs {
    pub(crate) fn get_sorted_keys(&self) -> Arc<Vec<(String, OsString)>> {
        let now = std::time::Instant::now();
        if let Ok(cache) = self.cached_all_dir.read()
            && now.duration_since(cache.0) < Duration::from_secs(15)
        {
            return cache.1.clone();
        }

        let mut key_names: Vec<(String, String)> = self
            .torrents
            .iter()
            .map(|e| (e.key().to_string(), e.value().name().to_string()))
            .collect();
        key_names.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let mut unique_entries = Vec::with_capacity(key_names.len());
        let mut prev_name = String::new();
        let mut dup_count = 0;

        for (key, name) in key_names {
            let display_name = if name == prev_name {
                dup_count += 1;
                format!("{} ({})", name, dup_count)
            } else {
                prev_name = name.clone();
                dup_count = 0;
                name
            };
            unique_entries.push((key, Self::sanitize_dirent_name(&display_name)));
        }

        let res = Arc::new(unique_entries);
        if let Ok(mut cache) = self.cached_all_dir.write() {
            *cache = (now, res.clone());
        }
        res
    }

    pub(crate) fn get_or_assign_torrent_inode(&self, key: &str) -> u64 {
        let key = key.to_string();
        *self.key_to_inode.entry(key.clone()).or_insert_with(|| {
            let inode = self.next_torrent_inode.fetch_add(1, Ordering::Relaxed);
            self.inode_to_key.insert(inode, key);
            inode
        })
    }

    pub(crate) fn inode_to_access_key(&self, inode: u64) -> Option<String> {
        self.inode_to_key.get(&inode).map(|r| r.clone())
    }

    pub(crate) fn sanitize_dirent_name(name: &str) -> OsString {
        let mut clean = name.replace(['\0', '/'], "_");
        if clean.len() > 255 {
            clean.truncate(255);
            while !clean.is_char_boundary(clean.len()) {
                clean.pop();
            }
        }
        OsString::from(clean)
    }

    pub(crate) fn dir_attr(&self, inode: u64) -> FileAttr {
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

    pub(crate) fn file_attr(&self, inode: u64, size: u64, mtime: Timestamp) -> FileAttr {
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

    pub(crate) fn torrent_mtime(mt: &ManagedTorrent) -> Timestamp {
        (UNIX_EPOCH + Duration::from_secs(mt.torrent.added.timestamp().max(0) as u64)).into()
    }

    pub fn torrent_inode(index: u64) -> u64 {
        INODE_TORRENT_BASE + index
    }

    pub fn file_inode(torrent_index: u64, file_index: u64) -> u64 {
        INODE_FILE_BASE + torrent_index * 10_000 + file_index
    }

    pub(crate) async fn ensure_torrent_info(
        &self,
        access_key: &str,
    ) -> Option<Arc<ManagedTorrent>> {
        self.torrent_manager.ensure_torrent_info(access_key).await
    }
}
