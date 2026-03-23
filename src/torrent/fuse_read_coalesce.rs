//! Coalesce parallel FUSE `read()` failures so we do not storm RD + repair for the same file.

use super::TorrentManager;

impl TorrentManager {
    /// `true` if this task should apply fatal-read side effects; `false` if another read is already doing so.
    pub(crate) async fn fuse_begin_fatal_read_repair(
        &self,
        access_key: &str,
        file_path: &str,
    ) -> bool {
        let key = (access_key.to_string(), file_path.to_string());
        let mut g = self.fuse_fatal_read_locks.lock().await;
        g.insert(key)
    }

    pub(crate) async fn fuse_end_fatal_read_repair(&self, access_key: &str, file_path: &str) {
        let key = (access_key.to_string(), file_path.to_string());
        let mut g = self.fuse_fatal_read_locks.lock().await;
        g.remove(&key);
    }
}
