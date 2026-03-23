//! Priority repair queue: enqueue one key, all keys, or front-of-queue “force” repair.

use crate::db::TorrentState;

use super::TorrentManager;

/// Options for [`TorrentManager::enqueue_repair_all`].
#[derive(Debug, Clone, Copy, Default)]
pub struct EnqueueRepairAllOptions {
    /// Clear `unrepairable_reason` (back to broken with no reason) before enqueueing.
    pub clear_unrepairable: bool,
}

impl TorrentManager {
    /// Enqueue every known torrent (access key), optionally clearing unrepairable reasons first.
    ///
    /// Dedupes against the current queue tail. Wakes the repair engine once.
    pub async fn enqueue_repair_all(&self, opts: EnqueueRepairAllOptions) {
        let keys: Vec<String> = self.torrents.iter().map(|e| e.key().clone()).collect();
        if opts.clear_unrepairable {
            for key in &keys {
                if self
                    .torrents
                    .get(key)
                    .is_some_and(|m| m.unrepairable_reason.is_some())
                {
                    let _ = self
                        .update_torrent_state(key, TorrentState::Broken, None)
                        .await;
                }
            }
        }
        let mut q = self.repair_queue.lock().await;
        for key in keys {
            if !q.iter().any(|k| k == &key) {
                q.push_back(key);
            }
        }
        drop(q);
        self.repair_notify.notify_one();
    }

    /// Like [`Self::enqueue_repair`] but moves the key to the front and optionally clears unrepairable.
    pub async fn enqueue_repair_front(&self, access_key: &str, clear_unrepairable: bool) {
        if clear_unrepairable {
            let _ = self
                .update_torrent_state(access_key, TorrentState::Broken, None)
                .await;
        }
        let mut q = self.repair_queue.lock().await;
        let owned = access_key.to_string();
        if let Some(pos) = q.iter().position(|k| k == &owned) {
            q.remove(pos);
        }
        q.push_front(owned);
        drop(q);
        self.repair_notify.notify_one();
    }

    /// Number of access keys waiting in the priority repair queue.
    pub async fn repair_pending_count(&self) -> usize {
        self.repair_queue.lock().await.len()
    }

    /// Next key to be repaired (front of queue), if any.
    pub async fn repair_peek_front(&self) -> Option<String> {
        self.repair_queue.lock().await.front().cloned()
    }
}
