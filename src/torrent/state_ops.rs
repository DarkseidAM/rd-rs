//! TorrentManager persistence: state updates, `file_states`, snapshots.
//!
//! Only this module and the refresh merge path should persist `file_states` / `TorrentRow` for
//! health metadata; repair preflight updates through [`TorrentManager::persist_torrent_snapshot`].

use std::sync::Arc;

use crate::db::TorrentState;

use super::{ManagedTorrent, TorrentManager};

impl TorrentManager {
    /// After reinsert/archive repair: replace `rd_ids`, clear stale detail & file_states, sync list `torrent.id`, persist.
    pub async fn replace_rd_ids_after_repair(
        &self,
        access_key: &str,
        rd_ids: Vec<String>,
    ) -> anyhow::Result<()> {
        let existing = self
            .torrents
            .get(access_key)
            .map(|r| r.value().as_ref().clone());
        let Some(mut m) = existing else {
            return Ok(());
        };
        m.rd_ids = rd_ids;
        m.info = None;
        m.file_states = None;
        if let Some(id) = m.rd_ids.first() {
            m.torrent.id.clone_from(id);
        }
        self.persist_torrent_snapshot(&m).await
    }

    /// Persist metadata (`file_states`, `info`, …) without changing [`TorrentState`].
    pub async fn persist_torrent_snapshot(&self, mt: &ManagedTorrent) -> anyhow::Result<()> {
        let updated = Arc::new(mt.clone());
        self.torrents.insert(mt.access_key.clone(), updated.clone());
        let row = crate::torrent::refresh::diff::torrent_to_row(&updated);
        let db = self.db.clone();
        db.call(move |conn| -> rusqlite::Result<()> {
            crate::db::Db::upsert_torrents_batch_conn(conn, &[row]).map_err(|e| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    e.to_string(),
                )))
            })?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("DB snapshot error: {}", e))?;
        Ok(())
    }

    /// Mark a single file path broken (zurg `broken_file`); persists (hook runs via `update_torrent_state` when paired).
    pub async fn mark_file_broken(&self, access_key: &str, file_path: &str) -> anyhow::Result<()> {
        let existing_mt = self
            .torrents
            .get(access_key)
            .map(|r| r.value().as_ref().clone());
        let Some(existing_mt) = existing_mt else {
            return Ok(());
        };
        let mut fs = existing_mt.file_states.clone().unwrap_or_default();
        fs.insert(file_path.to_string(), "broken".to_string());
        let updated = Arc::new(ManagedTorrent {
            file_states: Some(fs),
            ..existing_mt
        });
        self.torrents
            .insert(access_key.to_string(), updated.clone());
        let row = crate::torrent::refresh::diff::torrent_to_row(&updated);
        let db = self.db.clone();
        db.call(move |conn| -> rusqlite::Result<()> {
            crate::db::Db::upsert_torrents_batch_conn(conn, &[row]).map_err(|e| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    e.to_string(),
                )))
            })?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("DB mark_file_broken: {}", e))?;
        Ok(())
    }

    /// Update a torrent's state, persist to DB, and trigger webhook.
    pub async fn update_torrent_state(
        &self,
        access_key: &str,
        new_state: TorrentState,
        unrepairable_reason: Option<String>,
    ) -> anyhow::Result<()> {
        let existing = self.torrents.get(access_key).map(|r| r.value().clone());

        if let Some(existing_mt) = existing {
            let last_repaired_at = if new_state == TorrentState::Ok {
                Some(chrono::Utc::now().timestamp())
            } else {
                existing_mt.last_repaired_at
            };
            let under_repair_started_at = if new_state == TorrentState::UnderRepair {
                if existing_mt.state == TorrentState::UnderRepair {
                    existing_mt.under_repair_started_at
                } else {
                    Some(chrono::Utc::now().timestamp())
                }
            } else {
                None
            };
            let updated = Arc::new(ManagedTorrent {
                access_key: existing_mt.access_key.clone(),
                rd_ids: existing_mt.rd_ids.clone(),
                torrent: existing_mt.torrent.clone(),
                info: existing_mt.info.clone(),
                state: new_state.clone(),
                unrepairable_reason: unrepairable_reason.clone(),
                last_repaired_at,
                file_states: existing_mt.file_states.clone(),
                under_repair_started_at,
            });

            self.torrents
                .insert(access_key.to_string(), updated.clone());

            let row = crate::torrent::refresh::diff::torrent_to_row(&updated);

            let db = self.db.clone();
            db.call(move |conn| -> rusqlite::Result<()> {
                crate::db::Db::upsert_torrents_batch_conn(conn, &[row]).map_err(|e| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                        e.to_string(),
                    )))
                })?;
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("DB update error: {}", e))?;

            self.trigger_library_update(self.library_paths_for(&updated));
        }
        Ok(())
    }
}
