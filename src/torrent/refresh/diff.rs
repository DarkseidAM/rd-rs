//! Diff between current in-memory state and fresh RD list.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db::TorrentRow;
use crate::rd::types::Torrent;
use crate::torrent::{ManagedTorrent, access_key};

#[derive(Debug)]
pub struct DiffResult {
    pub added: Vec<(Torrent, Vec<String>)>,
    pub removed_keys: Vec<String>,
    pub changed: Vec<(Torrent, Vec<String>)>,
    pub duplicates: usize,
}

/// Returns the calculated difference between current local state and fresh RD state.
pub fn diff(
    current: &dashmap::DashMap<String, Arc<ManagedTorrent>>,
    fresh: &[Torrent],
) -> DiffResult {
    let mut fresh_map: HashMap<String, (Torrent, Vec<String>)> = HashMap::new();
    for t in fresh {
        let key = access_key(&t.hash, &t.name);
        fresh_map
            .entry(key)
            .and_modify(|(existing_t, ids)| {
                if !ids.contains(&t.id) {
                    ids.push(t.id.clone());
                }
                if t.progress > existing_t.progress {
                    *existing_t = t.clone();
                }
            })
            .or_insert_with(|| (t.clone(), vec![t.id.clone()]));
    }

    let current_keys: HashSet<String> = current.iter().map(|e| e.key().clone()).collect();
    let fresh_keys: HashSet<String> = fresh_map.keys().cloned().collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for key in fresh_keys.difference(&current_keys) {
        if let Some(val) = fresh_map.get(key) {
            added.push(val.clone());
        }
    }

    for key in current_keys.difference(&fresh_keys) {
        removed.push(key.clone());
    }

    for key in fresh_keys.intersection(&current_keys) {
        let (fresh_t, fresh_ids) = fresh_map.get(key).unwrap();
        let current_mt = current.get(key).unwrap();

        let status_changed = fresh_t.status != current_mt.torrent.status;
        let progress_changed = fresh_t.progress != current_mt.torrent.progress;
        let ids_changed = current_mt.rd_ids.len() != fresh_ids.len()
            || fresh_ids.iter().any(|id| !current_mt.rd_ids.contains(id));

        if status_changed || progress_changed || ids_changed {
            changed.push((fresh_t.clone(), fresh_ids.clone()));
        }
    }

    let duplicates = fresh.len() - fresh_map.len();

    DiffResult {
        added,
        removed_keys: removed,
        changed,
        duplicates,
    }
}

pub(super) fn torrent_to_row(mt: &ManagedTorrent) -> TorrentRow {
    let now = chrono::Utc::now().timestamp();
    TorrentRow {
        access_key: mt.access_key.clone(),
        rd_ids: mt.rd_ids.clone(),
        hash: mt.torrent.hash.clone(),
        name: mt.torrent.name.clone(),
        state: mt.state.clone(),
        unrepairable_reason: mt.unrepairable_reason.clone(),
        file_states: None,
        last_seen_at: Some(now),
        last_repaired_at: None,
    }
}
