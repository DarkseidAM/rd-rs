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
    let mut fresh_map: HashMap<String, (Torrent, HashSet<String>)> = HashMap::new();
    for t in fresh {
        let key = access_key(&t.hash, &t.name);
        fresh_map
            .entry(key)
            .and_modify(|(existing_t, ids)| {
                ids.insert(t.id.clone());
                if t.progress > existing_t.progress {
                    *existing_t = t.clone();
                }
            })
            .or_insert_with(|| {
                let mut ids = HashSet::new();
                ids.insert(t.id.clone());
                (t.clone(), ids)
            });
    }

    let current_keys: HashSet<String> = current.iter().map(|e| e.key().clone()).collect();
    let fresh_keys: HashSet<String> = fresh_map.keys().cloned().collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for key in fresh_keys.difference(&current_keys) {
        if let Some((t, ids)) = fresh_map.get(key) {
            let mut ids_vec: Vec<_> = ids.iter().cloned().collect();
            ids_vec.sort();
            added.push((t.clone(), ids_vec));
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
            || current_mt.rd_ids.iter().any(|id| !fresh_ids.contains(id));

        if status_changed || progress_changed || ids_changed {
            let mut ids_vec: Vec<_> = fresh_ids.iter().cloned().collect();
            ids_vec.sort();
            changed.push((fresh_t.clone(), ids_vec));
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

pub fn torrent_to_row(mt: &ManagedTorrent) -> TorrentRow {
    let now = chrono::Utc::now().timestamp();
    let file_states = mt
        .file_states
        .as_ref()
        .and_then(|m| serde_json::to_string(m).ok());
    TorrentRow {
        access_key: mt.access_key.clone(),
        rd_ids: mt.rd_ids.clone(),
        hash: mt.torrent.hash.clone(),
        name: mt.torrent.name.clone(),
        state: mt.state.clone(),
        unrepairable_reason: mt.unrepairable_reason.clone(),
        file_states,
        last_seen_at: Some(now),
        last_repaired_at: mt.last_repaired_at,
        under_repair_started_at: mt.under_repair_started_at,
    }
}
