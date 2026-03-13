//! Background refresh loop — polls the RD API every N seconds, diffs the
//! result against the in-memory DashMap, and persists changes to SQLite.
//!
//! Design mirrors Go's `TorrentManager.refresh()` in `internal/torrent/manager.go`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use super::{ManagedTorrent, TorrentManager, access_key};
use crate::db::{TorrentRow, TorrentState};
use crate::rd::types::Torrent;

/// How long to wait between refresh cycles.
/// TODO: make this a `[api]` config field.
const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// Entry point spawned by `TorrentManager::start()`.
pub async fn run_refresh_loop(mgr: Arc<TorrentManager>) {
    tracing::info!("Refresh loop: starting (interval={:?})", REFRESH_INTERVAL);

    let mut last_state: Option<LibraryState> = None;

    loop {
        tokio::select! {
            _ = sleep(REFRESH_INTERVAL) => {}
            _ = mgr.shutdown.cancelled() => {
                tracing::info!("Refresh loop: shutting down");
                return;
            }
        }

        match get_current_state(&mgr).await {
            Ok(current_state) => {
                if Some(&current_state) == last_state.as_ref() {
                    tracing::debug!("Refresh skipped: LibraryState unchanged");
                    continue;
                }

                tracing::info!(
                    "Refresh triggered: LibraryState changed (total: {}, active: {}, first: {:?})",
                    current_state.total_count,
                    current_state.active_count,
                    current_state.first_torrent_id
                );

                if let Err(e) = run_once(&mgr).await {
                    tracing::warn!("Refresh loop: error (will retry): {e:#}");
                } else {
                    last_state = Some(current_state);
                }
            }
            Err(e) => {
                tracing::warn!("Refresh loop: failed to get library state: {e:#}");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LibraryState {
    total_count: u32,
    active_count: usize,
    first_torrent_id: Option<String>,
}

async fn get_current_state(mgr: &TorrentManager) -> anyhow::Result<LibraryState> {
    // Fetch just 1 item from page 1 to get the total count and the first ID
    let (first_page, total_count) = mgr.rd.list_torrents(1, 1).await?;
    let active_count_resp = mgr.rd.get_active_count().await?;

    Ok(LibraryState {
        total_count,
        active_count: active_count_resp.downloading_count,
        first_torrent_id: first_page.first().map(|t| t.id.clone()),
    })
}

/// Run a single refresh cycle: list → diff → upsert.
async fn run_once(mgr: &TorrentManager) -> anyhow::Result<()> {
    let fresh = mgr.rd.list_all_torrents().await?;
    tracing::debug!("Refresh: got {} torrents from RD", fresh.len());

    let (added, removed, changed) = diff(&mgr.torrents, &fresh);

    if added.is_empty() && removed.is_empty() && changed.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Refresh diff: +{} -{} ~{}",
        added.len(),
        removed.len(),
        changed.len()
    );

    // Build rows to upsert
    let mut to_upsert: Vec<TorrentRow> = Vec::new();
    let mut changed_paths: Vec<String> = Vec::new();

    // ── Additions ──────────────────────────────────────────────────────────
    for torrent in &added {
        let key = access_key(&torrent.hash, &torrent.name);
        let mt = Arc::new(ManagedTorrent {
            access_key: key.clone(),
            rd_ids: vec![torrent.id.clone()],
            torrent: torrent.clone(),
            info: None,
            state: TorrentState::Ok,
            unrepairable_reason: None,
        });
        changed_paths.push(format!("__all__/{}", key));
        mgr.torrents.insert(key.clone(), mt.clone());
        to_upsert.push(torrent_to_row(&mt));
    }

    // ── Changes ────────────────────────────────────────────────────────────
    for torrent in &changed {
        let key = access_key(&torrent.hash, &torrent.name);

        // Extract and drop to avoid holding a DashMap Ref during insert (deadlock)
        let existing_val = mgr.torrents.get(&key).map(|r| r.value().clone());

        if let Some(existing) = existing_val {
            let updated = Arc::new(ManagedTorrent {
                access_key: key.clone(),
                rd_ids: merge_ids(&existing.rd_ids, &torrent.id),
                torrent: torrent.clone(),
                info: existing.info.clone(),
                state: existing.state.clone(),
                unrepairable_reason: existing.unrepairable_reason.clone(),
            });
            changed_paths.push(format!("__all__/{}", key));
            mgr.torrents.insert(key.clone(), updated.clone());
            to_upsert.push(torrent_to_row(&updated));
        }
    }

    // ── Removals ───────────────────────────────────────────────────────────
    for key in &removed {
        changed_paths.push(format!("__all__/{}", key));
        mgr.torrents.remove(key);
        // Keep SQLite row (for history / repair audit trail)
    }

    // Batch-persist to SQLite
    if !to_upsert.is_empty() {
        let rows = to_upsert;
        mgr.db
            .call(move |conn| -> rusqlite::Result<()> {
                crate::db::Db::upsert_torrents_batch_conn(conn, &rows).map_err(|e| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                        e.to_string(),
                    )))
                })?;
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("DB sync error: {}", e))?;
    }

    // Trigger on_library_update hook
    mgr.trigger_library_update(changed_paths);

    Ok(())
}

// ─── Diff helpers ─────────────────────────────────────────────────────────────

/// Returns `(added, removed_keys, changed)`.
fn diff(
    current: &dashmap::DashMap<String, Arc<ManagedTorrent>>,
    fresh: &[Torrent],
) -> (Vec<Torrent>, Vec<String>, Vec<Torrent>) {
    // Build a lookup of fresh torrents by access_key
    let fresh_map: HashMap<String, &Torrent> = fresh
        .iter()
        .map(|t| (access_key(&t.hash, &t.name), t))
        .collect();

    let current_keys: HashSet<String> = current.iter().map(|e| e.key().clone()).collect();
    let fresh_keys: HashSet<String> = fresh_map.keys().cloned().collect();

    let added_keys = fresh_keys.difference(&current_keys);
    let removed_keys: Vec<String> = current_keys.difference(&fresh_keys).cloned().collect();

    let added: Vec<Torrent> = added_keys
        .filter_map(|k| fresh_map.get(k).map(|t| (*t).clone()))
        .collect();

    // Changed = same key, but status or progress differs
    let changed: Vec<Torrent> = fresh_keys
        .intersection(&current_keys)
        .filter_map(|k| {
            let fresh_t = fresh_map.get(k)?;
            let current_t = current.get(k)?;
            if fresh_t.status != current_t.torrent.status
                || fresh_t.progress != current_t.torrent.progress
            {
                Some((*fresh_t).clone())
            } else {
                None
            }
        })
        .collect();

    (added, removed_keys, changed)
}

/// Merge a new ID into an existing ID list (dedup).
fn merge_ids(existing: &[String], new_id: &str) -> Vec<String> {
    let mut ids = existing.to_vec();
    if !ids.contains(&new_id.to_string()) {
        ids.push(new_id.to_string());
    }
    ids
}

// ─── Conversion helpers ───────────────────────────────────────────────────────

fn torrent_to_row(mt: &ManagedTorrent) -> TorrentRow {
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
