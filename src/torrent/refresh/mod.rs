//! Background refresh loop — polls RD API, diffs, persists to SQLite.

pub mod coordination;

use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::db::{TorrentRow, TorrentState};
use crate::repair::detect::is_stalled_download_from_list;
use crate::torrent::{ManagedTorrent, TorrentManager, access_key};

use coordination::{rd_id_belongs_to_under_repair, skip_local_remove_for_state};

pub mod diff;
mod loops;

pub use diff::{DiffResult, diff};
pub use loops::{run_downloads_check_loop, run_premium_check_loop};

#[derive(Debug, Clone, PartialEq, Eq)]
struct LibraryState {
    total_count: u32,
    active_count: usize,
    first_torrent_id: Option<String>,
}

async fn get_current_state(mgr: &TorrentManager) -> anyhow::Result<LibraryState> {
    let (first_page, total_count) = mgr.rd.list_torrents(1, 1).await?;
    let active_count_resp = mgr.rd.get_active_count().await?;

    Ok(LibraryState {
        total_count,
        active_count: active_count_resp.downloading_count,
        first_torrent_id: first_page.first().map(|t| t.id.clone()),
    })
}

/// Entry point spawned by `TorrentManager::start()`.
pub async fn run_refresh_loop(mgr: Arc<TorrentManager>) {
    let interval_secs = mgr.config.load().api.refresh_interval_secs.max(1);
    tracing::info!(
        "Refresh loop: starting (interval={}s, hot-reload applies)",
        interval_secs
    );

    let mut last_state: Option<LibraryState> = None;

    loop {
        let interval = Duration::from_secs(mgr.config.load().api.refresh_interval_secs.max(1));
        tokio::select! {
            _ = sleep(interval) => {}
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

async fn run_once(mgr: &TorrentManager) -> anyhow::Result<()> {
    let fresh_all = mgr.rd.list_all_torrents().await?;
    tracing::debug!("Refresh: got {} total torrents from RD", fresh_all.len());

    let mut fresh = Vec::with_capacity(fresh_all.len());
    let mut errored_ids = Vec::new();

    for t in fresh_all {
        if matches!(
            t.status.as_str(),
            "error" | "magnet_error" | "virus" | "dead"
        ) {
            errored_ids.push((t.id.clone(), t.name.clone()));
        } else {
            fresh.push(t);
        }
    }

    if !errored_ids.is_empty() {
        tracing::info!(
            "Refresh: found {} errored/invalid torrents, skipping them",
            errored_ids.len()
        );
        if mgr.config.load().repair.delete_error_torrents {
            for (id, name) in &errored_ids {
                if rd_id_belongs_to_under_repair(mgr, id) {
                    tracing::info!(
                        "Skip delete RD error torrent {} ({}) — id belongs to under_repair",
                        id,
                        name
                    );
                    continue;
                }
                match mgr.rd.delete_torrent(id).await {
                    Ok(()) => tracing::info!("Deleted RD error torrent {} ({})", id, name),
                    Err(e) => tracing::warn!("delete_torrent {} failed: {}", id, e),
                }
            }
        }
        for (id, name) in errored_ids {
            tracing::debug!("Skipping errored torrent {} ({})", id, name);
        }
    }

    let stalled_mins = mgr.config.load().repair.stalled_download_mins.max(1);
    for t in &fresh {
        if t.status != "downloading" {
            continue;
        }
        if !is_stalled_download_from_list(t, stalled_mins) {
            continue;
        }
        if rd_id_belongs_to_under_repair(mgr, &t.id) {
            tracing::info!("Skip stalled delete {} ({}) — under_repair", t.id, t.name);
            continue;
        }
        match mgr.rd.delete_torrent(&t.id).await {
            Ok(()) => tracing::warn!(
                "Deleted stalled downloading torrent {} ({}, {}%)",
                t.id,
                t.name,
                t.progress
            ),
            Err(e) => tracing::warn!("delete_torrent stalled {} failed: {}", t.id, e),
        }
    }

    let DiffResult {
        added,
        removed_keys,
        changed,
        duplicates,
    } = diff(&mgr.torrents, &fresh);

    if added.is_empty() && removed_keys.is_empty() && changed.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Refresh diff: +{} -{} ~{} (duplicates: {})",
        added.len(),
        removed_keys.len(),
        changed.len(),
        duplicates
    );

    let mut to_upsert: Vec<TorrentRow> = Vec::new();
    let mut changed_paths: Vec<String> = Vec::new();

    for (torrent, ids) in &added {
        let key = access_key(&torrent.hash, &torrent.name);
        let mt = Arc::new(ManagedTorrent {
            access_key: key.clone(),
            rd_ids: ids.clone(),
            torrent: torrent.clone(),
            info: None,
            state: TorrentState::Ok,
            unrepairable_reason: None,
            last_repaired_at: None,
            file_states: None,
            under_repair_started_at: None,
        });
        changed_paths.push(format!("__all__/{}", key));
        mgr.torrents.insert(key.clone(), mt.clone());
        to_upsert.push(diff::torrent_to_row(&mt));
    }

    for (torrent, ids) in &changed {
        let key = access_key(&torrent.hash, &torrent.name);

        let existing_val = mgr.torrents.get(&key).map(|r| r.value().clone());

        if let Some(existing) = existing_val {
            let rd_ids_changed = existing.rd_ids.len() != ids.len()
                || ids.iter().any(|id| !existing.rd_ids.contains(id))
                || existing.rd_ids.iter().any(|id| !ids.contains(id));
            // New RD id after re-add/repair: drop stale `TorrentInfo` (links/files differ).
            let info = if rd_ids_changed {
                None
            } else {
                existing.info.clone()
            };
            let updated = Arc::new(ManagedTorrent {
                access_key: key.clone(),
                rd_ids: ids.clone(),
                torrent: torrent.clone(),
                info,
                state: existing.state.clone(),
                unrepairable_reason: existing.unrepairable_reason.clone(),
                last_repaired_at: existing.last_repaired_at,
                file_states: existing.file_states.clone(),
                under_repair_started_at: existing.under_repair_started_at,
            });
            changed_paths.push(format!("__all__/{}", key));
            mgr.torrents.insert(key.clone(), updated.clone());
            to_upsert.push(diff::torrent_to_row(&updated));
        }
    }

    for key in &removed_keys {
        if mgr
            .torrents
            .get(key)
            .is_some_and(|e| skip_local_remove_for_state(e.state.clone()))
        {
            tracing::info!("Refresh: skip remove {} (still under_repair locally)", key);
            continue;
        }
        changed_paths.push(format!("__all__/{}", key));
        mgr.torrents.remove(key);
    }

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

    mgr.trigger_library_update(changed_paths);

    Ok(())
}
