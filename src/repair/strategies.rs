//! The 4-strategy cascade for repairing completely broken torrents.

use std::sync::Arc;

use tracing::{error, info, warn};

use super::detect::{duplicate_selected_file_ids, has_non_playable_selected, path_looks_playable};
use super::ephemeral_torrent::{EphemeralRdTorrent, info_ready};
use super::reasons;
use super::{CascadeOutcome, UnrepairableReason};
use crate::config::RepairConfig;
use crate::rd::RealDebrid;
use crate::rd::client::RdError;
use crate::torrent::ManagedTorrent;

fn finish_failure(torrent: &ManagedTorrent) -> CascadeOutcome {
    if torrent.selected_files().len() <= 1 {
        CascadeOutcome::Unrepairable(UnrepairableReason::LoneBroken)
    } else {
        CascadeOutcome::Unrepairable(UnrepairableReason::InvalidFileIDs)
    }
}

fn map_rd(e: RdError) -> CascadeOutcome {
    if e.is_bandwidth_limited() {
        CascadeOutcome::DeferBandwidth
    } else {
        let s = e.to_string();
        if let Some(r) = reasons::from_rd_error_message(&s) {
            CascadeOutcome::UnrepairableMsg(r.to_string())
        } else {
            CascadeOutcome::Unrepairable(UnrepairableReason::InvalidFileIDs)
        }
    }
}

/// Delete known `rd_ids`, add magnet, select files, wait for RD.  
/// `None` = Strategy 1 incomplete (caller may try Strategy 2+).  
/// `Some` = terminal outcome (success, unrepairable, defer, …).
async fn try_strategy_1_reinsert(
    rd: &Arc<RealDebrid>,
    torrent: &ManagedTorrent,
    files_to_select: &str,
    restrict_cached: bool,
) -> Option<CascadeOutcome> {
    info!("Strategy 1: ReinsertTorrent for {}", torrent.access_key);

    for rd_id in &torrent.rd_ids {
        if let Err(e) = rd.delete_torrent(rd_id).await {
            warn!("delete_torrent {} during repair: {}", rd_id, e);
        }
    }

    let magnet_resp = match rd.add_magnet(&torrent.torrent.hash).await {
        Ok(m) => m,
        Err(e) => {
            error!("add_magnet during repair: {e}");
            return Some(map_rd(e));
        }
    };

    let mut guard = EphemeralRdTorrent::new(Arc::clone(rd), magnet_resp.id.clone());

    if let Err(e) = rd.select_torrent_files(guard.id(), files_to_select).await {
        error!("select_torrent_files during repair: {e}");
        return Some(map_rd(e));
    }

    match info_ready(rd, guard.id(), restrict_cached).await {
        Ok(out) if out.is_ready => {
            info!("Strategy 1 succeeded for {}", torrent.access_key);
            let id = guard.id().to_string();
            guard.dismiss();
            Some(CascadeOutcome::Success {
                new_rd_ids: Some(vec![id]),
            })
        }
        Ok(out) if out.deleted_by_info_ready => {
            guard.dismiss();
            Some(CascadeOutcome::Unrepairable(UnrepairableReason::NotCached))
        }
        Err(e) => Some(map_rd(e)),
        Ok(_) => None,
    }
}

/// Executes the core repair strategies in order.
pub async fn execute_cascade(
    rd: &Arc<RealDebrid>,
    torrent: &ManagedTorrent,
    repair_cfg: &RepairConfig,
) -> CascadeOutcome {
    let restrict_cached = repair_cfg.restrict_to_cached;
    let batch_size = repair_cfg.batch_file_group_size.clamp(1, 32) as usize;

    let Some(info) = torrent.info.as_ref() else {
        if torrent.torrent.hash.is_empty() {
            warn!(
                key = %torrent.access_key,
                "repair cascade: no TorrentInfo and empty hash"
            );
            return CascadeOutcome::UnrepairableMsg(reasons::MISSING_TORRENT_DETAIL.to_string());
        }
        warn!(
            key = %torrent.access_key,
            "repair cascade: no TorrentInfo (e.g. stale rd id) — reinsert by hash, select all files"
        );
        if let Some(out) = try_strategy_1_reinsert(rd, torrent, "all", restrict_cached).await {
            return out;
        }
        return finish_failure(torrent);
    };

    if duplicate_selected_file_ids(info) {
        return CascadeOutcome::Unrepairable(UnrepairableReason::DuplicateFileIDs);
    }

    let selected: Vec<_> = torrent.selected_files();
    if selected.is_empty() {
        return CascadeOutcome::Unrepairable(UnrepairableReason::NoRepairableFiles);
    }
    if selected
        .iter()
        .all(|f| !path_looks_playable(f.path.as_str()))
    {
        return CascadeOutcome::Unrepairable(UnrepairableReason::NoRepairableFiles);
    }

    let selected_ids = selected
        .iter()
        .map(|f| f.id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let files_to_select = if selected_ids.is_empty() {
        "all".to_string()
    } else {
        selected_ids
    };

    if let Some(out) =
        try_strategy_1_reinsert(rd, torrent, files_to_select.as_str(), restrict_cached).await
    {
        return out;
    }

    warn!(
        "Strategy 1 incomplete for {}; trying individual files",
        torrent.access_key
    );

    // Strategy 2: IndividualFiles
    let mut all_fixed = true;
    for file in &selected {
        let file_id_str = file.id.to_string();
        match rd.add_magnet(&torrent.torrent.hash).await {
            Ok(m) => {
                let mut guard = EphemeralRdTorrent::new(Arc::clone(rd), m.id.clone());
                if let Err(e) = rd.select_torrent_files(guard.id(), &file_id_str).await {
                    error!("individual select: {e}");
                    if e.is_bandwidth_limited() {
                        return CascadeOutcome::DeferBandwidth;
                    }
                    all_fixed = false;
                    continue;
                }
                match info_ready(rd, guard.id(), restrict_cached).await {
                    Ok(out) if out.is_ready => {
                        info!("Strategy 2: file {} looks cached", file.path);
                    }
                    Ok(out) if out.deleted_by_info_ready => {
                        guard.dismiss();
                        return CascadeOutcome::Unrepairable(UnrepairableReason::NotCached);
                    }
                    Err(e) => {
                        if e.is_bandwidth_limited() {
                            return CascadeOutcome::DeferBandwidth;
                        }
                        all_fixed = false;
                    }
                    Ok(_) => {
                        all_fixed = false;
                    }
                }
            }
            Err(e) => {
                if e.is_bandwidth_limited() {
                    return CascadeOutcome::DeferBandwidth;
                }
                all_fixed = false;
            }
        }
    }

    if all_fixed {
        info!("Strategy 2 succeeded for {}", torrent.access_key);
        return CascadeOutcome::Success { new_rd_ids: None };
    }

    // Strategy 3: ArchiveAll (only when non-playable files are in the selection)
    if has_non_playable_selected(info) {
        info!("Strategy 3: ArchiveAll for {}", torrent.access_key);
        match rd.add_magnet(&torrent.torrent.hash).await {
            Ok(m) => {
                let mut guard = EphemeralRdTorrent::new(Arc::clone(rd), m.id.clone());
                if let Err(e) = rd.select_torrent_files(guard.id(), "all").await {
                    error!("archive select: {e}");
                    if e.is_bandwidth_limited() {
                        return CascadeOutcome::DeferBandwidth;
                    }
                } else {
                    match info_ready(rd, guard.id(), restrict_cached).await {
                        Ok(out) if out.is_ready => {
                            info!("Strategy 3 succeeded for {}", torrent.access_key);
                            let id = guard.id().to_string();
                            guard.dismiss();
                            return CascadeOutcome::Success {
                                new_rd_ids: Some(vec![id]),
                            };
                        }
                        Ok(out) if out.deleted_by_info_ready => {
                            guard.dismiss();
                            return CascadeOutcome::Unrepairable(UnrepairableReason::NotCached);
                        }
                        Err(e) => {
                            if e.is_bandwidth_limited() {
                                return CascadeOutcome::DeferBandwidth;
                            }
                        }
                        Ok(_) => {}
                    }
                }
            }
            Err(e) => {
                if e.is_bandwidth_limited() {
                    return CascadeOutcome::DeferBandwidth;
                }
            }
        }
    } else {
        info!(
            "Skipping Strategy 3 (archive all): only playable files selected for {}",
            torrent.access_key
        );
    }

    // Strategy 4: BatchDownload (only when multiple selected files — single-file packs have no batch path)
    let all_selected: Vec<_> = selected.iter().map(|f| f.id).collect();
    if all_selected.len() > 1 {
        info!("Strategy 4: BatchDownload for {}", torrent.access_key);
        let chunks: Vec<_> = all_selected.chunks(batch_size).collect();
        let n_chunks = chunks.len();
        let mut batches_ok = 0usize;

        for chunk in chunks {
            match rd.add_magnet(&torrent.torrent.hash).await {
                Ok(m) => {
                    let mut guard = EphemeralRdTorrent::new(Arc::clone(rd), m.id.clone());
                    let chunk_str = chunk
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    if let Err(e) = rd.select_torrent_files(guard.id(), &chunk_str).await {
                        if e.is_bandwidth_limited() {
                            return CascadeOutcome::DeferBandwidth;
                        }
                        continue;
                    }
                    match info_ready(rd, guard.id(), restrict_cached).await {
                        Ok(out) if out.is_ready => batches_ok += 1,
                        Ok(out) if out.deleted_by_info_ready => {
                            guard.dismiss();
                            return CascadeOutcome::Unrepairable(UnrepairableReason::NotCached);
                        }
                        Err(e) if e.is_bandwidth_limited() => {
                            return CascadeOutcome::DeferBandwidth;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    if e.is_bandwidth_limited() {
                        return CascadeOutcome::DeferBandwidth;
                    }
                }
            }
        }

        if batches_ok == n_chunks {
            info!("Strategy 4 succeeded for {}", torrent.access_key);
            return CascadeOutcome::Success { new_rd_ids: None };
        }
    } else {
        warn!(
            key = %torrent.access_key,
            n = all_selected.len(),
            "Strategy 4 skipped (batch path requires 2+ selected files); cascade exhausted → lone_broken if single selection"
        );
    }

    finish_failure(torrent)
}
