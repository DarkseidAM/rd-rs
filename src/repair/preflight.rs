//! Zurg-style steps before the strategy cascade: assign orphan links, probe selected links, verify-ok shortcut.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::client::RdError;
use crate::rd::types::{File, TorrentInfo};
use crate::torrent::ManagedTorrent;

use super::detect::path_looks_playable;

/// Result of pre-cascade repair work (zurg `repair()` front section).
#[derive(Debug)]
pub enum PreflightOutcome {
    Proceed(ManagedTorrent),
    VerifiedOk(ManagedTorrent),
    DeferBandwidth,
}

fn filename_lower(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().to_lowercase())
        .unwrap_or_default()
}

fn download_matches_file(file: &File, dl: &crate::rd::types::Download) -> bool {
    if file.bytes > 0 && dl.filesize > 0 && file.bytes != dl.filesize {
        return false;
    }
    let fp = filename_lower(&file.path);
    let dn = dl.filename.to_lowercase();
    !fp.is_empty() && !dn.is_empty() && (fp.ends_with(&dn) || dn.ends_with(&fp) || fp == dn)
}

/// RD `links` entries past the selected-file count (zurg unassigned pool).
pub fn orphan_rd_links(info: &TorrentInfo) -> Vec<String> {
    let n = info.files.iter().filter(|f| f.is_selected()).count();
    info.links.iter().skip(n).cloned().collect()
}

fn selected_rar_count(info: &TorrentInfo) -> usize {
    info.files
        .iter()
        .filter(|f| f.is_selected() && f.path.to_lowercase().ends_with(".rar"))
        .count()
}

/// Zurg `assignLinks`: exactly one selected `.rar` in the pack → after a successful orphan match,
/// mark every selected path `ok` in `file_states` (no STRM recreation here).
pub fn apply_lone_selected_rar_ok_policy(
    info: &TorrentInfo,
    file_states: &mut HashMap<String, String>,
    assigned_file_path: &str,
) {
    if selected_rar_count(info) != 1 {
        return;
    }
    if !assigned_file_path.to_lowercase().ends_with(".rar") {
        return;
    }
    for f in info.files.iter().filter(|f| f.is_selected()) {
        file_states.insert(f.path.clone(), "ok".to_string());
    }
}

fn ensure_link_slot(links: &mut Vec<String>, i: usize) {
    while links.len() <= i {
        links.push(String::new());
    }
}

fn verify_failed_like_bandwidth(e: &anyhow::Error) -> bool {
    let m = e.to_string().to_lowercase();
    m.contains("traffic") || m.contains("bandwidth") || m.contains("fair usage")
}

/// Try to place orphan RD links onto broken / empty selected slots (zurg `assignLinks` subset).
pub async fn assign_orphan_links(
    rd: &RealDebrid,
    cache: &UnrestrictCache,
    info: &mut TorrentInfo,
    file_states: &mut HashMap<String, String>,
) -> Result<(), RdError> {
    let orphans: Vec<String> = orphan_rd_links(info)
        .into_iter()
        .filter(|l| !l.is_empty())
        .collect();
    if orphans.is_empty() {
        return Ok(());
    }

    let selected: Vec<&File> = info.files.iter().filter(|f| f.is_selected()).collect();

    for link in orphans {
        let dl = match rd.unrestrict_link(cache, &link).await {
            Ok(d) => d,
            Err(e) => {
                if e.is_bandwidth_limited() {
                    return Err(e);
                }
                continue;
            }
        };

        if let Err(e) = rd.verify_link(&dl.download).await {
            if verify_failed_like_bandwidth(&e) {
                return Err(RdError::Api(
                    crate::rd::client::ApiError::TrafficExhausted {
                        message: e.to_string(),
                    },
                ));
            }
            warn!("assign link verify failed for {}: {e:#}", dl.filename);
            continue;
        }

        let mut assigned = false;
        for (sel_i, file) in selected.iter().enumerate() {
            let broken = file_states
                .get(&file.path)
                .map(|s| s == "broken")
                .unwrap_or(false);
            let slot_empty = info.links.get(sel_i).map(|s| s.is_empty()).unwrap_or(true);
            if !broken && !slot_empty {
                continue;
            }
            if download_matches_file(file, &dl) {
                ensure_link_slot(&mut info.links, sel_i);
                info.links[sel_i] = link.clone();
                file_states.insert(file.path.clone(), "ok".to_string());
                apply_lone_selected_rar_ok_policy(info, file_states, &file.path);
                debug!("Assigned orphan link to {} (slot {})", file.path, sel_i);
                assigned = true;
                break;
            }
        }

        if !assigned && dl.filename.to_lowercase().ends_with(".rar") {
            info!(
                "Unassigned RAR from RD ({}); may need archive strategy",
                dl.filename
            );
        }
    }

    Ok(())
}

/// Unrestrict each selected file's RD link; mark `broken` on failure (zurg unrestriction loop).
pub async fn probe_selected_links(
    rd: &RealDebrid,
    cache: &UnrestrictCache,
    info: &TorrentInfo,
    file_states: &mut HashMap<String, String>,
) -> Result<(), RdError> {
    let selected: Vec<&File> = info.files.iter().filter(|f| f.is_selected()).collect();
    for (sel_i, file) in selected.iter().enumerate() {
        let Some(link) = info.links.get(sel_i).filter(|s| !s.is_empty()) else {
            continue;
        };
        match rd.unrestrict_link(cache, link).await {
            Ok(dl) => {
                if let Err(e) = rd.verify_link(&dl.download).await {
                    if verify_failed_like_bandwidth(&e) {
                        return Err(RdError::Api(
                            crate::rd::client::ApiError::TrafficExhausted {
                                message: e.to_string(),
                            },
                        ));
                    }
                    file_states.insert(file.path.clone(), "broken".to_string());
                } else {
                    file_states
                        .entry(file.path.clone())
                        .or_insert_with(|| "ok".to_string());
                }
            }
            Err(e) => {
                if e.is_bandwidth_limited() {
                    return Err(e);
                }
                file_states.insert(file.path.clone(), "broken".to_string());
            }
        }
    }
    Ok(())
}

fn broken_playable_paths(info: &TorrentInfo, file_states: &HashMap<String, String>) -> Vec<String> {
    info.files
        .iter()
        .filter(|f| f.is_selected() && path_looks_playable(&f.path))
        .filter(|f| file_states.get(&f.path).is_some_and(|s| s == "broken"))
        .map(|f| f.path.clone())
        .collect()
}

fn any_playable_selected(info: &TorrentInfo) -> bool {
    info.files
        .iter()
        .any(|f| f.is_selected() && path_looks_playable(&f.path))
}

/// If there are no broken playable files but the torrent has videos, HEAD-verify one link (zurg path).
pub async fn verify_one_link_and_clear(
    rd: &RealDebrid,
    cache: &UnrestrictCache,
    mut mt: ManagedTorrent,
) -> Result<Option<ManagedTorrent>, RdError> {
    let Some(info) = mt.info.clone() else {
        return Ok(None);
    };
    if !any_playable_selected(&info) {
        return Ok(None);
    }

    let states = mt.file_states.get_or_insert_with(HashMap::new);
    if !broken_playable_paths(&info, states).is_empty() {
        return Ok(None);
    }

    let selected: Vec<&File> = info.files.iter().filter(|f| f.is_selected()).collect();
    for (sel_i, file) in selected.iter().enumerate() {
        let Some(link) = info.links.get(sel_i).filter(|s| !s.is_empty()) else {
            continue;
        };
        let dl = rd.unrestrict_link(cache, link).await?;
        if let Err(e) = rd.verify_link(&dl.download).await {
            if verify_failed_like_bandwidth(&e) {
                return Err(RdError::Api(
                    crate::rd::client::ApiError::TrafficExhausted {
                        message: e.to_string(),
                    },
                ));
            }
            return Ok(None);
        }
        info!(
            "Preflight: verified link for {}, marking paths ok without cascade",
            file.path
        );
        states.insert(file.path.clone(), "ok".to_string());
        for f in info
            .files
            .iter()
            .filter(|f| f.is_selected() && path_looks_playable(&f.path))
        {
            states
                .entry(f.path.clone())
                .or_insert_with(|| "ok".to_string());
        }
        mt.info = Some(info);
        return Ok(Some(mt));
    }
    Ok(None)
}

/// Run assign + probe + optional verify-ok shortcut.
pub async fn run_preflight(
    rd: &Arc<RealDebrid>,
    cache: &UnrestrictCache,
    mut mt: ManagedTorrent,
) -> PreflightOutcome {
    let Some(mut info) = mt.info.clone() else {
        return PreflightOutcome::Proceed(mt);
    };

    let states = mt.file_states.get_or_insert_with(HashMap::new);

    if let Err(e) = assign_orphan_links(rd, cache, &mut info, states).await
        && e.is_bandwidth_limited()
    {
        return PreflightOutcome::DeferBandwidth;
    }

    mt.info = Some(info.clone());

    if let Err(e) = probe_selected_links(rd, cache, &info, states).await
        && e.is_bandwidth_limited()
    {
        return PreflightOutcome::DeferBandwidth;
    }

    mt.info = Some(info);

    match verify_one_link_and_clear(rd, cache, mt.clone()).await {
        Ok(Some(fixed)) => return PreflightOutcome::VerifiedOk(fixed),
        Err(e) if e.is_bandwidth_limited() => return PreflightOutcome::DeferBandwidth,
        Err(_) | Ok(None) => {}
    }

    PreflightOutcome::Proceed(mt)
}
