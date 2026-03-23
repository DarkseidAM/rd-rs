//! Passive repair signals: unassigned links, stalled downloads, duplicate IDs.

use std::collections::HashSet;

use crate::db::TorrentState;
use crate::rd::types::{Torrent, TorrentInfo};
use crate::torrent::ManagedTorrent;

/// Selected-file index `i` should have `links[i]` non-empty (matches FUSE link resolution).
/// Same rule as the repair engine periodic scan: eligible for repair work (not skipped as unrepairable).
pub fn periodic_repair_eligible(mt: &ManagedTorrent) -> bool {
    if mt.unrepairable_reason.is_some() {
        return false;
    }
    let unassigned = mt
        .info
        .as_ref()
        .is_some_and(|info| unassigned_selected_link_count(info) > 0);
    let file_broken = mt.file_states.as_ref().is_some_and(|fs| {
        fs.iter()
            .any(|(p, s)| s == "broken" && path_looks_playable(p))
    });
    mt.state == TorrentState::Broken
        || mt.state == TorrentState::UnderRepair
        || (mt.state == TorrentState::Ok && (unassigned || file_broken))
}

pub fn unassigned_selected_link_count(info: &TorrentInfo) -> usize {
    let mut missing = 0usize;
    let mut sel_i = 0usize;
    for f in &info.files {
        if !f.is_selected() {
            continue;
        }
        let has = info
            .links
            .get(sel_i)
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has {
            missing += 1;
        }
        sel_i += 1;
    }
    missing
}

pub fn duplicate_selected_file_ids(info: &TorrentInfo) -> bool {
    let mut seen = HashSet::new();
    for f in info.files.iter().filter(|f| f.is_selected()) {
        if !seen.insert(f.id) {
            return true;
        }
    }
    false
}

pub fn path_looks_playable(path: &str) -> bool {
    let p = path.to_lowercase();
    p.ends_with(".mkv")
        || p.ends_with(".mp4")
        || p.ends_with(".avi")
        || p.ends_with(".webm")
        || p.ends_with(".m4v")
        || p.ends_with(".mpg")
        || p.ends_with(".mpeg")
        || p.ends_with(".mp3")
        || p.ends_with(".flac")
        || p.ends_with(".m4b")
        || p.ends_with(".aac")
        || p.ends_with(".opus")
        || p.ends_with(".wav")
}

/// Strategy 3 (archive all) applies when non-playable files are selected (RAR/extras).
pub fn has_non_playable_selected(info: &TorrentInfo) -> bool {
    info.files
        .iter()
        .filter(|f| f.is_selected())
        .any(|f| !path_looks_playable(&f.path))
}

/// RD-style stall rule: ≥1 minute per GB downloaded, floored by `min_mins`.
pub fn is_stalled_download(mt: &ManagedTorrent, min_mins: u64) -> bool {
    if mt.torrent.progress >= 100 {
        return false;
    }
    let bytes_total = mt.info.as_ref().map(|i| i.bytes).unwrap_or(0).max(0);
    let downloaded = (bytes_total as f64 * f64::from(mt.torrent.progress) / 100.0) as i64;
    let gb = downloaded as f64 / 1_000_000_000.0;
    let allowed = gb.max(min_mins as f64);
    let elapsed_mins = (chrono::Utc::now() - mt.torrent.added).num_seconds() as f64 / 60.0;
    elapsed_mins >= allowed
}

/// Stall heuristic when list API has no byte totals (min-wait vs `downloading` progress < 100).
pub fn is_stalled_download_from_list(torrent: &Torrent, min_mins: u64) -> bool {
    if torrent.progress >= 100 {
        return false;
    }
    let elapsed_mins = (chrono::Utc::now() - torrent.added).num_seconds() as f64 / 60.0;
    elapsed_mins >= min_mins as f64
}
