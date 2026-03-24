//! Passive CDN probe during periodic repair scan (`repair.head_check_*`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{info, warn};

use crate::config::RepairConfig;
use crate::db::TorrentState;
use crate::rd::RealDebrid;
use crate::torrent::TorrentManager;

use super::super::detect::{
    check_head_unreachable, passive_head_probe_slot_count, periodic_repair_eligible,
};

/// Returns how many torrents were appended to `keys` from passive HEAD failures.
pub(super) async fn append_passive_head_candidates(
    rd: &Arc<RealDebrid>,
    torrent_manager: &Arc<TorrentManager>,
    passive_head_last: &DashMap<String, Instant>,
    repair: &RepairConfig,
    keys: &mut Vec<String>,
) -> usize {
    if !repair.head_check_enabled {
        return 0;
    }

    let interval = Duration::from_secs(repair.head_check_min_interval_mins.max(1) * 60);
    let threshold = repair.head_unreachable_threshold.max(1);
    let mut head_candidates: Vec<String> = Vec::new();

    for e in torrent_manager.torrents.iter() {
        let mt = e.value();
        if mt.unrepairable_reason.is_some() {
            continue;
        }
        if periodic_repair_eligible(mt) {
            continue;
        }
        if mt.state != TorrentState::Ok {
            continue;
        }
        let Some(info) = mt.info.as_ref() else {
            continue;
        };
        if passive_head_probe_slot_count(info) == 0 {
            continue;
        }
        let k = mt.access_key.clone();
        if let Some(last) = passive_head_last.get(&k)
            && last.value().elapsed() < interval
        {
            continue;
        }
        head_candidates.push(k);
    }

    let rd = rd.clone();
    let cache = torrent_manager.unrestrict_cache.clone();
    let mut head_check_enqueued = 0usize;

    for access_key in head_candidates {
        let Some(mt_ent) = torrent_manager.torrents.get(&access_key) else {
            continue;
        };
        let mt = mt_ent.value().clone();
        drop(mt_ent);
        let Some(info) = mt.info.clone() else {
            continue;
        };
        match check_head_unreachable(&rd, &cache, &info).await {
            Ok(n) => {
                passive_head_last.insert(access_key.clone(), Instant::now());
                if n >= threshold {
                    head_check_enqueued += 1;
                    keys.push(access_key.clone());
                    info!(
                        access_key = %access_key,
                        unreachable_links = n,
                        "Repair engine: passive HEAD check found unreachable links"
                    );
                }
            }
            Err(e) if e.is_bandwidth_limited() => {
                passive_head_last.insert(access_key.clone(), Instant::now());
                warn!(
                    access_key = %access_key,
                    "Repair engine: passive HEAD check deferred (bandwidth)"
                );
            }
            Err(e) => {
                passive_head_last.insert(access_key.clone(), Instant::now());
                warn!(
                    access_key = %access_key,
                    error = %e,
                    "Repair engine: passive HEAD check error"
                );
            }
        }
    }

    head_check_enqueued
}
