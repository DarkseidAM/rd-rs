//! Disk / TTL eviction helpers for [`super::cache::Cache`].

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::config::parse_byte_size;

use super::cache::Cache;

/// Idle items are evictable after 1 minute with no open handles.
const ITEM_IDLE_TIMEOUT_SECS: u64 = 60;
/// Start evicting when total cached bytes exceeds 90% of `cache_max_size`.
const EVICT_THRESHOLD: f64 = 0.90;
/// Max rows considered per TTL cleanup pass (`stale_keys` query limit).
const TTL_STALE_KEYS_QUERY_LIMIT: usize = 10_000;
/// Skip on-disk cache files not in the item map until this many seconds after atime (avoid racing with active writes).
const DISK_ORPHAN_MIN_AGE_SECS: u64 = 5 * 60;

pub(super) fn evict(cache: &Cache) {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let to_remove: Vec<String> = cache
        .items
        .iter()
        .filter(|e| {
            let item = e.value();
            !item.is_open() && now_secs.saturating_sub(item.atime_secs()) >= ITEM_IDLE_TIMEOUT_SECS
        })
        .map(|e| e.key().clone())
        .collect();

    for key in &to_remove {
        if let Some((_, item)) = cache.items.remove(key) {
            item.flush_ranges(true);
        }
    }

    if !to_remove.is_empty() {
        tracing::debug!(
            "cache evict: removed {} idle items from map",
            to_remove.len()
        );
    }

    ttl_cleanup(cache, now_secs);

    let max_bytes = parse_byte_size(&cache.config.cache_max_size);
    if max_bytes == 0 {
        return;
    }
    let threshold = (max_bytes as f64 * EVICT_THRESHOLD) as u64;
    evict_disk_lru(cache, threshold, now_secs);
    check_free_space(cache);
}

pub(super) fn evict_disk_lru(cache: &Cache, threshold: u64, now_secs: u64) {
    let Ok(top) = std::fs::read_dir(&cache.cache_dir) else {
        return;
    };

    let mut candidates: Vec<(u64, PathBuf, u64)> = Vec::new();
    let mut total: u64 = 0;

    for entry in top.flatten() {
        let Ok(subs) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for sub in subs.flatten() {
            let path = sub.path();
            if path.extension().is_some() {
                continue;
            }
            if let Ok(meta) = path.metadata() {
                let size = meta.blocks() * 512;
                let atime = meta
                    .accessed()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                total += size;
                candidates.push((atime, path, size));
            }
        }
    }

    if total <= threshold {
        return;
    }

    candidates.sort_by_key(|(atime, _, _)| *atime);

    let mut freed: u64 = 0;
    for (atime, path, size) in candidates {
        if total - freed <= threshold {
            break;
        }
        let key = path_to_key(&cache.cache_dir, &path);
        if cache.items.contains_key(&key) {
            continue;
        }
        if now_secs.saturating_sub(atime) < DISK_ORPHAN_MIN_AGE_SECS {
            continue;
        }
        if std::fs::remove_file(&path).is_ok() {
            freed += size;
            tracing::debug!("cache evict: removed {:?} ({} bytes)", path, size);
        }
    }

    if freed > 0 {
        tracing::info!("cache evict: freed {} MB from disk", freed / 1_048_576);
    }
}

fn check_free_space(cache: &Cache) {
    let min_free = parse_byte_size(&cache.config.cache_min_free_space);
    if min_free == 0 {
        return;
    }

    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        if let Ok(path_cstr) = CString::new(cache.cache_dir.to_string_lossy().as_bytes()) {
            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            if unsafe { libc::statvfs(path_cstr.as_ptr(), &mut stat) } == 0 {
                let free_bytes = stat.f_bavail * stat.f_bsize;
                if free_bytes < min_free {
                    if !*cache.pause_downloads.borrow() {
                        tracing::warn!(
                            free_gb = free_bytes / 1_073_741_824,
                            min_free_gb = min_free / 1_073_741_824,
                            "cache dir low on disk space — pausing active downloads"
                        );
                        let _ = cache.pause_downloads.send(true);
                    }
                } else if *cache.pause_downloads.borrow() {
                    tracing::info!("cache dir disk space recovered — unpausing downloads");
                    let _ = cache.pause_downloads.send(false);
                }
            }
        }
    }
}

fn ttl_cleanup(cache: &Cache, now_secs: u64) {
    let age = parse_age_secs(&cache.config.cache_max_age);
    if age == 0 {
        return;
    }
    let cutoff = now_secs.saturating_sub(age) as i64;
    let stale_keys = match cache
        .range_db
        .stale_keys(cutoff, TTL_STALE_KEYS_QUERY_LIMIT)
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("cache ranges stale key lookup failed: {e:#}");
            return;
        }
    };
    if stale_keys.is_empty() {
        return;
    }

    let (keys_to_delete, deleted_files) = stale_keys
        .iter()
        .filter(|key| !cache.items.contains_key(*key))
        .fold(
            (Vec::<&str>::new(), 0usize),
            |(mut keys, mut deleted_files), key| {
                let path = cache.cache_dir.join(key);
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        deleted_files += 1;
                        keys.push(key.as_str());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        keys.push(key.as_str());
                    }
                    Err(e) => {
                        tracing::warn!(
                            cache_key = %key,
                            error = %e,
                            "cache ttl cleanup: failed to remove stale cache file; will retry"
                        );
                    }
                }
                (keys, deleted_files)
            },
        );
    match cache.range_db.delete_keys(&keys_to_delete) {
        Ok(rows) => {
            tracing::info!(
                ttl_rows = rows,
                ttl_files = deleted_files,
                "cache ttl cleanup"
            );
        }
        Err(e) => tracing::warn!("cache ranges ttl row delete failed: {e:#}"),
    }
    cache.range_db.maybe_checkpoint();
}

fn path_to_key(cache_dir: &Path, file_path: &Path) -> String {
    file_path
        .strip_prefix(cache_dir)
        .ok()
        .map(|rel| rel.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn parse_age_secs(s: &str) -> u64 {
    let raw = s.trim().to_ascii_lowercase();
    if raw.is_empty() || raw == "0" {
        return 0;
    }
    let (num, suffix) = raw
        .find(|c: char| c.is_ascii_alphabetic())
        .map(|i| (&raw[..i], &raw[i..]))
        .unwrap_or((raw.as_str(), ""));
    let base: u64 = num.trim().parse().unwrap_or(0);
    match suffix.trim() {
        "s" | "sec" | "secs" | "second" | "seconds" => base,
        "m" | "min" | "mins" | "minute" | "minutes" => base * 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => base * 3600,
        "d" | "day" | "days" => base * 86400,
        _ => base,
    }
}
