//! `Cache` — global manager for all `CacheItem`s.
//!
//! Responsible for:
//! - Creating / returning `CacheItem`s keyed by `"{access_key}/{filename}"`
//! - Periodic eviction (LRU + age + free-space guard)
//! - Aggregate stats (hits, misses, download speed)

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::time;

use crate::cache::item::CacheItem;
use crate::cache::range_db::RangeDb;

/// Idle items are evictable after 1 minute with no open handles.
const ITEM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
use crate::config::{VfsConfig, parse_byte_size};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Start evicting when total cached bytes exceeds 90% of `cache_max_size`.
const EVICT_THRESHOLD: f64 = 0.90;
/// How often the eviction task runs.
const EVICT_INTERVAL: Duration = Duration::from_secs(30);
/// How often the speed sampler fires.
const SPEED_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

// ─── Cache ────────────────────────────────────────────────────────────────────

pub struct Cache {
    pub cache_dir: PathBuf,
    config: Arc<VfsConfig>,
    items: Arc<DashMap<String, Arc<CacheItem>>>,
    range_db: Arc<RangeDb>,
    // Stats
    pub hits: AtomicI64,
    pub misses: AtomicI64,
    pub total_downloaded: AtomicI64,
    /// Current download speed in bytes/s (updated by speed_sample_loop).
    pub speed_bps: AtomicI64,
    pub active_circuit_breakers: AtomicI32,
    // Speed sampling state
    last_speed_bytes: AtomicI64,
    last_speed_time: AtomicI64,
    pub pause_downloads: tokio::sync::watch::Sender<bool>,
}

impl Cache {
    pub fn new(cache_dir: impl AsRef<Path>, config: Arc<VfsConfig>) -> Arc<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        let _ = std::fs::create_dir_all(&cache_dir);
        let range_db_path = cache_dir.join("cache_ranges.db");
        let range_db = Arc::new(RangeDb::open(&range_db_path).unwrap_or_else(|e| {
            panic!(
                "failed to open cache ranges db at {:?}: {e:#}",
                range_db_path
            )
        }));

        let (pause_tx, _) = tokio::sync::watch::channel(false);
        let cache = Arc::new(Self {
            cache_dir,
            config,
            items: Arc::new(DashMap::new()),
            range_db,
            hits: AtomicI64::new(0),
            misses: AtomicI64::new(0),
            total_downloaded: AtomicI64::new(0),
            speed_bps: AtomicI64::new(0),
            active_circuit_breakers: AtomicI32::new(0),
            last_speed_bytes: AtomicI64::new(0),
            last_speed_time: AtomicI64::new(0),
            pause_downloads: pause_tx,
        });

        // Spawn background tasks.
        let c1 = Arc::clone(&cache);
        tokio::spawn(async move { c1.evict_loop().await });

        let c2 = Arc::clone(&cache);
        tokio::spawn(async move { c2.speed_sample_loop().await });

        cache
    }

    // ─── Item access ─────────────────────────────────────────────────────────

    /// Build the DashMap key.
    pub fn build_key(access_key: &str, filename: &str) -> String {
        format!("{access_key}/{filename}")
    }

    /// Return (or create) the `CacheItem` for a given file.
    pub fn get_or_create(
        &self,
        access_key: &str,
        filename: &str,
        file_size: u64,
    ) -> anyhow::Result<Arc<CacheItem>> {
        let key = Self::build_key(access_key, filename);

        // Fast path.
        if let Some(item) = self.items.get(&key) {
            return Ok(Arc::clone(item.value()));
        }

        // Slow path — only one winner thanks to DashMap's per-shard locking.
        let path = self.cache_dir.join(access_key).join(filename);
        let item =
            CacheItem::open_or_create_with_db(path, key.clone(), file_size, self.range_db.clone())?;
        tracing::info!(file = %filename, key = %key, "cache open");
        self.items.entry(key).or_insert_with(|| Arc::clone(&item));
        Ok(item)
    }

    // ─── Stats ────────────────────────────────────────────────────────────────

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_downloaded_bytes(&self, n: i64) {
        self.total_downloaded.fetch_add(n, Ordering::Relaxed);
    }

    // ─── Background loops ─────────────────────────────────────────────────────

    async fn speed_sample_loop(self: &Arc<Self>) {
        let mut interval = time::interval(SPEED_SAMPLE_INTERVAL);
        loop {
            interval.tick().await;
            let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let current_bytes = self.total_downloaded.load(Ordering::Relaxed);

            let last_time = self.last_speed_time.swap(now_ns, Ordering::Relaxed);
            let last_bytes = self.last_speed_bytes.swap(current_bytes, Ordering::Relaxed);

            if last_time == 0 {
                continue; // first sample, no baseline yet
            }
            let elapsed_ns = (now_ns - last_time).max(1);
            let delta_bytes = current_bytes - last_bytes;
            let bps = (delta_bytes * 1_000_000_000) / elapsed_ns;
            self.speed_bps.store(bps.max(0), Ordering::Relaxed);
        }
    }

    async fn evict_loop(self: &Arc<Self>) {
        // Run once at startup to clear stale leftovers.
        self.evict();

        let mut interval = time::interval(EVICT_INTERVAL);
        loop {
            interval.tick().await;
            self.evict();
        }
    }

    fn evict(&self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Phase 1: remove idle in-memory items (closed handles + old atime).
        let idle_secs = ITEM_IDLE_TIMEOUT.as_secs();
        let to_remove: Vec<String> = self
            .items
            .iter()
            .filter(|e| {
                let item = e.value();
                !item.is_open() && now_secs.saturating_sub(item.atime_secs()) >= idle_secs
            })
            .map(|e| e.key().clone())
            .collect();

        for key in &to_remove {
            if let Some((_, item)) = self.items.remove(key) {
                item.flush_ranges(true);
            }
        }

        if !to_remove.is_empty() {
            tracing::debug!(
                "cache evict: removed {} idle items from map",
                to_remove.len()
            );
        }

        self.ttl_cleanup(now_secs);

        // Phase 2: disk eviction by LRU if over size threshold.
        let max_bytes = parse_byte_size(&self.config.cache_max_size);
        if max_bytes == 0 {
            return; // unlimited
        }
        let threshold = (max_bytes as f64 * EVICT_THRESHOLD) as u64;

        self.evict_disk_lru(threshold, now_secs);

        // Phase 3: free-space guard.
        self.check_free_space();
    }

    pub fn evict_disk_lru(&self, threshold: u64, now_secs: u64) {
        // Scan cache_dir for data files, collect (atime, path, size).
        let Ok(top) = std::fs::read_dir(&self.cache_dir) else {
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
                    continue; // skip .json sidecar files if any
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

        // Sort oldest first.
        candidates.sort_by_key(|(atime, _, _)| *atime);

        let mut freed: u64 = 0;
        for (atime, path, size) in candidates {
            if total - freed <= threshold {
                break;
            }
            // Don't evict files that are currently open in the items map.
            let key = path_to_key(&self.cache_dir, &path);
            if self.items.contains_key(&key) {
                continue;
            }
            // Grace period: do not delete files accessed within the last 5 minutes.
            if now_secs.saturating_sub(atime) < 300 {
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

    fn check_free_space(&self) {
        let min_free = parse_byte_size(&self.config.cache_min_free_space);
        if min_free == 0 {
            return;
        }

        #[cfg(target_os = "linux")]
        {
            use std::ffi::CString;
            if let Ok(path_cstr) = CString::new(self.cache_dir.to_string_lossy().as_bytes()) {
                let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
                if unsafe { libc::statvfs(path_cstr.as_ptr(), &mut stat) } == 0 {
                    let free_bytes = stat.f_bavail * stat.f_bsize;
                    if free_bytes < min_free {
                        if !*self.pause_downloads.borrow() {
                            tracing::warn!(
                                free_gb = free_bytes / 1_073_741_824,
                                min_free_gb = min_free / 1_073_741_824,
                                "cache dir low on disk space — pausing active downloads"
                            );
                            let _ = self.pause_downloads.send(true);
                        }
                    } else if *self.pause_downloads.borrow() {
                        tracing::info!("cache dir disk space recovered — unpausing downloads");
                        let _ = self.pause_downloads.send(false);
                    }
                }
            }
        }
    }

    fn ttl_cleanup(&self, now_secs: u64) {
        let age = parse_age_secs(&self.config.cache_max_age);
        if age == 0 {
            return;
        }
        let cutoff = now_secs.saturating_sub(age) as i64;
        let stale_keys = match self.range_db.stale_keys(cutoff, 10_000) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("cache ranges stale key lookup failed: {e:#}");
                return;
            }
        };
        if stale_keys.is_empty() {
            return;
        }

        let mut deleted_files = 0usize;
        let mut keys_to_delete = Vec::new();
        for key in &stale_keys {
            if self.items.contains_key(key) {
                continue;
            }
            keys_to_delete.push(key.clone());
            let path = self.cache_dir.join(key);
            if std::fs::remove_file(&path).is_ok() {
                deleted_files += 1;
            }
        }
        match self.range_db.delete_keys(&keys_to_delete) {
            Ok(rows) => {
                tracing::info!(
                    ttl_rows = rows,
                    ttl_files = deleted_files,
                    "cache ttl cleanup"
                );
            }
            Err(e) => tracing::warn!("cache ranges ttl row delete failed: {e:#}"),
        }
        self.range_db.maybe_checkpoint();
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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
