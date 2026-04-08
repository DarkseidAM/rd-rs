//! `Cache` — global manager for all `CacheItem`s.
//!
//! Responsible for:
//! - Creating / returning `CacheItem`s keyed by `"{access_key}/{filename}"`
//! - Periodic eviction (LRU + age + free-space guard) — see [`super::eviction`]
//! - Aggregate stats (hits, misses, download speed)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::time;

use crate::cache::item::CacheItem;
use crate::cache::range_db::RangeDb;
use crate::config::VfsConfig;

/// How often the eviction task runs.
const EVICT_INTERVAL: Duration = Duration::from_secs(30);
/// How often the speed sampler fires.
const SPEED_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub struct Cache {
    pub cache_dir: PathBuf,
    pub(super) config: Arc<VfsConfig>,
    pub(super) items: Arc<DashMap<String, Arc<CacheItem>>>,
    pub(super) range_db: Arc<RangeDb>,
    pub hits: AtomicI64,
    pub misses: AtomicI64,
    pub total_downloaded: AtomicI64,
    pub speed_bps: AtomicI64,
    pub active_circuit_breakers: AtomicI32,
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

        let c1 = Arc::clone(&cache);
        tokio::spawn(async move { c1.evict_loop().await });

        let c2 = Arc::clone(&cache);
        tokio::spawn(async move { c2.speed_sample_loop().await });

        cache
    }

    pub fn build_key(access_key: &str, filename: &str) -> String {
        format!("{access_key}/{filename}")
    }

    /// Returns the live [`CacheItem`] for `key`, creating it on first use.
    ///
    /// Uses `DashMap::entry` so concurrent callers share one canonical `Arc` instead of
    /// duplicating items (Decypharr B3-style creation race).
    pub fn get_or_create(
        &self,
        access_key: &str,
        filename: &str,
        file_size: u64,
    ) -> anyhow::Result<Arc<CacheItem>> {
        let key = Self::build_key(access_key, filename);

        let item = match self.items.entry(key.clone()) {
            Entry::Occupied(o) => Arc::clone(o.get()),
            Entry::Vacant(v) => {
                let path = self.cache_dir.join(access_key).join(filename);
                let item = CacheItem::open_or_create_with_db(
                    path,
                    key.clone(),
                    file_size,
                    self.range_db.clone(),
                )?;
                tracing::info!(file = %filename, key = %key, "cache open");
                let r = v.insert(item);
                Arc::clone(r.value())
            }
        };

        // Mark open *before* returning so the eviction loop cannot remove this key while a caller
        // is in-flight between get_or_create() and CacheItem::open().
        item.open();
        Ok(item)
    }

    /// Drop in-memory entry, remove the sparse file, and delete persisted range metadata.
    pub fn invalidate(&self, access_key: &str, filename: &str) {
        let key = Self::build_key(access_key, filename);
        self.items.remove(&key);
        let path = self.cache_dir.join(access_key).join(filename);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!("cache invalidate: remove {path:?}: {e:#}");
        }
        if let Err(e) = self.range_db.delete_keys(&[key.as_str()]) {
            tracing::warn!("cache invalidate: range db {key}: {e:#}");
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_downloaded_bytes(&self, n: i64) {
        self.total_downloaded.fetch_add(n, Ordering::Relaxed);
    }

    async fn speed_sample_loop(self: &Arc<Self>) {
        let mut interval = time::interval(SPEED_SAMPLE_INTERVAL);
        loop {
            interval.tick().await;
            let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let current_bytes = self.total_downloaded.load(Ordering::Relaxed);

            let last_time = self.last_speed_time.swap(now_ns, Ordering::Relaxed);
            let last_bytes = self.last_speed_bytes.swap(current_bytes, Ordering::Relaxed);

            if last_time == 0 {
                continue;
            }
            let elapsed_ns = (now_ns - last_time).max(1);
            let delta_bytes = current_bytes - last_bytes;
            let bps = (delta_bytes * 1_000_000_000) / elapsed_ns;
            self.speed_bps.store(bps.max(0), Ordering::Relaxed);
        }
    }

    async fn evict_loop(self: &Arc<Self>) {
        super::eviction::evict(self);
        let mut interval = time::interval(EVICT_INTERVAL);
        loop {
            interval.tick().await;
            super::eviction::evict(self);
        }
    }

    /// Run one eviction pass immediately (useful for tests / ops).
    pub fn evict_once(&self) {
        super::eviction::evict(self);
    }

    pub fn evict_disk_lru(&self, threshold: u64, now_secs: u64) {
        super::eviction::evict_disk_lru(self, threshold, now_secs);
    }
}
