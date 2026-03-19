//! Torrent domain types and the `TorrentManager` entry point.
//!
//! `TorrentManager` owns:
//!   - An in-memory `DashMap` of every torrent keyed by `access_key`
//!   - A background refresh loop (15 s default) that syncs from the RD API
//!   - A debounced `on_library_update` hook worker

pub mod hook;
pub mod refresh;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::db::TorrentRow;
use crate::db::TorrentState;
use crate::rd::RealDebrid;
use crate::rd::api::UnrestrictCache;
use crate::rd::types::{Torrent, TorrentInfo};
use arc_swap::ArcSwap;

// ─── Domain types ─────────────────────────────────────────────────────────────

/// A torrent as known to rd-rs — combines the lightweight list-API snapshot
/// with an optional detailed `TorrentInfo` and our local health state.
#[derive(Debug, Clone)]
pub struct ManagedTorrent {
    /// Stable identifier: `<hash>/<name>` (matches Go's access key convention).
    pub access_key: String,

    /// All RD torrent IDs that map to this content (multi-ID packs merged here).
    pub rd_ids: Vec<String>,

    /// Lightweight snapshot from `GET /torrents` — always present.
    pub torrent: Torrent,

    /// Full file list from `GET /torrents/info/{id}` — loaded lazily.
    pub info: Option<TorrentInfo>,

    /// Our local health state (ok / broken / under_repair).
    pub state: TorrentState,

    /// Reason the torrent cannot be repaired (if unrepairable).
    pub unrepairable_reason: Option<String>,
}

impl ManagedTorrent {
    /// Human-readable display name (same as `torrent.name`).
    pub fn name(&self) -> &str {
        &self.torrent.name
    }

    /// Returns the files from the TorrentInfo, or an empty slice.
    pub fn files(&self) -> &[crate::rd::types::File] {
        self.info
            .as_ref()
            .map(|i| i.files.as_slice())
            .unwrap_or(&[])
    }
}

/// Derive a stable access key from hash + name.
/// `<hash>/<name>` — matches Go's `GetKey(torrent)` implementation.
pub fn access_key(hash: &str, name: &str) -> String {
    format!("{}/{}", hash, name)
}

// ─── TorrentManager ───────────────────────────────────────────────────────────

/// Central torrent registry — holds the live DashMap and manages all
/// background tasks (refresh loop, hook worker).
pub struct TorrentManager {
    /// Live in-memory registry. Other components hold `Arc` clones of this.
    pub torrents: Arc<DashMap<String, Arc<ManagedTorrent>>>,

    /// RD API client (shared with FUSE layer for unrestrict calls).
    pub rd: Arc<RealDebrid>,

    /// Async SQLite handle (from `tokio-rusqlite`).
    pub db: Arc<tokio_rusqlite::Connection>,

    /// Live config (ArcSwap for hot-reload; refresh loop and hook read current snapshot).
    pub config: Arc<ArcSwap<Config>>,

    /// Unrestrict cache shared between TorrentManager and FUSE read().
    pub unrestrict_cache: UnrestrictCache,

    /// Channel to send path lists to the hook worker.
    pub(crate) hook_tx: mpsc::Sender<Vec<String>>,

    /// Receiver for the hook worker (extracted once on start).
    pub(crate) hook_rx: std::sync::Mutex<Option<mpsc::Receiver<Vec<String>>>>,

    /// CancellationToken — call `.cancel()` to stop all background tasks.
    pub(crate) shutdown: CancellationToken,
}

impl TorrentManager {
    /// Create a new `TorrentManager` and warm it from the SQLite cache.
    ///
    /// Background tasks are NOT started yet — call [`TorrentManager::start`] next.
    pub async fn new(
        rd: Arc<RealDebrid>,
        db: Arc<tokio_rusqlite::Connection>,
        config: Arc<ArcSwap<Config>>,
    ) -> anyhow::Result<Self> {
        let torrents = Arc::new(DashMap::new());
        let unrestrict_cache = crate::rd::api::new_unrestrict_cache();
        let shutdown = CancellationToken::new();
        let (hook_tx, hook_rx) = mpsc::channel(256);

        let mgr = Self {
            torrents,
            rd,
            db,
            config,
            unrestrict_cache,
            hook_tx,
            hook_rx: std::sync::Mutex::new(Some(hook_rx)),
            shutdown,
        };

        // Warm from SQLite before first RD sync (< 1s for 20K rows)
        mgr.load_from_db().await?;
        tracing::info!(
            "TorrentManager: loaded {} torrents from SQLite",
            mgr.torrents.len()
        );

        Ok(mgr)
    }

    /// Spawn the refresh loop and hook worker. Must be called once after `new`.
    pub fn start(self: &Arc<Self>) {
        // Refresh loop
        let mgr = self.clone();
        tokio::spawn(async move {
            refresh::run_refresh_loop(mgr).await;
        });

        // Premium status & Traffic monitoring
        let mgr = self.clone();
        tokio::spawn(async move {
            refresh::run_premium_check_loop(mgr).await;
        });

        // Non-RD Downloads
        let mgr = self.clone();
        tokio::spawn(async move {
            refresh::run_downloads_check_loop(mgr).await;
        });

        // on_library_update hook worker
        if let Some(rx) = self.hook_rx.lock().unwrap().take() {
            let worker = hook::HookWorker {
                receiver: rx,
                config: self.config.clone(),
                shutdown: self.shutdown.clone(),
            };
            worker.spawn();
        }

        tracing::info!("TorrentManager: background tasks started");
    }

    // ─── SQLite warm-load ─────────────────────────────────────────────────────

    async fn load_from_db(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        let rows: Vec<TorrentRow> = db
            .call(|conn| -> rusqlite::Result<Vec<TorrentRow>> {
                crate::db::Db::get_all_torrents_conn(conn)
            })
            .await
            .map_err(|e| anyhow::anyhow!("DB load error: {}", e))?;

        for row in rows {
            let mt = ManagedTorrent {
                access_key: row.access_key.clone(),
                rd_ids: row.rd_ids.clone(),
                torrent: Torrent {
                    id: row.rd_ids_first_or_empty(&row.access_key),
                    hash: row.hash.clone(),
                    name: row.name.clone(),
                    progress: 100,
                    status: "downloaded".to_string(),
                    links: vec![],
                    added: chrono::Utc::now(),
                },
                info: None,
                state: row.state.clone(),
                unrepairable_reason: row.unrepairable_reason.clone(),
            };
            if row.state == TorrentState::UnderRepair {
                // In Phase 3, this will be pushed to the RepairManager queue.
                tracing::warn!(
                    "Startup: Torrent {} is under_repair (awaiting repair loop)",
                    row.access_key
                );
            }

            self.torrents.insert(row.access_key, Arc::new(mt));
        }

        Ok(())
    }

    // ─── Hook trigger ─────────────────────────────────────────────────────────

    /// Enqueue paths for the on_library_update hook (non-blocking, fire-and-forget).
    pub fn trigger_library_update(&self, paths: Vec<String>) {
        if paths.is_empty() {
            return;
        }
        let _ = self.hook_tx.try_send(paths);
    }

    /// Build the filesystem paths that represent a torrent.
    /// v1: only `__all__/<access_key>` (directory-level notification).
    pub fn library_paths_for(&self, mt: &ManagedTorrent) -> Vec<String> {
        vec![format!("__all__/{}", mt.access_key)]
    }

    // ─── Shutdown ─────────────────────────────────────────────────────────────

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
