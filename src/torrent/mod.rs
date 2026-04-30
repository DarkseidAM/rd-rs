//! Torrent domain types and the `TorrentManager` entry point.
//!
//! `TorrentManager` owns:
//!   - An in-memory `DashMap` of every torrent keyed by `access_key`
//!   - A background refresh loop (15 s default) that syncs from the RD API
//!   - A debounced `on_library_update` hook worker
//!
//! ## `file_states` concurrency
//!
//! All mutations to [`ManagedTorrent::file_states`] go through `TorrentManager` (`mark_file_broken`,
//! `persist_torrent_snapshot`, refresh merge, repair preflight). We do not model zurg's per-file
//! FSM mutex graph; a single writer discipline keeps FUSE, refresh, and repair consistent.

mod fuse_read_coalesce;
pub mod hook;
pub mod refresh;
mod repair_enqueue;
mod state_ops;

pub use repair_enqueue::EnqueueRepairAllOptions;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc};
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

    /// Unix time when a repair last completed successfully (`state` → ok).
    pub last_repaired_at: Option<i64>,

    /// Per-file health from repair / FUSE (`path` → `ok` | `broken`), persisted as JSON.
    pub file_states: Option<HashMap<String, String>>,

    /// When current `under_repair` began (for zurg-style repair timeout).
    pub under_repair_started_at: Option<i64>,
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

    /// Returns only the selected files from the TorrentInfo.
    pub fn selected_files(&self) -> Vec<&crate::rd::types::File> {
        self.info
            .as_ref()
            .map(|i| i.files.iter().filter(|f| f.is_selected()).collect())
            .unwrap_or_default()
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
    pub(crate) hook_tx: mpsc::UnboundedSender<Vec<String>>,

    /// Receiver for the hook worker (extracted once on start).
    pub(crate) hook_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<Vec<String>>>>,

    /// CancellationToken — call `.cancel()` to stop all background tasks.
    pub(crate) shutdown: CancellationToken,

    /// Wake the repair engine (priority keys).
    pub(crate) repair_notify: Arc<Notify>,

    pub(crate) repair_queue: Arc<Mutex<VecDeque<String>>>,

    /// One in-flight "fatal read → mark broken + enqueue repair" handler per `(access_key, file_path)`.
    pub(crate) fuse_fatal_read_locks: Arc<Mutex<HashSet<(String, String)>>>,

    /// Per-torrent watch channel — fires on every state change.
    /// FUSE repair-waiters subscribe here instead of polling.
    pub(crate) repair_state_tx: DashMap<String, Arc<tokio::sync::watch::Sender<TorrentState>>>,

    /// Broadcast channel that fires the access_key of each torrent that is removed during
    /// refresh. RdFs subscribes to clean up inode maps and `broken_read_warn_ts`.
    pub(crate) torrent_removed_tx: tokio::sync::broadcast::Sender<String>,
}

impl TorrentManager {
    /// Create a new `TorrentManager` and warm it from the SQLite cache.
    ///
    /// Background tasks are NOT started yet — call [`TorrentManager::start`] next.
    pub async fn new(
        rd: Arc<RealDebrid>,
        db: Arc<tokio_rusqlite::Connection>,
        config: Arc<ArcSwap<Config>>,
        unrestrict_cache: crate::rd::api::UnrestrictCache,
    ) -> anyhow::Result<Self> {
        let torrents = Arc::new(DashMap::new());
        let shutdown = CancellationToken::new();
        let (hook_tx, hook_rx) = mpsc::unbounded_channel();
        let repair_notify = Arc::new(Notify::new());
        let repair_queue = Arc::new(Mutex::new(VecDeque::new()));
        let fuse_fatal_read_locks = Arc::new(Mutex::new(HashSet::new()));
        let repair_state_tx = DashMap::new();
        let (torrent_removed_tx, _) = tokio::sync::broadcast::channel(256);

        let mgr = Self {
            torrents,
            rd,
            db,
            config,
            unrestrict_cache,
            hook_tx,
            hook_rx: std::sync::Mutex::new(Some(hook_rx)),
            shutdown,
            repair_notify,
            repair_queue,
            fuse_fatal_read_locks,
            repair_state_tx,
            torrent_removed_tx,
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
            let file_states = row
                .file_states
                .as_ref()
                .and_then(|s| serde_json::from_str(s).ok());
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
                last_repaired_at: row.last_repaired_at,
                file_states,
                under_repair_started_at: row.under_repair_started_at,
            };
            if row.state == TorrentState::UnderRepair {
                tracing::info!(
                    "Startup: torrent {} was under_repair (repair engine will resume)",
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
        // Unbounded channel: only fails if receiver is dropped (shutdown), safe to ignore.
        let _ = self.hook_tx.send(paths);
    }

    /// Build the filesystem paths that represent a torrent.
    /// v1: only `__all__/<access_key>` (directory-level notification).
    pub fn library_paths_for(&self, mt: &ManagedTorrent) -> Vec<String> {
        vec![format!("__all__/{}", mt.access_key)]
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Subscribe to state changes for a torrent.
    /// Returns a Receiver initialised with its current state.
    /// Creates the sender lazily if not yet present.
    pub fn subscribe_repair_state(
        &self,
        access_key: &str,
    ) -> tokio::sync::watch::Receiver<TorrentState> {
        let current = self
            .torrents
            .get(access_key)
            .map(|m| m.state.clone())
            .unwrap_or(TorrentState::Broken);

        let tx = self
            .repair_state_tx
            .entry(access_key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::watch::channel(current).0))
            .value()
            .clone();

        // Re-sync with latest state to close race condition window between initial get and channel creation.
        if let Some(m) = self.torrents.get(access_key) {
            let _ = tx.send(m.state.clone());
        }

        tx.subscribe()
    }

    /// Load `TorrentInfo` for this key if missing (FUSE / repair).
    pub async fn ensure_torrent_info(&self, access_key: &str) -> Option<Arc<ManagedTorrent>> {
        let mt = self.torrents.get(access_key)?.value().clone();
        if mt.info.is_some() {
            return Some(mt);
        }
        let rd_id = mt.rd_ids.first()?.clone();
        let info: TorrentInfo = match self.rd.get_torrent_info(&rd_id).await {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(
                    "Failed to load torrent info for {} ({}): {}",
                    access_key,
                    rd_id,
                    e
                );
                return Some(mt);
            }
        };
        // Securely inject the loaded info without wiping out any concurrent
        // state changes (like state=Broken from FUSE) that happened during the HTTP fetch.
        let mut final_arc = None;
        self.torrents
            .entry(access_key.to_string())
            .and_modify(|arc_mt| {
                let updated = Arc::new(ManagedTorrent {
                    info: Some(info.clone()),
                    ..(**arc_mt).clone()
                });
                *arc_mt = updated.clone();
                final_arc = Some(updated);
            });

        if final_arc.is_some() {
            final_arc
        } else {
            // Torrent was removed concurrently during info fetch.
            // Return a detached snapshot with the info.
            Some(Arc::new(ManagedTorrent {
                info: Some(info),
                ..(*mt).clone()
            }))
        }
    }

    /// Priority repair queue (deduped). Repair engine drains this before periodic scans.
    pub async fn enqueue_repair(&self, access_key: String) {
        let mut q = self.repair_queue.lock().await;
        if !q.iter().any(|k| k == &access_key) {
            q.push_back(access_key);
        }
        drop(q);
        self.repair_notify.notify_one();
    }

    /// Publish a removal event so that subscribers (e.g. RdFs inode maps) can clean up.
    pub(crate) fn notify_torrent_removed(&self, access_key: &str) {
        // Ignore send errors — no subscribers is fine (e.g., during startup repair CLI).
        let _ = self.torrent_removed_tx.send(access_key.to_string());
    }

    /// Subscribe to torrent-removed events. Used by RdFs to clean up inode maps.
    pub fn subscribe_torrent_removed(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.torrent_removed_tx.subscribe()
    }

    // ─── Shutdown ─────────────────────────────────────────────────────────────

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
