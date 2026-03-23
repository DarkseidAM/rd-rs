//! Background repair loop: throttled periodic scan, priority queue, single-flight sessions.

mod repair_one;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::Config;
use crate::db::{Db, TorrentState};
use crate::rd::RealDebrid;
use crate::torrent::TorrentManager;

use super::detect::periodic_repair_eligible;

/// Coordinates background repair (one session at a time; mirrors zurg repair queue).
pub struct RepairEngine {
    pub rd: Arc<RealDebrid>,
    pub db: Arc<tokio_rusqlite::Connection>,
    pub config: Arc<ArcSwap<Config>>,
    pub torrent_manager: Arc<TorrentManager>,
    pub(crate) shutdown: CancellationToken,
    session: Mutex<()>,
}

impl RepairEngine {
    pub fn new(
        rd: Arc<RealDebrid>,
        db: Arc<tokio_rusqlite::Connection>,
        config: Arc<ArcSwap<Config>>,
        torrent_manager: Arc<TorrentManager>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            rd,
            db,
            config,
            torrent_manager,
            shutdown,
            session: Mutex::new(()),
        }
    }

    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            self.run_loop().await;
        });
    }

    pub async fn run_one_pass_for_cli(&self, periodic: bool) {
        let _hold = self.session.lock().await;
        self.run_session_impl(periodic, true).await;
    }

    async fn run_session_impl(&self, periodic: bool, ignore_repair_disabled: bool) {
        let cfg = self.config.load();
        if !ignore_repair_disabled && !cfg.repair.enable {
            return;
        }

        let mut keys: Vec<String> = Vec::new();
        loop {
            let k = self.torrent_manager.repair_queue.lock().await.pop_front();
            if let Some(k) = k {
                keys.push(k);
            } else {
                break;
            }
        }

        if periodic {
            let mut skipped_unrepairable = 0usize;
            let mut periodic_eligible = 0usize;
            for e in self.torrent_manager.torrents.iter() {
                let mt = e.value();
                if mt.unrepairable_reason.is_some() {
                    skipped_unrepairable += 1;
                    continue;
                }
                if periodic_repair_eligible(mt) {
                    periodic_eligible += 1;
                    keys.push(mt.access_key.clone());
                }
            }
            info!(
                periodic_eligible,
                skipped_unrepairable, "Repair engine: periodic candidate scan"
            );
        }

        keys.sort();
        keys.dedup();

        if keys.is_empty() {
            tracing::trace!("Repair engine: nothing to repair (empty queue and no periodic adds)");
            return;
        }

        info!("Repair engine: repairing {} torrent(s)", keys.len());

        for access_key in keys {
            if self.shutdown.is_cancelled() {
                return;
            }
            repair_one::repair_one_torrent(self, &access_key).await;
        }
    }

    async fn run_loop(self: Arc<Self>) {
        time::sleep(Duration::from_secs(10)).await;

        let resume: Vec<String> = self
            .torrent_manager
            .torrents
            .iter()
            .filter(|e| e.value().state == TorrentState::UnderRepair)
            .map(|e| e.key().clone())
            .collect();
        if !resume.is_empty() {
            let mut q = self.torrent_manager.repair_queue.lock().await;
            for k in resume {
                if !q.iter().any(|x| x == &k) {
                    q.push_back(k);
                }
            }
            drop(q);
            self.torrent_manager.repair_notify.notify_one();
        }

        let mut tick = time::interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    info!("Repair engine: shutdown");
                    return;
                }
                _ = self.torrent_manager.repair_notify.notified() => {
                    let _hold = self.session.lock().await;
                    self.run_session_impl(false, false).await;
                }
                _ = tick.tick() => {
                    let cfg = self.config.load();
                    if !cfg.repair.enable {
                        continue;
                    }
                    let wait = self.throttle_wait_duration(&cfg).await;
                    let mut ran_notify_instead = false;
                    if wait > Duration::ZERO {
                        info!(
                            "Repair engine: periodic scan throttled, waiting {:?} (or priority notify)",
                            wait
                        );
                        tokio::select! {
                            biased;
                            _ = self.shutdown.cancelled() => {
                                info!("Repair engine: shutdown");
                                return;
                            }
                            _ = self.torrent_manager.repair_notify.notified() => {
                                let _hold = self.session.lock().await;
                                self.run_session_impl(false, false).await;
                                ran_notify_instead = true;
                            }
                            _ = time::sleep(wait) => {}
                        }
                    }
                    if ran_notify_instead {
                        continue;
                    }
                    let _hold = self.session.lock().await;
                    self.run_session_impl(true, false).await;
                    drop(_hold);
                    if let Err(e) = self.persist_cycle_ts().await {
                        warn!("Repair engine: failed to persist cycle timestamp: {e}");
                    }
                }
            }
        }
    }

    async fn throttle_wait_duration(&self, cfg: &Config) -> Duration {
        let interval = Duration::from_secs(cfg.repair.every_mins.max(1) * 60);
        let key = Db::META_LAST_REPAIR_CYCLE_UNIX.to_string();
        let db = self.db.clone();
        let last: Option<i64> = match db.call(move |conn| Db::get_meta_i64_conn(conn, &key)).await {
            Ok(v) => v,
            Err(e) => {
                warn!("meta read: {e}");
                None
            }
        };
        let now = chrono::Utc::now().timestamp();
        let Some(ts) = last else {
            return Duration::ZERO;
        };
        let age = Duration::from_secs((now - ts).max(0) as u64);
        if age < interval {
            interval - age
        } else {
            Duration::ZERO
        }
    }

    async fn persist_cycle_ts(&self) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        let key = Db::META_LAST_REPAIR_CYCLE_UNIX.to_string();
        let db = self.db.clone();
        db.call(move |conn| Db::set_meta_i64_conn(conn, &key, now))
            .await?;
        Ok(())
    }
}
