//! Background repair loop: throttled periodic scan, priority queue, single-flight sessions.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::db::{Db, RepairJobRow, RepairJobStatus, TorrentState};
use crate::rd::RealDebrid;
use crate::torrent::TorrentManager;

use super::CascadeOutcome;
use super::capacity::wait_for_repair_capacity;
use super::detect::{path_looks_playable, unassigned_selected_link_count};
use super::preflight::{self, PreflightOutcome};

/// Coordinates background repair (one session at a time; mirrors zurg repair queue).
pub struct RepairEngine {
    pub rd: Arc<RealDebrid>,
    pub db: Arc<tokio_rusqlite::Connection>,
    pub config: Arc<ArcSwap<Config>>,
    pub torrent_manager: Arc<TorrentManager>,
    shutdown: CancellationToken,
    /// Only one repair session (periodic or notify-driven) runs at a time.
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

    /// One repair pass (drains the priority queue; optionally adds periodic-eligible torrents).
    ///
    /// Used by the `rd-rs repair` CLI. Ignores `repair.enable = false` so operators can still
    /// trigger a pass. Does not start the background loop.
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
            for e in self.torrent_manager.torrents.iter() {
                let mt = e.value();
                if mt.unrepairable_reason.is_some() {
                    continue;
                }
                let unassigned = mt
                    .info
                    .as_ref()
                    .is_some_and(|info| unassigned_selected_link_count(info) > 0);
                let file_broken = mt.file_states.as_ref().is_some_and(|fs| {
                    fs.iter()
                        .any(|(p, s)| s == "broken" && path_looks_playable(p))
                });
                let eligible = mt.state == TorrentState::Broken
                    || mt.state == TorrentState::UnderRepair
                    || (mt.state == TorrentState::Ok && (unassigned || file_broken));
                if eligible {
                    keys.push(mt.access_key.clone());
                }
            }
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
            self.repair_one_torrent(&access_key).await;
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
                    // Do not sleep across the outer `select!` — that would starve `repair_notify`
                    // (FUSE enqueue) for up to `every_mins`. Wait only in an inner select.
                    let wait = self.throttle_wait_duration(&cfg).await;
                    let mut ran_notify_instead = false;
                    if wait > Duration::ZERO {
                        debug!(
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

    /// How long to wait before running a periodic repair cycle (`Duration::ZERO` = run now).
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

    async fn repair_one_torrent(&self, access_key: &str) {
        let cfg = self.config.load();
        let timeout = Duration::from_secs(cfg.repair.timeout_mins.max(1) * 60);
        let now = chrono::Utc::now().timestamp();

        let Some(mt) = self.torrent_manager.ensure_torrent_info(access_key).await else {
            warn!(
                key = %access_key,
                "repair_one: torrent not in registry (ensure_torrent_info returned none)"
            );
            return;
        };
        if let Some(reason) = mt.unrepairable_reason.as_deref() {
            warn!(
                key = %access_key,
                reason = %reason,
                "repair_one: skipped (unrepairable set in DB — no delete/reinsert; clear `unrepairable` or `rd-rs repair --clear-unrepairable`)"
            );
            return;
        }

        // zurg `isRepairTooLong` — only while already under repair.
        if mt.state == TorrentState::UnderRepair
            && let Some(ts) = mt.under_repair_started_at
            && now - ts > cfg.repair.timeout_mins.max(1) as i64 * 60
        {
            warn!(
                "Repair timeout (stuck under_repair) for {}, resetting to broken",
                access_key
            );
            let _ = self
                .torrent_manager
                .update_torrent_state(access_key, TorrentState::Broken, None)
                .await;
            return;
        }

        // zurg `canCapacityHandle` before entering under_repair.
        if !wait_for_repair_capacity(&self.rd, &self.shutdown).await {
            warn!("Repair skipped for {} (RD capacity / shutdown)", access_key);
            return;
        }

        let job_id = Uuid::new_v4().to_string();
        let now_ts = chrono::Utc::now().timestamp();
        let job = RepairJobRow {
            id: job_id.clone(),
            torrent_key: access_key.to_string(),
            strategy: "cascade".to_string(),
            status: RepairJobStatus::Running,
            started_at: Some(now_ts),
            completed_at: None,
        };
        if let Err(e) = Db::insert_repair_job_on_conn(&self.db, &job).await {
            warn!("repair_jobs insert: {e}");
        }

        if let Err(e) = self
            .torrent_manager
            .update_torrent_state(access_key, TorrentState::UnderRepair, None)
            .await
        {
            error!("set under_repair for {}: {e}", access_key);
            return;
        }

        let Some(mt) = self.torrent_manager.ensure_torrent_info(access_key).await else {
            return;
        };

        let cache = self.torrent_manager.unrestrict_cache.clone();
        let pre = preflight::run_preflight(&self.rd, &cache, (*mt).clone()).await;

        let mt_for_cascade = match pre {
            PreflightOutcome::DeferBandwidth => {
                let mut failed = job.clone();
                failed.status = RepairJobStatus::Failed;
                failed.completed_at = Some(chrono::Utc::now().timestamp());
                let _ = Db::update_repair_job_on_conn(&self.db, &failed).await;
                let _ = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Broken, None)
                    .await;
                return;
            }
            PreflightOutcome::VerifiedOk(m) => {
                if let Err(e) = self.torrent_manager.persist_torrent_snapshot(&m).await {
                    warn!("persist after preflight verify: {e}");
                }
                let mut done = job.clone();
                done.status = RepairJobStatus::Done;
                done.completed_at = Some(chrono::Utc::now().timestamp());
                let _ = Db::update_repair_job_on_conn(&self.db, &done).await;
                if let Err(e) = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Ok, None)
                    .await
                {
                    error!("persist ok after preflight for {}: {e}", access_key);
                } else {
                    info!("Repaired torrent {} (preflight verify only)", access_key);
                }
                return;
            }
            PreflightOutcome::Proceed(m) => {
                if let Err(e) = self.torrent_manager.persist_torrent_snapshot(&m).await {
                    warn!("persist after preflight: {e}");
                }
                m
            }
        };

        let rd = self.rd.clone();
        let repair_cfg = cfg.repair.clone();
        let cascade = async move {
            super::strategies::execute_cascade(&rd, &mt_for_cascade, &repair_cfg).await
        };

        let outcome = match time::timeout(timeout, cascade).await {
            Ok(o) => o,
            Err(_) => {
                warn!("Repair cascade timeout for {}", access_key);
                let mut failed = job;
                failed.status = RepairJobStatus::Failed;
                failed.completed_at = Some(chrono::Utc::now().timestamp());
                let _ = Db::update_repair_job_on_conn(&self.db, &failed).await;
                let _ = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Broken, None)
                    .await;
                return;
            }
        };

        let mut done = job;
        done.completed_at = Some(chrono::Utc::now().timestamp());

        match outcome {
            CascadeOutcome::Success => {
                done.status = RepairJobStatus::Done;
                let _ = Db::update_repair_job_on_conn(&self.db, &done).await;
                if let Err(e) = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Ok, None)
                    .await
                {
                    error!("persist ok for {}: {e}", access_key);
                } else {
                    info!("Repaired torrent {}", access_key);
                }
            }
            CascadeOutcome::Unrepairable(reason) => {
                done.status = RepairJobStatus::Failed;
                let _ = Db::update_repair_job_on_conn(&self.db, &done).await;
                warn!("Unrepairable {}: {}", access_key, reason);
                if let Err(e) = self
                    .torrent_manager
                    .update_torrent_state(
                        access_key,
                        TorrentState::Broken,
                        Some(reason.to_string()),
                    )
                    .await
                {
                    error!("persist unrepairable for {}: {e}", access_key);
                }
            }
            CascadeOutcome::UnrepairableMsg(msg) => {
                done.status = RepairJobStatus::Failed;
                let _ = Db::update_repair_job_on_conn(&self.db, &done).await;
                warn!("Unrepairable {}: {}", access_key, msg);
                if let Err(e) = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Broken, Some(msg))
                    .await
                {
                    error!("persist unrepairable for {}: {e}", access_key);
                }
            }
            CascadeOutcome::DeferBandwidth => {
                done.status = RepairJobStatus::Failed;
                let _ = Db::update_repair_job_on_conn(&self.db, &done).await;
                warn!(
                    "Repair deferred (bandwidth) for {}; will retry later",
                    access_key
                );
                let _ = self
                    .torrent_manager
                    .update_torrent_state(access_key, TorrentState::Broken, None)
                    .await;
            }
        }
    }
}
