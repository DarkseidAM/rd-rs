//! Single-torrent repair pass (preflight + cascade).

use std::time::Duration;

use tokio::time;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::db::{Db, RepairJobRow, RepairJobStatus, TorrentState};

use super::RepairEngine;
use crate::repair::CascadeOutcome;
use crate::repair::capacity::wait_for_repair_capacity;
use crate::repair::preflight::{self, PreflightOutcome};
use crate::repair::strategies;

pub(super) async fn repair_one_torrent(engine: &RepairEngine, access_key: &str) {
    let cfg = engine.config.load();
    let timeout = Duration::from_secs(cfg.repair.timeout_mins.max(1) * 60);
    let now = chrono::Utc::now().timestamp();

    let Some(mt) = engine.torrent_manager.ensure_torrent_info(access_key).await else {
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

    if mt.state == TorrentState::UnderRepair
        && let Some(ts) = mt.under_repair_started_at
        && now - ts > cfg.repair.timeout_mins.max(1) as i64 * 60
    {
        warn!(
            "Repair timeout (stuck under_repair) for {}, resetting to broken",
            access_key
        );
        let _ = engine
            .torrent_manager
            .update_torrent_state(access_key, TorrentState::Broken, None)
            .await;
        return;
    }

    if !wait_for_repair_capacity(&engine.rd, &engine.shutdown).await {
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
    if let Err(e) = Db::insert_repair_job_on_conn(&engine.db, &job).await {
        warn!("repair_jobs insert: {e}");
    }

    if let Err(e) = engine
        .torrent_manager
        .update_torrent_state(access_key, TorrentState::UnderRepair, None)
        .await
    {
        error!("set under_repair for {}: {e}", access_key);
        return;
    }

    let Some(mt) = engine.torrent_manager.ensure_torrent_info(access_key).await else {
        return;
    };

    let cache = engine.torrent_manager.unrestrict_cache.clone();
    let pre = preflight::run_preflight(&engine.rd, &cache, (*mt).clone()).await;

    let mt_for_cascade = match pre {
        PreflightOutcome::DeferBandwidth => {
            let mut failed = job.clone();
            failed.status = RepairJobStatus::Failed;
            failed.completed_at = Some(chrono::Utc::now().timestamp());
            let _ = Db::update_repair_job_on_conn(&engine.db, &failed).await;
            let _ = engine
                .torrent_manager
                .update_torrent_state(access_key, TorrentState::Broken, None)
                .await;
            return;
        }
        PreflightOutcome::VerifiedOk(m) => {
            if let Err(e) = engine.torrent_manager.persist_torrent_snapshot(&m).await {
                warn!("persist after preflight verify: {e}");
            }
            let mut done = job.clone();
            done.status = RepairJobStatus::Done;
            done.completed_at = Some(chrono::Utc::now().timestamp());
            let _ = Db::update_repair_job_on_conn(&engine.db, &done).await;
            if let Err(e) = engine
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
            if let Err(e) = engine.torrent_manager.persist_torrent_snapshot(&m).await {
                warn!("persist after preflight: {e}");
            }
            m
        }
    };

    let rd = engine.rd.clone();
    let repair_cfg = cfg.repair.clone();
    let cascade =
        async move { strategies::execute_cascade(&rd, &mt_for_cascade, &repair_cfg).await };

    let outcome = match time::timeout(timeout, cascade).await {
        Ok(o) => o,
        Err(_) => {
            warn!("Repair cascade timeout for {}", access_key);
            let mut failed = job;
            failed.status = RepairJobStatus::Failed;
            failed.completed_at = Some(chrono::Utc::now().timestamp());
            let _ = Db::update_repair_job_on_conn(&engine.db, &failed).await;
            let _ = engine
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
            let _ = Db::update_repair_job_on_conn(&engine.db, &done).await;
            if let Err(e) = engine
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
            let _ = Db::update_repair_job_on_conn(&engine.db, &done).await;
            warn!("Unrepairable {}: {}", access_key, reason);
            if let Err(e) = engine
                .torrent_manager
                .update_torrent_state(access_key, TorrentState::Broken, Some(reason.to_string()))
                .await
            {
                error!("persist unrepairable for {}: {e}", access_key);
            }
        }
        CascadeOutcome::UnrepairableMsg(msg) => {
            done.status = RepairJobStatus::Failed;
            let _ = Db::update_repair_job_on_conn(&engine.db, &done).await;
            warn!("Unrepairable {}: {}", access_key, msg);
            if let Err(e) = engine
                .torrent_manager
                .update_torrent_state(access_key, TorrentState::Broken, Some(msg))
                .await
            {
                error!("persist unrepairable for {}: {e}", access_key);
            }
        }
        CascadeOutcome::DeferBandwidth => {
            done.status = RepairJobStatus::Failed;
            let _ = Db::update_repair_job_on_conn(&engine.db, &done).await;
            warn!(
                "Repair deferred (bandwidth) for {}; will retry later",
                access_key
            );
            let _ = engine
                .torrent_manager
                .update_torrent_state(access_key, TorrentState::Broken, None)
                .await;
        }
    }
}
