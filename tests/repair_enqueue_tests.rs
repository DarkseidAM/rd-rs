//! Repair queue control plane (`enqueue_repair_all`, `enqueue_repair_front`).

use arc_swap::ArcSwap;
use chrono::Utc;
use rd_rs::config::Config;
use rd_rs::db::{Db, TorrentState};
use rd_rs::rd::RealDebrid;
use rd_rs::rd::types::Torrent;
use rd_rs::torrent::{EnqueueRepairAllOptions, ManagedTorrent, TorrentManager};
use std::sync::Arc;

fn test_config() -> Config {
    Config::load("config.toml").unwrap_or_else(|_| {
        let json = r#"{
            "token": "dummy",
            "mount_path": "/tmp/rd-rs-mount",
            "cache_dir": "/tmp/rd-rs-cache"
        }"#;
        serde_json::from_str(json).unwrap()
    })
}

async fn one_torrent_tm() -> (TorrentManager, String) {
    let cfg = test_config();
    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();
    let tm = TorrentManager::new(
        rd,
        Arc::new(db.conn.clone()),
        Arc::new(ArcSwap::from_pointee(cfg)),
    )
    .await
    .unwrap();

    let t = Torrent {
        id: "id1".into(),
        hash: "hash1".into(),
        name: "x.mkv".into(),
        progress: 100,
        status: "downloaded".into(),
        links: vec![],
        added: Utc::now(),
    };
    let key = rd_rs::torrent::access_key(&t.hash, &t.name);
    let mt = Arc::new(ManagedTorrent {
        access_key: key.clone(),
        rd_ids: vec!["id1".into()],
        torrent: t,
        info: None,
        state: TorrentState::Ok,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: None,
    });
    tm.torrents.insert(key.clone(), mt);
    (tm, key)
}

#[tokio::test]
async fn enqueue_repair_all_pushes_all_keys() {
    let (tm, key) = one_torrent_tm().await;
    tm.enqueue_repair_all(EnqueueRepairAllOptions {
        all: true,
        ..Default::default()
    })
    .await;
    assert_eq!(tm.repair_pending_count().await, 1);
    assert_eq!(tm.repair_peek_front().await.as_deref(), Some(key.as_str()));
}

#[tokio::test]
async fn enqueue_repair_all_clears_unrepairable_when_configured() {
    let (tm, key) = one_torrent_tm().await;
    tm.update_torrent_state(&key, TorrentState::Broken, Some("lone_broken".into()))
        .await
        .unwrap();
    tm.enqueue_repair_all(EnqueueRepairAllOptions {
        clear_unrepairable: true,
        ..Default::default()
    })
    .await;
    let mt = tm.torrents.get(&key).unwrap().value().clone();
    assert!(mt.unrepairable_reason.is_none());
}

#[tokio::test]
async fn enqueue_repair_front_moves_to_head() {
    let (tm, k1) = one_torrent_tm().await;
    let t2 = Torrent {
        id: "id2".into(),
        hash: "hash2".into(),
        name: "y.mkv".into(),
        progress: 100,
        status: "downloaded".into(),
        links: vec![],
        added: Utc::now(),
    };
    let k2 = rd_rs::torrent::access_key(&t2.hash, &t2.name);
    tm.torrents.insert(
        k2.clone(),
        Arc::new(ManagedTorrent {
            access_key: k2.clone(),
            rd_ids: vec!["id2".into()],
            torrent: t2,
            info: None,
            state: TorrentState::Ok,
            unrepairable_reason: None,
            last_repaired_at: None,
            file_states: None,
            under_repair_started_at: None,
        }),
    );

    tm.enqueue_repair_all(EnqueueRepairAllOptions {
        all: true,
        ..Default::default()
    })
    .await;
    tm.enqueue_repair_front(&k1, false).await;

    assert_eq!(tm.repair_pending_count().await, 2);
    assert_eq!(tm.repair_peek_front().await.as_deref(), Some(k1.as_str()));
}
