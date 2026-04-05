//! `TorrentManager` persistence: `file_states`, `under_repair_started_at`, snapshots.

use std::collections::HashMap;

use arc_swap::ArcSwap;
use chrono::Utc;
use rd_rs::config::Config;
use rd_rs::db::{Db, TorrentState};
use rd_rs::rd::RealDebrid;
use rd_rs::rd::api::new_unrestrict_cache;
use rd_rs::rd::types::Torrent;
use rd_rs::torrent::{ManagedTorrent, TorrentManager};
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

fn dummy_torrent(id: &str, hash: &str, name: &str) -> Torrent {
    Torrent {
        id: id.to_string(),
        hash: hash.to_string(),
        name: name.to_string(),
        progress: 100,
        status: "downloaded".to_string(),
        links: vec![],
        added: Utc::now(),
    }
}

async fn setup_tm() -> (TorrentManager, Db, String) {
    let cfg = test_config();
    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();
    let tm = TorrentManager::new(
        rd,
        Arc::new(db.conn.clone()),
        Arc::new(ArcSwap::from_pointee(cfg)),
        new_unrestrict_cache(),
    )
    .await
    .unwrap();
    let t = dummy_torrent("id1", "hash1", "Show.mkv");
    let access_key = rd_rs::torrent::access_key(&t.hash, &t.name);
    let mt = Arc::new(ManagedTorrent {
        access_key: access_key.clone(),
        rd_ids: vec!["id1".into()],
        torrent: t,
        info: None,
        state: TorrentState::Ok,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: None,
    });
    tm.torrents.insert(access_key.clone(), mt);
    (tm, db, access_key)
}

#[tokio::test]
async fn mark_file_broken_persists_file_states_json() {
    let (tm, db, key) = setup_tm().await;
    let path = "/Season 1/episode.mkv";

    tm.mark_file_broken(&key, path).await.unwrap();

    let in_mem = tm.torrents.get(&key).unwrap().value().clone();
    let fs = in_mem.file_states.as_ref().unwrap();
    assert_eq!(fs.get(path).map(String::as_str), Some("broken"));

    let rows = db.get_all_torrents().await.unwrap();
    assert_eq!(rows.len(), 1);
    let raw = rows[0].file_states.as_ref().expect("file_states column");
    let parsed: HashMap<String, String> = serde_json::from_str(raw).unwrap();
    assert_eq!(parsed.get(path).map(String::as_str), Some("broken"));
}

#[tokio::test]
async fn under_repair_started_at_set_once_and_cleared_on_ok() {
    let (tm, db, key) = setup_tm().await;

    tm.update_torrent_state(&key, TorrentState::Broken, None)
        .await
        .unwrap();
    tm.update_torrent_state(&key, TorrentState::UnderRepair, None)
        .await
        .unwrap();

    let ts1 = tm
        .torrents
        .get(&key)
        .unwrap()
        .under_repair_started_at
        .expect("under repair should set timestamp");
    assert!(ts1 > 0);

    let row1 = &db.get_all_torrents().await.unwrap()[0];
    assert_eq!(row1.under_repair_started_at, Some(ts1));

    tm.update_torrent_state(&key, TorrentState::UnderRepair, None)
        .await
        .unwrap();
    let ts2 = tm
        .torrents
        .get(&key)
        .unwrap()
        .under_repair_started_at
        .unwrap();
    assert_eq!(ts1, ts2, "staying under repair must not reset clock");

    tm.update_torrent_state(&key, TorrentState::Ok, None)
        .await
        .unwrap();
    assert!(
        tm.torrents
            .get(&key)
            .unwrap()
            .under_repair_started_at
            .is_none()
    );
    let row_ok = &db.get_all_torrents().await.unwrap()[0];
    assert!(row_ok.under_repair_started_at.is_none());
}

#[tokio::test]
async fn persist_torrent_snapshot_writes_without_state_change() {
    let (tm, db, key) = setup_tm().await;

    tm.update_torrent_state(&key, TorrentState::Ok, None)
        .await
        .unwrap();

    let mut mt = tm.torrents.get(&key).unwrap().value().as_ref().clone();
    let mut fs = HashMap::new();
    fs.insert("/foo.mkv".into(), "ok".into());
    mt.file_states = Some(fs.clone());

    tm.persist_torrent_snapshot(&mt).await.unwrap();

    let loaded = tm.torrents.get(&key).unwrap().value().clone();
    assert_eq!(loaded.file_states, Some(fs));

    let raw = db.get_all_torrents().await.unwrap()[0]
        .file_states
        .clone()
        .expect("snapshot should persist file_states");
    let parsed: HashMap<String, String> = serde_json::from_str(&raw).unwrap();
    assert_eq!(parsed.get("/foo.mkv").map(String::as_str), Some("ok"));
}
