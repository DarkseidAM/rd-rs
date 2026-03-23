//! Refresh vs repair coordination helpers.

use arc_swap::ArcSwap;
use chrono::Utc;
use rd_rs::config::Config;
use rd_rs::db::{Db, TorrentState};
use rd_rs::rd::RealDebrid;
use rd_rs::rd::types::Torrent;
use rd_rs::torrent::refresh::coordination::{
    rd_id_belongs_to_under_repair, skip_local_remove_for_state,
};
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

#[test]
fn skip_local_remove_only_under_repair() {
    assert!(skip_local_remove_for_state(TorrentState::UnderRepair));
    assert!(!skip_local_remove_for_state(TorrentState::Ok));
    assert!(!skip_local_remove_for_state(TorrentState::Broken));
}

#[tokio::test]
async fn rd_id_under_repair_detected() {
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
        id: "rd1".into(),
        hash: "h1".into(),
        name: "n1".into(),
        progress: 100,
        status: "downloaded".into(),
        links: vec![],
        added: Utc::now(),
    };
    let key = rd_rs::torrent::access_key(&t.hash, &t.name);
    let mt = Arc::new(ManagedTorrent {
        access_key: key.clone(),
        rd_ids: vec!["rd1".into()],
        torrent: t,
        info: None,
        state: TorrentState::UnderRepair,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: Some(Utc::now().timestamp()),
    });
    tm.torrents.insert(key, mt);

    assert!(rd_id_belongs_to_under_repair(&tm, "rd1"));
    assert!(!rd_id_belongs_to_under_repair(&tm, "other"));
}
