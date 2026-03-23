use arc_swap::ArcSwap;
use chrono::Utc;
use rd_rs::config::Config;
use rd_rs::db::{Db, TorrentState};
use rd_rs::rd::RealDebrid;
use rd_rs::rd::types::Torrent;
use rd_rs::repair::reasons;
use rd_rs::repair::{Strategy, UnrepairableReason};
use rd_rs::torrent::{ManagedTorrent, TorrentManager};
use std::sync::Arc;

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

#[tokio::test]
async fn test_torrent_state_transitions() {
    let cfg = Config::load("config.toml").unwrap_or_else(|_| {
        // Provide dummy config if config.toml parsing fails in CI
        // Since Config lacks Default, we construct it manually
        let json = r#"{
            "token": "dummy",
            "mount_path": "/tmp/rd-rs-mount",
            "cache_dir": "/tmp/rd-rs-cache"
        }"#;
        serde_json::from_str(json).unwrap()
    });
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

    let t = dummy_torrent("id1", "hash1", "My.Movie.mkv");
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

    // Insert into DashMap directly as if loaded
    tm.torrents.insert(access_key.clone(), mt);

    // 1. Update to Broken
    tm.update_torrent_state(&access_key, TorrentState::Broken, None)
        .await
        .unwrap();

    // Check DashMap
    let updated = tm.torrents.get(&access_key).unwrap().value().clone();
    assert_eq!(updated.state, TorrentState::Broken);
    assert_eq!(updated.unrepairable_reason, None);

    // Check DB
    let db_rows = db.get_all_torrents().await.unwrap();
    assert_eq!(db_rows.len(), 1);
    assert_eq!(db_rows[0].state, TorrentState::Broken);
    assert_eq!(db_rows[0].unrepairable_reason, None);

    // 2. Update to Broken + UnrepairableReason
    tm.update_torrent_state(
        &access_key,
        TorrentState::Broken,
        Some(UnrepairableReason::LoneBroken.to_string()),
    )
    .await
    .unwrap();

    let updated2 = tm.torrents.get(&access_key).unwrap().value().clone();
    assert_eq!(updated2.state, TorrentState::Broken);
    assert_eq!(updated2.unrepairable_reason.as_deref(), Some("lone_broken"));

    let db_rows2 = db.get_all_torrents().await.unwrap();
    assert_eq!(
        db_rows2[0].unrepairable_reason.as_deref(),
        Some("lone_broken")
    );

    // 3. Update to Ok (Successful repair)
    tm.update_torrent_state(&access_key, TorrentState::Ok, None)
        .await
        .unwrap();

    let updated3 = tm.torrents.get(&access_key).unwrap().value().clone();
    assert_eq!(updated3.state, TorrentState::Ok);
    assert_eq!(updated3.unrepairable_reason, None);
    assert!(updated3.last_repaired_at.is_some());
}

#[test]
fn test_repair_types_display_serialization() {
    assert_eq!(Strategy::ReinsertTorrent.to_string(), "reinsert_torrent");
    assert_eq!(Strategy::IndividualFiles.to_string(), "individual_files");
    assert_eq!(Strategy::ArchiveAll.to_string(), "archive_all");
    assert_eq!(Strategy::BatchDownload.to_string(), "batch_download");

    assert_eq!(UnrepairableReason::LoneBroken.to_string(), "lone_broken");
    assert_eq!(
        UnrepairableReason::NoRepairableFiles.to_string(),
        "no_repairable_files"
    );
    assert_eq!(
        UnrepairableReason::DuplicateFileIDs.to_string(),
        "duplicate_file_ids"
    );
    assert_eq!(
        UnrepairableReason::InvalidFileIDs.to_string(),
        "invalid_file_ids"
    );
    assert_eq!(
        UnrepairableReason::NotCached.to_string(),
        reasons::NOT_CACHED
    );
}
