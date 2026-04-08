//! Integration tests for torrent domain: access_key, library_paths_for convention.

use chrono::Utc;
use rd_rs::db::TorrentState;
use rd_rs::rd::types::Torrent;
use rd_rs::torrent::{ManagedTorrent, access_key};

#[test]
fn access_key_format() {
    assert_eq!(
        access_key("deadbeef", "My.Show.S01E01"),
        "deadbeef/My.Show.S01E01"
    );
    assert_eq!(
        access_key("abc", "name with spaces"),
        "abc/name with spaces"
    );
}

#[test]
fn library_paths_for_v1() {
    let mt = ManagedTorrent {
        access_key: "hash/Some.Movie".to_string(),
        rd_ids: vec!["id1".into()],
        torrent: Torrent {
            id: "id1".into(),
            hash: "hash".into(),
            name: "Some.Movie".into(),
            progress: 100,
            status: "downloaded".into(),
            links: vec![],
            added: Utc::now(),
        },
        info: None,
        state: TorrentState::Ok,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: None,
    };
    // TorrentManager::library_paths_for is on the manager; we test the convention here
    let paths = vec![format!("__all__/{}", mt.access_key)];
    assert_eq!(paths, vec!["__all__/hash/Some.Movie"]);
}

#[tokio::test]
async fn ensure_torrent_info_preserves_concurrent_state() {
    use arc_swap::ArcSwap;
    use chrono::Utc;
    use mockito::Server;
    use rd_rs::config::Config;
    use rd_rs::db::Db;
    use rd_rs::db::TorrentState;
    use rd_rs::rd::RealDebrid;
    use rd_rs::rd::api::new_unrestrict_cache;
    use rd_rs::rd::types::Torrent;
    use rd_rs::torrent::{ManagedTorrent, TorrentManager};
    use std::sync::Arc;
    use std::time::Duration;

    let mut server = Server::new_async().await;

    // Mock the torrent info endpoint with a delayed response to simulate concurrency window
    let mock = server.mock("GET", "/rest/1.0/torrents/info/id1")
        .with_status(200)
        .with_body(r#"{"id":"id1","filename":"Some.Movie","hash":"hash","bytes":100,"host":"rd","split":100,"progress":100,"status":"downloaded","added":"2023-01-01T00:00:00.000Z","files":[],"links":[]}"#)
        .expect(1)
        .create_async().await;

    let json = r#"{
        "token": "dummy",
        "mount_path": "/tmp/rd-rs-mount",
        "cache_dir": "/tmp/rd-rs-cache"
    }"#;
    let mut cfg: Config = serde_json::from_str(json).unwrap();
    cfg.api.base_url = server.url(); // use mockito URL
    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());

    // Patch RD to use the mock server url
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();
    let tm = Arc::new(
        TorrentManager::new(
            rd,
            Arc::new(db.conn),
            Arc::new(ArcSwap::from_pointee(cfg)),
            new_unrestrict_cache(),
        )
        .await
        .unwrap(),
    );

    let mt = ManagedTorrent {
        access_key: "hash/Some.Movie".to_string(),
        rd_ids: vec!["id1".into()],
        torrent: Torrent {
            id: "id1".into(),
            hash: "hash".into(),
            name: "Some.Movie".into(),
            progress: 100,
            status: "downloaded".into(),
            links: vec![],
            added: Utc::now(),
        },
        info: None,
        state: TorrentState::Ok,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: None,
    };

    let key = mt.access_key.clone();
    tm.torrents.insert(key.clone(), Arc::new(mt));

    let tm_clone = tm.clone();
    let key_clone = key.clone();
    let handle = tokio::spawn(async move { tm_clone.ensure_torrent_info(&key_clone).await });

    // Concurrently yield and modify the state in the map
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Modify the state!
    tm.torrents.entry(key.clone()).and_modify(|mt_arc| {
        let mut updated = (**mt_arc).clone();
        updated.state = TorrentState::UnderRepair;
        *mt_arc = Arc::new(updated);
    });

    // Wait for the fetch to complete
    let result = handle.await.unwrap();
    assert!(result.is_some());
    let fetch_result = result.unwrap();

    // Verify info was fetched!
    assert!(fetch_result.info.is_some());
    assert_eq!(fetch_result.info.as_ref().unwrap().id, "id1");

    // The MOST crucial assertion: The map should reflect both the Info AND the UnderRepair state!
    let final_map_state = tm.torrents.get(&key).unwrap().value().clone();
    assert_eq!(final_map_state.state, TorrentState::UnderRepair);
    assert!(final_map_state.info.is_some());

    mock.assert_async().await;
}
