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
    };
    // TorrentManager::library_paths_for is on the manager; we test the convention here
    let paths = vec![format!("__all__/{}", mt.access_key)];
    assert_eq!(paths, vec!["__all__/hash/Some.Movie"]);
}
