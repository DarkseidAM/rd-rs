//! Integration tests for refresh diff logic: added, removed, changed.

use chrono::Utc;
use dashmap::DashMap;
use rd_rs::db::TorrentState;
use rd_rs::rd::types::Torrent;
use rd_rs::torrent::refresh::{DiffResult, diff};
use rd_rs::torrent::{ManagedTorrent, access_key};
use std::sync::Arc;

fn make_torrent(id: &str, hash: &str, name: &str, progress: u8, status: &str) -> Torrent {
    Torrent {
        id: id.to_string(),
        hash: hash.to_string(),
        name: name.to_string(),
        progress,
        status: status.to_string(),
        links: vec![],
        added: Utc::now(),
    }
}

fn make_managed(t: &Torrent, rd_ids: Vec<String>) -> Arc<ManagedTorrent> {
    Arc::new(ManagedTorrent {
        access_key: access_key(&t.hash, &t.name),
        rd_ids,
        torrent: t.clone(),
        info: None,
        state: TorrentState::Ok,
        unrepairable_reason: None,
        last_repaired_at: None,
        file_states: None,
        under_repair_started_at: None,
    })
}

#[test]
fn diff_added_removed_changed() {
    let t1 = make_torrent("id1", "hash1", "A", 100, "downloaded");
    let t2 = make_torrent("id2", "hash2", "B", 100, "downloaded");
    let t3_updated = make_torrent("id3", "hash3", "C", 100, "downloaded");
    let t3_old = make_torrent("id3", "hash3", "C", 50, "downloading");

    let current = DashMap::new();
    current.insert(
        access_key("hash1", "A"),
        make_managed(&t1, vec!["id1".into()]),
    );
    current.insert(
        access_key("hash3", "C"),
        make_managed(&t3_old, vec!["id3".into()]),
    );
    current.insert(
        access_key("hash2", "B"),
        make_managed(&t2, vec!["id2".into()]),
    );

    let fresh = vec![
        t1.clone(),
        t3_updated.clone(),
        make_torrent("id4", "hash4", "D", 100, "downloaded"),
    ];

    let result: DiffResult = diff(&current, &fresh);

    assert_eq!(result.added.len(), 1);
    assert_eq!(result.added[0].0.id, "id4");
    assert_eq!(result.removed_keys.len(), 1);
    assert_eq!(result.removed_keys[0], "hash2/B");
    assert_eq!(result.changed.len(), 1);
    assert_eq!(result.changed[0].0.id, "id3");
    assert_eq!(result.changed[0].0.progress, 100);
}

#[test]
fn diff_multi_id_grouping() {
    let t1_base = make_torrent("id1", "hash1", "Pack", 100, "downloaded");

    let current = DashMap::new();
    let old_rd_ids = vec!["id1".to_string(), "id2".to_string()];
    current.insert(
        access_key("hash1", "Pack"),
        make_managed(&t1_base, old_rd_ids),
    );

    // Fresh list has id1, id2, and a new id3 for the same hash!
    // And it has an updated progress/status.
    let fresh = vec![
        make_torrent("id1", "hash1", "Pack", 100, "downloaded"),
        make_torrent("id2", "hash1", "Pack", 100, "downloaded"),
        make_torrent("id3", "hash1", "Pack", 100, "downloaded"),
    ];

    let result = diff(&current, &fresh);

    // Should detect 1 changed item (the pack)
    assert_eq!(result.added.len(), 0);
    assert_eq!(result.removed_keys.len(), 0);
    assert_eq!(result.changed.len(), 1);

    // The changed ids should contain id1, id2, and id3, length 3
    let changed_ids = &result.changed[0].1;
    assert_eq!(changed_ids.len(), 3);
    assert!(changed_ids.contains(&"id1".to_string()));
    assert!(changed_ids.contains(&"id2".to_string()));
    assert!(changed_ids.contains(&"id3".to_string()));
    assert_eq!(result.duplicates, 2); // 3 incoming torrents map to 1 key -> 2 duplicates
}
