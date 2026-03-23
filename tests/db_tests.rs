use rd_rs::db::{Db, RepairJobRow, RepairJobStatus, TorrentRow, TorrentState};
use std::str::FromStr;

fn make_row(key: &str) -> TorrentRow {
    TorrentRow {
        access_key: key.to_string(),
        rd_ids: vec!["id1".into()],
        hash: "deadbeef".into(),
        name: key.to_string(),
        state: TorrentState::Ok,
        unrepairable_reason: None,
        file_states: None,
        last_seen_at: Some(1_700_000_000),
        last_repaired_at: None,
        under_repair_started_at: None,
    }
}

#[tokio::test]
async fn schema_creates_core_tables() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();
    let c1 = db.table_count("torrents").await.unwrap();
    let c2 = db.table_count("repair_jobs").await.unwrap();
    let c3 = db.table_count("app_meta").await.unwrap();
    assert_eq!(
        c1 + c2 + c3,
        0,
        "torrents, repair_jobs, app_meta should exist and be empty"
    );
}

#[tokio::test]
async fn upsert_and_load() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();

    db.upsert_torrent(&make_row("key1")).await.unwrap();
    db.upsert_torrent(&make_row("key2")).await.unwrap();

    let rows = db.get_all_torrents().await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn batch_upsert_deduplicates() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();

    let rows: Vec<_> = (0..5).map(|i| make_row(&format!("key{i}"))).collect();
    for r in &rows {
        db.upsert_torrent(r).await.unwrap();
    }
    assert_eq!(db.get_all_torrents().await.unwrap().len(), 5);

    // Upsert 3 of the same keys — should not increase row count
    let upd: Vec<_> = (0..3).map(|i| make_row(&format!("key{i}"))).collect();
    for r in &upd {
        db.upsert_torrent(r).await.unwrap();
    }
    assert_eq!(db.get_all_torrents().await.unwrap().len(), 5);
}

#[tokio::test]
async fn state_roundtrip() {
    assert_eq!(TorrentState::from_str("ok").unwrap(), TorrentState::Ok);
    assert_eq!(
        TorrentState::from_str("broken").unwrap(),
        TorrentState::Broken
    );
    assert_eq!(
        TorrentState::from_str("under_repair").unwrap(),
        TorrentState::UnderRepair
    );
    assert!(TorrentState::from_str("unknown").is_err());
}

#[tokio::test]
async fn insert_and_update_repair_job() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();

    let mut job = RepairJobRow {
        id: "job-1".into(),
        torrent_key: "key1".into(),
        strategy: "rehash".into(),
        status: RepairJobStatus::Pending,
        started_at: None,
        completed_at: None,
    };
    db.insert_repair_job(&job).await.unwrap();

    job.status = RepairJobStatus::Done;
    job.completed_at = Some(1_700_000_001);
    db.update_repair_job(&job).await.unwrap();
}
