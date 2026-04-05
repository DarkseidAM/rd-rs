use rd_rs::db::{Db, RepairJobRow, RepairJobStatus, TorrentRow, TorrentState};
use rusqlite::params;
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
async fn app_meta_i64_roundtrip() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();
    db.set_meta_i64("test_meta_key", 1_700_000_042)
        .await
        .unwrap();
    assert_eq!(
        db.get_meta_i64("test_meta_key").await.unwrap(),
        Some(1_700_000_042)
    );
}

#[tokio::test]
async fn insert_repair_job_on_conflict_updates_row() {
    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();

    let j1 = RepairJobRow {
        id: "job-upsert".into(),
        torrent_key: "key1".into(),
        strategy: "first".into(),
        status: RepairJobStatus::Pending,
        started_at: None,
        completed_at: None,
    };
    db.insert_repair_job(&j1).await.unwrap();

    let count: i64 = db
        .conn
        .call(|c| c.query_row("SELECT COUNT(*) FROM repair_jobs", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(count, 1);

    let j2 = RepairJobRow {
        id: "job-upsert".into(),
        torrent_key: "key2".into(),
        strategy: "second".into(),
        status: RepairJobStatus::Pending,
        started_at: Some(99),
        completed_at: None,
    };
    db.insert_repair_job(&j2).await.unwrap();

    let count2: i64 = db
        .conn
        .call(|c| c.query_row("SELECT COUNT(*) FROM repair_jobs", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(count2, 1);

    let (strategy, torrent_key, started): (String, String, Option<i64>) = db
        .conn
        .call(|c| {
            c.query_row(
                "SELECT strategy, torrent_key, started_at FROM repair_jobs WHERE id = ?1",
                params!["job-upsert"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
        })
        .await
        .unwrap();
    assert_eq!(strategy, "second");
    assert_eq!(torrent_key, "key2");
    assert_eq!(started, Some(99));
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
