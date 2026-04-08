use arc_swap::ArcSwap;
use rd_rs::config::Config;
use rd_rs::db::Db;
use rd_rs::rd::RealDebrid;
use rd_rs::rd::api::new_unrestrict_cache;
use rd_rs::repair::engine::RepairEngine;
use rd_rs::torrent::TorrentManager;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_repair_engine_spawn_and_shutdown() {
    let mut cfg = Config::load("config.toml").unwrap_or_else(|_| {
        let json = r#"{
            "token": "dummy",
            "mount_path": "/tmp/rd-rs-mount",
            "cache_dir": "/tmp/rd-rs-cache"
        }"#;
        serde_json::from_str(json).unwrap()
    });

    // speed up loop for integration test
    cfg.repair.every_mins = 1;

    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());

    let db = Db::new_in_memory().await.unwrap();
    db.init_schema().await.unwrap();

    let tm = TorrentManager::new(
        rd.clone(),
        Arc::new(db.conn.clone()),
        Arc::new(ArcSwap::from_pointee(cfg.clone())),
        new_unrestrict_cache(),
    )
    .await
    .unwrap();
    let tm_arc = Arc::new(tm);

    // Instantiate and spawn the repair engine
    let engine = Arc::new(RepairEngine::new(
        rd.clone(),
        Arc::new(db.conn.clone()),
        Arc::new(ArcSwap::from_pointee(cfg)),
        tm_arc.clone(),
        tm_arc.cancel_token(),
    ));

    engine.spawn();

    // Sleep briefly to ensure it doesn't panic on startup
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown manager which effectively stops any internal things (though repair loop doesn't have a shutdown token yet, we just verify it spawns)
    tm_arc.shutdown();

    // Test passes if no panic
}
