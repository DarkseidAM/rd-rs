use std::sync::Arc;

use mockito::Matcher;
use mockito::Server;
use rd_rs::rd::api::UnrestrictCacheKey;
use rd_rs::rd::{RealDebrid, api::new_unrestrict_cache};
use tokio::sync::{Mutex, RwLock};

#[tokio::test]
async fn link_heal_clears_all_token_buckets_for_source_link() {
    use rd_rs::cache::link_heal::{MAX_SESSION_LINK_HEALS, attempt_cdn_link_refresh};
    use rd_rs::rd::client::{DownloadError, RdError};
    use std::sync::atomic::AtomicU32;

    let mut server = Server::new_async().await;
    let mock = server
        .mock("POST", "/rest/1.0/unrestrict/link")
        .match_header("content-type", "application/x-www-form-urlencoded")
        .match_body(Matcher::Regex("link=".into()))
        .with_status(200)
        .with_body(
            r#"{"filename":"f.mkv","filesize":1,"link":"L","download":"https://53.download.real-debrid.com/d/XYZ/file.mkv","streamable":1}"#,
        )
        .expect(1)
        .create_async()
        .await;

    let json = r#"{
        "token": "T0",
        "download_tokens": ["T1"],
        "mount_path": "/tmp/rd-rs-mount",
        "cache_dir": "/tmp/rd-rs-cache",
        "api": { "base_url": "http://127.0.0.1" }
    }"#;
    let mut cfg: rd_rs::config::Config = serde_json::from_str(json).unwrap();
    cfg.api.base_url = server.url();
    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());

    let cache = new_unrestrict_cache();
    let source_link = "https://example.com/source";

    // Pre-seed cache in two token buckets to simulate multi-token stale entries.
    cache.insert(
        UnrestrictCacheKey::from_strs("T0", source_link),
        (
            rd_rs::rd::types::Download {
                download: "https://old0/d/X".into(),
                token: "T0".into(),
                ..Default::default()
            },
            tokio::time::Instant::now(),
        ),
    );
    cache.insert(
        UnrestrictCacheKey::from_strs("T1", source_link),
        (
            rd_rs::rd::types::Download {
                download: "https://old1/d/X".into(),
                token: "T1".into(),
                ..Default::default()
            },
            tokio::time::Instant::now(),
        ),
    );

    let live = RwLock::new("https://old/d/X".to_string());
    let refresh_lock = Mutex::new(());
    let heal_remaining = AtomicU32::new(MAX_SESSION_LINK_HEALS);

    // Any error that triggers refresh via unrestrict.
    let err = RdError::Download(DownloadError::InvalidDownloadCode);

    let did = attempt_cdn_link_refresh(
        &err,
        &rd,
        &cache,
        source_link,
        &live,
        &refresh_lock,
        &heal_remaining,
    )
    .await;
    assert!(did);

    // Both buckets must be cleared before re-unrestricting; after refresh, exactly one fresh entry remains.
    assert_eq!(cache.len(), 1);

    mock.assert_async().await;
}
