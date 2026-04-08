use std::collections::HashMap;
use std::sync::Arc;

use mockito::Matcher;
use mockito::Server;
use rd_rs::config::Config;
use rd_rs::rd::cdn::RankedHosts;
use rd_rs::rd::{RealDebrid, api::new_unrestrict_cache};

#[tokio::test]
async fn unrestrict_includes_ip_when_force_location_enabled() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/rest/1.0/unrestrict/link")
        .match_header("content-type", "application/x-www-form-urlencoded")
        .match_body(Matcher::Regex("link=".into()))
        .match_body(Matcher::Regex("ip=203\\.0\\.113\\.10".into()))
        .with_status(200)
        .with_body(
            r#"{"filename":"f.mkv","filesize":1,"link":"L","download":"https://53.download.real-debrid.com/d/XYZ/file.mkv","streamable":1}"#,
        )
        .expect(1)
        .create_async()
        .await;

    let json = r#"{
        "token": "dummy",
        "mount_path": "/tmp/rd-rs-mount",
        "cache_dir": "/tmp/rd-rs-cache",
        "api": { "cdn_mode": "force_location", "cdn_location": "mum" }
    }"#;
    let mut cfg: Config = serde_json::from_str(json).unwrap();
    cfg.api.base_url = server.url();

    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());

    let mut ips = HashMap::new();
    ips.insert(
        "mum2-4.download.real-debrid.com".to_string(),
        "203.0.113.10".to_string(),
    );
    let hosts = RankedHosts {
        fastest_host: "mum2-4.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec!["mum2-4.download.real-debrid.com".to_string()]),
        ipv4_addresses: Arc::new(ips),
    };
    rd.ranked_hosts.store(Some(Arc::new(hosts)));

    let cache = new_unrestrict_cache();
    let _ = rd
        .unrestrict_link(&cache, "https://example.com/source")
        .await
        .unwrap();

    mock.assert_async().await;
}

#[tokio::test]
async fn unrestrict_does_not_require_ip_when_not_force_location() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/rest/1.0/unrestrict/link")
        .match_header("content-type", "application/x-www-form-urlencoded")
        .match_body(Matcher::Exact("link=https://example.com/source".into()))
        .with_status(200)
        .with_body(
            r#"{"filename":"f.mkv","filesize":1,"link":"L","download":"https://53.download.real-debrid.com/d/XYZ/file.mkv","streamable":1}"#,
        )
        .expect(1)
        .create_async()
        .await;

    let json = r#"{
        "token": "dummy",
        "mount_path": "/tmp/rd-rs-mount",
        "cache_dir": "/tmp/rd-rs-cache",
        "api": { "cdn_mode": "auto" }
    }"#;
    let mut cfg: Config = serde_json::from_str(json).unwrap();
    cfg.api.base_url = server.url();

    let rd = Arc::new(RealDebrid::new(&cfg).unwrap());
    let cache = new_unrestrict_cache();
    let _ = rd
        .unrestrict_link(&cache, "https://example.com/source")
        .await
        .unwrap();

    mock.assert_async().await;
}
