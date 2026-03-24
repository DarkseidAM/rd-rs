//! Passive HEAD probe helpers (`repair::detect`).

use chrono::Utc;
use rd_rs::config::Config;
use rd_rs::rd::RealDebrid;
use rd_rs::rd::api::new_unrestrict_cache;
use rd_rs::rd::types::{File, TorrentInfo};
use rd_rs::repair::{check_head_unreachable, passive_head_probe_slot_count};

fn torrent_info(links: &[&str], files: &[(String, i32)]) -> TorrentInfo {
    TorrentInfo {
        id: "tid".into(),
        name: "name.mkv".into(),
        hash: "deadbeef".into(),
        progress: 100,
        status: "downloaded".into(),
        added: Utc::now(),
        ended: None,
        bytes: 0,
        links: links.iter().map(|s| (*s).to_string()).collect(),
        original_name: String::new(),
        original_bytes: 0,
        files: files
            .iter()
            .enumerate()
            .map(|(i, (path, selected))| File {
                id: i as i32,
                path: path.clone(),
                bytes: 0,
                selected: *selected,
            })
            .collect(),
    }
}

#[test]
fn passive_head_probe_slot_count_skips_non_playable() {
    let info = torrent_info(
        &["https://real-debrid.com/d/abc"],
        &[("/readme.txt".into(), 1)],
    );
    assert_eq!(passive_head_probe_slot_count(&info), 0);
}

#[test]
fn passive_head_probe_slot_count_counts_mkv_with_link() {
    let info = torrent_info(
        &["https://real-debrid.com/d/abc"],
        &[("/movie.mkv".into(), 1)],
    );
    assert_eq!(passive_head_probe_slot_count(&info), 1);
}

#[test]
fn passive_head_probe_slot_count_empty_link_not_counted() {
    let info = torrent_info(&[""], &[("/movie.mkv".into(), 1)]);
    assert_eq!(passive_head_probe_slot_count(&info), 0);
}

#[test]
fn passive_head_probe_slot_count_two_playable_two_links() {
    let info = torrent_info(
        &["https://a", "https://b"],
        &[
            ("/a.mkv".into(), 1),
            ("/b.mkv".into(), 1),
            ("/extra.nfo".into(), 0),
        ],
    );
    assert_eq!(passive_head_probe_slot_count(&info), 2);
}

fn test_config() -> Config {
    let toml = r#"
token = "dummy-token-for-tests"
mount_path = "/tmp/rd-rs-mount"
cache_dir = "/tmp/rd-rs-cache"
"#;
    toml::from_str(toml).expect("parse test config")
}

#[tokio::test]
async fn check_head_unreachable_returns_zero_when_no_probe_slots() {
    let cfg = test_config();
    let rd = RealDebrid::new(&cfg).expect("RealDebrid");
    let cache = new_unrestrict_cache();
    // Non-playable selected file: loop never calls unrestrict / verify (no outbound HTTP).
    let info = torrent_info(
        &["https://real-debrid.com/d/abc"],
        &[("/notes.txt".into(), 1)],
    );
    let n = check_head_unreachable(&rd, &cache, &info)
        .await
        .expect("probe");
    assert_eq!(n, 0);
}
