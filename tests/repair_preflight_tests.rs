//! `repair::preflight` helpers (no live RD calls).

use std::collections::HashMap;

use chrono::Utc;
use rd_rs::rd::types::{File, TorrentInfo};
use rd_rs::repair::preflight::{apply_lone_selected_rar_ok_policy, orphan_rd_links};

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
fn orphan_rd_links_skips_one_link_per_selected_file() {
    let info = torrent_info(
        &["a", "b", "orphan1", "orphan2"],
        &[
            ("/a.mkv".into(), 1),
            ("/b.mkv".into(), 1),
            ("/c.mkv".into(), 0),
        ],
    );
    assert_eq!(orphan_rd_links(&info), vec!["orphan1", "orphan2"]);
}

#[test]
fn orphan_rd_links_all_links_when_none_selected() {
    let info = torrent_info(&["x", "y"], &[("/only.mkv".into(), 0)]);
    assert_eq!(orphan_rd_links(&info), vec!["x", "y"]);
}

#[test]
fn orphan_rd_links_empty_when_counts_match() {
    let info = torrent_info(&["a", "b"], &[("/1.mkv".into(), 1), ("/2.mkv".into(), 1)]);
    assert!(orphan_rd_links(&info).is_empty());
}

#[test]
fn orphan_rd_links_extra_selected_still_skips_min_of_links_len() {
    let info = torrent_info(&["only"], &[("/1.mkv".into(), 1), ("/2.mkv".into(), 1)]);
    assert!(orphan_rd_links(&info).is_empty());
}

#[test]
fn lone_rar_policy_marks_all_selected_ok() {
    let info = torrent_info(&[], &[("/pack.rar".into(), 1), ("/sample.mkv".into(), 1)]);
    let mut fs = HashMap::new();
    apply_lone_selected_rar_ok_policy(&info, &mut fs, "/pack.rar");
    assert_eq!(fs.get("/pack.rar").map(String::as_str), Some("ok"));
    assert_eq!(fs.get("/sample.mkv").map(String::as_str), Some("ok"));
}

#[test]
fn lone_rar_policy_no_op_when_two_rars_selected() {
    let info = torrent_info(&[], &[("/a.rar".into(), 1), ("/b.rar".into(), 1)]);
    let mut fs = HashMap::new();
    apply_lone_selected_rar_ok_policy(&info, &mut fs, "/a.rar");
    assert!(fs.is_empty());
}
