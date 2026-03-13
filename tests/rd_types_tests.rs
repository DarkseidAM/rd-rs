use chrono::Timelike;
use rd_rs::rd::types::*;

#[test]
fn parse_paris_time_basic() {
    // "2024-01-15T14:30:00Z" = 14:30 Paris = 13:30 UTC (CET = UTC+1 in January)
    let utc = parse_paris_time("2024-01-15T14:30:00Z").unwrap();
    assert_eq!(utc.hour(), 13);
    assert_eq!(utc.minute(), 30);
}

#[test]
fn parse_paris_time_with_millis() {
    let utc = parse_paris_time("2024-07-15T14:30:00.000Z").unwrap();
    // CEST = UTC+2 in July
    assert_eq!(utc.hour(), 12);
    assert_eq!(utc.minute(), 30);
}

#[test]
fn deserialize_torrent() {
    let json = r#"{
        "id": "ABC123",
        "filename": "My.Show.S01E01",
        "hash": "deadbeef",
        "progress": 100.0,
        "status": "downloaded",
        "links": ["https://real-debrid.com/d/LINK1"],
        "added": "2024-01-15T14:30:00Z"
    }"#;
    let t: Torrent = serde_json::from_str(json).unwrap();
    assert_eq!(t.id, "ABC123");
    assert_eq!(t.progress, 100);
    assert_eq!(t.links.len(), 1);
    // Paris CET January → UTC = 13:30
    assert_eq!(t.added.hour(), 13);
}

#[test]
fn deserialize_torrent_info_with_files() {
    let json = r#"{
        "id": "T1",
        "filename": "Movie",
        "hash": "abc",
        "progress": 50.7,
        "status": "downloading",
        "added": "2024-06-01T10:00:00Z",
        "bytes": 1073741824,
        "links": [],
        "files": [
            {"id": 1, "path": "/movie.mkv", "bytes": 1073741824, "selected": 1}
        ]
    }"#;
    let t: TorrentInfo = serde_json::from_str(json).unwrap();
    assert_eq!(t.progress, 50); // floor(50.7)
    assert_eq!(t.files.len(), 1);
    assert!(t.files[0].is_selected());
}

#[test]
fn download_extension() {
    let d = Download {
        download: "https://host.real-debrid.com/d/ABC/movie.mkv".into(),
        ..Default::default()
    };
    assert_eq!(d.extension(), Some("mkv"));
}

#[test]
fn user_is_premium() {
    let u = User {
        id: 1,
        username: "test".into(),
        email: "test@test.com".into(),
        account_type: "premium".into(),
        premium: 86400,
    };
    assert!(u.is_premium());
}
