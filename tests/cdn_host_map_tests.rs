use rd_rs::rd::cdn::RankedHosts;

#[test]
fn test_rewrite_url_success() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
    };

    let pined = hosts
        .rewrite_url("https://53.download.real-debrid.com/d/XYZ/file.mkv")
        .unwrap();
    assert_eq!(
        pined,
        "https://mum1-1.download.real-debrid.com/d/XYZ/file.mkv"
    );

    let pined2 = hosts
        .rewrite_url("https://13.download.real-debrid.net/path?q=1")
        .unwrap();
    assert_eq!(pined2, "https://mum1-1.download.real-debrid.com/path?q=1");
}

#[test]
fn test_rewrite_url_ignores_non_cdn() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
    };

    let pined = hosts.rewrite_url("https://api.real-debrid.com/rest/1.0/user");
    assert_eq!(pined, None);

    let pined2 = hosts.rewrite_url("https://google.com/d/XYZ/file.mkv");
    assert_eq!(pined2, None);
}
