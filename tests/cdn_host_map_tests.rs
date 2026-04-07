use rd_rs::rd::cdn::RankedHosts;
use std::sync::Arc;

#[test]
fn test_rewrite_url_success() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec![
            "53.download.real-debrid.com".to_string(),
            "13.download.real-debrid.com".to_string(),
            "mum1-1.download.real-debrid.com".to_string(),
            "lax2-4.download.real-debrid.com".to_string(),
        ]),
        ipv4_addresses: Arc::new(Default::default()),
    };

    let pined = hosts
        .rewrite_url(
            "https://53.download.real-debrid.com/d/XYZ/file.mkv",
            rd_rs::config::CdnMode::Auto,
            None,
        )
        .unwrap();
    assert_eq!(
        pined,
        "https://mum1-1.download.real-debrid.com/d/XYZ/file.mkv"
    );

    let pined2 = hosts
        .rewrite_url(
            "https://13.download.real-debrid.net/path?q=1",
            rd_rs::config::CdnMode::Auto,
            None,
        )
        .unwrap();
    assert_eq!(pined2, "https://mum1-1.download.real-debrid.com/path?q=1");
}

#[test]
fn test_rewrite_url_ignores_non_cdn() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec![]),
        ipv4_addresses: Arc::new(Default::default()),
    };

    let pined = hosts.rewrite_url(
        "https://api.real-debrid.com/rest/1.0/user",
        rd_rs::config::CdnMode::Auto,
        None,
    );
    assert_eq!(pined, None);

    let pined2 = hosts.rewrite_url(
        "https://google.com/d/XYZ/file.mkv",
        rd_rs::config::CdnMode::Auto,
        None,
    );
    assert_eq!(pined2, None);
}

#[test]
fn test_auto_preserves_geo_host() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec![
            "lax2-4.download.real-debrid.com".to_string(),
            "mum1-1.download.real-debrid.com".to_string(),
        ]),
        ipv4_addresses: Arc::new(Default::default()),
    };

    let keep = hosts.rewrite_url(
        "https://lax2-4.download.real-debrid.com/d/XYZ/file.mkv",
        rd_rs::config::CdnMode::Auto,
        None,
    );
    assert_eq!(keep, None);
}

#[test]
fn test_force_cloudflare_rewrites_tld() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec![]),
        ipv4_addresses: Arc::new(Default::default()),
    };
    let pinned = hosts
        .rewrite_url(
            "https://53.download.real-debrid.com/d/XYZ/file.mkv",
            rd_rs::config::CdnMode::ForceCloudflare,
            None,
        )
        .unwrap();
    assert_eq!(
        pinned,
        "https://53.download.real-debrid.cloud/d/XYZ/file.mkv"
    );
}

#[test]
fn test_force_numbered_picks_numbered_host() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec![
            "53.download.real-debrid.com".to_string(),
            "13.download.real-debrid.com".to_string(),
        ]),
        ipv4_addresses: Arc::new(Default::default()),
    };
    let pinned = hosts
        .rewrite_url(
            "https://lax2-4.download.real-debrid.com/d/XYZ/file.mkv",
            rd_rs::config::CdnMode::ForceNumbered,
            None,
        )
        .unwrap();
    assert!(
        pinned.contains("13.download.real-debrid.com")
            || pinned.contains("53.download.real-debrid.com")
    );
}

#[test]
fn test_force_location_requires_match() {
    let hosts = RankedHosts {
        fastest_host: "mum1-1.download.real-debrid.com".to_string(),
        reachable_ipv4_hosts: Arc::new(vec!["mum2-4.download.real-debrid.com".to_string()]),
        ipv4_addresses: Arc::new(Default::default()),
    };
    let pinned = hosts
        .rewrite_url(
            "https://53.download.real-debrid.com/d/XYZ/file.mkv",
            rd_rs::config::CdnMode::ForceLocation,
            Some("mum"),
        )
        .unwrap();
    assert!(pinned.contains("mum2-4.download.real-debrid.com"));
}
