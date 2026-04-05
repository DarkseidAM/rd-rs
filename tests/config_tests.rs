use rd_rs::config::Config;
use std::path::PathBuf;

#[test]
fn parse_minimal_toml() {
    let toml = r#"token = "TESTTOKEN""#;
    let cfg = Config::from_toml(toml).unwrap();
    assert_eq!(cfg.token, "TESTTOKEN");
    assert_eq!(cfg.mount_path, PathBuf::from("/mnt/zurg"));
    assert_eq!(cfg.api.rate_limit_per_minute, 250);
    assert_eq!(cfg.api.refresh_interval_secs, 15);
    assert_eq!(cfg.api.cdn_reprobe_interval_mins, 0);
    assert_eq!(cfg.api.download_read_timeout_secs, 300);
    assert_eq!(cfg.repair.every_mins, 60);
    assert_eq!(cfg.vfs.chunk_size, "4M");
}

#[test]
fn parse_full_toml() {
    let toml = r#"
        token = "ABC"
        mount_path = "/mnt/test"
        cache_dir = "/tmp/cache"
        download_tokens = ["DEF", "GHI"]

        [on_library_update]
        command = "curl http://localhost:5000/webhook"

        [repair]
        enable = true
        every_mins = 30

        [api]
        rate_limit_per_minute = 100
        timeout_secs = 30
        refresh_interval_secs = 45

        [vfs]
        chunk_size = "8M"
        max_parallel_streams = 4
    "#;
    let cfg = Config::from_toml(toml).unwrap();
    assert_eq!(cfg.download_tokens.len(), 2);
    assert_eq!(cfg.repair.every_mins, 30);
    assert_eq!(cfg.api.rate_limit_per_minute, 100);
    assert_eq!(cfg.api.refresh_interval_secs, 45);
    assert_eq!(cfg.vfs.chunk_size, "8M");
    assert_eq!(cfg.vfs.max_parallel_streams, 4);
}

#[test]
fn empty_token_fails_validation() {
    let toml = r#"token = """#;
    assert!(Config::from_toml(toml).is_err());
}

#[test]
fn all_download_tokens_deduplicates_primary() {
    let toml = "token = \"ABC\"\ndownload_tokens = [\"ABC\", \"DEF\"]";
    let cfg = Config::from_toml(toml).unwrap();
    let tokens = cfg.all_download_tokens();
    assert_eq!(tokens, vec!["ABC", "DEF"]);
}
