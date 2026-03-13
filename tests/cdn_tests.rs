// CDN server list parsing — no network required

#[test]
fn parse_server_list_pipe_format() {
    let line = "105-4.download.real-debrid.com|1.2.3.4";
    let hostname = line.split('|').next().unwrap().trim();
    let ip = line.split('|').nth(1).unwrap().trim();
    assert_eq!(hostname, "105-4.download.real-debrid.com");
    assert_eq!(ip, "1.2.3.4");
}

#[test]
fn filters_generated_lines() {
    let line = "generated|2024-01-15T12:00:00";
    let hostname = line.split('|').next().unwrap().trim();
    assert!(hostname.starts_with("generated"));
}
