//! `integration_concurrent_chunking_limits`: mock CDN Range responses, 100 MiB sparse file,
//! `max_parallel_streams = 8`, global download semaphore = 4, overlapping `read_at` calls.

use rd_rs::cache::item::CacheItem;
use rd_rs::config::Config;
use rd_rs::rd::RealDebrid;
use rd_rs::rd::api::new_unrestrict_cache;
use rd_rs::rd::types::Download;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

const MB: u64 = 1024 * 1024;
const FILE_LEN: u64 = 100 * MB;
const STRIPE: u64 = 5 * MB;
const STRIPES: u64 = FILE_LEN / STRIPE;
const GLOBAL_SEM: usize = 4;

fn parse_bytes_range(req: &mockito::Request) -> Option<(u64, u64)> {
    let hv = req.header("range").first()?.to_str().ok()?;
    let rest = hv.strip_prefix("bytes=")?;
    let (a, b) = rest.split_once('-')?;
    let start: u64 = a.parse().ok()?;
    let end_inc: u64 = b.parse().ok()?;
    Some((start, end_inc.saturating_add(1)))
}

fn range_payload(start: u64, len: u64) -> Vec<u8> {
    (0..len).map(|i| ((start + i) % 256) as u8).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn integration_concurrent_chunking_limits() {
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("GET", "/cdn.bin")
        .with_status(206)
        .with_header("content-type", "application/octet-stream")
        .with_header_from_request("content-range", move |req: &mockito::Request| {
            let (start, end_excl) = parse_bytes_range(req).expect("mock: Range for Content-Range");
            format!(
                "bytes {}-{}/{}",
                start,
                end_excl.saturating_sub(1),
                FILE_LEN
            )
        })
        .with_body_from_request(move |req| {
            let (start, end_excl) =
                parse_bytes_range(req).expect("mock: missing or invalid Range header");
            let len = end_excl.saturating_sub(start);
            assert!(
                end_excl <= FILE_LEN,
                "range end {end_excl} beyond file {FILE_LEN}"
            );
            range_payload(start, len)
        })
        .expect_at_least(30)
        .create();

    let cfg = Config::from_toml(
        r#"
token = "integration-test"
[vfs]
read_ahead = "0"
chunk_size = "4M"
max_parallel_streams = 8
[api]
timeout_secs = 120
retries_until_failed = 1
"#,
    )
    .expect("config");

    let rd = Arc::new(RealDebrid::new_with_connection_limit(&cfg, GLOBAL_SEM).expect("RealDebrid"));

    let stop_poll = Arc::new(AtomicBool::new(false));
    let min_available = Arc::new(AtomicU32::new(GLOBAL_SEM as u32));
    let rd_poll = Arc::clone(&rd);
    let stop_for_poller = Arc::clone(&stop_poll);
    let min_for_poller = Arc::clone(&min_available);
    let poller = tokio::spawn(async move {
        while !stop_for_poller.load(Ordering::Relaxed) {
            let a = rd_poll.connection_semaphore.available_permits() as u32;
            min_for_poller.fetch_min(a, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    let dir = tempdir().expect("tempdir");
    let item = Arc::new(
        CacheItem::open_or_create(dir.path().join("chunk_limits.bin"), FILE_LEN)
            .expect("cache item"),
    );

    let cdn_url = format!("{}/cdn.bin", server.url());
    let download = Download {
        filename: "big.bin".into(),
        filesize: FILE_LEN as i64,
        link: "https://real-debrid.com/d/TEST".into(),
        download: cdn_url,
        streamable: 1,
        generated_at: None,
        token: "t".into(),
    };

    let unrestrict = new_unrestrict_cache();
    let fuse = CancellationToken::new();
    let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

    let mut tasks = Vec::new();
    for i in 0..STRIPES {
        let item = Arc::clone(&item);
        let rd = Arc::clone(&rd);
        let dl = download.clone();
        let fuse = fuse.clone();
        let cfg = cfg.clone();
        let unc = Arc::clone(&unrestrict);
        let p_rx = pause_rx.clone();
        tasks.push(tokio::spawn(async move {
            let off = i * STRIPE;
            item.read_at(fuse, off, STRIPE as u32, &dl, &rd, &unc, &cfg, p_rx)
                .await
        }));
    }

    for t in tasks {
        t.await.expect("join").expect("read_at");
    }

    mock.assert();

    stop_poll.store(true, Ordering::Relaxed);
    poller.await.expect("poller join");

    let min_a = min_available.load(Ordering::Relaxed);
    assert!(
        min_a <= GLOBAL_SEM as u32 - 2,
        "expected at least 2 concurrent chunk downloads (min semaphore available permits was {min_a}, cap {GLOBAL_SEM})"
    );

    assert!(
        item.has_range(0, FILE_LEN),
        "full file should be cached after parallel stripes"
    );
    assert_eq!(item.total_cached_bytes(), FILE_LEN);

    for i in 0..100u64 {
        let off = i * MB;
        let buf = item.read_from_file(off, 512).expect("read");
        for (j, &b) in buf.iter().enumerate() {
            assert_eq!(b, ((off + j as u64) % 256) as u8, "off={off} j={j}");
        }
    }

    let mut seq = 0u64;
    while seq < FILE_LEN {
        let end = (seq + 64 * 1024).min(FILE_LEN);
        let got = item
            .read_at(
                fuse.clone(),
                seq,
                (end - seq) as u32,
                &download,
                &rd,
                &unrestrict,
                &cfg,
                pause_rx.clone(),
            )
            .await
            .expect("sequential read_at");
        assert_eq!(got.len() as u64, end - seq);
        for (j, &b) in got.iter().enumerate() {
            assert_eq!(b, ((seq + j as u64) % 256) as u8);
        }
        seq = end;
    }
}
