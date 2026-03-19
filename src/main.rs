use anyhow::Result;
use arc_swap::ArcSwap;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::FormatTime;

use fuse3::raw::Session;
use rd_rs::config::Config;
use rd_rs::db::Db;
use rd_rs::fuse::RdFs;
use rd_rs::torrent::TorrentManager;

struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(
            w,
            "{}",
            chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_env_filter(filter)
        .init();

    let cfg = Config::load("config.toml")?;
    tracing::info!("Config loaded: mount_path={}", cfg.mount_path.display());

    let db_path = cfg.cache_dir.join("rd-rs.db");
    let db = Db::open(&db_path).await?;
    db.init_schema().await?;
    tracing::info!("SQLite schema initialised (WAL) at: {}", db_path.display());

    // Live config for hot-reload (TorrentManager, FUSE); RD client keeps initial config.
    let config = Arc::new(ArcSwap::from_pointee(cfg.clone()));
    let _config_watcher = match Config::watch("config.toml") {
        Ok((mut rx, watcher)) => {
            let config_clone = config.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let updated = rx.borrow_and_update().clone();
                    config_clone.store(Arc::new(updated));
                }
            });
            tracing::info!("Config hot-reload enabled (watching config.toml)");
            Some(watcher)
        }
        Err(_) => {
            tracing::debug!("Config watch not started (hot-reload disabled)");
            None
        }
    };

    // Wait for the CDN latency test to complete before starting torrent refresh
    let rd_client = rd_rs::rd::RealDebrid::new(&cfg).await?;
    tracing::info!("RealDebrid clients ready");
    rd_rs::rd::cdn::run_network_test(&rd_client, &cfg).await;

    let rd_client = Arc::new(rd_client);
    let db = Arc::new(db.conn);

    // Create and start TorrentManager (uses live config for refresh interval, hook, etc.)
    tracing::info!("Starting TorrentManager...");
    let torrent_mgr = TorrentManager::new(rd_client.clone(), db, config.clone()).await?;
    let torrent_mgr = Arc::new(torrent_mgr);
    torrent_mgr.start();

    // Setup FUSE options
    let mut mount_options = fuse3::MountOptions::default();
    mount_options
        .allow_other(true)
        .read_only(true)
        .fs_name("rd-rs")
        .uid(unsafe { libc::getuid() })
        .gid(unsafe { libc::getgid() })
        // Stateless readdir: no opendir/releasedir; kernel sends readdir only.
        .no_open_dir_support(true);

    // Ensure mount path exists (from current config)
    let mount_path = config.load().mount_path.clone();
    if !mount_path.exists() {
        tracing::info!("Creating mount path: {}", mount_path.display());
        std::fs::create_dir_all(&mount_path)?;
    }

    // Mount RdFs
    tracing::info!("Mounting FUSE filesystem at {}...", mount_path.display());
    // Force unmount any stale mount first (Transport endpoint is not connected)
    let umount_res = tokio::process::Command::new("fusermount3")
        .arg("-uz")
        .arg(&mount_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;

    if let Ok(st) = umount_res
        && st.success()
    {
        tracing::info!("Successfully unmounted stale FUSE directory");
    }

    let fs = RdFs::new(torrent_mgr.torrents.clone(), rd_client, config);
    let mut mount_handle = Session::new(mount_options).mount(fs, &mount_path).await?;

    // Wait for shutdown signal or mount to exit
    tokio::select! {
        res = &mut mount_handle => {
            if let Err(e) = res {
                tracing::error!("FUSE mount exited with error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down...");
            torrent_mgr.shutdown();

            // FUSE unmount logic
            tracing::info!("Unmounting FUSE filesystem...");
            // TODO: Add retries here later
            if let Err(e) = mount_handle.unmount().await {
                tracing::error!("Failed to unmount dynamically: {}", e);
            }
        }
    }

    tracing::info!("rd-rs shutdown complete.");
    Ok(())
}
