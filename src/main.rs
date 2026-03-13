use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::FormatTime;

use fuse3::raw::Session;
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

    let cfg = rd_rs::config::Config::load("config.toml")?;
    tracing::info!("Config loaded: mount_path={}", cfg.mount_path.display());

    let db_path = cfg.cache_dir.join("rd-rs.db");
    let db = Db::open(&db_path).await?;
    db.init_schema().await?;
    tracing::info!("SQLite schema initialised (WAL) at: {}", db_path.display());

    // Wait for the CDN latency test to complete before starting torrent refresh
    let rd_client = rd_rs::rd::RealDebrid::new(&cfg).await?;
    tracing::info!("RealDebrid clients ready");
    rd_rs::rd::cdn::run_network_test(&rd_client, &cfg).await;

    let rd_client = Arc::new(rd_client);
    let db = Arc::new(db.conn);

    // Create and start TorrentManager
    tracing::info!("Starting TorrentManager...");
    let cfg_arc = Arc::new(cfg.clone());
    let torrent_mgr = TorrentManager::new(rd_client.clone(), db, cfg_arc.clone()).await?;
    let torrent_mgr = Arc::new(torrent_mgr);
    torrent_mgr.start();

    // Setup FUSE options
    let mut mount_options = fuse3::MountOptions::default();
    mount_options
        .allow_other(true)
        .read_only(true)
        .uid(unsafe { libc::getuid() })
        .gid(unsafe { libc::getgid() });

    // Ensure mount path exists
    let mount_path = cfg.mount_path.clone();
    if !mount_path.exists() {
        tracing::info!("Creating mount path: {}", mount_path.display());
        std::fs::create_dir_all(&mount_path)?;
    }

    // Mount RdFs
    tracing::info!("Mounting FUSE filesystem at {}...", mount_path.display());
    let fs = RdFs::new(torrent_mgr.torrents.clone(), rd_client, cfg_arc);
    let mut mount_handle = Session::new(mount_options)
        .mount_with_unprivileged(fs, &mount_path)
        .await?;

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
            if let Err(e) = mount_handle.unmount().await {
                tracing::error!("Failed to unmount dynamically: {}", e);
            }
        }
    }

    tracing::info!("rd-rs shutdown complete.");
    Ok(())
}
