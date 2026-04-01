use anyhow::Result;
use arc_swap::ArcSwap;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::FormatTime;

use clap::{Parser, Subcommand};
use fuse3::raw::Session;
use rd_rs::config::Config;
use rd_rs::db::Db;
use rd_rs::fuse::RdFs;
use rd_rs::torrent::{EnqueueRepairAllOptions, TorrentManager};

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

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_env_filter(filter)
        .init();
}

#[derive(Parser)]
#[command(name = "rd-rs", about = "Real-Debrid FUSE filesystem")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Enqueue torrents for repair and run one repair pass (no FUSE mount, no refresh loops).
    Repair(RepairCli),
}

#[derive(Parser)]
struct RepairCli {
    /// Clear `unrepairable_reason` on every torrent before enqueueing.
    #[arg(long)]
    clear_unrepairable: bool,
    /// Also add periodic-scan candidates (unassigned links, broken playable files, etc.).
    #[arg(long)]
    periodic_eligible: bool,
    /// Enqueue every torrent (~full library repair). Default: only torrents that need repair.
    #[arg(long)]
    all: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        None => run_fuse_mount().await,
        Some(Commands::Repair(r)) => run_repair_cli(r).await,
    }
}

async fn run_repair_cli(args: RepairCli) -> Result<()> {
    let cfg = Config::load("config.toml")?;
    tracing::info!(
        "repair CLI: one pass (clear_unrepairable={}, periodic_eligible={}, enqueue_all={})",
        args.clear_unrepairable,
        args.periodic_eligible,
        args.all
    );

    let db_path = cfg.cache_dir.join("rd-rs.db");
    let db = Db::open(&db_path).await?;
    db.init_schema().await?;

    let config = Arc::new(ArcSwap::from_pointee(cfg.clone()));
    let rd_client = rd_rs::rd::RealDebrid::new(&cfg)?;
    rd_rs::rd::cdn::run_network_test(&rd_client, &cfg).await;
    if let Some(pin) = rd_rs::rd::cdn::RankedHosts::try_load() {
        rd_client.ranked_hosts.store(Some(pin));
    }
    let rd_client = Arc::new(rd_client);
    let db = Arc::new(db.conn);

    let torrent_mgr = TorrentManager::new(rd_client.clone(), db.clone(), config.clone()).await?;
    let torrent_mgr = Arc::new(torrent_mgr);

    torrent_mgr
        .enqueue_repair_all(EnqueueRepairAllOptions {
            clear_unrepairable: args.clear_unrepairable,
            all: args.all,
        })
        .await;

    let cancel = tokio_util::sync::CancellationToken::new();
    let engine = Arc::new(rd_rs::repair::engine::RepairEngine::new(
        rd_client,
        db,
        config,
        torrent_mgr,
        cancel,
    ));
    engine.run_one_pass_for_cli(args.periodic_eligible).await;
    tracing::info!("repair CLI: pass complete");
    Ok(())
}

async fn run_fuse_mount() -> Result<()> {
    let cfg = Config::load("config.toml")?;
    tracing::info!("Config loaded: mount_path={}", cfg.mount_path.display());

    let db_path = cfg.cache_dir.join("rd-rs.db");
    let db = Db::open(&db_path).await?;
    db.init_schema().await?;
    tracing::info!("SQLite schema initialised (WAL) at: {}", db_path.display());
    let config = Arc::new(ArcSwap::from_pointee(cfg.clone()));

    let rd_client = rd_rs::rd::RealDebrid::new(&cfg)?;
    tracing::info!("RealDebrid clients ready");
    rd_rs::rd::cdn::run_network_test(&rd_client, &cfg).await;
    if let Some(pin) = rd_rs::rd::cdn::RankedHosts::try_load() {
        rd_client.ranked_hosts.store(Some(pin));
    }
    let rd_client = Arc::new(rd_client);

    let _config_watcher = match Config::watch("config.toml") {
        Ok((mut rx, watcher)) => {
            let config_clone = config.clone();
            let rd_clone = rd_client.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    // 30-second debounce: absorbs rapid saves (e.g., editor backup writes)
                    // so we don't thrash credential updates on every keystroke.
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    // watch channels hold only the latest value; one call picks up the
                    // most recent config and marks it as seen.
                    let updated = rx.borrow_and_update().clone();
                    rd_clone.reload_credentials(&updated);
                    config_clone.store(Arc::new(updated));
                }
            });
            tracing::info!("Config hot-reload enabled (watching config.toml, 30s debounce)");
            Some(watcher)
        }
        Err(_) => {
            tracing::debug!("Config watch not started (hot-reload disabled)");
            None
        }
    };

    let db = Arc::new(db.conn);

    tracing::info!("Starting TorrentManager...");
    let torrent_mgr = TorrentManager::new(rd_client.clone(), db.clone(), config.clone()).await?;
    let torrent_mgr = Arc::new(torrent_mgr);
    torrent_mgr.start();

    tracing::info!("Starting RepairEngine...");
    let repair_engine = Arc::new(rd_rs::repair::engine::RepairEngine::new(
        rd_client.clone(),
        db.clone(),
        config.clone(),
        torrent_mgr.clone(),
        torrent_mgr.cancel_token(),
    ));
    repair_engine.spawn();

    let mut mount_options = fuse3::MountOptions::default();
    mount_options
        .allow_other(true)
        .read_only(true)
        .fs_name("rd-rs")
        .uid(unsafe { libc::getuid() })
        .gid(unsafe { libc::getgid() })
        .no_open_dir_support(true);

    let mount_path = config.load().mount_path.clone();
    if !mount_path.exists() {
        tracing::info!("Creating mount path: {}", mount_path.display());
        std::fs::create_dir_all(&mount_path)?;
    }

    tracing::info!("Mounting FUSE filesystem at {}...", mount_path.display());
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

    let cache_dir = config.load().cache_dir.clone();
    tracing::info!(cache_dir = %cache_dir.display(), "cache directory");
    let cache =
        rd_rs::cache::Cache::new(&cache_dir, std::sync::Arc::new(config.load().vfs.clone()));

    let fs = RdFs::new(torrent_mgr.clone(), rd_client, config, cache);
    let mut mount_handle = Session::new(mount_options).mount(fs, &mount_path).await?;

    tokio::select! {
        res = &mut mount_handle => {
            if let Err(e) = res {
                tracing::error!("FUSE mount exited with error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down...");
            torrent_mgr.shutdown();

            tracing::info!("Unmounting FUSE filesystem...");
            if let Err(e) = mount_handle.unmount().await {
                tracing::error!("Failed to unmount dynamically: {}", e);
            }
        }
    }

    tracing::info!("rd-rs shutdown complete.");
    Ok(())
}
