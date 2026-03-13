//! SQLite state persistence — WAL mode, torrents + repair_jobs tables.
//!
//! Uses synchronous `rusqlite` (not async) opened once at startup.
//! All writes are batched into a single transaction for performance at scale
//! (20K torrents = 20K INSERT OR REPLACE ops in one BEGIN/COMMIT → <1s).

use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use rusqlite::params;
use serde::{Deserialize, Serialize};

// ─── State types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentState {
    Ok,
    Broken,
    UnderRepair,
}

impl std::fmt::Display for TorrentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "ok"),
            Self::Broken => write!(f, "broken"),
            Self::UnderRepair => write!(f, "under_repair"),
        }
    }
}

impl std::str::FromStr for TorrentState {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "ok" => Ok(Self::Ok),
            "broken" => Ok(Self::Broken),
            "under_repair" => Ok(Self::UnderRepair),
            _ => anyhow::bail!("unknown torrent state: {s}"),
        }
    }
}

/// One row in the `torrents` table.
#[derive(Debug, Clone)]
pub struct TorrentRow {
    /// Stable key: `hash + "_" + name` (matches Go access key).
    pub access_key: String,
    /// JSON array of RD torrent IDs (for multi-ID merge).
    pub rd_ids: Vec<String>,
    pub hash: String,
    pub name: String,
    pub state: TorrentState,
    /// Reason why the torrent cannot be repaired (if applicable).
    pub unrepairable_reason: Option<String>,
    /// JSON map of `filename → state` for per-file broken tracking.
    pub file_states: Option<String>,
    pub last_seen_at: Option<i64>,
    pub last_repaired_at: Option<i64>,
}

impl TorrentRow {
    /// Helper to get the first RD ID or the access key if empty.
    pub fn rd_ids_first_or_empty(&self, default: &str) -> String {
        self.rd_ids
            .first()
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairJobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

impl std::fmt::Display for RepairJobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
        };
        write!(f, "{s}")
    }
}

/// One row in the `repair_jobs` table.
#[derive(Debug, Clone)]
pub struct RepairJobRow {
    pub id: String,
    pub torrent_key: String,
    pub strategy: String,
    pub status: RepairJobStatus,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}

// ─── Db ──────────────────────────────────────────────────────────────────────

/// Synchronous SQLite handle (WAL mode).
///
/// In Phase 2 this will be wrapped in `tokio_rusqlite::Connection` for
/// async access from the TorrentManager refresh loop.
pub struct Db {
    pub conn: tokio_rusqlite::Connection,
}

impl Db {
    /// Open (or create) the SQLite database at `path` with WAL mode enabled.
    pub async fn open(path: &Path) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir: {}", parent.display()))?;
        }

        // Connect to SQLite with tokio-rusqlite
        let conn = tokio_rusqlite::Connection::open(path).await?;

        // Setup initial pragmas in WAL mode
        conn.call(|c| -> rusqlite::Result<()> {
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            Ok::<(), rusqlite::Error>(())
        })
        .await?;

        Ok(Self { conn })
    }

    /// Open an in-memory database (for tests).
    pub async fn new_in_memory() -> Result<Self> {
        let conn = tokio_rusqlite::Connection::open_in_memory().await?;
        conn.call(|c| -> rusqlite::Result<()> {
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            Ok::<(), rusqlite::Error>(())
        })
        .await?;
        Ok(Self { conn })
    }

    /// Create all tables and indexes if they don't exist.
    pub async fn init_schema(&self) -> Result<()> {
        self.conn
            .call(|conn| {
                conn.execute_batch(
                    r#"
            CREATE TABLE IF NOT EXISTS torrents (
                access_key       TEXT PRIMARY KEY,
                rd_ids           TEXT NOT NULL,
                hash             TEXT,
                name             TEXT,
                state            TEXT NOT NULL DEFAULT 'ok',
                unrepairable     TEXT,
                file_states      TEXT,
                last_seen_at     INTEGER,
                last_repaired_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_state
                ON torrents(state);

            CREATE INDEX IF NOT EXISTS idx_torrents_hash
                ON torrents(hash);

            CREATE TABLE IF NOT EXISTS repair_jobs (
                id           TEXT PRIMARY KEY,
                torrent_key  TEXT NOT NULL,
                strategy     TEXT,
                status       TEXT NOT NULL DEFAULT 'pending',
                started_at   INTEGER,
                completed_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_repair_jobs_torrent
                ON repair_jobs(torrent_key);
            "#,
                )?;
                Ok::<(), rusqlite::Error>(())
            })
            .await?;
        tracing::debug!("SQLite schema initialised");
        Ok(())
    }

    // ── Torrents ──────────────────────────────────────────────────────────────

    /// Upsert a single torrent row.
    pub async fn upsert_torrent(&self, row: &TorrentRow) -> Result<()> {
        let rd_ids_json = serde_json::to_string(&row.rd_ids)?;
        let row_cloned = row.clone();

        self.conn
            .call(move |conn| -> rusqlite::Result<()> {
                conn.execute(
                    r#"INSERT OR REPLACE INTO torrents
               (access_key, rd_ids, hash, name, state, unrepairable, file_states,
                last_seen_at, last_repaired_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
                    params![
                        row_cloned.access_key,
                        rd_ids_json,
                        row_cloned.hash,
                        row_cloned.name,
                        row_cloned.state.to_string(),
                        row_cloned.unrepairable_reason,
                        row_cloned.file_states,
                        row_cloned.last_seen_at,
                        row_cloned.last_repaired_at,
                    ],
                )?;
                Ok::<(), rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Sync version of `upsert_torrents_batch_conn` (used inside `conn.call()`).
    pub fn upsert_torrents_batch_conn(
        conn: &mut rusqlite::Connection,
        torrents: &[TorrentRow],
    ) -> Result<()> {
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO torrents (
                    access_key, rd_ids, hash, name, state,
                    unrepairable, file_states, last_seen_at, last_repaired_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for row in torrents {
                let rd_ids_json = serde_json::to_string(&row.rd_ids)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let file_states_json = row.file_states.as_ref().map(|fs| {
                    serde_json::to_string(fs)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
                });
                let file_states_val = match file_states_json {
                    Some(Ok(v)) => Some(v),
                    Some(Err(e)) => return Err(e.into()),
                    None => None,
                };
                stmt.execute(params![
                    row.access_key,
                    rd_ids_json,
                    row.hash,
                    row.name,
                    row.state.to_string(),
                    row.unrepairable_reason,
                    file_states_val,
                    row.last_seen_at,
                    row.last_repaired_at,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn row_to_torrent(row: &rusqlite::Row) -> rusqlite::Result<TorrentRow> {
        let state_str: String = row.get(4)?;
        let state = TorrentState::from_str(&state_str).unwrap_or(TorrentState::Ok);

        let rd_ids_str: String = row.get(1)?;
        let rd_ids = serde_json::from_str(&rd_ids_str).unwrap_or_default();

        Ok(TorrentRow {
            access_key: row.get(0)?,
            rd_ids,
            hash: row.get(2)?,
            name: row.get(3)?,
            state,
            unrepairable_reason: row.get(5)?,
            file_states: row.get(6)?,
            last_seen_at: row.get(7)?,
            last_repaired_at: row.get(8)?,
        })
    }

    /// Load all torrent rows from the database.
    pub fn get_all_torrents_conn(
        conn: &rusqlite::Connection,
    ) -> Result<Vec<TorrentRow>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT * FROM torrents")?;

        let rows = stmt
            .query_map([], Self::row_to_torrent)?
            .collect::<std::result::Result<Vec<_>, rusqlite::Error>>()?;
        Ok(rows)
    }

    /// Fetch all torrent rows from the database (async wrapper).
    pub async fn get_all_torrents(&self) -> Result<Vec<TorrentRow>> {
        let rows = self
            .conn
            .call(|conn| Self::get_all_torrents_conn(conn))
            .await?;
        Ok(rows)
    }

    // ── Repair Jobs ───────────────────────────────────────────────────────────

    pub async fn insert_repair_job(&self, job: &RepairJobRow) -> Result<()> {
        let job_cloned = job.clone();
        self.conn
            .call(move |conn| -> rusqlite::Result<()> {
                conn.execute(
                    r#"INSERT OR REPLACE INTO repair_jobs
                   (id, torrent_key, strategy, status, started_at, completed_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    params![
                        job_cloned.id,
                        job_cloned.torrent_key,
                        job_cloned.strategy,
                        job_cloned.status.to_string(),
                        job_cloned.started_at,
                        job_cloned.completed_at,
                    ],
                )?;
                Ok::<(), rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    pub async fn update_repair_job(&self, job: &RepairJobRow) -> Result<()> {
        let job_cloned = job.clone();
        self.conn
            .call(move |conn| -> rusqlite::Result<()> {
                conn.execute(
                    r#"UPDATE repair_jobs SET strategy = ?1, status = ?2,
                   completed_at = ?3 WHERE id = ?4"#,
                    params![
                        job_cloned.strategy,
                        job_cloned.status.to_string(),
                        job_cloned.completed_at,
                        job_cloned.id,
                    ],
                )?;
                Ok::<(), rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Count rows in an arbitrary table (useful for tests/metrics).
    pub async fn table_count(&self, table: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(1) FROM {}", table);
        let count: i64 = self
            .conn
            .call(move |conn| -> rusqlite::Result<i64> { conn.query_row(&sql, [], |r| r.get(0)) })
            .await?;
        Ok(count)
    }
}
