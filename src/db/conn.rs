//! Db connection and operations.

use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use rusqlite::params;

use super::types::{RepairJobRow, TorrentRow, TorrentState};

/// Synchronous SQLite handle (WAL mode).
pub struct Db {
    pub conn: tokio_rusqlite::Connection,
}

impl Db {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir: {}", parent.display()))?;
        }

        let conn = tokio_rusqlite::Connection::open(path).await?;

        conn.call(|c| -> rusqlite::Result<()> {
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            Ok::<(), rusqlite::Error>(())
        })
        .await?;

        Ok(Self { conn })
    }

    pub async fn new_in_memory() -> Result<Self> {
        let conn = tokio_rusqlite::Connection::open_in_memory().await?;
        conn.call(|c| -> rusqlite::Result<()> {
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            Ok::<(), rusqlite::Error>(())
        })
        .await?;
        Ok(Self { conn })
    }

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

    pub fn get_all_torrents_conn(
        conn: &rusqlite::Connection,
    ) -> Result<Vec<TorrentRow>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT * FROM torrents")?;

        let rows = stmt
            .query_map([], Self::row_to_torrent)?
            .collect::<std::result::Result<Vec<_>, rusqlite::Error>>()?;
        Ok(rows)
    }

    pub async fn get_all_torrents(&self) -> Result<Vec<TorrentRow>> {
        let rows = self
            .conn
            .call(|conn| Self::get_all_torrents_conn(conn))
            .await?;
        Ok(rows)
    }

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

    pub async fn table_count(&self, table: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(1) FROM {}", table);
        let count: i64 = self
            .conn
            .call(move |conn| -> rusqlite::Result<i64> { conn.query_row(&sql, [], |r| r.get(0)) })
            .await?;
        Ok(count)
    }
}
