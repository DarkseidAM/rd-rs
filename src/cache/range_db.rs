//! Persistent cache-range metadata (`cache_ranges.db`).

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::{OptionalExtension, params};

use crate::cache::bitmap::ByteRanges;

/// Bumped to 2: ranges_blob is now a little-endian binary BLOB instead of JSON.
/// Old JSON rows (schema_version = 1) are silently discarded on read.
const SCHEMA_VERSION: i64 = 2;

#[derive(Debug, Clone)]
pub struct RangeRow {
    pub file_size: u64,
    pub updated_at: i64,
    pub ranges: ByteRanges,
}

#[derive(Clone)]
pub struct RangeDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl RangeDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = rusqlite::Connection::open(path.as_ref())
            .with_context(|| format!("open cache ranges db {:?}", path.as_ref()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Self::init_conn(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::init_conn(conn)
    }

    fn init_conn(conn: rusqlite::Connection) -> Result<Self> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cache_ranges (
                cache_key TEXT PRIMARY KEY,
                file_size INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                ranges_blob BLOB NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 2
            );
            CREATE INDEX IF NOT EXISTS idx_cache_ranges_updated_at
            ON cache_ranges(updated_at);
            "#,
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get(&self, cache_key: &str) -> Result<Option<RangeRow>> {
        let conn = self.conn.lock().expect("range db mutex poisoned");
        let row = conn
            .query_row(
                "SELECT file_size, updated_at, ranges_blob, schema_version
                 FROM cache_ranges WHERE cache_key = ?1",
                params![cache_key],
                |r| {
                    let file_size: i64 = r.get(0)?;
                    let updated_at: i64 = r.get(1)?;
                    let ranges_blob: Vec<u8> = r.get(2)?;
                    let schema_version: i64 = r.get(3)?;
                    Ok((file_size, updated_at, ranges_blob, schema_version))
                },
            )
            .optional()?;
        let Some((file_size, updated_at, ranges_blob, schema_version)) = row else {
            return Ok(None);
        };
        if schema_version != SCHEMA_VERSION {
            // Old JSON row: silently discard; will be re-written as binary on next flush.
            return Ok(None);
        }
        let parsed = decode_ranges_blob(&ranges_blob)?;
        Ok(Some(RangeRow {
            file_size: file_size.max(0) as u64,
            updated_at,
            ranges: ByteRanges::from_intervals(parsed),
        }))
    }

    pub fn upsert(
        &self,
        cache_key: &str,
        file_size: u64,
        updated_at: i64,
        ranges: &ByteRanges,
    ) -> Result<()> {
        let blob = encode_ranges_blob(ranges.intervals());
        let conn = self.conn.lock().expect("range db mutex poisoned");
        conn.execute(
            "INSERT INTO cache_ranges(cache_key, file_size, updated_at, ranges_blob, schema_version)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(cache_key) DO UPDATE SET
               file_size=excluded.file_size,
               updated_at=excluded.updated_at,
               ranges_blob=excluded.ranges_blob,
               schema_version=excluded.schema_version",
            params![
                cache_key,
                file_size as i64,
                updated_at,
                blob,
                SCHEMA_VERSION
            ],
        )?;
        Ok(())
    }

    pub fn delete_keys(&self, keys: &[&str]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let keys_json =
            serde_json::to_string(keys).context("serialize cache keys for batch delete")?;
        let mut conn = self.conn.lock().expect("range db mutex poisoned");
        let tx = conn.transaction()?;
        let deleted = tx.execute(
            "DELETE FROM cache_ranges
             WHERE cache_key IN (SELECT value FROM json_each(?1))",
            params![keys_json],
        )?;
        tx.commit()?;
        Ok(deleted)
    }

    pub fn stale_keys(&self, older_than_ts: i64, limit: usize) -> Result<Vec<String>> {
        let conn = self.conn.lock().expect("range db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT cache_key FROM cache_ranges WHERE updated_at < ?1
             ORDER BY updated_at ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![older_than_ts, limit as i64], |r| r.get(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn maybe_checkpoint(&self) {
        let conn = self.conn.lock().expect("range db mutex poisoned");
        if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
            tracing::warn!(error = %e, "cache ranges wal_checkpoint failed");
        }
    }
}

// ─── Binary encoding helpers ─────────────────────────────────────────────────

/// Encode `(u64, u64)` intervals as a packed little-endian binary blob.
/// Each interval occupies 16 bytes: 8 bytes for `start`, 8 bytes for `end`.
fn encode_ranges_blob(intervals: &[(u64, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(intervals.len() * 16);
    for (start, end) in intervals {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&end.to_le_bytes());
    }
    out
}

/// Decode a packed LE binary blob back into `(u64, u64)` intervals.
/// Returns an error if the blob length is not a multiple of 16.
fn decode_ranges_blob(blob: &[u8]) -> Result<Vec<(u64, u64)>> {
    anyhow::ensure!(
        blob.len().is_multiple_of(16),
        "ranges_blob has invalid length {} (must be multiple of 16)",
        blob.len()
    );
    let mut out = Vec::with_capacity(blob.len() / 16);
    for chunk in blob.chunks_exact(16) {
        let start = u64::from_le_bytes(chunk[..8].try_into().unwrap());
        let end = u64::from_le_bytes(chunk[8..].try_into().unwrap());
        out.push((start, end));
    }
    Ok(out)
}
