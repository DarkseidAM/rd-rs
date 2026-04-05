pub const UPSERT_TORRENT_SQL: &str = r#"
INSERT INTO torrents (
    access_key, rd_ids, hash, name, state, unrepairable, file_states,
    last_seen_at, last_repaired_at, under_repair_started_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
ON CONFLICT(access_key) DO UPDATE SET
    rd_ids = excluded.rd_ids,
    hash = excluded.hash,
    name = excluded.name,
    state = excluded.state,
    unrepairable = excluded.unrepairable,
    file_states = excluded.file_states,
    last_seen_at = excluded.last_seen_at,
    last_repaired_at = excluded.last_repaired_at,
    under_repair_started_at = excluded.under_repair_started_at
"#;

pub const UPSERT_REPAIR_JOB_SQL: &str = r#"
INSERT INTO repair_jobs (
    id, torrent_key, strategy, status, started_at, completed_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT(id) DO UPDATE SET
    torrent_key = excluded.torrent_key,
    strategy = excluded.strategy,
    status = excluded.status,
    started_at = excluded.started_at,
    completed_at = excluded.completed_at
"#;
