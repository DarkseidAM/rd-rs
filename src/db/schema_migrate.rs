//! One-shot schema migrations for existing SQLite files.

use rusqlite::Connection;

/// If `app_meta.value` was created as INTEGER, rebuild the table with TEXT values.
pub(crate) fn migrate_app_meta_value_to_text(conn: &Connection) -> rusqlite::Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='app_meta')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(());
    }

    let needs_migrate = {
        let mut stmt = conn.prepare("PRAGMA table_info(app_meta)")?;
        let mut rows = stmt.query([])?;
        let mut found = false;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            if name == "value" && col_type.eq_ignore_ascii_case("INTEGER") {
                found = true;
                break;
            }
        }
        found
    };

    if !needs_migrate {
        return Ok(());
    }

    conn.execute_batch(
        r#"
        BEGIN;
        CREATE TABLE app_meta_new (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO app_meta_new SELECT key, CAST(value AS TEXT) FROM app_meta;
        DROP TABLE app_meta;
        ALTER TABLE app_meta_new RENAME TO app_meta;
        COMMIT;
        "#,
    )
}
