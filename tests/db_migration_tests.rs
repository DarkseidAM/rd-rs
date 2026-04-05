use rd_rs::db::Db;
use rusqlite::Connection;

#[tokio::test]
async fn app_meta_migrates_integer_value_column_to_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE app_meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            INSERT INTO app_meta (key, value) VALUES ('last_repair_cycle_unix', 1700000000);
            "#,
        )
        .unwrap();
    }

    let db = Db::open(&path).await.unwrap();
    db.init_schema().await.unwrap();

    assert_eq!(
        db.get_meta_i64(Db::META_LAST_REPAIR_CYCLE_UNIX)
            .await
            .unwrap(),
        Some(1_700_000_000)
    );

    let value_type: String = db
        .conn
        .call(|c| {
            let mut stmt = c.prepare("PRAGMA table_info(app_meta)")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let name: String = row.get(1)?;
                if name == "value" {
                    return row.get::<_, String>(2);
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows)
        })
        .await
        .unwrap();
    assert!(
        value_type.eq_ignore_ascii_case("text"),
        "expected TEXT column, got {value_type}"
    );
}
