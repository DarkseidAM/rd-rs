//! SQLite state persistence — WAL mode, torrents + repair_jobs tables.

mod conn;
mod schema_migrate;
mod sql;
mod types;

pub use conn::Db;
pub use types::{RepairJobRow, RepairJobStatus, TorrentRow, TorrentState};
