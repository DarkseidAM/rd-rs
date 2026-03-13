//! Top-level error types for zurg-rs.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZurgError {
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("rd client error: {0}")]
    Rd(String),
}
