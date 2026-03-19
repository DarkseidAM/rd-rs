//! Configuration loading and hot-reload.
//!
//! `Config::load(path)` reads a TOML file and validates it.
//! `Config::watch()` spawns a background watcher; changes are broadcast
//! via a `tokio::sync::watch` channel so downstream components can react
//! without a restart.

mod defaults;
mod load;
mod structs;

pub use structs::{ApiConfig, Config, OnLibraryUpdateConfig, RepairConfig, VfsConfig};
