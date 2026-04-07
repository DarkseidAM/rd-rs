//! Cache layer — sparse file VFS cache for rd-rs.
//!
//! Public surface:
//! - [`Cache`]     — global store; call `Cache::new()` once at startup
//! - [`CacheItem`] — per-file sparse file handle; obtain via `Cache::get_or_create`
//! - [`bitmap::ByteRanges`] — interval list tracking downloaded byte ranges

pub mod bitmap;
#[allow(clippy::module_inception)]
pub mod cache;
pub mod download_session;
mod eviction;
pub mod item;
pub(crate) mod item_read_at;
pub(crate) mod link_heal;
pub(crate) mod range_db;
pub(crate) mod worker;

pub use cache::Cache;
pub use item::{CacheItem, CacheReadError};
