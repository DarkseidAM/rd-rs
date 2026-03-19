//! FUSE constants and inode ranges.

use std::time::Duration;

/// Top-level directory name served under the mount point.
pub(crate) const ALL_DIR: &str = "__all__";

/// Attribute cache TTL — matches Go's `attr_timeout = 1m`.
pub(crate) const ATTR_TTL: Duration = Duration::from_secs(60);

/// Directory entry cache TTL — matches Go's `dir_cache_time = 10m`.
pub(crate) const ENTRY_TTL: Duration = Duration::from_secs(600);

/// Inode ranges (stable for the FUSE layer). Exposed for tests.
pub const INODE_ROOT: u64 = 1;
pub const INODE_ALL: u64 = 2;
/// Torrent dirs start at 3, files at INODE_FILE_BASE + offset.
pub const INODE_TORRENT_BASE: u64 = 3;
pub const INODE_FILE_BASE: u64 = 1 << 32; // high range to avoid collisions
