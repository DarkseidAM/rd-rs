//! FUSE constants and inode ranges.

/// Top-level directory name served under the mount point.
pub(crate) const ALL_DIR: &str = "__all__";

/// Inode ranges (stable for the FUSE layer). Exposed for tests.
pub const INODE_ROOT: u64 = 1;
pub const INODE_ALL: u64 = 2;
/// Torrent dirs start at 3, files at INODE_FILE_BASE + offset.
pub const INODE_TORRENT_BASE: u64 = 3;
pub const INODE_FILE_BASE: u64 = 1 << 32; // high range to avoid collisions
