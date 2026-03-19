//! Tests for FUSE layer (inode math, constants).
//!
//! Full FUSE methods (lookup, readdir, getattr, open, read) are exercised by the
//! running service. With the mount live at e.g. `/mnt/test`:
//!
//! - `ls /mnt/test` → readdir(root)
//! - `ls /mnt/test/__all__` → readdir(INODE_ALL)
//! - `stat /mnt/test/__all__/<access_key>` → lookup(getattr) for a torrent dir
//! - `ls /mnt/test/__all__/<access_key>` → readdir(torrent dir), ensure_torrent_info
//! - `stat /mnt/test/__all__/<access_key>/<file>` → lookup(getattr) for a file
//! - `cat /mnt/test/__all__/.../file` → read (Phase 3; currently ENOSYS)

use rd_rs::fuse::RdFs;

#[test]
fn inode_constants_non_overlapping() {
    assert_eq!(RdFs::torrent_inode(0), rd_rs::fuse::INODE_TORRENT_BASE);
    assert_eq!(RdFs::torrent_inode(1), rd_rs::fuse::INODE_TORRENT_BASE + 1);
    assert!(RdFs::torrent_inode(0) < rd_rs::fuse::INODE_FILE_BASE);
    assert!(RdFs::file_inode(0, 0) >= rd_rs::fuse::INODE_FILE_BASE);
}

#[test]
fn file_inode_formula() {
    assert_eq!(RdFs::file_inode(0, 0), rd_rs::fuse::INODE_FILE_BASE);
    assert_eq!(RdFs::file_inode(0, 1), rd_rs::fuse::INODE_FILE_BASE + 1);
    assert_eq!(
        RdFs::file_inode(1, 0),
        rd_rs::fuse::INODE_FILE_BASE + 10_000
    );
    assert_eq!(
        RdFs::file_inode(2, 5),
        rd_rs::fuse::INODE_FILE_BASE + 20_005
    );
}
