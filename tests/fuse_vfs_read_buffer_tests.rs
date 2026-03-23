use bytes::Bytes;
use rd_rs::fuse::vfs_read_buffer::{PrepareRead, VfsReadBuffer};

#[test]
fn sequential_hits_after_first_fetch() {
    let mut b = VfsReadBuffer::new();
    let bs = 1024u64;
    let file_size = 10_000u64;

    match b.prepare_read(0, 100, file_size, bs) {
        PrepareRead::Miss {
            fill_offset,
            fill_len,
            take,
        } => {
            assert_eq!(fill_offset, 0);
            assert_eq!(fill_len, 1024);
            assert_eq!(take, 100);
        }
        _ => panic!("expected miss"),
    }

    let payload = Bytes::from(vec![7u8; 1024]);
    b.after_fetch(0, payload, 100);

    match b.prepare_read(100, 100, file_size, bs) {
        PrepareRead::Hit(h) => {
            assert_eq!(h.len(), 100);
            assert!(h.iter().all(|&x| x == 7));
        }
        _ => panic!("expected hit"),
    }
}

#[test]
fn seek_invalidates() {
    let mut b = VfsReadBuffer::new();
    let bs = 256u64;
    let file_size = 10_000u64;

    let _ = b.prepare_read(0, 50, file_size, bs);
    b.after_fetch(0, Bytes::from(vec![1u8; 256]), 50);

    match b.prepare_read(200, 10, file_size, bs) {
        PrepareRead::Miss { fill_offset, .. } => assert_eq!(fill_offset, 200),
        _ => panic!("seek should miss"),
    }
}

#[test]
fn fill_len_respects_cap_and_need() {
    let mut b = VfsReadBuffer::new();
    match b.prepare_read(0, 100, 1_000_000, 4096) {
        PrepareRead::Miss { fill_len, .. } => assert_eq!(fill_len, 4096),
        _ => panic!(),
    }
    b.clear();
    match b.prepare_read(0, 5000, 1_000_000, 4096) {
        PrepareRead::Miss { fill_len, .. } => assert_eq!(fill_len, 5000),
        _ => panic!(),
    }
    b.clear();
    match b.prepare_read(999_990, 100, 1_000_000, 1_000_000) {
        PrepareRead::Miss { fill_len, .. } => assert_eq!(fill_len, 10),
        _ => panic!(),
    }
}
