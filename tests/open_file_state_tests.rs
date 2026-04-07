use rd_rs::fuse::open_file::OpenFileState;

#[test]
fn open_file_state_cancels_token() {
    let st = OpenFileState::new();
    let tok = st.cancel_token();
    assert!(!tok.is_cancelled());
    st.cancel();
    assert!(tok.is_cancelled());
}
