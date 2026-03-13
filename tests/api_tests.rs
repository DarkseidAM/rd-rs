use rd_rs::rd::api::extract_base_download_url;

#[test]
fn extract_base_url_strips_filename() {
    let full = "https://105-4.download.real-debrid.com/d/7H2TJ22MQDHRW/movie.mkv";
    let base = extract_base_download_url(full);
    assert_eq!(
        base,
        "https://105-4.download.real-debrid.com/d/7H2TJ22MQDHRW"
    );
}

#[test]
fn extract_base_url_no_filename() {
    let url = "https://host/d/CODE";
    assert_eq!(extract_base_download_url(url), url);
}
