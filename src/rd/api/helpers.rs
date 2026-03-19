//! API helper functions.

/// Percent-encode for form data (just the essential chars).
pub(crate) fn urlencoding_encode(s: &str) -> String {
    s.replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
}

/// Strip the filename from a RD CDN download URL, keeping the base code URL.
/// Example: `https://host/d/CODE/movie.mkv` → `https://host/d/CODE`
pub fn extract_base_download_url(url: &str) -> String {
    if let Some(idx) = url.find("/d/") {
        let hash_start = idx + 3;
        if let Some(slash) = url[hash_start..].find('/') {
            return url[..hash_start + slash].to_string();
        }
    }
    url.to_string()
}
