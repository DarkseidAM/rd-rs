use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::vfs_read_buffer::VfsReadBuffer;

/// Per-FUSE-file-handle state: read buffer + cancellation.
///
/// Cancellation is tied to the kernel file-handle lifecycle (`release`) and to
/// each in-flight `read` via [`super::FuseReadCancelRegistration`] (FUSE
/// `interrupt` when the client aborts a blocked read, e.g. Ctrl+C).
pub struct OpenFileState {
    pub buffer: Arc<tokio::sync::Mutex<VfsReadBuffer>>,
    cancel: CancellationToken,
}

impl Default for OpenFileState {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenFileState {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(tokio::sync::Mutex::new(VfsReadBuffer::default())),
            cancel: CancellationToken::new(),
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}
