use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::panel::entry::FileEntry;

/// Common interface for remote filesystem operations.
///
/// Implementations must be `Send` so they can be constructed on a background thread
/// and sent to the main thread via `mpsc::channel`. Once on the main thread, they're
/// wrapped in `Rc<dyn RemoteFs>` (not Send) and used exclusively from the UI thread.
///
/// All current implementations shell out to CLI tools and hold only String/PathBuf
/// fields, so they are naturally Send.
pub trait RemoteFs {
    /// List directory contents.
    fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>>;
    /// Create a directory.
    fn mkdir(&self, path: &Path) -> Result<()>;
    /// Remove a file or directory recursively.
    fn remove_recursive(&self, path: &Path) -> Result<()>;
    /// Rename a file or directory.
    fn rename(&self, src: &Path, dst: &Path) -> Result<()>;
    /// Download a remote file to local. Returns bytes transferred.
    fn download(&self, remote: &Path, local: &Path) -> Result<u64>;
    /// Upload a local file to remote. Returns bytes transferred.
    fn upload(&self, local: &Path, remote: &Path) -> Result<u64>;
    /// Download a directory recursively.
    fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64>;
    /// Upload a directory recursively.
    fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64>;
    /// Get the home/root directory.
    fn home_dir(&self) -> PathBuf;
    /// Display label for the panel header.
    fn display_label(&self) -> String;
}
