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
    /// Default implementation walks entries via `read_dir`/`download`.
    fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        std::fs::create_dir_all(local)?;
        let entries = self.read_dir(remote)?;
        let mut total = 0u64;
        for entry in entries {
            if entry.name == ".." {
                continue;
            }
            let local_dest = local.join(&entry.name);
            if entry.is_dir {
                total += self.download_dir(&entry.path, &local_dest)?;
            } else {
                total += self.download(&entry.path, &local_dest)?;
            }
        }
        Ok(total)
    }
    /// Upload a directory recursively.
    /// Default implementation walks local entries via `mkdir`/`upload`.
    fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let _ = self.mkdir(remote); // ignore if exists
        let mut total = 0u64;
        for entry in std::fs::read_dir(local)? {
            let entry = entry?;
            let remote_dest = remote.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                total += self.upload_dir(&entry.path(), &remote_dest)?;
            } else {
                total += self.upload(&entry.path(), &remote_dest)?;
            }
        }
        Ok(total)
    }
    /// Get the home/root directory.
    fn home_dir(&self) -> PathBuf;
    /// Display label for the panel header.
    fn display_label(&self) -> String;
}

/// Convert a panel path to an object-store prefix (no leading slash, trailing slash for dirs).
/// Shared by S3, GCS, and Azure Blob.
pub fn path_to_prefix(path: &Path) -> String {
    let s = path.to_string_lossy();
    let clean = s.trim_start_matches('/');
    if clean.is_empty() {
        String::new()
    } else if clean.ends_with('/') {
        clean.to_string()
    } else {
        format!("{}/", clean)
    }
}

/// Return the application config directory (`~/.config/middle-manager/`).
pub fn config_dir() -> PathBuf {
    if let Some(config_dir) = std::env::var_os("XDG_CONFIG_HOME") {
        Path::new(&config_dir).join("middle-manager")
    } else if let Some(home) = std::env::var_os("HOME") {
        Path::new(&home).join(".config").join("middle-manager")
    } else {
        PathBuf::from(".")
    }
}

/// Format a byte count as a human-readable string (B / KB / MB / GB).
pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
