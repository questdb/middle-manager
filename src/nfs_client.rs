use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// An NFS connection that mounts to a temp directory and delegates to local FS operations.
pub struct NfsConnection {
    host: String,
    export: String,
    mount_point: PathBuf,
}

impl NfsConnection {
    pub fn connect(host: &str, export: &str, options: &str) -> Result<Self> {
        // Create unique temp mount point (PID + timestamp to avoid collisions)
        let mount_point = std::env::temp_dir().join(format!(
            "mm-nfs-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        ));
        std::fs::create_dir_all(&mount_point)
            .context("Failed to create NFS mount point")?;

        let source = format!("{}:{}", host, export);
        let mut cmd = Command::new("mount");
        cmd.arg("-t").arg("nfs");
        if !options.is_empty() {
            cmd.arg("-o").arg(options);
        } else {
            cmd.arg("-o").arg("soft,timeo=10");
        }
        cmd.arg(&source).arg(&mount_point);

        let output = cmd.output().context(
            "Failed to run mount. Is nfs-common installed? You may need sudo.",
        )?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Clean up mount point
            let _ = std::fs::remove_dir(&mount_point);
            anyhow::bail!("NFS mount failed: {}", stderr.trim());
        }

        Ok(Self {
            host: host.to_string(),
            export: export.to_string(),
            mount_point,
        })
    }

    /// Resolve a panel path to the actual local mount path.
    fn local_path(&self, path: &Path) -> PathBuf {
        let relative = path.to_string_lossy();
        let clean = relative.trim_start_matches('/');
        let result = if clean.is_empty() {
            self.mount_point.clone()
        } else {
            self.mount_point.join(clean)
        };
        // Ensure the path doesn't escape the mount point via ..
        let normalized = normalize_path(&result);
        let mount_normalized = normalize_path(&self.mount_point);
        if !normalized.starts_with(&mount_normalized) {
            return self.mount_point.clone();
        }
        result
    }

    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let local = self.local_path(path);
        let mut entries = Vec::new();

        match std::fs::read_dir(&local) {
            Ok(read_dir) => {
                for entry in read_dir.flatten() {
                    match FileEntry::from_dir_entry(&entry) {
                        Ok(mut fe) => {
                            // Remap path to be relative to the NFS root (not the mount point)
                            if let Ok(relative) = fe.path.strip_prefix(&self.mount_point) {
                                fe.path = Path::new("/").join(relative);
                            }
                            entries.push(fe);
                        }
                        Err(_) => continue,
                    }
                }
            }
            Err(e) => anyhow::bail!("NFS read_dir error: {}", e),
        }

        Ok(entries)
    }

    pub fn mkdir(&self, path: &Path) -> Result<()> {
        let local = self.local_path(path);
        std::fs::create_dir_all(&local)?;
        Ok(())
    }

    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let local = self.local_path(path);
        if local.is_dir() {
            std::fs::remove_dir_all(&local)?;
        } else {
            std::fs::remove_file(&local)?;
        }
        Ok(())
    }

    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_local = self.local_path(src);
        let dst_local = self.local_path(dst);
        std::fs::rename(&src_local, &dst_local)?;
        Ok(())
    }

    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let src = self.local_path(remote);
        std::fs::copy(&src, local)?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let dst = self.local_path(remote);
        std::fs::copy(local, &dst)?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        let src = self.local_path(remote);
        copy_dir_recursive(&src, local)
    }

    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let dst = self.local_path(remote);
        copy_dir_recursive(local, &dst)
    }

    pub fn display_label(&self) -> String {
        format!("NFS: {}:{}", self.host, self.export)
    }

    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("/")
    }
}

impl Drop for NfsConnection {
    fn drop(&mut self) {
        // Attempt to unmount; try lazy unmount as fallback
        let unmounted = Command::new("umount")
            .arg(&self.mount_point)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        if !unmounted {
            // Lazy unmount as fallback (will complete when no longer in use)
            let _ = Command::new("umount")
                .arg("-l")
                .arg(&self.mount_point)
                .output();
            // Don't remove dir -- lazy unmount is still in progress
            return;
        }
        // Only remove the directory if unmount succeeded
        let _ = std::fs::remove_dir(&self.mount_point);
    }
}

impl crate::remote_fs::RemoteFs for NfsConnection {
    fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> { self.read_dir(path) }
    fn mkdir(&self, path: &Path) -> Result<()> { self.mkdir(path) }
    fn remove_recursive(&self, path: &Path) -> Result<()> { self.remove_recursive(path) }
    fn rename(&self, src: &Path, dst: &Path) -> Result<()> { self.rename(src, dst) }
    fn download(&self, remote: &Path, local: &Path) -> Result<u64> { self.download(remote, local) }
    fn upload(&self, local: &Path, remote: &Path) -> Result<u64> { self.upload(local, remote) }
    fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> { self.download_dir(remote, local) }
    fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> { self.upload_dir(local, remote) }
    fn home_dir(&self) -> PathBuf { self.home_dir() }
    fn display_label(&self) -> String { self.display_label() }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => { components.pop(); }
            std::path::Component::CurDir => {}
            c => components.push(c),
        }
    }
    components.iter().collect()
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<u64> {
    std::fs::create_dir_all(dst)?;
    let mut total = 0u64;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dest = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            total += copy_dir_recursive(&entry.path(), &dest)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}
