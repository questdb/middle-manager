use std::fs::{self, DirEntry, Metadata};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::Result;
use chrono::{DateTime, Local};

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub modified: SystemTime,
    pub permissions: u32,
}

impl FileEntry {
    pub fn from_dir_entry(entry: &DirEntry) -> Result<Self> {
        let metadata = entry.metadata()?;
        let symlink_meta = fs::symlink_metadata(entry.path())?;
        let is_symlink = symlink_meta.file_type().is_symlink();

        Ok(Self::from_metadata(
            entry.file_name().to_string_lossy().into_owned(),
            entry.path(),
            &metadata,
            is_symlink,
        ))
    }

    pub fn from_metadata(
        name: String,
        path: PathBuf,
        metadata: &Metadata,
        is_symlink: bool,
    ) -> Self {
        Self {
            name,
            path,
            is_dir: metadata.is_dir(),
            is_symlink,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            permissions: metadata.permissions().mode(),
        }
    }

    pub fn parent_entry(path: PathBuf) -> Self {
        Self {
            name: "..".to_string(),
            path,
            is_dir: true,
            is_symlink: false,
            size: 0,
            modified: SystemTime::UNIX_EPOCH,
            permissions: 0,
        }
    }

    pub fn formatted_size(&self) -> String {
        if self.is_dir {
            "<DIR>".to_string()
        } else if self.size < 1024 {
            format!("{}", self.size)
        } else if self.size < 1024 * 1024 {
            format!("{}K", self.size / 1024)
        } else if self.size < 1024 * 1024 * 1024 {
            format!("{:.1}M", self.size as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1}G", self.size as f64 / (1024.0 * 1024.0 * 1024.0))
        }
    }

    pub fn formatted_date(&self) -> String {
        let datetime: DateTime<Local> = self.modified.into();
        datetime.format("%Y-%m-%d %H:%M").to_string()
    }

    pub fn formatted_permissions(&self) -> String {
        if self.name == ".." {
            return String::new();
        }
        let mode = self.permissions;
        let mut s = String::with_capacity(9);
        let flags = [
            (0o400, 'r'),
            (0o200, 'w'),
            (0o100, 'x'),
            (0o040, 'r'),
            (0o020, 'w'),
            (0o010, 'x'),
            (0o004, 'r'),
            (0o002, 'w'),
            (0o001, 'x'),
        ];
        for (bit, ch) in flags {
            s.push(if mode & bit != 0 { ch } else { '-' });
        }
        s
    }
}
