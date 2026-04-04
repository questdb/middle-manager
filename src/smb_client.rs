use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// An SMB share connection that shells out to `smbclient`.
pub struct SmbConnection {
    /// SMB share path, e.g. "//server/share"
    pub share: String,
    pub host: String,
    pub share_name: String,
    pub username: String,
    password: String,
}

impl SmbConnection {
    /// Connect to an SMB share. Validates the connection by listing the root.
    pub fn connect(host: &str, share_name: &str, username: &str, password: &str) -> Result<Self> {
        let share = format!("//{}/{}", host, share_name);
        let conn = Self {
            share: share.clone(),
            host: host.to_string(),
            share_name: share_name.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        };
        // Validate connection by listing root
        conn.run_cmd("ls")?;
        Ok(conn)
    }

    /// Display label for panel header.
    pub fn display_label(&self) -> String {
        format!(r"\\{}\{}", self.host, self.share_name)
    }

    /// Run an smbclient command and return stdout.
    fn run_cmd(&self, cmd: &str) -> Result<String> {
        crate::debug_log::log(&format!("SMB [{}] cmd: {}", self.share, cmd));
        let mut child = Command::new("smbclient")
            .arg(&self.share)
            .arg("-U")
            .arg(&self.username)
            .arg("--authentication-file=/dev/stdin")
            .arg("--timeout=15")
            .arg("-c")
            .arg(cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to run smbclient. Is it installed?")?;

        // Feed password via stdin (avoids exposing in process listing)
        if let Some(mut stdin) = child.stdin.take() {
            // Write directly without allocating a combined string
            let _ = stdin.write_all(b"password=");
            let _ = stdin.write_all(self.password.as_bytes());
            let _ = stdin.write_all(b"\n");
            // stdin is dropped here, closing the pipe
        }

        let output = child
            .wait_with_output()
            .context("smbclient process failed")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("smbclient", false, &stderr);
            anyhow::bail!("smbclient error: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// List directory contents.
    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let smb_path = to_smb_path(path);
        let cmd = if smb_path.is_empty() || smb_path == "\\" {
            "ls".to_string()
        } else {
            format!("cd {}; ls", smb_quote(&smb_path))
        };

        let output = self.run_cmd(&cmd)?;
        parse_smbclient_ls(&output, path)
    }

    /// Create a remote directory.
    pub fn mkdir(&self, path: &Path) -> Result<()> {
        let smb_path = to_smb_path(path);
        self.run_cmd(&format!("mkdir {}", smb_quote(&smb_path)))?;
        Ok(())
    }

    /// Remove a remote file.
    pub fn remove_file(&self, path: &Path) -> Result<()> {
        let smb_path = to_smb_path(path);
        self.run_cmd(&format!("rm {}", smb_quote(&smb_path)))?;
        Ok(())
    }

    /// Remove a remote directory (recursive).
    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let entries = self.read_dir(path)?;
        for entry in &entries {
            if entry.name == ".." {
                continue;
            }
            if entry.is_dir {
                self.remove_recursive(&entry.path)?;
            } else {
                self.remove_file(&entry.path)?;
            }
        }
        let smb_path = to_smb_path(path);
        self.run_cmd(&format!("rmdir {}", smb_quote(&smb_path)))?;
        Ok(())
    }

    /// Rename a remote file or directory.
    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_smb = to_smb_path(src);
        let dst_smb = to_smb_path(dst);
        self.run_cmd(&format!("rename {} {}", smb_quote(&src_smb), smb_quote(&dst_smb)))?;
        Ok(())
    }

    /// Download a remote file to a local path.
    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let smb_path = to_smb_path(remote);
        let local_str = local.to_string_lossy();
        self.run_cmd(&format!("get {} \"{}\"", smb_quote(&smb_path), local_str))?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    /// Upload a local file to a remote path.
    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let smb_path = to_smb_path(remote);
        let local_str = local.to_string_lossy();
        self.run_cmd(&format!("put \"{}\" {}", local_str, smb_quote(&smb_path)))?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    /// Download a directory recursively.
    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
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
    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
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

    /// Home directory (share root).
    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("\\")
    }
}

impl crate::remote_fs::RemoteFs for SmbConnection {
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

/// Convert a PathBuf (forward slashes) to SMB path (backslashes), escaped for smbclient commands.
fn to_smb_path(path: &Path) -> String {
    let s = path.to_string_lossy();
    // Escape backslashes, double quotes, and semicolons to prevent command injection.
    // Backslashes must be escaped first, before '/' is converted to '\'.
    s.replace('\\', "\\\\")
        .replace('/', "\\")
        .replace('"', "\\\"")
        .replace(';', "\\;")
}

/// Wrap a path in quotes for smbclient commands.
fn smb_quote(path: &str) -> String {
    format!("\"{}\"", path)
}

/// Parse `smbclient ls` output into FileEntry items.
///
/// smbclient ls output looks like:
/// ```text
///   .                                   D        0  Mon Mar 10 12:00:00 2025
///   ..                                  D        0  Mon Mar 10 12:00:00 2025
///   Documents                           D        0  Tue Jan  7 09:30:00 2025
///   photo.jpg                           A  1234567  Wed Feb 12 14:15:00 2025
///   readme.txt                          A     4096  Thu Mar  6 11:00:00 2025
///
///                 12345678 blocks of size 1024. 9876543 blocks available
/// ```
fn parse_smbclient_ls(output: &str, parent: &Path) -> Result<Vec<FileEntry>> {
    let mut entries = Vec::new();

    for line in output.lines() {
        let line = line.trim_end();
        // Skip empty lines and the "blocks" summary line
        if line.is_empty() || line.contains("blocks of size") || line.contains("blocks available") {
            continue;
        }

        // Format: "  name                              ATTRS    SIZE  DATE"
        // The name is left-padded with 2 spaces, then right-padded to ~40 chars
        // Attributes field is at a fixed-ish position, then size, then date
        if let Some(entry) = parse_smbclient_ls_line(line, parent) {
            // Skip . entry (we add our own parent entry)
            if entry.name != "." {
                entries.push(entry);
            }
        }
    }

    Ok(entries)
}

fn parse_smbclient_ls_line(line: &str, parent: &Path) -> Option<FileEntry> {
    // smbclient output: "  filename                          D        0  Mon Mar 10 12:00:00 2025"
    // The attributes field (D, A, H, S, R, N) appears after the filename
    // We scan from the right to find the date, size, and attributes

    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return None;
    }

    // Find the attributes marker. smbclient puts attributes like "D", "A", "N", "H", "S", "R"
    // or combinations, right-justified around column 36-40 from the start of the trimmed line.
    // The format is: name (padded) ATTRS SIZE DATE
    // Let's parse from the right: the date is the last ~24 chars, then size, then attrs, then name

    // Strategy: split on two-or-more spaces to find the fields after the filename
    // The line ends with something like "D        0  Mon Mar 10 12:00:00 2025"

    // Find the last occurrence of a size pattern (digits possibly with spaces before)
    // Actually, let's use a regex-like approach: find attribute chars pattern

    // Find position of attributes: scan for a segment that's only D/A/H/S/R/N chars
    // preceded by whitespace and followed by whitespace + digits
    let bytes = trimmed.as_bytes();
    let len = bytes.len();

    // Scan backwards for the month abbreviation in the date (Jan, Feb, Mar, etc.)
    // This anchors us to parse the date + size + attrs
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let mut date_start = None;
    for i in (0..len.saturating_sub(3)).rev() {
        let slice = &trimmed[i..i + 3];
        if months.contains(&slice) {
            // Check it's preceded by spaces + digits (day)  or is part of a date string
            // Date format: "Mon Mar 10 12:00:00 2025"
            // The weekday is 3 chars before the month
            if i >= 4 {
                date_start = Some(i - 4); // weekday + space
                break;
            }
        }
    }

    let date_start = date_start?;

    // Everything before date_start is: "name  ATTRS  SIZE  "
    let before_date = trimmed[..date_start].trim_end();

    // Parse size (last number before the date)
    let last_space = before_date.rfind(|c: char| !c.is_ascii_digit())?;
    let size_str = &before_date[last_space + 1..];
    let size: u64 = size_str.parse().ok()?;

    let before_size = before_date[..=last_space].trim_end();

    // Parse attributes (last word before size)
    let attr_space = before_size.rfind(|c: char| c.is_whitespace())?;
    let attrs = &before_size[attr_space + 1..];
    let name = before_size[..attr_space].trim_end();

    if name.is_empty() {
        return None;
    }

    let is_dir = attrs.contains('D');
    let path = if name == ".." {
        parent.parent().unwrap_or(Path::new("\\")).to_path_buf()
    } else {
        parent.join(name)
    };

    // Parse date string
    let date_str = &trimmed[date_start..];
    let modified = parse_smbclient_date(date_str).unwrap_or(UNIX_EPOCH);

    Some(FileEntry {
        name: name.to_string(),
        path,
        is_dir,
        is_symlink: false,
        size,
        modified,
        permissions: if is_dir { 0o755 } else { 0o644 },
    })
}

fn parse_smbclient_date(s: &str) -> Option<SystemTime> {
    // Format: "Mon Mar 10 12:00:00 2025"
    use chrono::NaiveDateTime;
    let dt = NaiveDateTime::parse_from_str(s.trim(), "%a %b %e %H:%M:%S %Y").ok()?;
    let timestamp = dt.and_utc().timestamp();
    if timestamp >= 0 {
        Some(UNIX_EPOCH + Duration::from_secs(timestamp as u64))
    } else {
        Some(UNIX_EPOCH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ls_line_directory() {
        let line = "  Documents                         D        0  Mon Mar 10 12:00:00 2025";
        let parent = Path::new("/");
        let entry = parse_smbclient_ls_line(line, parent).unwrap();
        assert_eq!(entry.name, "Documents");
        assert!(entry.is_dir);
        assert_eq!(entry.size, 0);
    }

    #[test]
    fn parse_ls_line_file() {
        let line = "  photo.jpg                         A  1234567  Wed Feb 12 14:15:00 2025";
        let parent = Path::new("/share");
        let entry = parse_smbclient_ls_line(line, parent).unwrap();
        assert_eq!(entry.name, "photo.jpg");
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 1234567);
        assert_eq!(entry.path, Path::new("/share/photo.jpg"));
    }

    #[test]
    fn parse_ls_line_dotdot() {
        let line = "  ..                                D        0  Mon Mar 10 12:00:00 2025";
        let parent = Path::new("/some/dir");
        let entry = parse_smbclient_ls_line(line, parent).unwrap();
        assert_eq!(entry.name, "..");
        assert!(entry.is_dir);
    }

    #[test]
    fn parse_ls_skip_summary() {
        let output = "\
  .                                   D        0  Mon Mar 10 12:00:00 2025
  ..                                  D        0  Mon Mar 10 12:00:00 2025
  readme.txt                          A     4096  Thu Mar  6 11:00:00 2025

                12345678 blocks of size 1024. 9876543 blocks available
";
        let entries = parse_smbclient_ls(output, Path::new("/")).unwrap();
        // "." is skipped, ".." and "readme.txt" remain
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "..");
        assert_eq!(entries[1].name, "readme.txt");
    }

    #[test]
    fn parse_ls_empty_output() {
        let output = "\n                12345678 blocks of size 1024. 9876543 blocks available\n";
        let entries = parse_smbclient_ls(output, Path::new("/")).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_ls_hidden_file() {
        let line = "  .bashrc                           AH      256  Mon Jan  1 00:00:00 2024";
        let entry = parse_smbclient_ls_line(line, Path::new("/")).unwrap();
        assert_eq!(entry.name, ".bashrc");
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 256);
    }

    #[test]
    fn to_smb_path_conversion() {
        assert_eq!(to_smb_path(Path::new("/docs/file.txt")), "\\docs\\file.txt");
        assert_eq!(to_smb_path(Path::new("/")), "\\");
    }

    #[test]
    fn smb_quote_special_chars() {
        assert_eq!(smb_quote("normal"), "\"normal\"");
        assert_eq!(smb_quote("has\\\"both"), "\"has\\\"both\"");
    }

    #[test]
    fn display_label_format() {
        let conn = SmbConnection {
            share: "//server/share".to_string(),
            host: "server".to_string(),
            share_name: "myshare".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert_eq!(conn.display_label(), r"\\server\myshare");
    }

    #[test]
    fn home_dir_is_root() {
        let conn = SmbConnection {
            share: "//s/s".to_string(),
            host: "s".to_string(),
            share_name: "s".to_string(),
            username: String::new(),
            password: String::new(),
        };
        assert_eq!(conn.home_dir(), Path::new("\\"));
    }
}
