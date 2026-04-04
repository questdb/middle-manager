use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;
use crate::ssh::SshHost;

/// An SFTP connection that shells out to the `sftp` CLI (OpenSSH).
/// All SSH config, agent, key, and ProxyJump settings are inherited automatically.
pub struct SftpConnection {
    /// The sftp target string (e.g. "user@host" or just "host")
    target: String,
    /// Extra SSH args (-p port, -i identity, -J jump)
    ssh_args: Vec<String>,
}

impl SftpConnection {
    /// Connect and validate by listing the remote home directory.
    pub fn connect(host: &SshHost) -> Result<Self> {
        let target = host.display_label();

        let mut ssh_args = Vec::new();
        if let Some(port) = host.port {
            ssh_args.push("-P".into()); // sftp uses -P (uppercase) for port
            ssh_args.push(port.to_string());
        }
        if let Some(ref identity) = host.identity_file {
            ssh_args.push("-i".into());
            ssh_args.push(identity.clone());
        }
        if let Some(ref jump) = host.jump_host {
            ssh_args.push("-J".into());
            ssh_args.push(jump.clone());
        }

        let conn = Self {
            target,
            ssh_args,
        };

        // Validate connection by listing root
        conn.run_batch("pwd")?;
        Ok(conn)
    }

    /// Run an sftp batch command and return stdout.
    fn run_batch(&self, batch_cmd: &str) -> Result<String> {
        crate::debug_log::log(&format!("SFTP [{}] batch: {}", self.target, batch_cmd.lines().next().unwrap_or("")));

        let mut cmd = Command::new("sftp");
        cmd.arg("-oBatchMode=yes")
            .arg("-oConnectTimeout=15")
            .arg("-b")
            .arg("/dev/stdin");

        for arg in &self.ssh_args {
            cmd.arg(arg);
        }
        cmd.arg(&self.target);

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().context("Failed to run sftp. Is OpenSSH installed?")?;

        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            let _ = stdin.write_all(batch_cmd.as_bytes());
            let _ = stdin.write_all(b"\n");
        }

        let output = child.wait_with_output().context("sftp process failed")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let err_msg = stderr.trim();
            crate::debug_log::log_cmd_result("sftp", false, err_msg);
            if !err_msg.is_empty() {
                anyhow::bail!("sftp error: {}", err_msg);
            }
            anyhow::bail!("sftp command failed with status {}", output.status);
        }

        crate::debug_log::log_cmd_result("sftp", true, "");
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Display label for panel header.
    pub fn display_label(&self) -> String {
        self.target.clone()
    }

    /// List directory contents.
    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let path_str = path.to_string_lossy();
        let cmd = format!("ls -la {}", shell_quote(&path_str));
        let output = self.run_batch(&cmd)?;
        parse_sftp_ls(&output, path)
    }

    /// Create a remote directory.
    pub fn mkdir(&self, path: &Path) -> Result<()> {
        let cmd = format!("mkdir {}", shell_quote(&path.to_string_lossy()));
        self.run_batch(&cmd)?;
        Ok(())
    }

    /// Remove a remote file.
    #[allow(dead_code)]
    pub fn remove_file(&self, path: &Path) -> Result<()> {
        let cmd = format!("rm {}", shell_quote(&path.to_string_lossy()));
        self.run_batch(&cmd)?;
        Ok(())
    }

    /// Remove a remote directory (recursive).
    /// Collects all delete commands first, then executes them in batches.
    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let mut cmds = Vec::new();
        self.collect_remove_cmds(path, &mut cmds)?;
        // Execute in batches of 500 to avoid excessive memory/command length
        for chunk in cmds.chunks(500) {
            self.run_batch(&chunk.join("\n"))?;
        }
        Ok(())
    }

    /// Recursively collect rm/rmdir commands (files first, then directories bottom-up).
    fn collect_remove_cmds(&self, path: &Path, cmds: &mut Vec<String>) -> Result<()> {
        let entries = self.read_dir(path)?;
        for entry in &entries {
            if entry.name == ".." {
                continue;
            }
            if entry.is_dir {
                self.collect_remove_cmds(&entry.path, cmds)?;
            } else {
                cmds.push(format!("rm {}", shell_quote(&entry.path.to_string_lossy())));
            }
        }
        cmds.push(format!("rmdir {}", shell_quote(&path.to_string_lossy())));
        Ok(())
    }

    /// Rename a remote file or directory.
    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let cmd = format!(
            "rename {} {}",
            shell_quote(&src.to_string_lossy()),
            shell_quote(&dst.to_string_lossy())
        );
        self.run_batch(&cmd)?;
        Ok(())
    }

    /// Download a remote file to a local path.
    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let cmd = format!(
            "get {} {}",
            shell_quote(&remote.to_string_lossy()),
            shell_quote(&local.to_string_lossy())
        );
        self.run_batch(&cmd)?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    /// Upload a local file to a remote path.
    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let cmd = format!(
            "put {} {}",
            shell_quote(&local.to_string_lossy()),
            shell_quote(&remote.to_string_lossy())
        );
        self.run_batch(&cmd)?;
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

    /// Get the home directory on the remote host.
    pub fn home_dir(&self) -> PathBuf {
        // Run `pwd` to get the initial directory (which is the home dir)
        match self.run_batch("pwd") {
            Ok(output) => {
                // Output is like: "Remote working directory: /home/user\n"
                for line in output.lines() {
                    let line = line.trim();
                    if let Some(path) = line.strip_prefix("Remote working directory: ") {
                        return PathBuf::from(path.trim());
                    }
                    // Some sftp versions just print the path
                    if line.starts_with('/') {
                        return PathBuf::from(line);
                    }
                }
                PathBuf::from("/")
            }
            Err(_) => PathBuf::from("/"),
        }
    }
}

impl crate::remote_fs::RemoteFs for SftpConnection {
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

/// Quote a string for sftp batch commands (wrap in double quotes, escape inner quotes).
///
/// Rejects newlines and other control characters to prevent command injection
/// in SFTP batch mode (which processes commands line-by-line).
fn shell_quote(s: &str) -> String {
    // Reject control characters (including \n, \r) that could inject batch commands
    if s.bytes().any(|b| b < 0x20 && b != b'\t') {
        return "\"<invalid-filename>\"".to_string();
    }
    if s.contains('"') || s.contains(' ') || s.contains('\\') {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        format!("\"{}\"", s)
    }
}

/// Parse `sftp ls -la` output into FileEntry items.
///
/// Output format (like `ls -la`):
/// ```text
/// drwxr-xr-x    5 user     group        4096 Mar 10 12:00 Documents
/// -rw-r--r--    1 user     group     1234567 Feb 12 14:15 photo.jpg
/// lrwxrwxrwx    1 user     group          11 Jan  5 09:00 link -> target
/// ```
fn parse_sftp_ls(output: &str, parent: &Path) -> Result<Vec<FileEntry>> {
    let mut entries = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        // Skip empty lines and sftp status messages
        if line.is_empty()
            || line.starts_with("sftp>")
            || line.starts_with("Fetching")
            || line.starts_with("Couldn't")
        {
            continue;
        }

        // Must start with a permission string: drwx, -rw-, lrwx, etc.
        let first_char = line.chars().next().unwrap_or(' ');
        if !matches!(first_char, 'd' | '-' | 'l' | 'c' | 'b' | 's' | 'p') {
            continue;
        }

        if let Some(entry) = parse_ls_line(line, parent) {
            // Skip . entry (we add our own parent entry)
            if entry.name != "." {
                entries.push(entry);
            }
        }
    }

    Ok(entries)
}

/// Parse a single `ls -la` line.
fn parse_ls_line(line: &str, parent: &Path) -> Option<FileEntry> {
    // Fields: permissions links user group size month day time/year name
    // Example: "drwxr-xr-x    5 nick  staff      160 Mar 10 12:00 Documents"
    let mut parts = line.split_whitespace();

    let perms_str = parts.next()?;
    let _links = parts.next()?;
    let _user = parts.next()?;
    let _group = parts.next()?;
    let size_str = parts.next()?;
    let month = parts.next()?;
    let day = parts.next()?;
    let time_or_year = parts.next()?;
    // Remaining is the filename (may contain spaces)
    let name: String = parts.collect::<Vec<&str>>().join(" ");

    if name.is_empty() {
        return None;
    }

    // Handle symlinks: "link -> target"
    let (display_name, is_symlink) = if let Some(arrow_pos) = name.find(" -> ") {
        (name[..arrow_pos].to_string(), true)
    } else {
        (name, false)
    };

    let is_dir = perms_str.starts_with('d');
    let size: u64 = size_str.parse().unwrap_or(0);
    let permissions = parse_permission_string(perms_str);

    let path = if display_name == ".." {
        parent.parent().unwrap_or(Path::new("/")).to_path_buf()
    } else {
        parent.join(&display_name)
    };

    let modified = parse_ls_date(month, day, time_or_year).unwrap_or(UNIX_EPOCH);

    Some(FileEntry {
        name: display_name,
        path,
        is_dir,
        is_symlink,
        size,
        modified,
        permissions,
    })
}

/// Parse ls permission string like "drwxr-xr-x" into a numeric mode.
fn parse_permission_string(s: &str) -> u32 {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() < 10 {
        return 0;
    }
    let mut mode: u32 = 0;
    let bits = [
        (1, 'r', 0o400),
        (2, 'w', 0o200),
        (3, 'x', 0o100),
        (4, 'r', 0o040),
        (5, 'w', 0o020),
        (6, 'x', 0o010),
        (7, 'r', 0o004),
        (8, 'w', 0o002),
        (9, 'x', 0o001),
    ];
    for (idx, expected, bit) in bits {
        if chars[idx] == expected || chars[idx] == 's' || chars[idx] == 't' {
            mode |= bit;
        }
    }
    mode
}

/// Parse ls date: "Mar 10 12:00" or "Mar 10  2024"
fn parse_ls_date(month: &str, day: &str, time_or_year: &str) -> Option<SystemTime> {
    use chrono::NaiveDateTime;
    let year_or_time = time_or_year;

    if year_or_time.contains(':') {
        // Format: "Mar 10 12:00" — current year
        let now = chrono::Local::now();
        let date_str = format!("{} {} {} {}", month, day, year_or_time, now.format("%Y"));
        let dt = NaiveDateTime::parse_from_str(&date_str, "%b %d %H:%M %Y").ok()?;
        let ts = dt.and_utc().timestamp();
        if ts >= 0 {
            Some(UNIX_EPOCH + Duration::from_secs(ts as u64))
        } else {
            Some(UNIX_EPOCH)
        }
    } else {
        // Format: "Mar 10  2024" — year given, no time
        let date_str = format!("{} {} 00:00 {}", month, day, year_or_time);
        let dt = NaiveDateTime::parse_from_str(&date_str, "%b %d %H:%M %Y").ok()?;
        let ts = dt.and_utc().timestamp();
        if ts >= 0 {
            Some(UNIX_EPOCH + Duration::from_secs(ts as u64))
        } else {
            Some(UNIX_EPOCH)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ls_directory() {
        let line = "drwxr-xr-x    5 nick     staff        160 Mar 10 12:00 Documents";
        let entry = parse_ls_line(line, Path::new("/home")).unwrap();
        assert_eq!(entry.name, "Documents");
        assert!(entry.is_dir);
        assert_eq!(entry.size, 160);
        assert_eq!(entry.path, Path::new("/home/Documents"));
    }

    #[test]
    fn parse_ls_file() {
        let line = "-rw-r--r--    1 nick     staff    1234567 Feb 12 14:15 photo.jpg";
        let entry = parse_ls_line(line, Path::new("/data")).unwrap();
        assert_eq!(entry.name, "photo.jpg");
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 1234567);
        assert!(!entry.is_symlink);
    }

    #[test]
    fn parse_ls_symlink() {
        let line = "lrwxrwxrwx    1 nick     staff         11 Jan  5 09:00 link -> target";
        let entry = parse_ls_line(line, Path::new("/")).unwrap();
        assert_eq!(entry.name, "link");
        assert!(entry.is_symlink);
    }

    #[test]
    fn parse_ls_file_with_year() {
        let line = "-rw-r--r--    1 nick     staff      4096 Dec 25  2023 old_file.txt";
        let entry = parse_ls_line(line, Path::new("/")).unwrap();
        assert_eq!(entry.name, "old_file.txt");
        assert_eq!(entry.size, 4096);
    }

    #[test]
    fn parse_permission_string_basic() {
        assert_eq!(parse_permission_string("drwxr-xr-x"), 0o755);
        assert_eq!(parse_permission_string("-rw-r--r--"), 0o644);
        assert_eq!(parse_permission_string("-rwxrwxrwx"), 0o777);
        assert_eq!(parse_permission_string("----------"), 0o000);
    }

    #[test]
    fn parse_full_ls_output() {
        let output = "\
sftp> ls -la /home/nick
drwxr-xr-x    5 nick     staff        160 Mar 10 12:00 .
drwxr-xr-x    3 root     root        4096 Jan  1 00:00 ..
drwxr-xr-x    2 nick     staff          64 Feb  5 09:30 Documents
-rw-r--r--    1 nick     staff      123456 Mar  1 14:15 readme.md
";
        let entries = parse_sftp_ls(output, Path::new("/home/nick")).unwrap();
        // "." is skipped, ".." and the rest remain
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, "..");
        assert_eq!(entries[1].name, "Documents");
        assert!(entries[1].is_dir);
        assert_eq!(entries[2].name, "readme.md");
        assert_eq!(entries[2].size, 123456);
    }

    #[test]
    fn shell_quote_basic() {
        assert_eq!(shell_quote("/simple/path"), "\"/simple/path\"");
        assert_eq!(shell_quote("has spaces"), "\"has spaces\"");
        assert_eq!(shell_quote("has\"quotes"), "\"has\\\"quotes\"");
    }

    #[test]
    fn shell_quote_normal_filename() {
        // No special characters — just wrapped in double quotes
        assert_eq!(shell_quote("readme.txt"), "\"readme.txt\"");
    }

    #[test]
    fn shell_quote_spaces() {
        assert_eq!(shell_quote("my file.txt"), "\"my file.txt\"");
    }

    #[test]
    fn shell_quote_double_quotes() {
        assert_eq!(shell_quote("say\"hello"), "\"say\\\"hello\"");
    }

    #[test]
    fn shell_quote_backslashes() {
        assert_eq!(shell_quote("back\\slash"), "\"back\\\\slash\"");
    }

    #[test]
    fn shell_quote_newline_returns_sentinel() {
        assert_eq!(shell_quote("bad\nname"), "\"<invalid-filename>\"");
    }

    #[test]
    fn shell_quote_carriage_return_returns_sentinel() {
        assert_eq!(shell_quote("bad\rname"), "\"<invalid-filename>\"");
    }

    #[test]
    fn shell_quote_null_byte_returns_sentinel() {
        assert_eq!(shell_quote("bad\0name"), "\"<invalid-filename>\"");
    }

    #[test]
    fn shell_quote_tab_allowed() {
        // Tab (0x09) is the only control char below 0x20 that is NOT rejected
        assert_eq!(shell_quote("col1\tcol2"), "\"col1\tcol2\"");
    }

    #[test]
    fn shell_quote_backtick() {
        // Backtick is not a control char and needs no special escaping in sftp batch mode
        assert_eq!(shell_quote("file`name"), "\"file`name\"");
    }

    #[test]
    fn shell_quote_empty_string() {
        assert_eq!(shell_quote(""), "\"\"");
    }

    #[test]
    fn parse_ls_filename_with_spaces() {
        let line = "-rw-r--r--    1 nick     staff       100 Jan  1 00:00 my file name.txt";
        let entry = parse_ls_line(line, Path::new("/")).unwrap();
        assert_eq!(entry.name, "my file name.txt");
        assert_eq!(entry.size, 100);
    }

    #[test]
    fn parse_ls_empty_line() {
        assert!(parse_ls_line("", Path::new("/")).is_none());
    }

    #[test]
    fn parse_ls_non_entry_line() {
        assert!(parse_ls_line("total 128", Path::new("/")).is_none());
        assert!(parse_ls_line("Fetching /home/nick...", Path::new("/")).is_none());
    }

    #[test]
    fn parse_sftp_ls_filters_sftp_prompt() {
        let output = "sftp> ls -la /data\n-rw-r--r--    1 root root 42 Jan 1 00:00 test.txt\n";
        let entries = parse_sftp_ls(output, Path::new("/data")).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "test.txt");
    }

    #[test]
    fn parse_permission_string_setuid() {
        // setuid/setgid show as 's' instead of 'x'
        assert_eq!(parse_permission_string("-rwsr-xr-x"), 0o755);
    }

    #[test]
    fn parse_permission_string_short() {
        // Too short string returns 0
        assert_eq!(parse_permission_string("drwx"), 0);
    }

    #[test]
    fn parse_ls_dotdot_at_root() {
        let line = "drwxr-xr-x    2 root root 4096 Jan  1 00:00 ..";
        let entry = parse_ls_line(line, Path::new("/")).unwrap();
        assert_eq!(entry.name, "..");
        assert_eq!(entry.path, Path::new("/"));
    }
}
