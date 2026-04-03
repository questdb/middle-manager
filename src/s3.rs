use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// An S3 connection that shells out to `aws s3api` / `aws s3`.
pub struct S3Connection {
    bucket: String,
    profile: Option<String>,
    endpoint_url: Option<String>,
    region: Option<String>,
}

impl S3Connection {
    pub fn connect(
        bucket: &str,
        profile: Option<&str>,
        endpoint_url: Option<&str>,
        region: Option<&str>,
    ) -> Result<Self> {
        let conn = Self {
            bucket: bucket.to_string(),
            profile: profile.map(|s| s.to_string()),
            endpoint_url: endpoint_url.map(|s| s.to_string()),
            region: region.map(|s| s.to_string()),
        };
        // Validate by listing root prefix
        conn.run_s3api(&["list-objects-v2", "--max-items", "1"])?;
        Ok(conn)
    }

    /// Build common aws CLI args (profile, endpoint, region).
    fn common_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(ref p) = self.profile {
            args.push("--profile".into());
            args.push(p.clone());
        }
        if let Some(ref e) = self.endpoint_url {
            args.push("--endpoint-url".into());
            args.push(e.clone());
        }
        if let Some(ref r) = self.region {
            args.push("--region".into());
            args.push(r.clone());
        }
        args
    }

    /// Run `aws s3api <subcommand> --bucket <bucket> [extra_args] --output json` and return parsed JSON.
    fn run_s3api(&self, extra_args: &[&str]) -> Result<serde_json::Value> {
        crate::debug_log::log(&format!("S3 [{}] s3api {:?}", self.bucket, extra_args));
        let mut cmd = Command::new("aws");
        cmd.arg("s3api");
        for arg in extra_args {
            cmd.arg(arg);
        }
        cmd.arg("--bucket").arg(&self.bucket);
        for arg in self.common_args() {
            cmd.arg(arg);
        }
        cmd.arg("--output").arg("json");

        let output = cmd
            .output()
            .context("Failed to run aws CLI. Is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("aws s3api", false, &stderr);
            anyhow::bail!("aws s3api error: {}", stderr.trim());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(serde_json::Value::Object(serde_json::Map::new()));
        }
        serde_json::from_str(&stdout).context("Failed to parse aws s3api JSON output")
    }

    /// Run `aws s3 <subcommand> [args]` (for cp, rm, mv which use s3:// URIs).
    fn run_s3(&self, args: &[&str]) -> Result<String> {
        crate::debug_log::log(&format!("S3 [{}] s3 {:?}", self.bucket, args));
        let mut cmd = Command::new("aws");
        cmd.arg("s3");
        for arg in args {
            cmd.arg(arg);
        }
        for arg in self.common_args() {
            cmd.arg(arg);
        }

        let output = cmd
            .output()
            .context("Failed to run aws CLI. Is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("aws s3", false, &stderr);
            anyhow::bail!("aws s3 error: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Build an S3 URI for a path.
    fn s3_uri(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        let clean = path_str.trim_start_matches('/');
        if clean.is_empty() {
            format!("s3://{}/", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, clean)
        }
    }

    /// Convert a panel path to an S3 prefix (no leading slash, with trailing slash for dirs).
    fn to_prefix(&self, path: &Path) -> String {
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

    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let prefix = self.to_prefix(path);

        let mut args = vec![
            "list-objects-v2",
            "--delimiter",
            "/",
            "--no-paginate",
        ];

        let prefix_owned;
        if !prefix.is_empty() {
            prefix_owned = prefix.clone();
            args.push("--prefix");
            args.push(&prefix_owned);
        }

        let json = self.run_s3api(&args)?;

        let mut entries = Vec::new();

        // Parse CommonPrefixes (virtual directories)
        if let Some(prefixes) = json.get("CommonPrefixes").and_then(|v| v.as_array()) {
            for p in prefixes {
                if let Some(prefix_str) = p.get("Prefix").and_then(|v| v.as_str()) {
                    let name = prefix_str
                        .strip_prefix(&prefix)
                        .unwrap_or(prefix_str)
                        .trim_end_matches('/');
                    if name.is_empty() {
                        continue;
                    }
                    entries.push(FileEntry {
                        name: name.to_string(),
                        path: path.join(name),
                        is_dir: true,
                        is_symlink: false,
                        size: 0,
                        modified: UNIX_EPOCH,
                        permissions: 0o755,
                    });
                }
            }
        }

        // Parse Contents (objects/files)
        if let Some(contents) = json.get("Contents").and_then(|v| v.as_array()) {
            for obj in contents {
                let key = obj.get("Key").and_then(|v| v.as_str()).unwrap_or("");
                let name = key
                    .strip_prefix(&prefix)
                    .unwrap_or(key);
                // Skip the prefix itself (empty name) and directory markers
                if name.is_empty() || name == "/" {
                    continue;
                }
                let size = obj.get("Size").and_then(|v| v.as_u64()).unwrap_or(0);
                let modified = obj
                    .get("LastModified")
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso8601)
                    .unwrap_or(UNIX_EPOCH);

                entries.push(FileEntry {
                    name: name.to_string(),
                    path: path.join(name),
                    is_dir: false,
                    is_symlink: false,
                    size,
                    modified,
                    permissions: 0o644,
                });
            }
        }

        Ok(entries)
    }

    pub fn mkdir(&self, path: &Path) -> Result<()> {
        let key = self.to_prefix(path);
        self.run_s3api(&[
            "put-object",
            "--key",
            &key,
            "--content-length",
            "0",
        ])?;
        Ok(())
    }

    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let uri = self.s3_uri(path);
        // aws s3 rm handles both files and prefixes with --recursive
        self.run_s3(&["rm", &uri, "--recursive"])?;
        Ok(())
    }

    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_uri = self.s3_uri(src);
        let dst_uri = self.s3_uri(dst);
        self.run_s3(&["mv", &src_uri, &dst_uri, "--recursive"])?;
        Ok(())
    }

    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let uri = self.s3_uri(remote);
        self.run_s3(&["cp", &uri, &local.to_string_lossy()])?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let uri = self.s3_uri(remote);
        self.run_s3(&["cp", &local.to_string_lossy(), &uri])?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        std::fs::create_dir_all(local)?;
        let uri = format!("{}/", self.s3_uri(remote).trim_end_matches('/'));
        self.run_s3(&["cp", &uri, &local.to_string_lossy(), "--recursive"])?;
        Ok(dir_size(local))
    }

    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let uri = format!("{}/", self.s3_uri(remote).trim_end_matches('/'));
        self.run_s3(&["cp", &local.to_string_lossy(), &uri, "--recursive"])?;
        Ok(dir_size(local))
    }

    pub fn display_label(&self) -> String {
        if let Some(ref endpoint) = self.endpoint_url {
            format!("S3: {}/{}", endpoint, self.bucket)
        } else {
            format!("S3: {}", self.bucket)
        }
    }

    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("/")
    }
}

impl crate::remote_fs::RemoteFs for S3Connection {
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

pub fn parse_iso8601(s: &str) -> Option<SystemTime> {
    // Format: "2024-01-15T10:30:27.000Z" or "2024-01-15T10:30:27+00:00"
    use chrono::DateTime;
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        let ts = dt.timestamp();
        if ts >= 0 {
            return Some(UNIX_EPOCH + Duration::from_secs(ts as u64));
        }
    }
    // Try without timezone
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
        let ts = dt.and_utc().timestamp();
        if ts >= 0 {
            return Some(UNIX_EPOCH + Duration::from_secs(ts as u64));
        }
    }
    None
}

pub fn dir_size(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| {
                    if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        dir_size(&e.path())
                    } else {
                        e.metadata().map(|m| m.len()).unwrap_or(0)
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

/// Parse S3 list-objects-v2 JSON into FileEntry items.
#[cfg(test)]
pub fn parse_s3_listing(json: &serde_json::Value, prefix: &str, parent: &Path) -> Vec<FileEntry> {
    let mut entries = Vec::new();

    if let Some(prefixes) = json.get("CommonPrefixes").and_then(|v| v.as_array()) {
        for p in prefixes {
            if let Some(prefix_str) = p.get("Prefix").and_then(|v| v.as_str()) {
                let name = prefix_str
                    .strip_prefix(prefix)
                    .unwrap_or(prefix_str)
                    .trim_end_matches('/');
                if name.is_empty() {
                    continue;
                }
                entries.push(FileEntry {
                    name: name.to_string(),
                    path: parent.join(name),
                    is_dir: true,
                    is_symlink: false,
                    size: 0,
                    modified: UNIX_EPOCH,
                    permissions: 0o755,
                });
            }
        }
    }

    if let Some(contents) = json.get("Contents").and_then(|v| v.as_array()) {
        for obj in contents {
            let key = obj.get("Key").and_then(|v| v.as_str()).unwrap_or("");
            let name = key.strip_prefix(prefix).unwrap_or(key);
            if name.is_empty() || name == "/" {
                continue;
            }
            let size = obj.get("Size").and_then(|v| v.as_u64()).unwrap_or(0);
            let modified = obj
                .get("LastModified")
                .and_then(|v| v.as_str())
                .and_then(parse_iso8601)
                .unwrap_or(UNIX_EPOCH);

            entries.push(FileEntry {
                name: name.to_string(),
                path: parent.join(name),
                is_dir: false,
                is_symlink: false,
                size,
                modified,
                permissions: 0o644,
            });
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_iso8601_rfc3339() {
        let t = parse_iso8601("2024-01-15T10:30:27.000Z").unwrap();
        assert!(t > UNIX_EPOCH);
    }

    #[test]
    fn parse_iso8601_with_offset() {
        let t = parse_iso8601("2024-01-15T10:30:27+00:00").unwrap();
        assert!(t > UNIX_EPOCH);
    }

    #[test]
    fn parse_iso8601_invalid() {
        assert!(parse_iso8601("not a date").is_none());
        assert!(parse_iso8601("").is_none());
    }

    #[test]
    fn parse_s3_listing_basic() {
        let json: serde_json::Value = serde_json::from_str(r#"{
            "Contents": [
                {"Key": "docs/readme.md", "Size": 1234, "LastModified": "2024-01-15T10:30:27.000Z"},
                {"Key": "docs/notes.txt", "Size": 567, "LastModified": "2024-02-01T12:00:00.000Z"}
            ],
            "CommonPrefixes": [
                {"Prefix": "docs/images/"},
                {"Prefix": "docs/data/"}
            ]
        }"#).unwrap();

        let entries = parse_s3_listing(&json, "docs/", Path::new("/docs"));
        assert_eq!(entries.len(), 4);

        // Directories come first in our parsing
        assert_eq!(entries[0].name, "images");
        assert!(entries[0].is_dir);
        assert_eq!(entries[1].name, "data");
        assert!(entries[1].is_dir);

        // Then files
        assert_eq!(entries[2].name, "readme.md");
        assert!(!entries[2].is_dir);
        assert_eq!(entries[2].size, 1234);
        assert_eq!(entries[3].name, "notes.txt");
        assert_eq!(entries[3].size, 567);
    }

    #[test]
    fn parse_s3_listing_empty_bucket() {
        let json: serde_json::Value = serde_json::from_str(r#"{}"#).unwrap();
        let entries = parse_s3_listing(&json, "", Path::new("/"));
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_s3_listing_root_prefix() {
        let json: serde_json::Value = serde_json::from_str(r#"{
            "Contents": [
                {"Key": "file.txt", "Size": 100, "LastModified": "2024-01-01T00:00:00Z"}
            ],
            "CommonPrefixes": [
                {"Prefix": "folder/"}
            ]
        }"#).unwrap();

        let entries = parse_s3_listing(&json, "", Path::new("/"));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "folder");
        assert!(entries[0].is_dir);
        assert_eq!(entries[1].name, "file.txt");
    }

    #[test]
    fn parse_s3_listing_skips_prefix_marker() {
        let json: serde_json::Value = serde_json::from_str(r#"{
            "Contents": [
                {"Key": "mydir/", "Size": 0, "LastModified": "2024-01-01T00:00:00Z"},
                {"Key": "mydir/file.txt", "Size": 42, "LastModified": "2024-01-01T00:00:00Z"}
            ]
        }"#).unwrap();

        let entries = parse_s3_listing(&json, "mydir/", Path::new("/mydir"));
        // The "mydir/" key results in empty name after prefix stripping → skipped
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "file.txt");
    }

    #[test]
    fn s3_uri_building() {
        let conn = S3Connection {
            bucket: "my-bucket".to_string(),
            profile: None,
            endpoint_url: None,
            region: None,
        };
        assert_eq!(conn.s3_uri(Path::new("/")), "s3://my-bucket/");
        assert_eq!(conn.s3_uri(Path::new("/docs/file.txt")), "s3://my-bucket/docs/file.txt");
        assert_eq!(conn.s3_uri(Path::new("")), "s3://my-bucket/");
    }

    #[test]
    fn s3_to_prefix() {
        let conn = S3Connection {
            bucket: "b".to_string(),
            profile: None,
            endpoint_url: None,
            region: None,
        };
        assert_eq!(conn.to_prefix(Path::new("/")), "");
        assert_eq!(conn.to_prefix(Path::new("/docs")), "docs/");
        assert_eq!(conn.to_prefix(Path::new("/docs/")), "docs/");
        assert_eq!(conn.to_prefix(Path::new("")), "");
    }

    #[test]
    fn display_label_plain() {
        let conn = S3Connection {
            bucket: "my-bucket".to_string(),
            profile: None,
            endpoint_url: None,
            region: None,
        };
        assert_eq!(conn.display_label(), "S3: my-bucket");
    }

    #[test]
    fn display_label_with_endpoint() {
        let conn = S3Connection {
            bucket: "my-bucket".to_string(),
            profile: None,
            endpoint_url: Some("https://minio.local:9000".to_string()),
            region: None,
        };
        assert_eq!(conn.display_label(), "S3: https://minio.local:9000/my-bucket");
    }
}
