use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// A GCS connection that shells out to `gcloud storage`.
pub struct GcsConnection {
    bucket: String,
    project: Option<String>,
}

impl GcsConnection {
    pub fn connect(bucket: &str, project: Option<&str>) -> Result<Self> {
        let conn = Self {
            bucket: bucket.to_string(),
            project: project.map(|s| s.to_string()),
        };
        // Validate by listing root
        conn.run_gcloud(&["ls", &format!("gs://{}/", bucket), "--format=json"])?;
        Ok(conn)
    }

    fn common_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(ref p) = self.project {
            args.push("--project".into());
            args.push(p.clone());
        }
        args
    }

    fn run_gcloud(&self, args: &[&str]) -> Result<String> {
        crate::debug_log::log(&format!("GCS [{}] gcloud storage {:?}", self.bucket, args));
        let mut cmd = Command::new("gcloud");
        cmd.arg("storage");
        for arg in args {
            cmd.arg(arg);
        }
        for arg in self.common_args() {
            cmd.arg(arg);
        }

        let output = cmd
            .output()
            .context("Failed to run gcloud CLI. Is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("gcloud", false, &stderr);
            anyhow::bail!("gcloud error: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn gs_uri(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        let clean = path_str.trim_start_matches('/');
        if clean.is_empty() {
            format!("gs://{}/", self.bucket)
        } else {
            format!("gs://{}/{}", self.bucket, clean)
        }
    }

    fn to_prefix(&self, path: &Path) -> String {
        crate::remote_fs::path_to_prefix(path)
    }

    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let uri = format!("{}/", self.gs_uri(path).trim_end_matches('/'));
        let output = self.run_gcloud(&["ls", &uri, "--format=json"])?;

        let prefix = self.to_prefix(path);
        let full_prefix = format!("gs://{}/{}", self.bucket, prefix);

        // gcloud storage ls --format=json returns a JSON array
        let json: serde_json::Value = if output.trim().is_empty() {
            serde_json::Value::Array(vec![])
        } else {
            serde_json::from_str(&output).context("Failed to parse gcloud JSON output")?
        };

        let mut entries = Vec::new();
        const MAX_ENTRIES: usize = 10_000;

        if let Some(items) = json.as_array() {
            for item in items {
                // Each item has a "url" or "name" field like "gs://bucket/prefix/name"
                let url = item
                    .get("url")
                    .or_else(|| item.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Strip the gs://bucket/ prefix to get relative name
                let relative = url
                    .strip_prefix(&full_prefix)
                    .or_else(|| url.strip_prefix(&format!("gs://{}/", self.bucket)))
                    .unwrap_or(url);

                let is_dir = relative.ends_with('/');
                let name = relative.trim_end_matches('/');
                if name.is_empty() || name.contains('/') {
                    continue; // Skip nested items
                }

                let size = item
                    .get("size")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_u64())
                    })
                    .unwrap_or(0);
                let modified = item
                    .get("updated")
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso8601)
                    .unwrap_or(UNIX_EPOCH);

                if entries.len() < MAX_ENTRIES {
                    entries.push(FileEntry {
                        name: name.to_string(),
                        path: path.join(name),
                        is_dir,
                        is_symlink: false,
                        size: if is_dir { 0 } else { size },
                        modified,
                        permissions: if is_dir { 0o755 } else { 0o644 },
                    });
                }
            }
        }

        Ok(entries)
    }

    pub fn mkdir(&self, path: &Path) -> Result<()> {
        // Create a zero-byte marker object by piping empty stdin to gcloud storage cp
        let uri = format!("{}/", self.gs_uri(path).trim_end_matches('/'));
        let mut cmd = Command::new("gcloud");
        cmd.arg("storage").arg("cp").arg("-").arg(&uri);
        for arg in self.common_args() {
            cmd.arg(arg);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().context("Failed to run gcloud")?;
        // Close stdin immediately to send zero bytes
        drop(child.stdin.take());
        let output = child.wait_with_output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("gcloud mkdir error: {}", stderr.trim());
        }
        Ok(())
    }

    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let uri = self.gs_uri(path);
        self.run_gcloud(&["rm", "-r", &uri])?;
        Ok(())
    }

    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_uri = self.gs_uri(src);
        let dst_uri = self.gs_uri(dst);
        self.run_gcloud(&["mv", &src_uri, &dst_uri])?;
        Ok(())
    }

    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let uri = self.gs_uri(remote);
        self.run_gcloud(&["cp", &uri, &local.to_string_lossy()])?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let uri = self.gs_uri(remote);
        self.run_gcloud(&["cp", &local.to_string_lossy(), &uri])?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        std::fs::create_dir_all(local)?;
        let uri = format!("{}/", self.gs_uri(remote).trim_end_matches('/'));
        self.run_gcloud(&["cp", "-r", &uri, &local.to_string_lossy()])?;
        Ok(crate::s3::dir_size(local))
    }

    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let uri = format!("{}/", self.gs_uri(remote).trim_end_matches('/'));
        self.run_gcloud(&["cp", "-r", &local.to_string_lossy(), &uri])?;
        Ok(crate::s3::dir_size(local))
    }

    pub fn display_label(&self) -> String {
        format!("GCS: {}", self.bucket)
    }

    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("/")
    }
}

impl crate::remote_fs::RemoteFs for GcsConnection {
    fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        self.read_dir(path)
    }
    fn mkdir(&self, path: &Path) -> Result<()> {
        self.mkdir(path)
    }
    fn remove_recursive(&self, path: &Path) -> Result<()> {
        self.remove_recursive(path)
    }
    fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        self.rename(src, dst)
    }
    fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        self.download(remote, local)
    }
    fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        self.upload(local, remote)
    }
    fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        self.download_dir(remote, local)
    }
    fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        self.upload_dir(local, remote)
    }
    fn home_dir(&self) -> PathBuf {
        self.home_dir()
    }
    fn display_label(&self) -> String {
        self.display_label()
    }
}

fn parse_iso8601(s: &str) -> Option<SystemTime> {
    crate::s3::parse_iso8601(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_gcs_listing_basic() {
        // Simulates gcloud storage ls --format=json output
        let json: serde_json::Value = serde_json::from_str(r#"[
            {"url": "gs://mybucket/docs/readme.md", "size": "1234", "updated": "2024-01-15T10:30:27Z"},
            {"url": "gs://mybucket/docs/subdir/", "size": "0", "updated": "2024-01-01T00:00:00Z"}
        ]"#).unwrap();

        let prefix = "docs/";
        let full_prefix = "gs://mybucket/docs/";
        let mut entries = Vec::new();
        if let Some(items) = json.as_array() {
            for item in items {
                let url = item.get("url").and_then(|v| v.as_str()).unwrap_or("");
                let relative = url.strip_prefix(full_prefix).unwrap_or(url);
                let is_dir = relative.ends_with('/');
                let name = relative.trim_end_matches('/');
                if name.is_empty() || name.contains('/') {
                    continue;
                }
                let size = item
                    .get("size")
                    .and_then(|v| v.as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0u64);
                entries.push((name.to_string(), is_dir, size));
            }
        }
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "readme.md");
        assert!(!entries[0].1);
        assert_eq!(entries[0].2, 1234);
        assert_eq!(entries[1].0, "subdir");
        assert!(entries[1].1);
    }

    #[test]
    fn gcs_uri_building() {
        let conn = GcsConnection {
            bucket: "my-bucket".to_string(),
            project: None,
        };
        assert_eq!(conn.gs_uri(Path::new("/")), "gs://my-bucket/");
        assert_eq!(
            conn.gs_uri(Path::new("/docs/file.txt")),
            "gs://my-bucket/docs/file.txt"
        );
    }

    #[test]
    fn gcs_to_prefix() {
        let conn = GcsConnection {
            bucket: "b".to_string(),
            project: None,
        };
        assert_eq!(conn.to_prefix(Path::new("/")), "");
        assert_eq!(conn.to_prefix(Path::new("/data")), "data/");
    }

    #[test]
    fn gcs_display_label() {
        let conn = GcsConnection {
            bucket: "my-bucket".to_string(),
            project: Some("proj".to_string()),
        };
        assert_eq!(conn.display_label(), "GCS: my-bucket");
    }
}
