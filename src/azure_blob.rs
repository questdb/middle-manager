use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// An Azure Blob Storage connection that shells out to `az storage blob`.
/// When `container` is empty, operates at the account level (listing/creating containers).
/// When browsing, the first path segment is used as the container name.
pub struct AzureBlobConnection {
    account: String,
    /// Default container. If empty, first path segment is the container.
    container: String,
    sas_token: Option<String>,
    connection_string: Option<String>,
}

impl AzureBlobConnection {
    pub fn connect(
        account: &str,
        container: &str,
        sas_token: Option<&str>,
        connection_string: Option<&str>,
    ) -> Result<Self> {
        let conn = Self {
            account: account.to_string(),
            container: container.to_string(),
            sas_token: sas_token.map(|s| s.to_string()),
            connection_string: connection_string.map(|s| s.to_string()),
        };
        // Validate: list containers (account level) or blobs (container level)
        if conn.container.is_empty() {
            conn.list_containers()?;
        } else {
            conn.run_az_list("", 1)?;
        }
        Ok(conn)
    }

    /// Auth-only CLI args (no container). Sensitive values (connection string,
    /// SAS token) are passed via environment variables — see `set_auth_env`.
    fn auth_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(ref cs) = self.connection_string {
            // Connection string is passed via AZURE_STORAGE_CONNECTION_STRING env var
            if let Some(endpoint) = extract_connection_string_field(cs, "BlobEndpoint") {
                args.push("--blob-endpoint".into());
                args.push(endpoint);
            }
        } else {
            args.push("--account-name".into());
            args.push(self.account.clone());
            // SAS token is passed via AZURE_STORAGE_SAS_TOKEN env var
        }
        args
    }

    /// Set authentication environment variables on a Command so that secrets
    /// are not visible in the process table.
    fn set_auth_env(&self, cmd: &mut Command) {
        if let Some(ref cs) = self.connection_string {
            cmd.env("AZURE_STORAGE_CONNECTION_STRING", cs);
        } else if let Some(ref sas) = self.sas_token {
            cmd.env("AZURE_STORAGE_SAS_TOKEN", sas);
        }
    }

    /// Auth args + container name.
    fn common_args(&self) -> Vec<String> {
        let mut args = self.auth_args();
        args.push("--container-name".into());
        args.push(self.container.clone());
        args
    }

    /// List containers in the storage account.
    fn list_containers(&self) -> Result<serde_json::Value> {
        let mut cmd = Command::new("az");
        cmd.arg("storage").arg("container").arg("list");
        self.set_auth_env(&mut cmd);
        for arg in self.auth_args() {
            cmd.arg(arg);
        }
        cmd.arg("--output").arg("json");

        crate::debug_log::log(&format!("Azure [{}] az storage container list", self.account));
        let output = cmd.output().context("Failed to run az CLI. Is it installed?")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("az", false, &stderr);
            let stderr_lower = stderr.to_lowercase();
            if stderr_lower.contains("api version") && stderr_lower.contains("not supported") {
                anyhow::bail!(
                    "API version not supported by server.\n\n\
                     If using Azurite, restart it with:\n  \
                     azurite --skipApiVersionCheck"
                );
            }
            anyhow::bail!("az storage container list error: {}", stderr.trim());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        serde_json::from_str(&stdout).context("Failed to parse az JSON output")
    }

    /// Create a new container.
    fn create_container(&self, name: &str) -> Result<()> {
        let mut cmd = Command::new("az");
        cmd.arg("storage").arg("container").arg("create")
            .arg("--name").arg(name);
        self.set_auth_env(&mut cmd);
        for arg in self.auth_args() {
            cmd.arg(arg);
        }
        crate::debug_log::log(&format!("Azure [{}] az storage container create --name {}", self.account, name));
        let output = cmd.output().context("Failed to run az CLI")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to create container: {}", stderr.trim());
        }
        Ok(())
    }

    fn run_az(&self, subcmd: &[&str], extra_args: &[&str]) -> Result<String> {
        self.run_az_inner(subcmd, extra_args)
    }

    /// Run az storage blob with a specific container (used when container is dynamic).
    fn run_az_with_container(&self, subcmd: &[&str], extra_args: &[&str], container: &str) -> Result<String> {
        crate::debug_log::log(&format!(
            "Azure [{}/{}] az storage blob {:?} {:?}",
            self.account, container, subcmd, extra_args
        ));
        let mut cmd = Command::new("az");
        cmd.arg("storage").arg("blob");
        for arg in subcmd {
            cmd.arg(arg);
        }
        self.set_auth_env(&mut cmd);
        for arg in self.args_for_container(container) {
            cmd.arg(arg);
        }
        for arg in extra_args {
            cmd.arg(arg);
        }
        let output = cmd.output().context("Failed to run az CLI. Is it installed?")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("az", false, &stderr);
            let stderr_lower = stderr.to_lowercase();
            if stderr_lower.contains("api version") && stderr_lower.contains("not supported") {
                anyhow::bail!(
                    "API version not supported by server.\n\n\
                     If using Azurite, restart it with:\n  \
                     azurite --skipApiVersionCheck"
                );
            }
            anyhow::bail!("az storage blob error: {}", stderr.trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn run_az_inner(&self, subcmd: &[&str], extra_args: &[&str]) -> Result<String> {
        crate::debug_log::log(&format!(
            "Azure [{}/{}] az storage blob {:?} {:?}",
            self.account, self.container, subcmd, extra_args
        ));
        let mut cmd = Command::new("az");
        cmd.arg("storage").arg("blob");
        for arg in subcmd {
            cmd.arg(arg);
        }
        self.set_auth_env(&mut cmd);
        for arg in self.common_args() {
            cmd.arg(arg);
        }
        for arg in extra_args {
            cmd.arg(arg);
        }

        let output = cmd
            .output()
            .context("Failed to run az CLI. Is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            crate::debug_log::log_cmd_result("az", false, &stderr);

            // Provide helpful error for Azurite API version mismatch
            let stderr_lower = stderr.to_lowercase();
            if stderr_lower.contains("api version") && stderr_lower.contains("not supported") {
                anyhow::bail!(
                    "API version not supported by server.\n\n\
                     If using Azurite, restart it with:\n  \
                     azurite --skipApiVersionCheck"
                );
            }

            anyhow::bail!("az storage blob error: {}", stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn run_az_list(&self, prefix: &str, num_results: u32) -> Result<serde_json::Value> {
        let num_str = num_results.to_string();
        let mut extra = vec![
            "--delimiter", "/",
            "--num-results", &num_str,
            "--output", "json",
        ];
        if !prefix.is_empty() {
            extra.push("--prefix");
            extra.push(prefix);
        }

        let output = self.run_az(&["list"], &extra)?;
        if output.trim().is_empty() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        serde_json::from_str(&output).context("Failed to parse az JSON output")
    }

    /// Resolve effective container and blob prefix from a path.
    /// When `self.container` is set, the path is the blob prefix directly.
    /// When empty, the first path segment is the container, rest is the prefix.
    fn resolve_path(&self, path: &Path) -> (String, String) {
        if !self.container.is_empty() {
            return (self.container.clone(), self.to_prefix(path));
        }
        let s = path.to_string_lossy();
        let clean = s.trim_start_matches('/');
        if clean.is_empty() {
            return (String::new(), String::new()); // Account level
        }
        match clean.find('/') {
            Some(pos) => {
                let container = clean[..pos].to_string();
                let rest = &clean[pos + 1..];
                let prefix = if rest.is_empty() {
                    String::new()
                } else if rest.ends_with('/') {
                    rest.to_string()
                } else {
                    format!("{}/", rest)
                };
                (container, prefix)
            }
            None => (clean.to_string(), String::new()), // Container root
        }
    }

    /// Get common_args with the resolved container.
    fn args_for_container(&self, container: &str) -> Vec<String> {
        let mut args = self.auth_args();
        args.push("--container-name".into());
        args.push(container.to_string());
        args
    }

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
        let (container, prefix) = self.resolve_path(path);

        // Account level: list containers
        if container.is_empty() {
            return self.read_dir_containers(path);
        }

        // Container level: list blobs with the resolved container
        let num_str = "1000".to_string();
        let mut extra: Vec<&str> = vec![
            "--delimiter", "/",
            "--num-results", &num_str,
            "--output", "json",
        ];
        let prefix_owned = prefix.clone();
        if !prefix.is_empty() {
            extra.push("--prefix");
            extra.push(&prefix_owned);
        }
        let output = self.run_az_with_container(&["list"], &extra, &container)?;
        let json: serde_json::Value = if output.trim().is_empty() {
            serde_json::Value::Array(vec![])
        } else {
            serde_json::from_str(&output).context("Failed to parse az JSON output")?
        };

        let mut entries = Vec::new();

        if let Some(items) = json.as_array() {
            for item in items {
                let name_full = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let relative = name_full.strip_prefix(&prefix).unwrap_or(name_full);

                // Virtual directory: name ends with /
                let is_dir = relative.ends_with('/');
                let name = relative.trim_end_matches('/');
                if name.is_empty() || name.contains('/') {
                    continue;
                }

                let size = item
                    .get("properties")
                    .and_then(|p| p.get("contentLength"))
                    .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                    .unwrap_or(0);

                let modified = item
                    .get("properties")
                    .and_then(|p| p.get("lastModified"))
                    .and_then(|v| v.as_str())
                    .and_then(crate::s3::parse_iso8601)
                    .unwrap_or(UNIX_EPOCH);

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

        Ok(entries)
    }

    /// List containers as FileEntry directories.
    fn read_dir_containers(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let json = self.list_containers()?;
        let mut entries = Vec::new();

        if let Some(items) = json.as_array() {
            for item in items {
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if name.is_empty() {
                    continue;
                }
                let modified = item
                    .get("properties")
                    .and_then(|p| p.get("lastModified"))
                    .and_then(|v| v.as_str())
                    .and_then(crate::s3::parse_iso8601)
                    .unwrap_or(UNIX_EPOCH);

                entries.push(FileEntry {
                    name: name.to_string(),
                    path: path.join(name),
                    is_dir: true,
                    is_symlink: false,
                    size: 0,
                    modified,
                    permissions: 0o755,
                });
            }
        }
        Ok(entries)
    }

    pub fn mkdir(&self, path: &Path) -> Result<()> {
        // At account level: create a container
        if self.container.is_empty() {
            let name = path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if !name.is_empty() {
                return self.create_container(&name);
            }
        }
        let key = self.to_prefix(path);
        // Create zero-byte blob as directory marker
        self.run_az(
            &["upload"],
            &["--data", "", "--name", &key, "--overwrite"],
        )?;
        Ok(())
    }

    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let (container, prefix) = self.resolve_path(path);
        if container.is_empty() {
            anyhow::bail!("Cannot delete at account level");
        }
        if prefix.is_empty() && self.container.is_empty() {
            // Deleting a container
            let mut cmd = Command::new("az");
            cmd.arg("storage").arg("container").arg("delete")
                .arg("--name").arg(&container);
            self.set_auth_env(&mut cmd);
            for arg in self.auth_args() {
                cmd.arg(arg);
            }
            let output = cmd.output()?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("Failed to delete container: {}", stderr.trim());
            }
            return Ok(());
        }
        let pattern = format!("{}*", prefix);
        self.run_az_with_container(
            &["delete-batch"],
            &["--source", &container, "--pattern", &pattern],
            &container,
        )?;
        Ok(())
    }

    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let (src_container, src_prefix) = self.resolve_path(src);
        let (dst_container, dst_prefix) = self.resolve_path(dst);
        let src_blob = src_prefix.trim_end_matches('/');
        let dst_blob = dst_prefix.trim_end_matches('/');

        // Build source URI for copy
        let endpoint = self.connection_string.as_ref()
            .and_then(|cs| extract_connection_string_field(cs, "BlobEndpoint"));
        let source_uri = if let Some(ref ep) = endpoint {
            format!("{}/{}/{}", ep.trim_end_matches('/'), src_container, src_blob)
        } else {
            format!("https://{}.blob.core.windows.net/{}/{}", self.account, src_container, src_blob)
        };

        self.run_az_with_container(
            &["copy", "start"],
            &["--source-uri", &source_uri, "--destination-blob", dst_blob],
            &dst_container,
        )?;

        // Poll until copy completes
        let mut copy_succeeded = false;
        for _ in 0..60 {
            let output = self.run_az_with_container(
                &["show"],
                &["--name", dst_blob, "--output", "json"],
                &dst_container,
            )?;
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&output) {
                let status = json
                    .get("properties")
                    .and_then(|p| p.get("copy"))
                    .and_then(|c| c.get("status"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("pending");
                match status {
                    "success" => {
                        copy_succeeded = true;
                        break;
                    }
                    "failed" | "aborted" => {
                        anyhow::bail!("Azure blob copy failed with status: {}", status);
                    }
                    _ => {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                }
            } else {
                anyhow::bail!("Azure blob copy: unable to parse status response");
            }
        }

        if !copy_succeeded {
            anyhow::bail!("Azure blob copy timed out after 30s without completing");
        }

        self.run_az_with_container(&["delete"], &["--name", src_blob], &src_container)?;
        Ok(())
    }

    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let (container, prefix) = self.resolve_path(remote);
        let name = prefix.trim_end_matches('/');
        self.run_az_with_container(
            &["download"],
            &["--name", name, "--file", &local.to_string_lossy()],
            &container,
        )?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let (container, prefix) = self.resolve_path(remote);
        let name = prefix.trim_end_matches('/');
        self.run_az_with_container(
            &["upload"],
            &[
                "--file", &local.to_string_lossy(),
                "--name", name,
                "--overwrite",
            ],
            &container,
        )?;
        let meta = std::fs::metadata(local)?;
        Ok(meta.len())
    }

    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        std::fs::create_dir_all(local)?;
        let (container, prefix) = self.resolve_path(remote);
        let pattern = format!("{}*", prefix);
        self.run_az_with_container(
            &["download-batch"],
            &[
                "--source", &container,
                "--destination", &local.to_string_lossy(),
                "--pattern", &pattern,
            ],
            &container,
        )?;
        Ok(crate::s3::dir_size(local))
    }

    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let (container, prefix) = self.resolve_path(remote);
        self.run_az_with_container(
            &["upload-batch"],
            &[
                "--source", &local.to_string_lossy(),
                "--destination", &container,
                "--destination-path", &prefix,
            ],
            &container,
        )?;
        Ok(crate::s3::dir_size(local))
    }

    pub fn display_label(&self) -> String {
        format!("Azure: {}/{}", self.account, self.container)
    }

    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("/")
    }
}

/// Extract a field value from an Azure connection string (e.g. "BlobEndpoint=http://...").
fn extract_connection_string_field(cs: &str, field: &str) -> Option<String> {
    let prefix = format!("{}=", field);
    cs.split(';')
        .find_map(|part| part.strip_prefix(&prefix).map(|v| v.to_string()))
}

impl crate::remote_fs::RemoteFs for AzureBlobConnection {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_azure_listing_basic() {
        let json: serde_json::Value = serde_json::from_str(r#"[
            {
                "name": "data/file.csv",
                "properties": {
                    "contentLength": 98765,
                    "lastModified": "2024-03-10T08:00:00+00:00"
                }
            },
            {
                "name": "data/subdir/",
                "properties": {
                    "contentLength": 0,
                    "lastModified": "2024-01-01T00:00:00+00:00"
                }
            }
        ]"#).unwrap();

        let prefix = "data/";
        let mut entries = Vec::new();
        if let Some(items) = json.as_array() {
            for item in items {
                let name_full = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let relative = name_full.strip_prefix(prefix).unwrap_or(name_full);
                let is_dir = relative.ends_with('/');
                let name = relative.trim_end_matches('/');
                if name.is_empty() || name.contains('/') { continue; }
                let size = item.get("properties")
                    .and_then(|p| p.get("contentLength"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                entries.push((name.to_string(), is_dir, size));
            }
        }
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "file.csv");
        assert!(!entries[0].1);
        assert_eq!(entries[0].2, 98765);
        assert_eq!(entries[1].0, "subdir");
        assert!(entries[1].1);
    }

    #[test]
    fn azure_to_prefix() {
        let conn = AzureBlobConnection {
            account: "acc".to_string(),
            container: "cont".to_string(),
            sas_token: None,
            connection_string: None,
        };
        assert_eq!(conn.to_prefix(Path::new("/")), "");
        assert_eq!(conn.to_prefix(Path::new("/logs")), "logs/");
        assert_eq!(conn.to_prefix(Path::new("/logs/")), "logs/");
    }

    #[test]
    fn azure_display_label() {
        let conn = AzureBlobConnection {
            account: "myaccount".to_string(),
            container: "mycontainer".to_string(),
            sas_token: Some("token".to_string()),
            connection_string: None,
        };
        assert_eq!(conn.display_label(), "Azure: myaccount/mycontainer");
    }

    #[test]
    fn azure_empty_listing() {
        let json: serde_json::Value = serde_json::from_str(r#"[]"#).unwrap();
        let items = json.as_array().unwrap();
        assert!(items.is_empty());
    }
}
