use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};

/// A saved connection entry that can represent any protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedConnection {
    pub name: String,
    pub protocol: String, // "ssh", "sftp", "smb", "webdav", "s3", "gcs", "azure", "nfs"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_options: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jump_host: Option<String>,
}

impl SavedConnection {
    /// Display label for the connection list.
    pub fn display_label(&self) -> String {
        let proto = self.protocol.to_uppercase();
        match self.protocol.as_str() {
            "ssh" | "sftp" => {
                let target = match (&self.user, &self.host) {
                    (Some(u), Some(h)) => format!("{}@{}", u, h),
                    (None, Some(h)) => h.clone(),
                    _ => "unknown".to_string(),
                };
                format!("{}: {}", proto, target)
            }
            "smb" => format!(
                "SMB: {}\\{}",
                self.host.as_deref().unwrap_or("?"),
                self.share.as_deref().unwrap_or("?")
            ),
            "webdav" => format!("WebDAV: {}", self.url.as_deref().unwrap_or("?")),
            "s3" => {
                if let Some(ref ep) = self.endpoint_url {
                    format!("S3: {}/{}", ep, self.bucket.as_deref().unwrap_or("?"))
                } else {
                    format!("S3: {}", self.bucket.as_deref().unwrap_or("?"))
                }
            }
            "gcs" => format!("GCS: {}", self.bucket.as_deref().unwrap_or("?")),
            "azure" => format!(
                "Azure: {}/{}",
                self.account.as_deref().unwrap_or("?"),
                self.container.as_deref().unwrap_or("?")
            ),
            "nfs" => format!(
                "NFS: {}:{}",
                self.host.as_deref().unwrap_or("?"),
                self.export.as_deref().unwrap_or("?")
            ),
            _ => format!("{}: {}", proto, self.name),
        }
    }
}

/// Load saved connections from `~/.config/middle-manager/connections.json`.
pub fn load_connections() -> Vec<SavedConnection> {
    let path = connections_path();
    match std::fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Save connections to disk with restrictive file permissions (0600).
pub fn save_connections(connections: &[SavedConnection]) {
    let path = connections_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
        // Restrict directory to owner-only access
        #[cfg(unix)]
        {
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    if let Ok(json) = serde_json::to_string_pretty(connections) {
        let _ = std::fs::write(&path, &json);
        // Restrict file to owner read/write only
        #[cfg(unix)]
        {
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

fn connections_path() -> PathBuf {
    if let Some(config_dir) = std::env::var_os("XDG_CONFIG_HOME") {
        Path::new(&config_dir)
            .join("middle-manager")
            .join("connections.json")
    } else if let Some(home) = std::env::var_os("HOME") {
        Path::new(&home)
            .join(".config")
            .join("middle-manager")
            .join("connections.json")
    } else {
        PathBuf::from("connections.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_label_ssh() {
        let c = SavedConnection {
            name: "my-server".to_string(),
            protocol: "ssh".to_string(),
            host: Some("example.com".to_string()),
            user: Some("nick".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "SSH: nick@example.com");
    }

    #[test]
    fn display_label_s3() {
        let c = SavedConnection {
            name: "prod".to_string(),
            protocol: "s3".to_string(),
            bucket: Some("my-data".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "S3: my-data");
    }

    #[test]
    fn display_label_s3_with_endpoint() {
        let c = SavedConnection {
            name: "minio".to_string(),
            protocol: "s3".to_string(),
            bucket: Some("local".to_string()),
            endpoint_url: Some("http://minio:9000".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "S3: http://minio:9000/local");
    }

    fn default_connection() -> SavedConnection {
        SavedConnection {
            name: String::new(),
            protocol: String::new(),
            host: None, port: None, user: None, password: None,
            share: None, url: None, bucket: None, profile: None,
            endpoint_url: None, region: None, project: None,
            account: None, container: None, sas_token: None,
            connection_string: None, export: None, mount_options: None,
            identity_file: None, jump_host: None,
        }
    }
}
