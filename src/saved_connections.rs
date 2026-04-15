#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;

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
    crate::remote_fs::config_dir().join("connections.json")
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
            host: None,
            port: None,
            user: None,
            password: None,
            share: None,
            url: None,
            bucket: None,
            profile: None,
            endpoint_url: None,
            region: None,
            project: None,
            account: None,
            container: None,
            sas_token: None,
            connection_string: None,
            export: None,
            mount_options: None,
            identity_file: None,
            jump_host: None,
        }
    }

    /// Serialize and deserialize a SavedConnection round-trip via serde_json.
    #[test]
    fn serde_round_trip() {
        let conn = SavedConnection {
            name: "dev-box".to_string(),
            protocol: "ssh".to_string(),
            host: Some("10.0.0.1".to_string()),
            port: Some(2222),
            user: Some("deploy".to_string()),
            password: Some("s3cret".to_string()),
            identity_file: Some("/home/deploy/.ssh/id_ed25519".to_string()),
            ..default_connection()
        };
        let json = serde_json::to_string_pretty(&vec![conn.clone()]).unwrap();
        let loaded: Vec<SavedConnection> = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "dev-box");
        assert_eq!(loaded[0].protocol, "ssh");
        assert_eq!(loaded[0].host.as_deref(), Some("10.0.0.1"));
        assert_eq!(loaded[0].port, Some(2222));
        assert_eq!(loaded[0].user.as_deref(), Some("deploy"));
        assert_eq!(loaded[0].password.as_deref(), Some("s3cret"));
        assert_eq!(
            loaded[0].identity_file.as_deref(),
            Some("/home/deploy/.ssh/id_ed25519")
        );
    }

    /// Optional fields that are None should not appear in serialized JSON.
    #[test]
    fn serde_skips_none_fields() {
        let conn = SavedConnection {
            name: "minimal".to_string(),
            protocol: "s3".to_string(),
            bucket: Some("data".to_string()),
            ..default_connection()
        };
        let json = serde_json::to_string(&vec![conn]).unwrap();
        // These None fields should be absent from the JSON
        assert!(!json.contains("\"host\""));
        assert!(!json.contains("\"port\""));
        assert!(!json.contains("\"password\""));
        assert!(!json.contains("\"export\""));
        // These should be present
        assert!(json.contains("\"bucket\""));
        assert!(json.contains("\"name\""));
    }

    /// Deserializing JSON with missing optional fields should default to None.
    #[test]
    fn serde_missing_fields_default_to_none() {
        let json = r#"[{"name":"test","protocol":"sftp"}]"#;
        let loaded: Vec<SavedConnection> = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "test");
        assert!(loaded[0].host.is_none());
        assert!(loaded[0].port.is_none());
        assert!(loaded[0].password.is_none());
    }

    #[test]
    fn display_label_webdav() {
        let c = SavedConnection {
            name: "nextcloud".to_string(),
            protocol: "webdav".to_string(),
            url: Some("https://cloud.example.com/dav".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "WebDAV: https://cloud.example.com/dav");
    }

    #[test]
    fn display_label_smb() {
        let c = SavedConnection {
            name: "nas".to_string(),
            protocol: "smb".to_string(),
            host: Some("nas.local".to_string()),
            share: Some("public".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "SMB: nas.local\\public");
    }

    #[test]
    fn display_label_nfs() {
        let c = SavedConnection {
            name: "nfs-share".to_string(),
            protocol: "nfs".to_string(),
            host: Some("storage.lan".to_string()),
            export: Some("/exports/data".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "NFS: storage.lan:/exports/data");
    }

    #[test]
    fn display_label_azure() {
        let c = SavedConnection {
            name: "az".to_string(),
            protocol: "azure".to_string(),
            account: Some("myaccount".to_string()),
            container: Some("mycontainer".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "Azure: myaccount/mycontainer");
    }

    #[test]
    fn display_label_gcs() {
        let c = SavedConnection {
            name: "gcloud".to_string(),
            protocol: "gcs".to_string(),
            bucket: Some("my-gcs-bucket".to_string()),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "GCS: my-gcs-bucket");
    }

    #[test]
    fn display_label_unknown_protocol() {
        let c = SavedConnection {
            name: "custom".to_string(),
            protocol: "ftp".to_string(),
            ..default_connection()
        };
        assert_eq!(c.display_label(), "FTP: custom");
    }

    // ---------------------------------------------------------------
    // File-system integration tests — use XDG_CONFIG_HOME override
    // to redirect save/load to a temp directory.
    // These must run serially since they mutate a process-global env var.
    // ---------------------------------------------------------------

    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Helper: run a closure with XDG_CONFIG_HOME pointing at `dir`.
    fn with_temp_config<F: FnOnce()>(dir: &Path, f: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let prev = std::env::var_os("XDG_CONFIG_HOME");
        std::env::set_var("XDG_CONFIG_HOME", dir);
        f();
        // Restore
        match prev {
            Some(v) => std::env::set_var("XDG_CONFIG_HOME", v),
            None => std::env::remove_var("XDG_CONFIG_HOME"),
        }
    }

    #[test]
    fn save_and_load_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let conns = vec![
            SavedConnection {
                name: "alpha".to_string(),
                protocol: "ssh".to_string(),
                host: Some("host-a.example.com".to_string()),
                user: Some("root".to_string()),
                ..default_connection()
            },
            SavedConnection {
                name: "beta".to_string(),
                protocol: "s3".to_string(),
                bucket: Some("archive".to_string()),
                region: Some("us-east-1".to_string()),
                ..default_connection()
            },
        ];

        with_temp_config(tmp.path(), || {
            save_connections(&conns);
            let loaded = load_connections();
            assert_eq!(loaded.len(), 2);
            assert_eq!(loaded[0].name, "alpha");
            assert_eq!(loaded[0].protocol, "ssh");
            assert_eq!(loaded[0].host.as_deref(), Some("host-a.example.com"));
            assert_eq!(loaded[1].name, "beta");
            assert_eq!(loaded[1].bucket.as_deref(), Some("archive"));
            assert_eq!(loaded[1].region.as_deref(), Some("us-east-1"));
        });
    }

    #[test]
    fn save_creates_file_with_valid_json() {
        let tmp = tempfile::tempdir().unwrap();
        let conns = vec![SavedConnection {
            name: "test".to_string(),
            protocol: "sftp".to_string(),
            host: Some("sftp.example.com".to_string()),
            ..default_connection()
        }];

        with_temp_config(tmp.path(), || {
            save_connections(&conns);
            let path = connections_path();
            assert!(path.exists(), "connections file should exist after save");

            let raw = std::fs::read_to_string(&path).unwrap();
            let parsed: Vec<SavedConnection> = serde_json::from_str(&raw).unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].name, "test");
        });
    }

    #[cfg(unix)]
    #[test]
    fn save_sets_file_permissions_0600() {
        let tmp = tempfile::tempdir().unwrap();
        let conns = vec![SavedConnection {
            name: "secure".to_string(),
            protocol: "ssh".to_string(),
            password: Some("topsecret".to_string()),
            ..default_connection()
        }];

        with_temp_config(tmp.path(), || {
            save_connections(&conns);
            let path = connections_path();
            let meta = std::fs::metadata(&path).unwrap();
            let mode = meta.permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o600,
                "file permissions should be 0600, got {:o}",
                mode
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn save_sets_directory_permissions_0700() {
        let tmp = tempfile::tempdir().unwrap();
        let conns = vec![SavedConnection {
            name: "x".to_string(),
            protocol: "ssh".to_string(),
            ..default_connection()
        }];

        with_temp_config(tmp.path(), || {
            save_connections(&conns);
            let path = connections_path();
            let parent = path.parent().unwrap();
            let meta = std::fs::metadata(parent).unwrap();
            let mode = meta.permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o700,
                "directory permissions should be 0700, got {:o}",
                mode
            );
        });
    }

    #[test]
    fn load_nonexistent_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        with_temp_config(tmp.path(), || {
            // No file saved — load should return an empty vec
            let loaded = load_connections();
            assert!(loaded.is_empty());
        });
    }

    #[test]
    fn load_invalid_json_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        with_temp_config(tmp.path(), || {
            let path = connections_path();
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, "not valid json {{{{").unwrap();
            let loaded = load_connections();
            assert!(loaded.is_empty(), "invalid JSON should produce empty vec");
        });
    }

    #[test]
    fn save_empty_list() {
        let tmp = tempfile::tempdir().unwrap();
        with_temp_config(tmp.path(), || {
            save_connections(&[]);
            let loaded = load_connections();
            assert!(loaded.is_empty());
        });
    }

    #[test]
    fn save_overwrites_previous() {
        let tmp = tempfile::tempdir().unwrap();
        let first = vec![SavedConnection {
            name: "first".to_string(),
            protocol: "ssh".to_string(),
            ..default_connection()
        }];
        let second = vec![SavedConnection {
            name: "second".to_string(),
            protocol: "s3".to_string(),
            bucket: Some("b".to_string()),
            ..default_connection()
        }];

        with_temp_config(tmp.path(), || {
            save_connections(&first);
            save_connections(&second);
            let loaded = load_connections();
            assert_eq!(loaded.len(), 1);
            assert_eq!(loaded[0].name, "second");
        });
    }
}
