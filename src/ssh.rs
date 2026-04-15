use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// A saved SSH host connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshHost {
    pub name: String,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jump_host: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Where this host was discovered from (for display purposes).
    #[serde(skip)]
    pub source: HostSource,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum HostSource {
    #[default]
    Saved,
    SshConfig,
}

impl SshHost {
    /// Build the display label for this host (used in the picker).
    pub fn display_label(&self) -> String {
        match &self.user {
            Some(user) => format!("{}@{}", user, self.hostname),
            None => self.hostname.clone(),
        }
    }

    /// Build the ssh command arguments from this host.
    pub fn ssh_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(port) = self.port {
            args.push("-p".into());
            args.push(port.to_string());
        }
        if let Some(ref identity) = self.identity_file {
            args.push("-i".into());
            args.push(identity.clone());
        }
        if let Some(ref jump) = self.jump_host {
            args.push("-J".into());
            args.push(jump.clone());
        }
        for extra in &self.extra_args {
            args.push(extra.clone());
        }
        // Target
        let target = match &self.user {
            Some(user) => format!("{}@{}", user, self.hostname),
            None => self.hostname.clone(),
        };
        args.push(target);
        args
    }

    /// Parse a quick-connect string like `user@host[:port]` into an SshHost.
    pub fn from_quick_connect(input: &str) -> Option<Self> {
        let input = input.trim();
        if input.is_empty() {
            return None;
        }

        let (user, rest) = if let Some(at_pos) = input.find('@') {
            (Some(input[..at_pos].to_string()), &input[at_pos + 1..])
        } else {
            (None, input)
        };

        let (hostname, port) = if let Some(colon_pos) = rest.rfind(':') {
            let port_str = &rest[colon_pos + 1..];
            if let Ok(port) = port_str.parse::<u16>() {
                (rest[..colon_pos].to_string(), Some(port))
            } else {
                (rest.to_string(), None)
            }
        } else {
            (rest.to_string(), None)
        };

        if hostname.is_empty() {
            return None;
        }

        let name = match (&user, port) {
            (Some(u), Some(p)) => format!("{}@{}:{}", u, hostname, p),
            (Some(u), None) => format!("{}@{}", u, hostname),
            (None, Some(p)) => format!("{}:{}", hostname, p),
            (None, None) => hostname.clone(),
        };

        Some(SshHost {
            name,
            hostname,
            port,
            user,
            identity_file: None,
            group: None,
            jump_host: None,
            extra_args: Vec::new(),
            source: HostSource::Saved,
        })
    }
}

/// Load saved SSH hosts from `~/.config/middle-manager/ssh_hosts.json`.
pub fn load_saved_hosts() -> Vec<SshHost> {
    let path = ssh_hosts_path();
    match std::fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Load hosts from `~/.ssh/config` (read-only discovery).
pub fn load_ssh_config_hosts() -> Vec<SshHost> {
    let path = ssh_config_path();
    match std::fs::read_to_string(&path) {
        Ok(content) => parse_ssh_config(&content),
        Err(_) => Vec::new(),
    }
}

/// Load all hosts: saved hosts first, then SSH config hosts (deduped by hostname).
pub fn load_all_hosts() -> Vec<SshHost> {
    let mut hosts = load_saved_hosts();
    let config_hosts = load_ssh_config_hosts();

    // Deduplicate: skip SSH config hosts whose hostname already appears in saved hosts
    let saved_hostnames: std::collections::HashSet<String> = hosts
        .iter()
        .flat_map(|h| [h.hostname.clone(), h.name.clone()])
        .collect();

    for mut host in config_hosts {
        if !saved_hostnames.contains(&host.hostname) && !saved_hostnames.contains(&host.name) {
            host.source = HostSource::SshConfig;
            hosts.push(host);
        }
    }

    hosts
}

/// Parse an OpenSSH config file into SshHost entries.
/// Only extracts: Host, HostName, User, Port, IdentityFile.
fn parse_ssh_config(content: &str) -> Vec<SshHost> {
    let mut hosts = Vec::new();
    let mut current: Option<SshHost> = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split on first whitespace or '='
        let (key, value) = match line.find(|c: char| c.is_whitespace() || c == '=') {
            Some(pos) => {
                let key = &line[..pos];
                let value = line[pos + 1..].trim().trim_start_matches('=').trim();
                (key, value)
            }
            None => continue,
        };

        match key.to_lowercase().as_str() {
            "host" => {
                // Flush previous host
                if let Some(h) = current.take() {
                    if !h.hostname.is_empty() && !h.name.contains('*') && !h.name.contains('?') {
                        hosts.push(h);
                    }
                }
                // Skip wildcard patterns
                if value.contains('*') || value.contains('?') {
                    current = None;
                } else {
                    current = Some(SshHost {
                        name: value.to_string(),
                        hostname: value.to_string(), // Default: hostname = alias
                        port: None,
                        user: None,
                        identity_file: None,
                        group: Some("~/.ssh/config".to_string()),
                        jump_host: None,
                        extra_args: Vec::new(),
                        source: HostSource::SshConfig,
                    });
                }
            }
            "hostname" => {
                if let Some(ref mut h) = current {
                    h.hostname = value.to_string();
                }
            }
            "user" => {
                if let Some(ref mut h) = current {
                    h.user = Some(value.to_string());
                }
            }
            "port" => {
                if let Some(ref mut h) = current {
                    h.port = value.parse().ok();
                }
            }
            "identityfile" => {
                if let Some(ref mut h) = current {
                    // Keep the first identity file (SSH config tries them in order)
                    if h.identity_file.is_none() {
                        h.identity_file = Some(value.to_string());
                    }
                }
            }
            "proxyjump" => {
                if let Some(ref mut h) = current {
                    h.jump_host = Some(value.to_string());
                }
            }
            _ => {}
        }
    }

    // Flush last host
    if let Some(h) = current {
        if !h.hostname.is_empty() && !h.name.contains('*') && !h.name.contains('?') {
            hosts.push(h);
        }
    }

    hosts
}

fn ssh_hosts_path() -> PathBuf {
    crate::remote_fs::config_dir().join("ssh_hosts.json")
}

fn ssh_config_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        Path::new(&home).join(".ssh").join("config")
    } else {
        PathBuf::from("/etc/ssh/ssh_config")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_connect_user_host() {
        let h = SshHost::from_quick_connect("nick@example.com").unwrap();
        assert_eq!(h.user.as_deref(), Some("nick"));
        assert_eq!(h.hostname, "example.com");
        assert_eq!(h.port, None);
        assert_eq!(h.name, "nick@example.com");
    }

    #[test]
    fn quick_connect_user_host_port() {
        let h = SshHost::from_quick_connect("root@10.0.0.1:2222").unwrap();
        assert_eq!(h.user.as_deref(), Some("root"));
        assert_eq!(h.hostname, "10.0.0.1");
        assert_eq!(h.port, Some(2222));
    }

    #[test]
    fn quick_connect_host_only() {
        let h = SshHost::from_quick_connect("myserver").unwrap();
        assert_eq!(h.user, None);
        assert_eq!(h.hostname, "myserver");
    }

    #[test]
    fn quick_connect_empty() {
        assert!(SshHost::from_quick_connect("").is_none());
        assert!(SshHost::from_quick_connect("   ").is_none());
    }

    #[test]
    fn parse_ssh_config_basic() {
        let config = "\
Host myserver
    HostName 192.168.1.10
    User admin
    Port 2222
    IdentityFile ~/.ssh/id_myserver

Host *.example.com
    User deploy

Host bastion
    HostName bastion.corp.com
    ProxyJump gateway
";
        let hosts = parse_ssh_config(config);
        assert_eq!(hosts.len(), 2); // wildcard skipped

        assert_eq!(hosts[0].name, "myserver");
        assert_eq!(hosts[0].hostname, "192.168.1.10");
        assert_eq!(hosts[0].user.as_deref(), Some("admin"));
        assert_eq!(hosts[0].port, Some(2222));
        assert_eq!(
            hosts[0].identity_file.as_deref(),
            Some("~/.ssh/id_myserver")
        );

        assert_eq!(hosts[1].name, "bastion");
        assert_eq!(hosts[1].hostname, "bastion.corp.com");
        assert_eq!(hosts[1].jump_host.as_deref(), Some("gateway"));
    }

    #[test]
    fn parse_ssh_config_multiple_identity_files() {
        let config = "\
Host myhost
    HostName example.com
    IdentityFile ~/.ssh/id_ed25519
    IdentityFile ~/.ssh/id_rsa
";
        let hosts = parse_ssh_config(config);
        assert_eq!(hosts.len(), 1);
        // Should keep the first identity file
        assert_eq!(hosts[0].identity_file.as_deref(), Some("~/.ssh/id_ed25519"));
    }

    #[test]
    fn parse_ssh_config_empty() {
        let hosts = parse_ssh_config("");
        assert!(hosts.is_empty());
    }

    #[test]
    fn parse_ssh_config_comments_only() {
        let config = "# This is a comment\n# Another comment\n";
        let hosts = parse_ssh_config(config);
        assert!(hosts.is_empty());
    }

    #[test]
    fn parse_ssh_config_equals_syntax() {
        let config = "Host=myhost\n    HostName=10.0.0.1\n    User=root\n";
        let hosts = parse_ssh_config(config);
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].hostname, "10.0.0.1");
        assert_eq!(hosts[0].user.as_deref(), Some("root"));
    }

    #[test]
    fn ssh_host_ssh_args() {
        let host = SshHost {
            name: "test".to_string(),
            hostname: "example.com".to_string(),
            port: Some(2222),
            user: Some("nick".to_string()),
            identity_file: Some("/key".to_string()),
            group: None,
            jump_host: Some("bastion".to_string()),
            extra_args: vec![],
            source: HostSource::Saved,
        };
        let args = host.ssh_args();
        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&"2222".to_string()));
        assert!(args.contains(&"-i".to_string()));
        assert!(args.contains(&"/key".to_string()));
        assert!(args.contains(&"-J".to_string()));
        assert!(args.contains(&"bastion".to_string()));
        assert!(args.contains(&"nick@example.com".to_string()));
    }

    #[test]
    fn quick_connect_at_sign_only() {
        // "@" should produce empty user, empty host
        assert!(SshHost::from_quick_connect("@").is_none());
    }

    #[test]
    fn quick_connect_unicode_host() {
        let h = SshHost::from_quick_connect("user@münchen.de").unwrap();
        assert_eq!(h.hostname, "münchen.de");
        assert_eq!(h.user.as_deref(), Some("user"));
    }
}
