use std::process::Command;

const SESSION_PREFIX: &str = "mm-";

/// A discovered middle-manager tmux session.
#[derive(Debug, Clone)]
pub struct MmSession {
    /// Full tmux session name (e.g. "mm-project-a").
    pub name: String,
    /// Display name without prefix (e.g. "project-a").
    pub display_name: String,
    /// Whether a client is currently attached.
    pub attached: bool,
    /// Creation timestamp (human-readable).
    pub created: String,
    /// Number of windows in the session.
    pub windows: u32,
}

/// Check if tmux is available on the system.
pub fn tmux_available() -> bool {
    Command::new("tmux")
        .arg("-V")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// List all middle-manager tmux sessions (prefixed with "mm-").
pub fn list_sessions() -> Vec<MmSession> {
    let output = match Command::new("tmux")
        .args([
            "list-sessions",
            "-F",
            "#{session_name}\t#{session_attached}\t#{session_created_string}\t#{session_windows}",
        ])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(), // tmux not running or no sessions
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut sessions = Vec::new();

    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 4 {
            continue;
        }
        let name = parts[0];
        if !name.starts_with(SESSION_PREFIX) {
            continue;
        }
        let display_name = name.strip_prefix(SESSION_PREFIX).unwrap_or(name).to_string();
        let attached = parts[1] != "0";
        let created = parts[2].to_string();
        let windows = parts[3].parse().unwrap_or(1);

        sessions.push(MmSession {
            name: name.to_string(),
            display_name,
            attached,
            created,
            windows,
        });
    }

    sessions
}

/// Check if a specific session exists.
pub fn session_exists(name: &str) -> bool {
    let full_name = prefixed_name(name);
    Command::new("tmux")
        .args(["has-session", "-t", &full_name])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Create a new detached middle-manager tmux session.
/// The session runs `middle-manager` inside it.
pub fn create_session(name: &str) -> anyhow::Result<()> {
    let full_name = prefixed_name(name);
    let exe = std::env::current_exe()
        .unwrap_or_else(|_| std::path::PathBuf::from("middle-manager"));

    let status = Command::new("tmux")
        .args([
            "new-session",
            "-d",
            "-s",
            &full_name,
            exe.to_str().unwrap_or("middle-manager"),
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!("tmux new-session failed with status {}", status);
    }

    // Configure the session to not conflict with middle-manager keybindings:
    // - Change prefix from Ctrl+B to backtick (`) -- doesn't steal any Ctrl combos
    // - Disable status bar (middle-manager has its own footer)
    // - Pass through all function keys and ctrl combos
    // Configure tmux to not interfere with middle-manager:
    // - Backtick prefix (frees all Ctrl+ combos)
    // - No status bar (middle-manager has its own)
    // - xterm-keys for F-key passthrough
    // - Set TERM to support 256 colors and function keys
    let opts: &[(&str, &str)] = &[
        ("prefix", "`"),
        ("status", "off"),
        ("xterm-keys", "on"),
        ("default-terminal", "xterm-256color"),
        ("escape-time", "0"),          // No delay after Esc (faster key handling)
        ("mouse", "on"),               // Pass mouse events through
    ];
    for (key, val) in opts {
        let _ = Command::new("tmux")
            .args(["set-option", "-t", &full_name, key, val])
            .status();
    }
    // Bind ` ` to send literal backtick
    let _ = Command::new("tmux")
        .args(["bind-key", "`", "send-prefix"])
        .status();

    Ok(())
}

/// Kill a middle-manager tmux session.
pub fn kill_session(name: &str) -> anyhow::Result<()> {
    let full_name = if name.starts_with(SESSION_PREFIX) {
        name.to_string()
    } else {
        prefixed_name(name)
    };

    let status = Command::new("tmux")
        .args(["kill-session", "-t", &full_name])
        .status()?;

    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("tmux kill-session failed");
    }
}

/// Return the command and args needed to attach to a session.
/// This is used to exec into the session from the CLI.
pub fn attach_command(name: &str) -> (String, Vec<String>) {
    let full_name = if name.starts_with(SESSION_PREFIX) {
        name.to_string()
    } else {
        prefixed_name(name)
    };
    (
        "tmux".to_string(),
        vec!["attach-session".to_string(), "-t".to_string(), full_name],
    )
}

/// Format for display: session list as text (for --list-sessions).
pub fn format_session_list(sessions: &[MmSession]) -> String {
    if sessions.is_empty() {
        return "No middle-manager sessions found.".to_string();
    }

    let mut out = String::new();
    for s in sessions {
        let status = if s.attached { "attached" } else { "detached" };
        out.push_str(&format!(
            "{:<20} {:<10} {} ({} window{})\n",
            s.display_name,
            status,
            s.created,
            s.windows,
            if s.windows == 1 { "" } else { "s" }
        ));
    }
    out
}

/// Get the full tmux session name (with mm- prefix).
pub fn full_session_name(name: &str) -> String {
    prefixed_name(name)
}

fn prefixed_name(name: &str) -> String {
    if name.starts_with(SESSION_PREFIX) {
        name.to_string()
    } else {
        format!("{}{}", SESSION_PREFIX, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_name_adds_prefix() {
        assert_eq!(prefixed_name("foo"), "mm-foo");
        assert_eq!(prefixed_name("mm-foo"), "mm-foo");
    }

    #[test]
    fn attach_command_format() {
        let (cmd, args) = attach_command("project-a");
        assert_eq!(cmd, "tmux");
        assert_eq!(args, vec!["attach-session", "-t", "mm-project-a"]);
    }

    #[test]
    fn attach_command_already_prefixed() {
        let (_, args) = attach_command("mm-test");
        assert_eq!(args[2], "mm-test");
    }

    #[test]
    fn format_session_list_empty() {
        let output = format_session_list(&[]);
        assert_eq!(output, "No middle-manager sessions found.");
    }

    #[test]
    fn format_session_list_one_session() {
        let sessions = vec![MmSession {
            name: "mm-test".to_string(),
            display_name: "test".to_string(),
            attached: false,
            created: "2024-01-01".to_string(),
            windows: 1,
        }];
        let output = format_session_list(&sessions);
        assert!(output.contains("test"));
        assert!(output.contains("detached"));
        assert!(output.contains("1 window"));
    }

    #[test]
    fn format_session_list_plural_windows() {
        let sessions = vec![MmSession {
            name: "mm-multi".to_string(),
            display_name: "multi".to_string(),
            attached: true,
            created: "2024-01-01".to_string(),
            windows: 3,
        }];
        let output = format_session_list(&sessions);
        assert!(output.contains("attached"));
        assert!(output.contains("3 windows"));
    }
}
