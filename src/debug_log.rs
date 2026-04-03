use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();
static LOG_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Check if debug logging is enabled (MM_DEBUG=1 environment variable).
pub fn is_enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("MM_DEBUG").map(|v| v == "1").unwrap_or(false))
}

/// Get the log file path as a string (for display to user).
pub fn log_path_display() -> String {
    log_path().to_string_lossy().into_owned()
}

fn log_path() -> &'static PathBuf {
    LOG_PATH.get_or_init(|| {
        if let Some(config_dir) = std::env::var_os("XDG_CONFIG_HOME") {
            std::path::Path::new(&config_dir)
                .join("middle-manager")
                .join("debug.log")
        } else if let Some(home) = std::env::var_os("HOME") {
            std::path::Path::new(&home)
                .join(".config")
                .join("middle-manager")
                .join("debug.log")
        } else {
            PathBuf::from("middle-manager-debug.log")
        }
    })
}

/// Log a message to the debug log file.
/// Output is sanitized to printable ASCII + newlines for easy copy/paste.
pub fn log(msg: &str) {
    if !is_enabled() {
        return;
    }
    let path = log_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        // Sanitize: replace non-printable chars with '?' to keep log readable
        let clean: String = msg
            .chars()
            .map(|c| if c.is_control() && c != '\n' { '?' } else { c })
            .collect();
        let _ = writeln!(file, "[{}] {}", timestamp, clean);
    }
}

/// Log a shell command that is about to be executed.
#[allow(dead_code)]
pub fn log_cmd(program: &str, args: &[&str]) {
    if !is_enabled() {
        return;
    }
    let args_str = args
        .iter()
        .map(|a| {
            if a.contains(' ') || a.contains('"') {
                format!("\"{}\"", a.replace('"', "\\\""))
            } else {
                a.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    log(&format!("CMD: {} {}", program, args_str));
}

/// Log a shell command result.
pub fn log_cmd_result(program: &str, success: bool, stderr: &str) {
    if !is_enabled() {
        return;
    }
    if success {
        log(&format!("CMD OK: {}", program));
    } else {
        log(&format!("CMD FAIL: {} stderr={}", program, stderr.trim()));
    }
}

/// Log an error.
pub fn log_error(context: &str, err: &str) {
    if !is_enabled() {
        return;
    }
    log(&format!("ERROR [{}]: {}", context, err));
}
