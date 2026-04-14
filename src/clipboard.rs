use std::io::Write;
use std::process::{Command, Stdio};

/// Copy text to the system clipboard.
///
/// Tries platform-native tools first (pbcopy on macOS, wl-copy/xclip/xsel on Linux),
/// then falls back to the OSC 52 terminal escape sequence.
/// Returns true if a native tool succeeded.
pub fn copy(text: &str) -> bool {
    if try_native_copy(text) {
        return true;
    }
    osc52_copy(text);
    false
}

/// Try to copy via a native clipboard command. Returns true on success.
fn try_native_copy(text: &str) -> bool {
    let candidates: &[&[&str]] = if cfg!(target_os = "macos") {
        &[&["pbcopy"]]
    } else {
        // Linux: try Wayland first, then X11
        &[
            &["wl-copy"],
            &["xclip", "-selection", "clipboard"],
            &["xsel", "--clipboard", "--input"],
        ]
    };

    for cmd in candidates {
        if let Some(ok) = try_pipe(cmd, text) {
            return ok;
        }
    }
    false
}

/// Spawn a command, pipe text to its stdin, return Some(true) on success,
/// Some(false) on failure, None if the command was not found.
fn try_pipe(cmd: &[&str], text: &str) -> Option<bool> {
    let mut child = Command::new(cmd[0])
        .args(&cmd[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    if let Some(ref mut stdin) = child.stdin {
        let _ = stdin.write_all(text.as_bytes());
    }
    // Drop stdin to close pipe before waiting
    child.stdin.take();
    child.wait().ok().map(|s| s.success())
}

/// OSC 52 terminal escape: asks the terminal emulator to set the clipboard.
/// Works in iTerm2, kitty, alacritty (if enabled), and some others.
fn osc52_copy(text: &str) {
    let encoded = base64_encode(text.as_bytes());
    let osc = format!("\x1b]52;c;{}\x1b\\", encoded);
    let _ = std::io::stdout().write_all(osc.as_bytes());
    let _ = std::io::stdout().flush();
}

pub fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_basic() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"abc"), "YWJj");
    }
}
