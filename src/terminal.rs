use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use crate::event::WakeupSender;

pub struct TerminalPanel {
    /// VT100 parser holding the terminal screen state.
    parser: vt100::Parser,
    /// Channel receiving bytes from the PTY reader thread.
    rx: mpsc::Receiver<Vec<u8>>,
    /// Writer to the PTY master (for forwarding keystrokes).
    writer: Box<dyn Write + Send>,
    /// Child process handle.
    child: Box<dyn portable_pty::Child + Send + Sync>,
    /// PTY master handle (kept for resize).
    master: Box<dyn portable_pty::MasterPty + Send>,
    /// Current dimensions.
    pub cols: u16,
    pub rows: u16,
    /// Whether the child has exited.
    pub exited: bool,
    /// Title to display in the panel border.
    pub title: String,
    /// Directory the terminal was spawned in (for resolving relative paths).
    pub spawn_dir: PathBuf,
    /// Whether to show the hardware cursor (false for apps that render their own, like Claude Code).
    pub show_cursor: bool,
}

impl TerminalPanel {
    /// Spawn a command in a PTY with the given working directory and dimensions.
    fn spawn_cmd(
        command: &str,
        args: &[&str],
        dir: &Path,
        cols: u16,
        rows: u16,
        title: String,
        show_cursor: bool,
        wakeup: WakeupSender,
    ) -> anyhow::Result<Self> {
        let pty_system = native_pty_system();

        let pair = pty_system.openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        let mut cmd = CommandBuilder::new(command);
        for arg in args {
            cmd.arg(arg);
        }
        cmd.cwd(dir);
        cmd.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(cmd)?;
        drop(pair.slave);

        let writer = pair.master.take_writer()?;

        let (tx, rx) = mpsc::channel();
        let mut reader = pair.master.try_clone_reader()?;
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                        wakeup.wake();
                    }
                    Err(_) => break,
                }
            }
        });

        let parser = vt100::Parser::new(rows, cols, 10000);

        Ok(Self {
            parser,
            rx,
            writer,
            child,
            master: pair.master,
            cols,
            rows,
            exited: false,
            title,
            spawn_dir: dir.to_path_buf(),
            show_cursor,
        })
    }

    /// Spawn `claude` in a PTY (new session).
    pub fn spawn_claude(
        dir: &Path,
        cols: u16,
        rows: u16,
        wakeup: WakeupSender,
    ) -> anyhow::Result<Self> {
        Self::spawn_cmd(
            "claude",
            &[],
            dir,
            cols,
            rows,
            format!(" Claude — {} ", dir.display()),
            false,
            wakeup,
        )
    }

    /// Spawn `claude -c` in a PTY (continue last session).
    pub fn spawn_claude_continue(
        dir: &Path,
        cols: u16,
        rows: u16,
        wakeup: WakeupSender,
    ) -> anyhow::Result<Self> {
        Self::spawn_cmd(
            "claude",
            &["-c"],
            dir,
            cols,
            rows,
            format!(" Claude -c — {} ", dir.display()),
            false,
            wakeup,
        )
    }

    /// Spawn the user's default shell in a PTY.
    pub fn spawn_shell(
        dir: &Path,
        cols: u16,
        rows: u16,
        wakeup: WakeupSender,
    ) -> anyhow::Result<Self> {
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
        let shell_name = shell.rsplit('/').next().unwrap_or("shell");
        Self::spawn_cmd(
            &shell,
            &[],
            dir,
            cols,
            rows,
            format!(" {} — {} ", shell_name, dir.display()),
            true, // shell needs hardware cursor
            wakeup,
        )
    }

    /// Poll for new PTY output and check if the child exited.
    pub fn poll(&mut self) {
        // Drain all available output
        while let Ok(bytes) = self.rx.try_recv() {
            self.parser.process(&bytes);
        }

        // Check if child exited
        if !self.exited {
            if let Ok(Some(_status)) = self.child.try_wait() {
                self.exited = true;
            }
        }
    }

    /// Write raw bytes to the PTY (forward keystrokes).
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        let _ = self.writer.write_all(bytes);
        let _ = self.writer.flush();
    }

    /// Resize the PTY and the vt100 parser.
    pub fn resize(&mut self, cols: u16, rows: u16) {
        if cols == self.cols && rows == self.rows {
            return;
        }
        self.cols = cols;
        self.rows = rows;
        let _ = self.master.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        });
        self.parser.screen_mut().set_size(rows, cols);
    }

    /// Get the current terminal screen.
    pub fn screen(&self) -> &vt100::Screen {
        self.parser.screen()
    }

    /// Scroll up in the scrollback buffer.
    pub fn scroll_up(&mut self, lines: usize) {
        let current = self.parser.screen().scrollback();
        self.parser.screen_mut().set_scrollback(current + lines);
    }

    /// Scroll down (toward current output). Stops at 0 (live view).
    pub fn scroll_down(&mut self, lines: usize) {
        let current = self.parser.screen().scrollback();
        self.parser
            .screen_mut()
            .set_scrollback(current.saturating_sub(lines));
    }

    /// Jump back to live output (scroll offset 0).
    pub fn scroll_to_bottom(&mut self) {
        self.parser.screen_mut().set_scrollback(0);
    }

    /// Scan visible rows for file:line references, starting from the cursor row.
    /// as (resolved_path, line, col).
    pub fn find_file_reference(&self) -> Option<(PathBuf, usize, usize)> {
        let screen = self.parser.screen();
        // Scan from cursor row outward (cursor row first, then up, then down)
        let (cursor_row, _) = screen.cursor_position();
        let mut rows_to_check: Vec<u16> = vec![cursor_row];
        for offset in 1..self.rows {
            if cursor_row >= offset {
                rows_to_check.push(cursor_row - offset);
            }
            if cursor_row + offset < self.rows {
                rows_to_check.push(cursor_row + offset);
            }
        }

        for row in rows_to_check {
            let mut text = String::with_capacity(self.cols as usize);
            for col in 0..self.cols {
                if let Some(cell) = screen.cell(row, col) {
                    let contents = cell.contents();
                    if contents.is_empty() {
                        text.push(' ');
                    } else {
                        text.push_str(contents);
                    }
                } else {
                    text.push(' ');
                }
            }
            let text = text.trim_end();
            if let Some((path, line, col)) = parse_file_reference(text, &self.spawn_dir) {
                return Some((path, line, col));
            }
        }
        None
    }
}

/// Parse a line of text for file:line[:col] references.
/// Returns the first valid (existing file, line, col) found.
fn parse_file_reference(text: &str, base_dir: &Path) -> Option<(PathBuf, usize, usize)> {
    // Scan for patterns like path:line or path:line:col
    // Look for ':' followed by digits, then check if text before it looks like a file path.
    for (i, _) in text.match_indices(':') {
        // Text after the colon must start with digits
        let after = &text[i + 1..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            continue;
        }
        let line: usize = match digits.parse() {
            Ok(n) if n > 0 => n,
            _ => continue,
        };

        // Optional :col after the line number
        let col_start = i + 1 + digits.len();
        let col = if text.get(col_start..col_start + 1) == Some(":") {
            let after_col = &text[col_start + 1..];
            let col_digits: String = after_col
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            col_digits.parse::<usize>().unwrap_or(1)
        } else {
            1
        };

        // Extract the file path: scan backward from the colon to find the start
        let before = &text[..i];
        let path_start = before
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == '\'' || c == '"' || c == '`')
            .map(|pos| {
                // Advance past the matched character (which may be multi-byte)
                pos + before[pos..]
                    .chars()
                    .next()
                    .map(|c| c.len_utf8())
                    .unwrap_or(1)
            })
            .unwrap_or(0);
        let path_str = &before[path_start..];

        // Must look like a file path (has a dot or slash, not empty)
        if path_str.is_empty() {
            continue;
        }
        if !path_str.contains('.') && !path_str.contains('/') {
            continue;
        }

        // Resolve relative to base_dir
        let path = if Path::new(path_str).is_absolute() {
            PathBuf::from(path_str)
        } else {
            base_dir.join(path_str)
        };

        if path.is_file() {
            return Some((path, line, col));
        }
    }
    None
}

impl Drop for TerminalPanel {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

/// Encode a crossterm KeyEvent into the byte sequence a terminal would send.
pub fn encode_key_event(key: KeyEvent) -> Vec<u8> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);

    let mut bytes = match key.code {
        KeyCode::Char(c) if ctrl => {
            // Ctrl+A = 0x01, Ctrl+B = 0x02, ... Ctrl+Z = 0x1a
            let b = (c.to_ascii_lowercase() as u8)
                .wrapping_sub(b'a')
                .wrapping_add(1);
            vec![b]
        }
        KeyCode::Char(c) => {
            let mut buf = [0u8; 4];
            let s = c.encode_utf8(&mut buf);
            s.as_bytes().to_vec()
        }
        KeyCode::Enter => vec![b'\r'],
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Esc => vec![0x1b],
        KeyCode::Tab => vec![b'\t'],
        KeyCode::BackTab => b"\x1b[Z".to_vec(),
        KeyCode::Up => b"\x1b[A".to_vec(),
        KeyCode::Down => b"\x1b[B".to_vec(),
        KeyCode::Right => b"\x1b[C".to_vec(),
        KeyCode::Left => b"\x1b[D".to_vec(),
        KeyCode::Home => b"\x1b[H".to_vec(),
        KeyCode::End => b"\x1b[F".to_vec(),
        KeyCode::PageUp => b"\x1b[5~".to_vec(),
        KeyCode::PageDown => b"\x1b[6~".to_vec(),
        KeyCode::Delete => b"\x1b[3~".to_vec(),
        KeyCode::Insert => b"\x1b[2~".to_vec(),
        KeyCode::F(n) => match n {
            1 => b"\x1bOP".to_vec(),
            2 => b"\x1bOQ".to_vec(),
            3 => b"\x1bOR".to_vec(),
            4 => b"\x1bOS".to_vec(),
            5 => b"\x1b[15~".to_vec(),
            6 => b"\x1b[17~".to_vec(),
            7 => b"\x1b[18~".to_vec(),
            8 => b"\x1b[19~".to_vec(),
            9 => b"\x1b[20~".to_vec(),
            10 => b"\x1b[21~".to_vec(),
            11 => b"\x1b[23~".to_vec(),
            12 => b"\x1b[24~".to_vec(),
            _ => vec![],
        },
        _ => vec![],
    };

    if alt && !bytes.is_empty() {
        bytes.insert(0, 0x1b);
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Helper: parse_file_reference with a known base dir (this project's root)
    fn parse(text: &str) -> Option<(PathBuf, usize, usize)> {
        let base = std::env::current_dir().unwrap();
        parse_file_reference(text, &base)
    }

    #[test]
    fn parse_simple_file_line() {
        let result = parse("error at src/main.rs:42");
        assert!(result.is_some());
        let (path, line, col) = result.unwrap();
        assert!(path.ends_with("src/main.rs"));
        assert_eq!(line, 42);
        assert_eq!(col, 1);
    }

    #[test]
    fn parse_file_line_col() {
        let result = parse("src/app.rs:100:15 something");
        assert!(result.is_some());
        let (path, line, col) = result.unwrap();
        assert!(path.ends_with("src/app.rs"));
        assert_eq!(line, 100);
        assert_eq!(col, 15);
    }

    #[test]
    fn parse_in_parens() {
        let result = parse("see (src/main.rs:1) for details");
        assert!(result.is_some());
        let (path, line, _) = result.unwrap();
        assert!(path.ends_with("src/main.rs"));
        assert_eq!(line, 1);
    }

    #[test]
    fn parse_in_quotes() {
        let result = parse("file \"src/main.rs:10\" is relevant");
        assert!(result.is_some());
        let (_, line, _) = result.unwrap();
        assert_eq!(line, 10);
    }

    #[test]
    fn parse_no_file_ref() {
        assert!(parse("no file references here").is_none());
    }

    #[test]
    fn parse_colon_but_no_digits() {
        assert!(parse("key: value").is_none());
    }

    #[test]
    fn parse_nonexistent_file() {
        // File doesn't exist — should return None
        assert!(parse("nonexistent_file.xyz:42").is_none());
    }

    #[test]
    fn parse_line_zero_rejected() {
        // Line 0 is invalid (we require > 0)
        assert!(parse("src/main.rs:0").is_none());
    }

    #[test]
    fn parse_just_extension_no_path() {
        // ".rs:42" — has a dot, but .rs is not a real file
        assert!(parse(".rs:42").is_none());
    }

    #[test]
    fn encode_basic_keys() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let enter = encode_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(enter, vec![b'\r']);

        let esc = encode_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(esc, vec![0x1b]);

        let tab = encode_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(tab, vec![b'\t']);

        let up = encode_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(up, b"\x1b[A".to_vec());
    }

    #[test]
    fn encode_ctrl_c() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let ctrl_c = encode_key_event(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(ctrl_c, vec![3]); // ETX
    }

    #[test]
    fn encode_alt_prefix() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let alt_a = encode_key_event(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::ALT));
        assert_eq!(alt_a, vec![0x1b, b'a']);
    }

    #[test]
    fn encode_unicode_char() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let result = encode_key_event(KeyEvent::new(KeyCode::Char('é'), KeyModifiers::NONE));
        assert_eq!(result, "é".as_bytes().to_vec());
    }

    #[test]
    fn encode_function_keys() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let f1 = encode_key_event(KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE));
        assert_eq!(f1, b"\x1bOP".to_vec());
        let f12 = encode_key_event(KeyEvent::new(KeyCode::F(12), KeyModifiers::NONE));
        assert_eq!(f12, b"\x1b[24~".to_vec());
    }
}
