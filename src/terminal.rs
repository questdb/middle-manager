use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::path::Path;
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
}

impl TerminalPanel {
    /// Spawn `claude` in a PTY with the given working directory and dimensions.
    /// The `wakeup` sender is used to wake up the main event loop when PTY output arrives.
    pub fn spawn(dir: &Path, cols: u16, rows: u16, wakeup: WakeupSender) -> anyhow::Result<Self> {
        let pty_system = native_pty_system();

        let pair = pty_system.openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        let mut cmd = CommandBuilder::new("claude");
        cmd.cwd(dir);
        cmd.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(cmd)?;
        // Drop the slave side — the child owns it now.
        drop(pair.slave);

        let writer = pair.master.take_writer()?;

        // Background reader thread
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
                        // Wake up the main event loop (coalesced — no flood)
                        wakeup.wake();
                    }
                    Err(_) => break,
                }
            }
        });

        let parser = vt100::Parser::new(rows, cols, 0);

        Ok(Self {
            parser,
            rx,
            writer,
            child,
            master: pair.master,
            cols,
            rows,
            exited: false,
            title: format!(" Claude — {} ", dir.display()),
        })
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
        self.parser.set_size(rows, cols);
    }

    /// Get the current terminal screen.
    pub fn screen(&self) -> &vt100::Screen {
        self.parser.screen()
    }

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
            let b = (c.to_ascii_lowercase() as u8).wrapping_sub(b'a').wrapping_add(1);
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
