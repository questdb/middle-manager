mod action;
mod app;
mod azure_auth;
pub mod azure_blob;
mod ci;
mod clipboard;
mod debug_log;
mod diff_viewer;
mod editor;
mod event;
mod file_search;
mod fs_ops;
pub mod gcs;
mod hex_viewer;
mod nfs_client;
pub mod panel;
mod parquet_viewer;
mod pr_diff;
pub mod remote_fs;
pub mod s3;
mod saved_connections;
mod session;
pub mod sftp;
pub mod smb_client;
pub mod ssh;
mod state;
mod syntax;
mod terminal;
mod text_input;
mod theme;
mod ui;
mod vt;
mod watcher;
pub mod webdav;

use std::io::{self, Write};
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use app::App;
use event::{AppEvent, EventHandler};

fn main() -> Result<()> {
    // Initialize debug logging early -- print path to stderr so user can find it
    if debug_log::is_enabled() {
        let path = debug_log::log_path_display();
        eprintln!("MM_DEBUG: logging to {}", path);
        debug_log::log("=== middle-manager started ===");
    }

    // Handle CLI arguments before setting up the TUI
    let args: Vec<String> = std::env::args().collect();
    if let Some(action) = parse_cli_args(&args) {
        return handle_cli_action(action);
    }

    // Install panic hook that restores terminal before printing the panic.
    // Without this, panics are invisible because they print to the alternate screen.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = io::stdout().write_all(b"\x1b[>4m");
        let _ = execute!(io::stdout(), PopKeyboardEnhancementFlags);
        let _ = execute!(
            io::stdout(),
            LeaveAlternateScreen,
            DisableMouseCapture,
            crossterm::cursor::SetCursorStyle::DefaultUserShape
        );
        original_hook(panic_info);
    }));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    // Ask the terminal to forward every keystroke — including chords the
    // terminal would normally intercept (Shift+PageDown, Ctrl+Ins, Ctrl+C,
    // terminal-level copy/scroll shortcuts). Two orthogonal extensions are
    // pushed so at least one takes effect on whatever terminal is running:
    //
    //   1. kitty keyboard protocol (CSI = N u). Supported by Kitty, WezTerm,
    //      Ghostty, foot, Alacritty, recent Konsole/iTerm2. Flags:
    //        - DISAMBIGUATE_ESCAPE_CODES: encode modifier-qualified function
    //          keys as distinct CSI-u sequences.
    //        - REPORT_ALL_KEYS_AS_ESCAPE_CODES: report *every* key as a
    //          CSI-u sequence, even ones the terminal normally consumes
    //          (scroll shortcuts, copy/paste). This is what makes
    //          Shift+PageDown reach the app on Ghostty/Kitty/etc. without
    //          requiring per-user terminal config edits.
    //      We deliberately do not request REPORT_EVENT_TYPES — the app
    //      treats every KeyEvent as a press, so enabling release/repeat
    //      events would double actions.
    //   2. xterm modifyOtherKeys=2 (CSI > 4;2 m). Supported by xterm,
    //      gnome-terminal/VTE, Konsole, tmux, and most xterm-compatible
    //      terminals. Makes the terminal encode Shift+FKey etc. using the
    //      extended `CSI ... ; <mod> ~` form that crossterm already parses.
    //      This does NOT override GUI-level keybinds (those must be
    //      unbound in terminal settings) — it only ensures the modifier
    //      bits are included on keys the terminal does forward.
    //
    // Terminals that don't understand a given sequence ignore it silently,
    // so this is safe to send unconditionally.
    let _ = execute!(
        stdout,
        PushKeyboardEnhancementFlags(
            KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES,
        ),
    );
    let _ = stdout.write_all(b"\x1b[>4;2m");
    let _ = stdout.flush();

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal);

    // Restore terminal
    disable_raw_mode()?;
    let _ = execute!(terminal.backend_mut(), PopKeyboardEnhancementFlags);
    let _ = terminal.backend_mut().write_all(b"\x1b[>4m"); // modifyOtherKeys off
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
        crossterm::cursor::SetCursorStyle::DefaultUserShape
    )?;
    terminal.show_cursor()?;
    // Flush to ensure all escape sequences are fully written before the shell takes over
    let _ = io::stdout().flush();

    if let Err(e) = result {
        eprintln!("Error: {}", e);
    }

    Ok(())
}

enum CliAction {
    ListSessions,
    Session(String),
    Help,
}

fn parse_cli_args(args: &[String]) -> Option<CliAction> {
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--list-sessions" | "-l" => return Some(CliAction::ListSessions),
            "--session" | "-s" => {
                if i + 1 < args.len() {
                    return Some(CliAction::Session(args[i + 1].clone()));
                } else {
                    eprintln!("Error: --session requires a name argument");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => return Some(CliAction::Help),
            arg if arg.starts_with("--session=") => {
                let name = arg.strip_prefix("--session=").unwrap();
                return Some(CliAction::Session(name.to_string()));
            }
            arg if arg.starts_with("-s=") => {
                let name = arg.strip_prefix("-s=").unwrap();
                return Some(CliAction::Session(name.to_string()));
            }
            "session" => {
                // Bare subcommand: `middle-manager session foo`
                if i + 1 < args.len() {
                    return Some(CliAction::Session(args[i + 1].clone()));
                } else {
                    eprintln!("Error: 'session' requires a name argument");
                    std::process::exit(1);
                }
            }
            "list-sessions" | "sessions" => return Some(CliAction::ListSessions),
            _ => {
                // Unknown arg — ignore (let TUI start normally)
            }
        }
        i += 1;
    }
    None
}

fn handle_cli_action(action: CliAction) -> Result<()> {
    match action {
        CliAction::ListSessions => {
            let sessions = session::list_sessions();
            print!("{}", session::format_session_list(&sessions));
            Ok(())
        }
        CliAction::Session(name) => {
            if !session::tmux_available() {
                eprintln!("Error: tmux is not installed or not in PATH");
                std::process::exit(1);
            }

            // Prevent nested tmux -- refuse to attach from inside any tmux session
            if std::env::var("TMUX").is_ok() {
                eprintln!("Error: already inside a tmux session.");
                eprintln!(
                    "Detach first (` then d), then run: middle-manager --session {}",
                    name
                );
                eprintln!("Or use Ctrl+Y inside middle-manager to manage sessions.");
                std::process::exit(1);
            }

            let full_name = session::full_session_name(&name);

            if !session::session_exists(&name) {
                eprintln!("Creating session '{}'...", name);
                session::create_session(&name)?;
            }

            // Attach (replaces this process with tmux)
            let err = exec_replace(
                "tmux",
                &["attach-session".to_string(), "-t".to_string(), full_name],
            );
            eprintln!("Failed to attach: {}", err);
            eprintln!("If running via 'cargo run', try the built binary directly:");
            eprintln!(
                "  cargo build && ./target/debug/middle-manager --session {}",
                name
            );
            std::process::exit(1);
        }
        CliAction::Help => {
            println!("Middle Manager TUI — dual-panel file manager");
            println!();
            println!("Usage: middle-manager [OPTIONS]");
            println!();
            println!("Options:");
            println!("  --session, -s <name>   Launch in a persistent tmux session");
            println!("  --list-sessions, -l    List active middle-manager sessions");
            println!("  --help, -h             Show this help message");
            println!();
            println!("Session workflow:");
            println!("  Start:    middle-manager --session project-a");
            println!("  Detach:   Ctrl+B then D (tmux default)");
            println!("  Reattach: middle-manager --session project-a");
            println!("  List:     middle-manager --list-sessions");
            Ok(())
        }
    }
}

/// Replace the current process with the given command (Unix exec).
fn exec_replace(cmd: &str, args: &[String]) -> std::io::Error {
    use std::os::unix::process::CommandExt;
    // This only returns on error — on success the process is replaced
    Command::new(cmd).args(args).exec()
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    let mut app = App::new();
    // Apply persisted theme
    let theme_name = crate::theme::ThemeName::from_str(&app.persisted.theme);
    crate::theme::set_theme(theme_name);
    let mut events = EventHandler::new(Duration::from_millis(250));
    app.set_wakeup_sender(events.wakeup_sender());
    app.restore_bottom_panels();

    // First render to compute actual panel areas, then resize PTYs to match
    terminal.draw(|frame| ui::render(frame, &mut app))?;
    app.resize_all_bottom_panels();

    loop {
        // Clear if needed (e.g. after leaving a full-screen mode)
        if app.needs_clear {
            terminal.clear()?;
            app.needs_clear = false;
            app.dirty = true;
        }

        // Only redraw when state has changed (dirty flag).
        // Skipping draws on idle ticks lets the terminal's cursor blink undisturbed.
        if app.dirty {
            terminal.draw(|frame| ui::render(frame, &mut app))?;
            app.resize_all_bottom_panels();
            app.dialog_content_area = ui::take_dialog_content();
            app.dirty = false;

            // Manage cursor ourselves — ratatui always hides it (no set_cursor_position calls).
            let new_cursor = ui::take_cursor();
            match new_cursor {
                Some(pos) => {
                    let style = crate::theme::theme().editor_cursor;
                    execute!(
                        io::stdout(),
                        crossterm::cursor::MoveTo(pos.0, pos.1),
                        crossterm::cursor::Show,
                        style
                    )?;
                    app.last_cursor_pos = Some(pos);
                }
                None => {
                    if app.last_cursor_pos.is_some() {
                        execute!(io::stdout(), crossterm::cursor::Hide)?;
                        app.last_cursor_pos = None;
                    }
                }
            }
        }

        // Check for edit request
        if let Some(edit_path) = app.take_edit_request() {
            // Suspend terminal and launch editor
            disable_raw_mode()?;
            execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
            terminal.show_cursor()?;

            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
            let status = Command::new(&editor).arg(&edit_path).status();

            if let Err(e) = status {
                app.status_message = Some(format!("Editor error: {}", e));
            }

            // Restore terminal
            enable_raw_mode()?;
            execute!(terminal.backend_mut(), EnterAlternateScreen)?;
            terminal.clear()?;

            // Drain stale events accumulated while the editor was open
            events.drain();

            // Reload panels
            app.reload_panels();
            app.dirty = true;
            continue;
        }

        // Handle events
        match events.next()? {
            AppEvent::Key(key) => {
                let action = app.map_key_to_action(key);
                app.handle_action(action);
            }
            AppEvent::Mouse(mouse) => {
                let action = app.map_mouse_to_action(mouse);
                app.handle_action(action);
            }
            AppEvent::Resize(w, h) => {
                app.dirty = true;
                app.handle_action(action::Action::Resize(w, h));
            }
            AppEvent::Tick => {
                app.handle_action(action::Action::Tick);
            }
            AppEvent::Wakeup => {
                // Terminal has output — poll it and re-render.
                app.dirty = true;
                app.handle_action(action::Action::Tick);
                events.ack_wakeup();
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Save state and stop event thread before returning
    app.save_state();
    events.stop();

    Ok(())
}
