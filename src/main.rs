mod action;
mod app;
mod ci;
mod editor;
mod event;
mod file_search;
mod fs_ops;
mod hex_viewer;
mod panel;
mod parquet_viewer;
mod state;
mod syntax;
mod terminal;
mod text_input;
mod theme;
mod ui;
mod viewer;
mod vt;
mod watcher;

use std::io;
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use app::App;
use event::{AppEvent, EventHandler};

fn main() -> Result<()> {
    // Install panic hook that restores terminal before printing the panic.
    // Without this, panics are invisible because they print to the alternate screen.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
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
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal);

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
        crossterm::cursor::SetCursorStyle::DefaultUserShape
    )?;
    terminal.show_cursor()?;
    // Flush to ensure all escape sequences are fully written before the shell takes over
    use std::io::Write;
    let _ = io::stdout().flush();

    if let Err(e) = result {
        eprintln!("Error: {}", e);
    }

    Ok(())
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    let mut app = App::new();
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
