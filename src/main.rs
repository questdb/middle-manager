mod action;
mod app;
mod editor;
mod event;
mod fs_ops;
mod hex_viewer;
mod panel;
mod state;
mod syntax;
mod theme;
mod ui;
mod viewer;

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
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
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
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(e) = result {
        eprintln!("Error: {}", e);
    }

    Ok(())
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    let mut app = App::new();
    let mut events = EventHandler::new(Duration::from_millis(250));

    loop {
        // Clear if needed (e.g. after leaving a full-screen mode)
        if app.needs_clear {
            terminal.clear()?;
            app.needs_clear = false;
        }

        // Render
        terminal.draw(|frame| ui::render(frame, &mut app))?;

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
                app.handle_action(action::Action::Resize(w, h));
            }
            AppEvent::Tick => {
                app.handle_action(action::Action::Tick);
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
