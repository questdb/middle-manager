use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::theme::theme;

/// All help content as (section_title, &[(key, description)]).
const HELP_SECTIONS: &[(&str, &[(&str, &str)])] = &[
    (
        "File Panels",
        &[
            ("Up / Down", "Navigate files"),
            ("Shift+Up/Down", "Toggle selection and move"),
            ("Insert", "Toggle selection on current item"),
            ("Home / Left", "Jump to top"),
            ("End / Right", "Jump to bottom"),
            ("PageUp / PageDown", "Page through file list"),
            ("Enter", "Open directory / view file"),
            ("Backspace", "Go to parent directory"),
            ("Tab / Shift+Tab", "Switch panel forward / backward"),
            ("Type chars", "Quick search — jump to matching file"),
            ("Ctrl+F", "Fuzzy file search (open in editor)"),
            ("Ctrl+G", "Go to path (with tab completion)"),
            ("Ctrl+S", "Search in files (ripgrep-powered)"),
            ("Ctrl+D", "PR diff file tree"),
            ("Ctrl+C", "Copy filename to clipboard"),
            ("Ctrl+P", "Copy full path to clipboard"),
        ],
    ),
    (
        "File Operations",
        &[
            ("F3", "View file"),
            ("F4", "Edit file (built-in editor)"),
            ("Shift+F4", "Edit file with $EDITOR"),
            ("F5", "Copy file/selection"),
            ("F6", "Move file/selection"),
            ("Shift+F6", "Rename"),
            ("F7", "Create directory"),
            ("Shift+F7", "Create (touch) file"),
            ("Shift+F5", "Create archive (tar.zst/gz/xz/zip)"),
            ("F8", "Delete file/selection"),
            ("F9", "Cycle sort (name/size/date)"),
        ],
    ),
    (
        "CI Panel",
        &[
            ("F2", "Toggle CI panel"),
            ("Up / Down", "Navigate check tree"),
            ("PageUp / PageDown", "Page through checks"),
            ("Home / End", "Jump to top / bottom"),
            ("Right", "Expand check (load steps)"),
            ("Left", "Collapse check / jump to parent"),
            ("Enter", "Expand/collapse or download step log"),
            ("o", "Open check in browser"),
            ("Ctrl+E", "Extract all failures to file"),
            ("Alt+Up / Alt+Down", "Resize panel split"),
            ("Alt+Enter", "Maximize / restore panel"),
        ],
    ),
    (
        "PR Diff Panel (Ctrl+D)",
        &[
            ("Ctrl+D", "Open / close diff panel"),
            ("Up / Down", "Navigate file tree"),
            ("Enter", "Open file in side-by-side diff viewer"),
            ("F4", "Open file in editor"),
            ("Right", "Expand directory"),
            ("Left", "Collapse dir / jump to parent"),
            ("Type chars", "Quick search — jump to matching file"),
            ("PageUp / PageDown", "Page through tree"),
            ("Home / End", "Jump to top / bottom"),
            ("Tab / Shift+Tab", "Switch panel"),
            ("Alt+Up / Alt+Down", "Resize panel split"),
            ("Alt+Enter", "Maximize / restore panel"),
        ],
    ),
    (
        "Diff Viewer",
        &[
            ("Up / Down", "Move cursor line by line"),
            ("Left / Right", "Move cursor within line"),
            ("Tab", "Switch between left / right panel"),
            ("Home / End", "Cursor to line start / end"),
            ("PageUp / PageDown", "Move cursor by page"),
            ("Scroll / Trackpad", "Scroll through diff"),
            ("Shift+arrows", "Select text"),
            ("Ctrl+A", "Select all on current side"),
            ("Ctrl+C", "Copy selection to clipboard"),
            ("Ctrl+F", "Search in diff"),
            ("Alt+↓ / Alt+↑", "Next / previous diff hunk"),
            ("n / N", "Next / previous search match (or hunk)"),
            ("g", "Go to line"),
            ("F4", "Edit file (Esc returns to diff viewer)"),
            ("q / Esc", "Close diff viewer"),
        ],
    ),
    (
        "Shell Panel (Ctrl+O)",
        &[
            ("Ctrl+O", "Open / close shell in active panel"),
            ("F1", "Switch focus to file panel"),
            ("Scroll / Trackpad", "Scroll through output history"),
            ("Alt+Up / Alt+Down", "Resize panel split"),
            ("Alt+Enter", "Maximize / restore panel"),
            ("All other keys", "Forwarded to shell"),
        ],
    ),
    (
        "Claude Code Panel (F12)",
        &[
            ("F12", "Open / close (always maximized, opposite panel)"),
            ("F1", "Switch focus to file panel"),
            ("F5", "Open file:line reference in editor"),
            ("Scroll / Trackpad", "Scroll through output history"),
            ("All other keys", "Forwarded to Claude Code"),
        ],
    ),
    (
        "Connectivity (Ctrl+T / Shift+F2) [experimental]",
        &[
            ("Ctrl+T / Shift+F2", "Open remote dialog / close panel / disconnect"),
            ("Alt+Left/Right", "Switch protocol (SSH/SFTP/SMB/WebDAV/S3/GCS/Azure/NFS)"),
            ("Tab/BackTab", "Next/previous field"),
            ("F2", "Save current connection for quick access"),
            ("Enter", "Connect to selected / typed host"),
            ("Del", "Remove saved connection"),
            ("F1", "Switch focus to file panel"),
            ("Scroll / Trackpad", "Scroll through output history"),
            ("Alt+Up / Alt+Down", "Resize panel split"),
            ("Alt+Enter", "Maximize / restore panel"),
            ("All other keys", "Forwarded to SSH session"),
        ],
    ),
    (
        "Search Results (Ctrl+S)",
        &[
            ("Up / Down", "Navigate results"),
            ("PageUp / PageDown", "Page through results"),
            ("Home / End", "Jump to top / bottom"),
            ("Enter", "Open file in editor at match line"),
            ("Right", "Expand file matches"),
            ("Left", "Collapse file / jump to parent"),
            ("Tab / Shift+Tab", "Switch panel"),
            ("Esc", "Close search results"),
        ],
    ),
    (
        "Dialog Inputs (all dialogs)",
        &[
            ("Shift+Left/Right", "Select text"),
            ("Shift+Home/End", "Select to start / end"),
            ("Ctrl+A", "Select all"),
            ("Ctrl+C", "Copy selection to clipboard"),
            ("Ctrl+X", "Cut selection"),
            ("Ctrl+Z", "Undo"),
            ("Ctrl+Shift+Z", "Redo"),
            ("Delete", "Delete forward"),
            ("Mouse click", "Focus input field"),
        ],
    ),
    (
        "Viewer / Hex Viewer",
        &[
            ("Up / Down", "Scroll line by line"),
            ("PageUp / PageDown", "Scroll by page"),
            ("Home / End", "Jump to top / bottom"),
            ("g", "Go to line"),
            ("Tab / F4", "Toggle text / hex view"),
            ("q / Esc", "Close viewer"),
        ],
    ),
    (
        "Parquet Viewer",
        &[
            ("Up / Down", "Navigate tree / scroll table"),
            ("Right / Enter", "Expand node"),
            ("Left", "Collapse node / jump to parent"),
            ("PageUp / PageDown", "Page through tree or table"),
            ("Home / End", "Jump to top / bottom"),
            ("Tab / F4", "Toggle tree / table view"),
            ("g", "Go to row"),
            ("q / Esc", "Close viewer"),
        ],
    ),
    (
        "Editor",
        &[
            ("Arrow keys", "Move cursor"),
            ("Ctrl+Left/Right", "Word skip"),
            ("Home / End", "Line start / end"),
            ("PgUp / PgDn", "Page up / down"),
            ("Shift+arrows", "Select text"),
            ("Ctrl+A", "Select all"),
            ("Ctrl+C", "Copy selection to clipboard"),
            ("Ctrl+Z", "Undo"),
            ("Ctrl+Shift+Z", "Redo"),
            ("Ctrl+K", "Delete line"),
            ("Ctrl+G", "Go to line:col"),
            ("Ctrl+F / F7", "Search"),
            ("Shift+F7", "Find next"),
            ("F2 / Ctrl+S", "Save"),
            ("Esc", "Close (prompts if unsaved)"),
        ],
    ),
    (
        "Connectivity: Sessions (Ctrl+Y) [experimental]",
        &[
            ("Ctrl+Y", "Open / close session manager"),
            ("Up / Down", "Navigate sessions"),
            ("Enter", "Attach to selected session"),
            ("n", "Create new persistent session"),
            ("d / Delete", "Kill selected session"),
            ("Esc", "Close dialog"),
        ],
    ),
    (
        "Application",
        &[
            ("F1", "This help screen"),
            ("Shift+F1", "Settings (theme, etc.)"),
            ("F10 / Ctrl+Q", "Quit (with confirmation)"),
            ("F11", "Open PR in browser"),
        ],
    ),
];

/// Build the help content (cached — only built once).
fn help_lines() -> &'static Vec<Line<'static>> {
    use std::sync::OnceLock;
    static LINES: OnceLock<Vec<Line<'static>>> = OnceLock::new();
    LINES.get_or_init(build_help_lines)
}

fn build_help_lines() -> Vec<Line<'static>> {
    let t = theme();
    let section_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);
    let key_style = Style::default()
        .fg(t.dialog_title_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);
    let desc_style = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let blank = Line::from(Span::styled("", desc_style));

    let mut lines = Vec::new();

    for (i, (section, entries)) in HELP_SECTIONS.iter().enumerate() {
        if i > 0 {
            lines.push(blank.clone());
        }
        lines.push(Line::from(Span::styled(
            format!("  {}", section),
            section_style,
        )));
        lines.push(Line::from(Span::styled(
            format!("  {}", "─".repeat(section.len())),
            Style::default().fg(t.dialog_border_fg).bg(t.dialog_bg),
        )));

        for (key, desc) in *entries {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:>20}  ", key), key_style),
                Span::styled(desc.to_string(), desc_style),
            ]));
        }
    }

    lines
}

pub fn render(frame: &mut Frame, scroll: usize) -> Rect {
    let t = theme();
    let area = frame.area();

    // Use most of the screen
    let width = area.width.saturating_sub(8).min(70);
    let height = area.height.saturating_sub(4);
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let rect = Rect::new(x, y, width, height);

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(
            " Help — F1/Esc to close, ↑↓ to scroll ",
            t.dialog_title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let lines = help_lines();
    let total = lines.len();
    let visible = inner.height as usize;
    let max_scroll = total.saturating_sub(visible);
    let scroll = scroll.min(max_scroll);

    let visible_lines: Vec<Line> = lines.iter().skip(scroll).take(visible).cloned().collect();

    frame.render_widget(Paragraph::new(visible_lines), inner);

    rect
}
