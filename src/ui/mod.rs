pub mod archive_dialog;
pub mod azure_auth_dialog;
pub mod ci_view;
pub mod copy_dialog;
pub mod dialog;
pub mod dialog_helpers;
pub mod diff_view;
pub mod diff_viewer_view;
pub mod editor_view;
pub mod file_search_dialog;
pub mod footer;
pub mod header;
pub mod help_dialog;
pub mod hex_view;
pub mod menu;
pub mod mkdir_dialog;
pub mod panel_view;
pub mod parquet_view;
pub mod search_dialog;
pub mod search_results_view;
pub mod session_dialog;
pub mod settings_dialog;
mod shadow;
pub mod ssh_dialog;
pub mod terminal_view;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

/// Truncate a string so its display width (measured in terminal columns) does
/// not exceed `max_width`.  This is char-boundary safe and accounts for
/// double-width (CJK) characters.
pub(crate) fn truncate_to_width(s: &str, max_width: usize) -> &str {
    let mut end = 0;
    let mut width = 0;
    for (i, c) in s.char_indices() {
        let cw = UnicodeWidthChar::width(c).unwrap_or(0);
        if width + cw > max_width {
            break;
        }
        width += cw;
        end = i + c.len_utf8();
    }
    &s[..end]
}

use std::cell::Cell;

use crate::app::{App, AppMode, PanelFocus};
use crate::theme::theme;

thread_local! {
    /// Cursor position set during rendering. Read by main loop after draw.
    pub static CURSOR_POS: Cell<Option<(u16, u16)>> = const { Cell::new(None) };
    /// Dialog content area set during rendering. Used for click-to-focus.
    pub static DIALOG_CONTENT: Cell<Option<Rect>> = const { Cell::new(None) };
}

/// Set the cursor position from any render function. Use instead of frame.set_cursor_position.
pub fn set_cursor(x: u16, y: u16) {
    CURSOR_POS.set(Some((x, y)));
}

/// Read and clear the cursor position after draw.
pub fn take_cursor() -> Option<(u16, u16)> {
    CURSOR_POS.replace(None)
}

/// Set the dialog content area from a dialog renderer.
pub fn set_dialog_content(area: Rect) {
    DIALOG_CONTENT.set(Some(area));
}

/// Read and clear the dialog content area after draw.
pub fn take_dialog_content() -> Option<Rect> {
    DIALOG_CONTENT.replace(None)
}

/// Layout result for a panel column: (file, ci, diff, shell, claude, ssh).
type PanelColumnLayout = (
    Rect,
    Option<Rect>,
    Option<Rect>,
    Option<Rect>,
    Option<Rect>,
    Option<Rect>,
);

/// Split a panel column into file area + optional bottom panels (CI, diff, shell, Claude, SSH).
///
/// When `maximized` is true, the file panel gets 0 height and the bottom panels fill the column.
/// `split_pct` controls the top/bottom ratio (percentage for the file panel).
#[allow(clippy::too_many_arguments)]
fn split_panel_column(
    col: Rect,
    has_ci: bool,
    has_diff: bool,
    has_shell: bool,
    has_claude: bool,
    has_ssh: bool,
    split_pct: u16,
    maximized: bool,
) -> PanelColumnLayout {
    // When Claude is maximized, it takes the entire column
    if maximized && has_claude {
        let zero = Rect::new(col.x, col.y, col.width, 0);
        return (zero, None, None, None, Some(col), None);
    }

    let bottom_count = has_ci as usize
        + has_diff as usize
        + has_shell as usize
        + has_claude as usize
        + has_ssh as usize;
    if bottom_count == 0 {
        return (col, None, None, None, None, None);
    }

    // Split top (file panel) vs bottom (all sub-panels share equally)
    let top_pct = if maximized { 0 } else { split_pct };
    let bottom_pct = 100u16.saturating_sub(top_pct);

    let [file_area, bottom_area] = Layout::vertical([
        Constraint::Percentage(top_pct),
        Constraint::Percentage(bottom_pct),
    ])
    .areas(col);

    // Divide bottom area equally among active bottom panels
    let mut constraints: Vec<Constraint> = Vec::with_capacity(bottom_count);
    for _ in 0..bottom_count {
        constraints.push(Constraint::Ratio(1, bottom_count as u32));
    }
    let areas: Vec<Rect> = Layout::vertical(constraints).split(bottom_area).to_vec();

    let mut idx = 0;
    let ci_area = if has_ci {
        let a = areas[idx];
        idx += 1;
        Some(a)
    } else {
        None
    };
    let diff_area = if has_diff {
        let a = areas[idx];
        idx += 1;
        Some(a)
    } else {
        None
    };
    let shell_area = if has_shell {
        let a = areas[idx];
        idx += 1;
        Some(a)
    } else {
        None
    };
    let claude_area = if has_claude {
        let a = areas[idx];
        idx += 1;
        Some(a)
    } else {
        None
    };
    let ssh_area = if has_ssh {
        let a = areas[idx];
        let _ = idx;
        Some(a)
    } else {
        None
    };

    (
        file_area,
        ci_area,
        diff_area,
        shell_area,
        claude_area,
        ssh_area,
    )
}

pub fn render(frame: &mut Frame, app: &mut App) {
    match &app.mode {
        AppMode::HexViewing(_) => render_hex_viewer(frame, app),
        AppMode::ParquetViewing(_) => render_parquet_viewer(frame, app),
        AppMode::DiffViewing(_) => render_diff_viewer(frame, app),
        AppMode::Editing(_) => render_editor(frame, app),
        _ => render_normal(frame, app),
    }

    // Render goto-line dialog overlay if active
    if let Some(ref dlg) = app.goto_line_dialog {
        let area = render_goto_line_dialog(frame, dlg);
        shadow::render_shadow(frame, area);
    }

    // Render quit confirmation overlay
    if let Some(quit_focused) = app.quit_confirm {
        let area = render_quit_dialog(frame, quit_focused);
        shadow::render_shadow(frame, area);
    }

    // Render overwrite-ask dialog overlay
    if let Some(ref state) = app.overwrite_ask {
        let area = render_overwrite_ask_dialog(frame, state);
        shadow::render_shadow(frame, area);
    }

    // Render help dialog overlay (or contextual rg help when F1 pressed in search dialog)
    if let Some(ref state) = app.help_state {
        let area = match app.file_search_help {
            Some(crate::app::FileSearchHelp::FileTypes) => {
                file_search_dialog::render_type_list(frame, state.scroll, &state.filter)
            }
            Some(crate::app::FileSearchHelp::Glob) => {
                file_search_dialog::render_glob_help(frame, state.scroll)
            }
            Some(crate::app::FileSearchHelp::Field(f)) => {
                file_search_dialog::render_field_help(frame, f)
            }
            None => help_dialog::render(frame, state.scroll, &state.filter),
        };
        shadow::render_shadow(frame, area);
        // Hide the text cursor while a help/F1 overlay is shown
        CURSOR_POS.set(None);
    }

    // Render menu bar overlay (on top of everything, only when open)
    if let Some(ref state) = app.menu_state {
        let bar_area = Rect::new(frame.area().x, frame.area().y, frame.area().width, 1);
        let sort_fields = [app.panels[0].sort_field, app.panels[1].sort_field];
        app.menu_title_ranges = menu::render(frame, state, bar_area, sort_fields);
    } else {
        app.menu_title_ranges.clear();
    }
}

fn render_normal(frame: &mut Frame, app: &mut App) {
    let [header_area, panels_area, footer_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let [left_col, right_col] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(panels_area);

    // Split each column: file panel on top, bottom panels below
    let (left_area, left_ci_area, left_diff_area, left_shell_area, left_claude_area, left_ssh_area) =
        split_panel_column(
            left_col,
            app.ci_panels[0].is_some(),
            app.diff_panels[0].is_some(),
            app.shell_panels[0].is_some(),
            app.claude_panels[0].is_some(),
            app.ssh_panels[0].is_some(),
            app.bottom_split_pct[0],
            app.bottom_maximized[0],
        );
    let (
        right_area,
        right_ci_area,
        right_diff_area,
        right_shell_area,
        right_claude_area,
        right_ssh_area,
    ) = split_panel_column(
        right_col,
        app.ci_panels[1].is_some(),
        app.diff_panels[1].is_some(),
        app.shell_panels[1].is_some(),
        app.claude_panels[1].is_some(),
        app.ssh_panels[1].is_some(),
        app.bottom_split_pct[1],
        app.bottom_maximized[1],
    );

    app.panel_areas = [left_area, right_area];
    app.ci_panel_areas = [left_ci_area, right_ci_area];
    app.diff_panel_areas = [left_diff_area, right_diff_area];
    app.shell_panel_areas = [left_shell_area, right_shell_area];
    app.claude_panel_areas = [left_claude_area, right_claude_area];
    app.ssh_panel_areas = [left_ssh_area, right_ssh_area];

    header::render(frame, header_area, app);
    app.menu_bar_y = header_area.y;

    // File panels are active only when nothing else is focused
    let (left_active, right_active) = if app.focus == PanelFocus::FilePanel {
        (app.active_panel == 0, app.active_panel == 1)
    } else {
        (false, false)
    };

    let has_search = app.file_search.is_some();
    let search_side = app.file_search_side;

    // Render file panels or search results
    let [left_panel, right_panel] = app.panels.each_mut();

    // Left side
    if has_search && search_side == 0 {
        if let Some(ref mut state) = app.file_search {
            search_results_view::render(frame, left_area, state, app.focus == PanelFocus::Search);
        }
    } else if left_area.height > 0 {
        panel_view::render_with_overlays(
            frame,
            left_area,
            left_panel,
            left_active,
            app.goto_path[0].as_ref(),
            app.fuzzy_search[0].as_ref(),
        );
    }

    // Right side
    if has_search && search_side == 1 {
        if let Some(ref mut state) = app.file_search {
            search_results_view::render(frame, right_area, state, app.focus == PanelFocus::Search);
        }
    } else if right_area.height > 0 {
        panel_view::render_with_overlays(
            frame,
            right_area,
            right_panel,
            right_active,
            app.goto_path[1].as_ref(),
            app.fuzzy_search[1].as_ref(),
        );
    }

    // Render CI panels
    if let (Some(ci_area), Some(ref mut ci)) = (left_ci_area, &mut app.ci_panels[0]) {
        ci_view::render(frame, ci_area, ci, app.focus == PanelFocus::Ci(0));
    }
    if let (Some(ci_area), Some(ref mut ci)) = (right_ci_area, &mut app.ci_panels[1]) {
        ci_view::render(frame, ci_area, ci, app.focus == PanelFocus::Ci(1));
    }

    // Render diff panels
    if let (Some(diff_area), Some(ref mut diff)) = (left_diff_area, &mut app.diff_panels[0]) {
        diff_view::render(frame, diff_area, diff, app.focus == PanelFocus::Diff(0));
    }
    if let (Some(diff_area), Some(ref mut diff)) = (right_diff_area, &mut app.diff_panels[1]) {
        diff_view::render(frame, diff_area, diff, app.focus == PanelFocus::Diff(1));
    }

    // Render shell panels
    if let (Some(shell_area), Some(ref sp)) = (left_shell_area, &app.shell_panels[0]) {
        terminal_view::render(frame, shell_area, sp, app.focus == PanelFocus::Shell(0));
    }
    if let (Some(shell_area), Some(ref sp)) = (right_shell_area, &app.shell_panels[1]) {
        terminal_view::render(frame, shell_area, sp, app.focus == PanelFocus::Shell(1));
    }

    // Render Claude panels
    if let (Some(claude_area), Some(ref cp)) = (left_claude_area, &app.claude_panels[0]) {
        terminal_view::render(frame, claude_area, cp, app.focus == PanelFocus::Claude(0));
    }
    if let (Some(claude_area), Some(ref cp)) = (right_claude_area, &app.claude_panels[1]) {
        terminal_view::render(frame, claude_area, cp, app.focus == PanelFocus::Claude(1));
    }

    // Render SSH panels
    if let (Some(ssh_area), Some(ref sp)) = (left_ssh_area, &app.ssh_panels[0]) {
        terminal_view::render(frame, ssh_area, sp, app.focus == PanelFocus::Ssh(0));
    }
    if let (Some(ssh_area), Some(ref sp)) = (right_ssh_area, &app.ssh_panels[1]) {
        terminal_view::render(frame, ssh_area, sp, app.focus == PanelFocus::Ssh(1));
    }

    // Render file search dialog overlay (after bottom panels so it's on top)
    if let Some(ref state) = app.file_search_dialog {
        let area = file_search_dialog::render(frame, state);
        shadow::render_shadow(frame, area);
    }

    // Render SSH dialog overlay
    if let Some(ref state) = app.ssh_dialog {
        ssh_dialog::render(frame, state);
    }

    // Render session dialog overlay
    if let Some(ref state) = app.session_dialog {
        session_dialog::render(frame, state);
    }

    // Show status message in footer if set, otherwise show key hints
    if let Some(ref msg) = app.status_message {
        footer::render_status(frame, footer_area, msg);
    } else {
        match app.focus {
            PanelFocus::Search => footer::render_search(frame, footer_area),
            PanelFocus::Ssh(_) => footer::render_ssh(frame, footer_area),
            PanelFocus::Shell(_) => footer::render_shell(frame, footer_area),
            PanelFocus::Claude(_) => footer::render_terminal(frame, footer_area),
            PanelFocus::Ci(side) => {
                if let Some(ref ci) = app.ci_panels[side] {
                    footer::render_ci(frame, footer_area, &ci.view);
                } else {
                    footer::render(frame, footer_area);
                }
            }
            PanelFocus::Diff(_) => footer::render_diff(frame, footer_area),
            PanelFocus::FilePanel => footer::render(frame, footer_area),
        }
    }

    let dialog_area = match &app.mode {
        AppMode::Dialog(ref d) => Some(dialog::render(frame, d)),
        AppMode::MkdirDialog(ref s) => Some(mkdir_dialog::render(frame, s)),
        AppMode::CopyDialog(ref s) => Some(copy_dialog::render(frame, s)),
        AppMode::ArchiveDialog(ref s) => Some(archive_dialog::render(frame, s)),
        _ => None,
    };
    if let Some(area) = dialog_area {
        shadow::render_shadow(frame, area);
    }

    // Settings dialog
    if let Some(selected) = app.settings_open {
        let area = settings_dialog::render(frame, selected);
        shadow::render_shadow(frame, area);
    }

    // Azure auth dialog
    if let Some(ref state) = app.azure_auth_dialog {
        let area = azure_auth_dialog::render(frame, state);
        shadow::render_shadow(frame, area);
    }

    // Popup overlay (rendered last, on top of everything)
    if let Some((ref title, ref msg)) = app.popup {
        render_popup(frame, title, msg);
    }
}

fn render_popup(frame: &mut Frame, title: &str, msg: &str) {
    let t = crate::theme::theme();
    let lines: Vec<&str> = msg.lines().collect();
    let max_line_width = lines.iter().map(|l| l.len()).max().unwrap_or(20);
    let width = (max_line_width as u16 + 6)
        .min(frame.area().width.saturating_sub(4))
        .max(30);
    let height = (lines.len() as u16 + 4)
        .min(frame.area().height.saturating_sub(4))
        .max(5);

    let x = (frame.area().width.saturating_sub(width)) / 2;
    let y = (frame.area().height.saturating_sub(height)) / 2;
    let area = Rect::new(x, y, width, height);

    frame.render_widget(Clear, area);

    let is_error = title.contains("Error") || title.contains("error");
    let border_color = if is_error {
        t.popup_error_border
    } else {
        t.popup_success_border
    };

    let block = Block::default()
        .title(Span::styled(
            format!(" {} ", title),
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(t.dialog_bg));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Render message lines
    let text_style = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    for (i, line) in lines.iter().enumerate() {
        if i as u16 >= inner.height.saturating_sub(1) {
            break;
        }
        let rect = Rect::new(
            inner.x + 1,
            inner.y + i as u16,
            inner.width.saturating_sub(2),
            1,
        );
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(*line, text_style))),
            rect,
        );
    }

    // "Press any key" hint at bottom
    let hint_y = inner.y + inner.height.saturating_sub(1);
    let hint = "Press any key to dismiss";
    let hint_style = Style::default()
        .fg(t.dialog_text_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::DIM);
    let rect = Rect::new(inner.x + 1, hint_y, inner.width.saturating_sub(2), 1);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(hint, hint_style))),
        rect,
    );
}

fn render_hex_viewer(frame: &mut Frame, app: &mut App) {
    if let AppMode::HexViewing(ref mut hex) = app.mode {
        hex_view::render(frame, frame.area(), hex);
    }
    if let Some(ref state) = app.search_dialog {
        let area = search_dialog::render(frame, state);
        shadow::render_shadow(frame, area);
    }
    if let Some(focused) = app.unsaved_dialog {
        let area = render_unsaved_dialog(frame, focused);
        shadow::render_shadow(frame, area);
    }
}

fn render_parquet_viewer(frame: &mut Frame, app: &mut App) {
    if let AppMode::ParquetViewing(ref mut pq) = app.mode {
        parquet_view::render(frame, frame.area(), pq);
    }
}

fn render_diff_viewer(frame: &mut Frame, app: &mut App) {
    if let AppMode::DiffViewing(ref mut dv) = app.mode {
        diff_viewer_view::render(frame, frame.area(), dv);
    }
}

fn render_editor(frame: &mut Frame, app: &mut App) {
    if let AppMode::Editing(ref mut editor) = app.mode {
        editor_view::render(frame, frame.area(), editor);
    }
    if let Some(ref state) = app.search_dialog {
        let area = search_dialog::render(frame, state);
        shadow::render_shadow(frame, area);
    }
    if let Some(ref dlg) = app.search_wrap_dialog {
        let area = render_search_wrap_dialog(frame, dlg);
        shadow::render_shadow(frame, area);
    }
    if let Some(focused) = app.unsaved_dialog {
        let area = render_unsaved_dialog(frame, focused);
        shadow::render_shadow(frame, area);
    }
}

fn render_search_wrap_dialog(frame: &mut Frame, dlg: &crate::app::SearchWrapDialog) -> Rect {
    use crate::app::SearchDirection;

    let layout = dialog_helpers::render_dialog_frame(frame, " Search ", 46, 7);
    let (normal, highlight, _) = dialog_helpers::dialog_styles();

    let direction_label = match dlg.params.direction {
        SearchDirection::Forward => "end",
        SearchDirection::Backward => "beginning",
    };
    let msg = format!("Reached {} of file. Wrap around?", direction_label);

    dialog_helpers::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", msg, width = layout.cw),
            normal,
        )),
    );

    let t = theme();
    dialog_helpers::render_separator(
        frame,
        layout.area,
        layout.inner.y + 3,
        t.dialog_border_style(),
    );

    dialog_helpers::render_buttons(
        frame,
        layout.content,
        4,
        &[
            ("{ Stop }", !dlg.wrap_focused),
            ("[ Wrap ]", dlg.wrap_focused),
        ],
        normal,
        highlight,
    );

    layout.outer
}

fn render_unsaved_dialog(frame: &mut Frame, focused: crate::app::UnsavedDialogField) -> Rect {
    use crate::app::UnsavedDialogField;

    let layout = dialog_helpers::render_dialog_frame(frame, " Unsaved Changes ", 52, 7);
    let (normal, highlight, _) = dialog_helpers::dialog_styles();

    dialog_helpers::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!(
                "{:<width$}",
                "Save changes before closing?",
                width = layout.cw
            ),
            normal,
        )),
    );

    let t = theme();
    dialog_helpers::render_separator(
        frame,
        layout.area,
        layout.inner.y + 3,
        t.dialog_border_style(),
    );

    dialog_helpers::render_buttons(
        frame,
        layout.content,
        4,
        &[
            ("{ Save }", focused == UnsavedDialogField::Save),
            ("[ Don't Save ]", focused == UnsavedDialogField::Discard),
            ("[ Cancel ]", focused == UnsavedDialogField::Cancel),
        ],
        normal,
        highlight,
    );

    layout.outer
}

fn render_overwrite_ask_dialog(frame: &mut Frame, state: &crate::app::OverwriteAskState) -> Rect {
    use crate::app::OverwriteAskChoice;
    use ratatui::widgets::{Block, Borders, Clear};

    let t = theme();
    let dialog_width: u16 = 66;
    let dialog_height: u16 = 9;
    let margin: u16 = 2;
    let margin_v: u16 = 1;
    let pad: u16 = 2;

    let area = frame.area();
    let outer_w = (dialog_width + margin * 2).min(area.width.saturating_sub(2));
    let outer_h = (dialog_height + margin_v * 2).min(area.height.saturating_sub(2));
    let w = outer_w.min(area.width);
    let h = outer_h.min(area.height);
    let outer = Rect::new(
        area.x + area.width.saturating_sub(w) / 2,
        area.y + area.height.saturating_sub(h) / 2,
        w,
        h,
    );

    frame.render_widget(Clear, outer);
    let bg_style = t.overwrite_bg_style();
    let buf = frame.buffer_mut();
    for y in outer.top()..outer.bottom() {
        for x in outer.left()..outer.right() {
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_symbol(" ");
                cell.set_style(bg_style);
            }
        }
    }

    let box_area = Rect::new(
        outer.x + margin,
        outer.y + margin_v,
        outer.width.saturating_sub(margin * 2),
        outer.height.saturating_sub(margin_v * 2),
    );

    let block = Block::default()
        .title(Span::styled(" Overwrite ", t.overwrite_title_style()))
        .borders(Borders::ALL)
        .border_style(t.overwrite_border_style())
        .style(t.overwrite_bg_style());

    let inner = block.inner(box_area);
    frame.render_widget(block, box_area);

    let content = Rect::new(
        inner.x + pad,
        inner.y,
        inner.width.saturating_sub(pad * 2),
        inner.height,
    );
    let cw = content.width as usize;

    let normal = Style::default().fg(t.overwrite_text_fg).bg(t.overwrite_bg);
    let highlight = Style::default()
        .fg(t.overwrite_btn_active_fg)
        .bg(t.overwrite_btn_active_bg);

    // Show the conflicting filename
    let name = state
        .conflict_item
        .dst
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let msg = format!("\"{}\" already exists.", name);
    dialog_helpers::render_line(
        frame,
        content,
        1,
        Line::from(Span::styled(format!("{:<width$}", msg, width = cw), normal)),
    );
    dialog_helpers::render_line(
        frame,
        content,
        3,
        Line::from(Span::styled(
            format!("{:<width$}", "Overwrite?", width = cw),
            normal,
        )),
    );

    dialog_helpers::render_separator(frame, box_area, inner.y + 5, t.overwrite_border_style());

    dialog_helpers::render_buttons(
        frame,
        content,
        6,
        &[
            (
                "{ Overwrite }",
                state.focused == OverwriteAskChoice::Overwrite,
            ),
            ("[ Skip ]", state.focused == OverwriteAskChoice::Skip),
            ("[ Skip All ]", state.focused == OverwriteAskChoice::SkipAll),
            ("[ Cancel ]", state.focused == OverwriteAskChoice::Cancel),
        ],
        normal,
        highlight,
    );

    outer
}

fn render_quit_dialog(frame: &mut Frame, quit_focused: bool) -> Rect {
    let layout = dialog_helpers::render_dialog_frame(frame, " Quit ", 40, 7);
    let (normal, highlight, _) = dialog_helpers::dialog_styles();

    dialog_helpers::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", "Quit Middle Manager?", width = layout.cw),
            normal,
        )),
    );

    let t = theme();
    dialog_helpers::render_separator(
        frame,
        layout.area,
        layout.inner.y + 3,
        t.dialog_border_style(),
    );

    dialog_helpers::render_buttons(
        frame,
        layout.content,
        4,
        &[("{ Quit }", quit_focused), ("[ Cancel ]", !quit_focused)],
        normal,
        highlight,
    );

    layout.outer
}

fn render_goto_line_dialog(frame: &mut Frame, dlg: &crate::app::GotoLineDialogState) -> Rect {
    use crate::app::GotoLineDialogField;

    let (title, label) = if dlg.is_hex {
        (" Go to Offset ", "Offset (hex):")
    } else {
        (" Go to Line ", "Line[:Col]:")
    };

    // Layout (inner rows) — matches the F7 Search dialog's spacing:
    //   y=1  label
    //   y=2  input field
    //   y=3  (blank — breathing room between input and separator)
    //   y=4  separator
    //   y=5  buttons
    // + 2 rows of border → dialog_height = 8
    let layout = dialog_helpers::render_dialog_frame(frame, title, 44, 8);
    let (normal, highlight, input_normal) = dialog_helpers::dialog_styles();
    let t = theme();

    // y=1: label
    dialog_helpers::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", label, width = layout.cw),
            normal,
        )),
    );

    // y=2: input
    let input_focused = dlg.focused == GotoLineDialogField::Input;
    let input_style = if input_focused {
        highlight
    } else {
        input_normal
    };
    dialog_helpers::render_text_input(
        frame,
        layout.content,
        2,
        &dlg.input,
        input_focused,
        input_style,
        layout.cw,
    );

    // y=4: separator above the button row (y=3 is left blank on purpose).
    dialog_helpers::render_separator(
        frame,
        layout.area,
        layout.inner.y + 4,
        t.dialog_border_style(),
    );

    // y=5: buttons
    dialog_helpers::render_buttons(
        frame,
        layout.content,
        5,
        &[
            ("{ Go }", dlg.focused == GotoLineDialogField::ButtonOk),
            (
                "[ Cancel ]",
                dlg.focused == GotoLineDialogField::ButtonCancel,
            ),
        ],
        normal,
        highlight,
    );

    layout.outer
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── truncate_to_width ──

    #[test]
    fn truncate_ascii_shorter_than_max() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
    }

    #[test]
    fn truncate_ascii_exact_max() {
        assert_eq!(truncate_to_width("hello", 5), "hello");
    }

    #[test]
    fn truncate_ascii_longer_than_max() {
        assert_eq!(truncate_to_width("hello world", 5), "hello");
    }

    #[test]
    fn truncate_empty_string() {
        assert_eq!(truncate_to_width("", 5), "");
    }

    #[test]
    fn truncate_cjk_characters() {
        // Each CJK char is width 2. "日本語abc" = widths [2,2,2,1,1,1] = total 9.
        // At max_width 5: "日本" fits (width 4), "語" would be 6 — doesn't fit.
        assert_eq!(truncate_to_width("日本語abc", 5), "日本");
    }

    #[test]
    fn truncate_mixed_ascii_multibyte() {
        // "café": c(1) a(1) f(1) é(1) — all width 1.
        assert_eq!(truncate_to_width("café", 3), "caf");
        assert_eq!(truncate_to_width("café", 4), "café");
    }

    #[test]
    fn truncate_max_width_zero() {
        assert_eq!(truncate_to_width("hello", 0), "");
    }

    #[test]
    fn truncate_emoji() {
        // "a🎉b": a(1) 🎉(2) b(1).
        // max_width 1 → "a"
        assert_eq!(truncate_to_width("a🎉b", 1), "a");
        // max_width 2 → "a" (🎉 needs 2 more, total would be 3)
        assert_eq!(truncate_to_width("a🎉b", 2), "a");
        // max_width 3 → "a🎉"
        assert_eq!(truncate_to_width("a🎉b", 3), "a🎉");
        // max_width 4 → "a🎉b"
        assert_eq!(truncate_to_width("a🎉b", 4), "a🎉b");
    }
}
