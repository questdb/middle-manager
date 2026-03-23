pub mod ci_view;
pub mod copy_dialog;
pub mod dialog;
pub mod dialog_helpers;
pub mod editor_view;
pub mod footer;
pub mod header;
pub mod hex_view;
pub mod mkdir_dialog;
pub mod panel_view;
pub mod search_dialog;
mod shadow;
pub mod terminal_view;
pub mod viewer_view;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{App, AppMode};
use crate::theme::theme;

pub fn render(frame: &mut Frame, app: &mut App) {
    match &app.mode {
        AppMode::Viewing(_) => render_viewer(frame, app),
        AppMode::HexViewing(_) => render_hex_viewer(frame, app),
        AppMode::Editing(_) => render_editor(frame, app),
        _ => render_normal(frame, app),
    }

    // Render goto-line prompt overlay if active
    if let Some(ref input) = app.goto_line_input {
        render_goto_prompt(frame, input);
    }

    // Render quit confirmation overlay
    if let Some(quit_focused) = app.quit_confirm {
        let area = render_quit_dialog(frame, quit_focused);
        shadow::render_shadow(frame, area);
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

    let (left_area, left_ci_area) = if app.ci_panels[0].is_some() {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                .areas(left_col);
        (top, Some(bottom))
    } else {
        (left_col, None)
    };
    let (right_area, right_ci_area) = if app.ci_panels[1].is_some() {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                .areas(right_col);
        (top, Some(bottom))
    } else {
        (right_col, None)
    };

    app.panel_areas = [left_area, right_area];
    app.ci_panel_areas = [left_ci_area, right_ci_area];

    header::render(frame, header_area, app);

    // File panels are active only when no CI/terminal panel is focused
    let (left_active, right_active) = if app.ci_focused.is_some() || app.terminal_focused {
        (false, false)
    } else {
        (app.active_panel == 0, app.active_panel == 1)
    };

    let has_terminal = app.terminal_panel.is_some();
    let terminal_side = app.terminal_side;

    // Left panel: render terminal or file panel
    let [left_panel, right_panel] = app.panels.each_mut();
    if has_terminal && terminal_side == 0 {
        terminal_view::render(
            frame,
            left_area,
            app.terminal_panel.as_ref().unwrap(),
            app.terminal_focused,
        );
    } else {
        panel_view::render_with_overlays(
            frame,
            left_area,
            left_panel,
            left_active,
            app.goto_path[0].as_ref(),
            app.fuzzy_search[0].as_ref(),
        );
    }

    // Right panel: render terminal or file panel
    if has_terminal && terminal_side == 1 {
        terminal_view::render(
            frame,
            right_area,
            app.terminal_panel.as_ref().unwrap(),
            app.terminal_focused,
        );
    } else {
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
        ci_view::render(frame, ci_area, ci, app.ci_focused == Some(0));
    }
    if let (Some(ci_area), Some(ref mut ci)) = (right_ci_area, &mut app.ci_panels[1]) {
        ci_view::render(frame, ci_area, ci, app.ci_focused == Some(1));
    }

    // Show appropriate footer
    if app.terminal_focused {
        footer::render_terminal(frame, footer_area);
    } else if let Some(side) = app.ci_focused {
        if let Some(ref ci) = app.ci_panels[side] {
            footer::render_ci(frame, footer_area, &ci.view);
        } else {
            footer::render(frame, footer_area);
        }
    } else {
        footer::render(frame, footer_area);
    }

    let dialog_area = match &app.mode {
        AppMode::Dialog(ref d) => Some(dialog::render(frame, d)),
        AppMode::MkdirDialog(ref s) => Some(mkdir_dialog::render(frame, s)),
        AppMode::CopyDialog(ref s) => Some(copy_dialog::render(frame, s)),
        _ => None,
    };
    if let Some(area) = dialog_area {
        shadow::render_shadow(frame, area);
    }
}

fn render_viewer(frame: &mut Frame, app: &mut App) {
    if let AppMode::Viewing(ref mut viewer) = app.mode {
        viewer_view::render(frame, frame.area(), viewer);
    }
}

fn render_hex_viewer(frame: &mut Frame, app: &mut App) {
    if let AppMode::HexViewing(ref mut hex) = app.mode {
        hex_view::render(frame, frame.area(), hex);
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
    if let Some(focused) = app.unsaved_dialog {
        let area = render_unsaved_dialog(frame, focused);
        shadow::render_shadow(frame, area);
    }
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

fn render_goto_prompt(frame: &mut Frame, input: &str) {
    let t = theme();
    let width: u16 = 36;
    let height: u16 = 3;
    let area = frame.area();
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    let rect = Rect::new(x, y, width.min(area.width), height.min(area.height));

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(" Go to Line[:Col] ", t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let prompt = Line::from(vec![
        Span::styled(
            "> ",
            Style::default().fg(t.dialog_prompt_fg).bg(t.dialog_bg),
        ),
        Span::styled(
            input,
            Style::default()
                .fg(t.dialog_input_fg)
                .bg(t.dialog_bg)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("_", Style::default().fg(t.dialog_cursor_fg).bg(t.dialog_bg)),
    ]);
    frame.render_widget(Paragraph::new(prompt), inner);

    shadow::render_shadow(frame, rect);
}
