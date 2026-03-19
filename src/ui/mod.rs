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
}

fn render_normal(frame: &mut Frame, app: &mut App) {
    let [header_area, panels_area, footer_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let [left_area, right_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(panels_area);

    app.panel_areas = [left_area, right_area];

    header::render(frame, header_area, app);

    let (left_active, right_active) = (app.active_panel == 0, app.active_panel == 1);
    let [left_panel, right_panel] = app.panels.each_mut();
    panel_view::render(frame, left_area, left_panel, left_active);
    panel_view::render(frame, right_area, right_panel, right_active);

    footer::render(frame, footer_area);

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
