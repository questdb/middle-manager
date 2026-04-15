use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::SessionDialogState;
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &SessionDialogState) {
    let t = theme();
    let dialog_height = 18u16.min(frame.area().height.saturating_sub(4));
    let layout = dh::render_dialog_frame(
        frame,
        " Connectivity: Sessions [experimental] ",
        60,
        dialog_height,
    );
    let (normal, highlight, _input_normal) = dh::dialog_styles();

    if state.creating {
        // Creating mode: show input prompt
        dh::render_line(
            frame,
            layout.content,
            0,
            Line::from(Span::styled(
                format!("{:<width$}", "New session name:", width = layout.cw),
                normal,
            )),
        );
        dh::render_text_input(
            frame,
            layout.content,
            1,
            &state.input,
            true,
            highlight,
            layout.cw,
        );
        dh::render_line(
            frame,
            layout.content,
            3,
            Line::from(Span::styled(
                format!(
                    "{:<width$}",
                    "Enter: create  Esc: cancel",
                    width = layout.cw
                ),
                normal,
            )),
        );
        return;
    }

    // Header
    let header = if state.sessions.is_empty() {
        "No middle-manager sessions found"
    } else {
        "Active sessions:"
    };
    dh::render_line(
        frame,
        layout.content,
        0,
        Line::from(Span::styled(
            format!("{:<width$}", header, width = layout.cw),
            normal,
        )),
    );

    // Session list
    let list_start = 2u16;
    let list_height = layout.content.height.saturating_sub(list_start + 2) as usize;

    let total = state.sessions.len();
    let scroll = if total <= list_height {
        0
    } else if state.selected >= list_height {
        (state.selected + 1).saturating_sub(list_height)
    } else {
        0
    };

    for (vi, session) in state
        .sessions
        .iter()
        .skip(scroll)
        .take(list_height)
        .enumerate()
    {
        let is_selected = scroll + vi == state.selected;

        let status = if session.attached {
            "attached"
        } else {
            "detached"
        };
        let line_text = format!(
            "{:<20} {:<10} {}w",
            session.display_name, status, session.windows
        );
        let cw = layout.cw;
        let padded = format!("{:<width$}", line_text, width = cw);
        let display = if padded.len() > cw {
            super::truncate_to_width(&padded, cw).to_string()
        } else {
            padded
        };

        let style = if is_selected {
            Style::default()
                .fg(t.dialog_input_fg_focused)
                .bg(t.dialog_input_bg)
                .add_modifier(Modifier::BOLD)
        } else {
            normal
        };

        let row = list_start + vi as u16;
        if row < layout.content.height.saturating_sub(2) {
            let rect = Rect::new(
                layout.content.x,
                layout.content.y + row,
                layout.content.width,
                1,
            );
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(display, style))),
                rect,
            );
        }
    }

    // Footer hint
    let hint_row = layout.content.height.saturating_sub(1);
    dh::render_line(
        frame,
        layout.content,
        hint_row,
        Line::from(Span::styled(
            format!(
                "{:<width$}",
                "Enter: attach  n: new  d: kill  Esc: close",
                width = layout.cw
            ),
            Style::default()
                .fg(t.dialog_text_fg)
                .bg(t.dialog_bg)
                .add_modifier(Modifier::DIM),
        )),
    );
}
