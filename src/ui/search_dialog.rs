use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{SearchDialogField, SearchDialogState, SearchDirection};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &SearchDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let layout = dh::render_dialog_frame(frame, " Search ", 56, 11);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // y=1: "Search for:"
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", "Search for:", width = layout.cw),
            normal,
        )),
    );

    // y=2: query input
    let query_focused = state.focused == SearchDialogField::Query;
    let query_style = if query_focused {
        highlight
    } else {
        input_normal
    };
    let query_text = format!("{:<width$}", state.query, width = layout.cw);
    dh::render_line(
        frame,
        layout.content,
        2,
        Line::from(Span::styled(query_text, query_style)),
    );

    // y=4: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 4,
        t.dialog_border_style(),
    );

    // y=5: direction toggle
    let dir_focused = state.focused == SearchDialogField::Direction;
    let dir_label = match state.direction {
        SearchDirection::Forward => "Forward",
        SearchDirection::Backward => "Backward",
    };
    let dir_prefix = "Direction:  ";
    let dir_pad = layout.cw.saturating_sub(dir_prefix.len() + dir_label.len());
    dh::render_line(
        frame,
        layout.content,
        5,
        Line::from(vec![
            Span::styled(dir_prefix, normal),
            Span::styled(
                format!("{}{}", dir_label, " ".repeat(dir_pad)),
                if dir_focused {
                    highlight
                } else {
                    Style::default()
                        .fg(t.dialog_input_fg)
                        .bg(dbg)
                        .add_modifier(Modifier::BOLD)
                },
            ),
        ]),
    );

    // y=6: case sensitive checkbox
    dh::render_checkbox(
        frame,
        layout.content,
        6,
        "Case sensitive",
        state.case_sensitive,
        state.focused == SearchDialogField::CaseSensitive,
        normal,
        highlight,
    );

    // y=7: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 7,
        t.dialog_border_style(),
    );

    // y=8: buttons
    dh::render_buttons(
        frame,
        layout.content,
        8,
        &[
            ("{ Find }", state.focused == SearchDialogField::ButtonFind),
            (
                "[ Cancel ]",
                state.focused == SearchDialogField::ButtonCancel,
            ),
        ],
        normal,
        highlight,
    );

    // Blinking cursor
    if query_focused {
        let cursor_x = layout.content.x + state.cursor as u16;
        let cursor_y = layout.content.y + 2;
        if cursor_x < layout.content.x + layout.content.width {
            frame.set_cursor_position((cursor_x, cursor_y));
        }
    }

    layout.outer
}
