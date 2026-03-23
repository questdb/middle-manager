use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{MkdirDialogField, MkdirDialogState};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &MkdirDialogState) -> Rect {
    let t = theme();
    let layout = dh::render_dialog_frame(frame, " Make folder ", 66, 11);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // y=1: "Create the folder"
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", "Create the folder", width = layout.cw),
            normal,
        )),
    );

    // y=2: input field
    let input_focused = state.focused == MkdirDialogField::Input;
    let input_style = if input_focused {
        highlight
    } else {
        input_normal
    };
    let input_text = format!("{:<width$}", state.input, width = layout.cw);
    dh::render_line(
        frame,
        layout.content,
        2,
        Line::from(Span::styled(input_text, input_style)),
    );

    // y=4: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 4,
        t.dialog_border_style(),
    );

    // y=5: "Process multiple names" checkbox
    dh::render_checkbox(
        frame,
        layout.content,
        5,
        "Process multiple names",
        state.process_multiple,
        state.focused == MkdirDialogField::ProcessMultiple,
        normal,
        highlight,
    );

    // y=6: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 6,
        t.dialog_border_style(),
    );

    // y=7: buttons
    dh::render_buttons(
        frame,
        layout.content,
        7,
        &[
            ("{ OK }", state.focused == MkdirDialogField::ButtonOk),
            (
                "[ Cancel ]",
                state.focused == MkdirDialogField::ButtonCancel,
            ),
        ],
        normal,
        highlight,
    );

    // Blinking cursor
    if input_focused {
        let cursor_x = layout.content.x + state.cursor as u16;
        let cursor_y = layout.content.y + 2;
        if cursor_x < layout.content.x + layout.content.width {
            frame.set_cursor_position((cursor_x, cursor_y));
        }
    }

    layout.outer
}
