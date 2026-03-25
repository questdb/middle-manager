use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{DialogField, DialogState};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, dialog: &DialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let dialog_h: u16 = if dialog.has_input { 9 } else { 7 };
    let layout = dh::render_dialog_frame(frame, &format!(" {} ", dialog.title), 56, dialog_h);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // y=1: message
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", dialog.message, width = layout.cw),
            normal,
        )),
    );

    let buttons_y = if dialog.has_input {
        // y=2: input field
        let input_focused = dialog.focused == DialogField::Input;
        let input_style = if input_focused {
            highlight
        } else {
            input_normal
        };

        let input = &dialog.input;

        dh::render_text_input(
            frame,
            layout.content,
            2,
            input,
            input_focused,
            input_style,
            layout.cw,
        );

        // y=4: separator
        dh::render_separator(
            frame,
            layout.area,
            layout.inner.y + 4,
            t.dialog_border_style(),
        );
        5
    } else {
        // y=3: separator
        dh::render_separator(
            frame,
            layout.area,
            layout.inner.y + 3,
            t.dialog_border_style(),
        );
        4
    };

    let (ok_label, cancel_label) = if dialog.has_input {
        ("{ OK }", "[ Cancel ]")
    } else {
        ("{ Yes }", "[ No ]")
    };
    dh::render_buttons(
        frame,
        layout.content,
        buttons_y,
        &[
            (ok_label, dialog.focused == DialogField::ButtonOk),
            (cancel_label, dialog.focused == DialogField::ButtonCancel),
        ],
        Style::default().fg(t.dialog_text_fg).bg(dbg),
        highlight,
    );

    layout.outer
}
