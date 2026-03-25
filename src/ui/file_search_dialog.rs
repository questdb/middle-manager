use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{FileSearchDialogState, FileSearchField};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &FileSearchDialogState) -> Rect {
    let t = theme();
    let layout = dh::render_dialog_frame(frame, " Search in Files ", 66, 16);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // Row 1: "Search for:" (row 0 is empty gap below title)
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", "Search for:", width = layout.cw),
            normal,
        )),
    );
    let term_focused = state.focused == FileSearchField::Term;
    let term_style = if term_focused {
        highlight
    } else {
        input_normal
    };
    dh::render_text_input(
        frame,
        layout.content,
        2,
        &state.term,
        term_focused,
        term_style,
        layout.cw,
    );

    // Row 4: "Search path:"
    dh::render_line(
        frame,
        layout.content,
        4,
        Line::from(Span::styled(
            format!("{:<width$}", "Search path:", width = layout.cw),
            normal,
        )),
    );
    let path_focused = state.focused == FileSearchField::Path;
    let path_style = if path_focused {
        highlight
    } else {
        input_normal
    };
    dh::render_text_input(
        frame,
        layout.content,
        5,
        &state.path,
        path_focused,
        path_style,
        layout.cw,
    );

    // Row 7: "File filter:"
    dh::render_line(
        frame,
        layout.content,
        7,
        Line::from(Span::styled(
            format!("{:<width$}", "File filter (e.g. *.rs):", width = layout.cw),
            normal,
        )),
    );
    let filter_focused = state.focused == FileSearchField::Filter;
    let filter_style = if filter_focused {
        highlight
    } else {
        input_normal
    };
    dh::render_text_input(
        frame,
        layout.content,
        8,
        &state.filter,
        filter_focused,
        filter_style,
        layout.cw,
    );

    // Separator before options (row 9 is empty gap after filter)
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 10,
        t.dialog_border_style(),
    );

    // Row 11: regex checkbox
    dh::render_checkbox(
        frame,
        layout.content,
        11,
        "Use regular expression",
        state.is_regex,
        state.focused == FileSearchField::Regex,
        normal,
        highlight,
    );

    // Separator + Buttons
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 12,
        t.dialog_border_style(),
    );
    dh::render_buttons(
        frame,
        layout.content,
        13,
        &[
            ("{ Search }", state.focused == FileSearchField::ButtonSearch),
            ("[ Cancel ]", state.focused == FileSearchField::ButtonCancel),
        ],
        normal,
        highlight,
    );

    layout.outer
}
