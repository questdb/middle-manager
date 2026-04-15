use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::theme::{self, theme};

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, selected: usize) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let dialog_height = 8u16.min(frame.area().height.saturating_sub(4));
    let layout = dh::render_dialog_frame(
        frame,
        " Settings — Shift+F1/Esc to close ",
        50,
        dialog_height,
    );
    let (normal, highlight, _) = dh::dialog_styles();

    // Row 1: Theme toggle (same pattern as search dialog Direction)
    let theme_name = theme::current_theme_name();
    let is_selected = selected == 0;
    let label = "Theme:      ";
    let value = theme_name.label();
    let pad = layout.cw.saturating_sub(label.len() + value.len());
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(vec![
            Span::styled(label, normal),
            Span::styled(
                format!("{}{}", value, " ".repeat(pad)),
                if is_selected {
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

    // Separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 3,
        t.dialog_border_style(),
    );

    // Footer hint
    let hint_row = layout.content.height.saturating_sub(1);
    let hint_style = Style::default()
        .fg(t.dialog_text_fg)
        .bg(dbg)
        .add_modifier(Modifier::DIM);
    dh::render_line(
        frame,
        layout.content,
        hint_row,
        Line::from(Span::styled("Space/← → to change  Esc: close", hint_style)),
    );

    layout.outer
}
