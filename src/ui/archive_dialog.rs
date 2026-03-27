use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{ArchiveDialogField, ArchiveDialogState};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &ArchiveDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let layout = dh::render_dialog_frame(frame, " Create Archive ", 66, 14);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // y=1: "Archive {source_name} from {dir}"
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(vec![
            Span::styled("Archive ", normal),
            Span::styled(
                &state.source_name,
                ratatui::style::Style::default()
                    .fg(t.dialog_input_fg)
                    .bg(dbg)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    );

    // y=2: "Archive name:" label
    dh::render_line(
        frame,
        layout.content,
        2,
        Line::from(Span::styled(
            format!("{:<width$}", "Archive name:", width = layout.cw),
            normal,
        )),
    );

    // y=3: archive name TextInput
    let name_focused = state.focused == ArchiveDialogField::ArchiveName;
    let name_style = if name_focused { highlight } else { input_normal };
    dh::render_text_input(
        frame,
        layout.content,
        3,
        &state.archive_name,
        name_focused,
        name_style,
        layout.cw,
    );

    // y=5: "Destination:" label
    dh::render_line(
        frame,
        layout.content,
        5,
        Line::from(Span::styled(
            format!("{:<width$}", "Destination:", width = layout.cw),
            normal,
        )),
    );

    // y=6: destination TextInput
    let dest_focused = state.focused == ArchiveDialogField::Destination;
    let dest_style = if dest_focused { highlight } else { input_normal };
    dh::render_text_input(
        frame,
        layout.content,
        6,
        &state.destination,
        dest_focused,
        dest_style,
        layout.cw,
    );

    // y=8: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 8,
        t.dialog_border_style(),
    );

    // y=9: Format dropdown
    let fmt_focused = state.focused == ArchiveDialogField::Format;
    let fmt_label = state.format.label();
    let fmt_prefix = "Format: ";
    let fmt_pad = layout
        .cw
        .saturating_sub(fmt_prefix.len() + fmt_label.len());
    dh::render_line(
        frame,
        layout.content,
        9,
        Line::from(vec![
            Span::styled(fmt_prefix, normal),
            Span::styled(
                format!("{}{}", fmt_label, " ".repeat(fmt_pad)),
                if fmt_focused { highlight } else { input_normal },
            ),
        ]),
    );

    // y=10: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 10,
        t.dialog_border_style(),
    );

    // y=11: buttons
    dh::render_buttons(
        frame,
        layout.content,
        11,
        &[
            (
                "{ Archive }",
                state.focused == ArchiveDialogField::ButtonArchive,
            ),
            (
                "[ Cancel ]",
                state.focused == ArchiveDialogField::ButtonCancel,
            ),
        ],
        normal,
        highlight,
    );

    layout.outer
}
