use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{CopyDialogField, CopyDialogState};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &CopyDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let title = if state.is_move {
        " Rename/Move "
    } else {
        " Copy "
    };
    let layout = dh::render_dialog_frame(frame, title, 66, 19);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // y=1: "Copy {filename} to:"
    let action_word = if state.is_move { "Move" } else { "Copy" };
    dh::render_line(
        frame,
        layout.content,
        1,
        Line::from(vec![
            Span::styled(format!("{} ", action_word), normal),
            Span::styled(
                &state.source_name,
                Style::default()
                    .fg(t.dialog_input_fg)
                    .bg(dbg)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" to:", normal),
        ]),
    );

    // y=2: destination input
    let dest_focused = state.focused == CopyDialogField::Destination;
    let dest_style = if dest_focused {
        highlight
    } else {
        input_normal
    };

    let input = &state.destination;

    dh::render_text_input(
        frame,
        layout.content,
        2,
        input,
        dest_focused,
        dest_style,
        layout.cw,
    );

    // y=4: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 4,
        t.dialog_border_style(),
    );

    // y=5: "Already existing files:" dropdown
    let ow_focused = state.focused == CopyDialogField::OverwriteMode;
    let ow_label = state.overwrite_mode.label();
    let label_prefix = "Already existing files: ";
    let ow_pad = layout
        .cw
        .saturating_sub(label_prefix.len() + ow_label.len());
    dh::render_line(
        frame,
        layout.content,
        5,
        Line::from(vec![
            Span::styled(label_prefix, normal),
            Span::styled(
                format!("{}{}", ow_label, " ".repeat(ow_pad)),
                if ow_focused { highlight } else { input_normal },
            ),
        ]),
    );

    // y=6..11: checkboxes
    dh::render_checkbox(
        frame,
        layout.content,
        6,
        "Process multiple destinations",
        state.process_multiple,
        state.focused == CopyDialogField::ProcessMultiple,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        7,
        "Copy files access mode",
        state.copy_access_mode,
        state.focused == CopyDialogField::CopyAccessMode,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        8,
        "Copy extended attributes",
        state.copy_extended_attrs,
        state.focused == CopyDialogField::CopyExtendedAttrs,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        9,
        "Disable write cache",
        state.disable_write_cache,
        state.focused == CopyDialogField::DisableWriteCache,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        10,
        "Produce sparse files",
        state.produce_sparse,
        state.focused == CopyDialogField::ProduceSparse,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        11,
        "Use copy-on-write if possible",
        state.use_cow,
        state.focused == CopyDialogField::UseCow,
        normal,
        highlight,
    );

    // y=12: symlink mode dropdown
    let sym_focused = state.focused == CopyDialogField::SymlinkMode;
    let sym_label = state.symlink_mode.label();
    let sym_prefix = "With symlinks:    ";
    let sym_pad = layout.cw.saturating_sub(sym_prefix.len() + sym_label.len());
    dh::render_line(
        frame,
        layout.content,
        12,
        Line::from(vec![
            Span::styled(sym_prefix, normal),
            Span::styled(
                format!("{}{}", sym_label, " ".repeat(sym_pad)),
                if sym_focused { highlight } else { input_normal },
            ),
        ]),
    );

    // y=13: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 13,
        t.dialog_border_style(),
    );

    // y=14: Use filter checkbox
    dh::render_checkbox(
        frame,
        layout.content,
        14,
        "Use filter",
        state.use_filter,
        state.focused == CopyDialogField::UseFilter,
        normal,
        highlight,
    );

    // y=15: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 15,
        t.dialog_border_style(),
    );

    // y=16: buttons
    let btn_label = if state.is_move { "Move" } else { "Copy" };
    dh::render_buttons(
        frame,
        layout.content,
        16,
        &[
            (
                &format!("{{ {} }}", btn_label),
                state.focused == CopyDialogField::ButtonCopy,
            ),
            ("[ Cancel ]", state.focused == CopyDialogField::ButtonCancel),
        ],
        normal,
        highlight,
    );

    layout.outer
}
