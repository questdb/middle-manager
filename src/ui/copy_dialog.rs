use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{CopyDialogField, CopyDialogState};
use crate::theme::theme;

const DIALOG_WIDTH: u16 = 66;
const DIALOG_HEIGHT: u16 = 17;

pub fn render(frame: &mut Frame, state: &CopyDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let w = DIALOG_WIDTH.min(frame.area().width.saturating_sub(2));
    let h = DIALOG_HEIGHT.min(frame.area().height.saturating_sub(2));
    let area = centered_rect(w, h, frame.area());

    frame.render_widget(Clear, area);

    let title = if state.is_move {
        " Rename/Move "
    } else {
        " Copy "
    };
    let block = Block::default()
        .title(Span::styled(title, t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let normal = Style::default().fg(t.dialog_text_fg).bg(dbg);
    let highlight = Style::default().fg(t.highlight_fg).bg(t.highlight_bg);
    let input_normal = Style::default()
        .fg(t.dialog_input_fg)
        .bg(dbg)
        .add_modifier(Modifier::BOLD);

    let iw = inner.width as usize;

    // y=0: "Copy {filename} to:"
    let action_word = if state.is_move { "Move" } else { "Copy" };
    render_line(
        frame,
        inner,
        0,
        Line::from(vec![
            Span::styled(format!(" {} ", action_word), normal),
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

    // y=1: destination input
    let dest_focused = state.focused == CopyDialogField::Destination;
    let dest_style = if dest_focused {
        highlight
    } else {
        input_normal
    };
    let dest_text = format!(
        " {:<width$}",
        state.destination,
        width = iw.saturating_sub(2)
    );
    render_line(
        frame,
        inner,
        1,
        Line::from(Span::styled(dest_text, dest_style)),
    );

    // y=2: separator
    render_separator(frame, area, inner.y + 2, t.dialog_border_style());

    // y=3: "Already existing files:" dropdown
    let ow_focused = state.focused == CopyDialogField::OverwriteMode;
    let ow_label = state.overwrite_mode.label();
    let ow_pad = iw.saturating_sub(26);
    render_line(
        frame,
        inner,
        3,
        Line::from(vec![
            Span::styled(" Already existing files: ", normal),
            Span::styled(
                format!("{:<width$}", ow_label, width = ow_pad),
                if ow_focused { highlight } else { input_normal },
            ),
        ]),
    );

    // y=4..9: checkboxes
    render_checkbox(
        frame,
        inner,
        4,
        "Process multiple destinations",
        state.process_multiple,
        state.focused == CopyDialogField::ProcessMultiple,
        normal,
        highlight,
    );
    render_checkbox(
        frame,
        inner,
        5,
        "Copy files access mode",
        state.copy_access_mode,
        state.focused == CopyDialogField::CopyAccessMode,
        normal,
        highlight,
    );
    render_checkbox(
        frame,
        inner,
        6,
        "Copy extended attributes",
        state.copy_extended_attrs,
        state.focused == CopyDialogField::CopyExtendedAttrs,
        normal,
        highlight,
    );
    render_checkbox(
        frame,
        inner,
        7,
        "Disable write cache",
        state.disable_write_cache,
        state.focused == CopyDialogField::DisableWriteCache,
        normal,
        highlight,
    );
    render_checkbox(
        frame,
        inner,
        8,
        "Produce sparse files",
        state.produce_sparse,
        state.focused == CopyDialogField::ProduceSparse,
        normal,
        highlight,
    );
    render_checkbox(
        frame,
        inner,
        9,
        "Use copy-on-write if possible",
        state.use_cow,
        state.focused == CopyDialogField::UseCow,
        normal,
        highlight,
    );

    // y=10: symlink mode dropdown
    let sym_focused = state.focused == CopyDialogField::SymlinkMode;
    let sym_label = state.symlink_mode.label();
    let sym_pad = iw.saturating_sub(19);
    render_line(
        frame,
        inner,
        10,
        Line::from(vec![
            Span::styled(" With symlinks:    ", normal),
            Span::styled(
                format!("{:<width$}", sym_label, width = sym_pad),
                if sym_focused { highlight } else { input_normal },
            ),
        ]),
    );

    // y=11: separator
    render_separator(frame, area, inner.y + 11, t.dialog_border_style());

    // y=12: Use filter checkbox
    render_checkbox(
        frame,
        inner,
        12,
        "Use filter",
        state.use_filter,
        state.focused == CopyDialogField::UseFilter,
        normal,
        highlight,
    );

    // y=13: separator
    render_separator(frame, area, inner.y + 13, t.dialog_border_style());

    // y=14: buttons
    let copy_focused = state.focused == CopyDialogField::ButtonCopy;
    let cancel_focused = state.focused == CopyDialogField::ButtonCancel;
    let btn_label = if state.is_move { "Move" } else { "Copy" };
    render_line(
        frame,
        inner,
        14,
        Line::from(vec![
            Span::styled("        ", normal),
            Span::styled(
                format!("{{ {} }}", btn_label),
                if copy_focused { highlight } else { normal },
            ),
            Span::styled("                  ", normal),
            Span::styled(
                "[ Cancel ]",
                if cancel_focused { highlight } else { normal },
            ),
        ]),
    );

    area
}

fn render_line(frame: &mut Frame, inner: Rect, y_off: u16, line: Line) {
    let rect = Rect::new(inner.x, inner.y + y_off, inner.width, 1);
    frame.render_widget(Paragraph::new(line), rect);
}

fn render_checkbox(
    frame: &mut Frame,
    inner: Rect,
    y_off: u16,
    label: &str,
    checked: bool,
    focused: bool,
    normal: Style,
    highlight: Style,
) {
    let check = if checked { "x" } else { " " };
    let style = if focused { highlight } else { normal };
    let iw = inner.width as usize;
    let text = format!(
        " [{}] {:<width$}",
        check,
        label,
        width = iw.saturating_sub(6)
    );
    render_line(frame, inner, y_off, Line::from(Span::styled(text, style)));
}

fn render_separator(frame: &mut Frame, outer: Rect, y: u16, style: Style) {
    let mut s = String::with_capacity(outer.width as usize);
    s.push('\u{251c}'); // ├
    for _ in 0..outer.width.saturating_sub(2) {
        s.push('\u{2500}'); // ─
    }
    s.push('\u{2524}'); // ┤
    let rect = Rect::new(outer.x, y, outer.width, 1);
    frame.render_widget(Paragraph::new(Span::styled(s, style)), rect);
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}
