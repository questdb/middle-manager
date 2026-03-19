use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{CopyDialogField, CopyDialogState};
use crate::theme::theme;

const DIALOG_WIDTH: u16 = 66;
const DIALOG_HEIGHT: u16 = 19; // content rows + top/bottom padding

const PAD: usize = 2;    // horizontal padding inside border
const MARGIN: u16 = 2;   // outer margin left/right
const MARGIN_V: u16 = 1; // outer margin top/bottom

pub fn render(frame: &mut Frame, state: &CopyDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;

    // Outer area includes margin
    let outer_w = (DIALOG_WIDTH + MARGIN * 2).min(frame.area().width.saturating_sub(2));
    let outer_h = (DIALOG_HEIGHT + MARGIN_V * 2).min(frame.area().height.saturating_sub(2));
    let outer = centered_rect(outer_w, outer_h, frame.area());

    // Clear and fill outer margin with dialog background
    frame.render_widget(Clear, outer);
    let bg_style = t.dialog_bg_style();
    let buf = frame.buffer_mut();
    for y in outer.top()..outer.bottom() {
        for x in outer.left()..outer.right() {
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_symbol(" ");
                cell.set_style(bg_style);
            }
        }
    }

    // Border box inside the margin
    let area = Rect::new(
        outer.x + MARGIN,
        outer.y + MARGIN_V,
        outer.width.saturating_sub(MARGIN * 2),
        outer.height.saturating_sub(MARGIN_V * 2),
    );

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
    let highlight = Style::default().fg(t.dialog_input_fg_focused).bg(t.dialog_input_bg);
    let input_normal = Style::default()
        .fg(t.dialog_input_fg)
        .bg(dbg)
        .add_modifier(Modifier::BOLD);

    // Padded content area
    let content = Rect::new(
        inner.x + PAD as u16,
        inner.y,
        inner.width.saturating_sub(PAD as u16 * 2),
        inner.height,
    );
    let cw = content.width as usize;

    // y=0: empty padding row
    // y=1: "Copy {filename} to:"
    let action_word = if state.is_move { "Move" } else { "Copy" };
    render_line(
        frame,
        content,
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
    let dest_style = if dest_focused { highlight } else { input_normal };
    let dest_text = format!("{:<width$}", state.destination, width = cw);
    render_line(
        frame,
        content,
        2,
        Line::from(Span::styled(dest_text, dest_style)),
    );

    // y=3: empty padding row
    // y=4: separator
    render_separator(frame, area, inner.y + 4, t.dialog_border_style());

    // y=5: "Already existing files:" dropdown
    let ow_focused = state.focused == CopyDialogField::OverwriteMode;
    let ow_label = state.overwrite_mode.label();
    let label_prefix = "Already existing files: ";
    let ow_pad = cw.saturating_sub(label_prefix.len() + ow_label.len());
    render_line(
        frame,
        content,
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
    render_checkbox(frame, content, 6, "Process multiple destinations", state.process_multiple, state.focused == CopyDialogField::ProcessMultiple, normal, highlight);
    render_checkbox(frame, content, 7, "Copy files access mode", state.copy_access_mode, state.focused == CopyDialogField::CopyAccessMode, normal, highlight);
    render_checkbox(frame, content, 8, "Copy extended attributes", state.copy_extended_attrs, state.focused == CopyDialogField::CopyExtendedAttrs, normal, highlight);
    render_checkbox(frame, content, 9, "Disable write cache", state.disable_write_cache, state.focused == CopyDialogField::DisableWriteCache, normal, highlight);
    render_checkbox(frame, content, 10, "Produce sparse files", state.produce_sparse, state.focused == CopyDialogField::ProduceSparse, normal, highlight);
    render_checkbox(frame, content, 11, "Use copy-on-write if possible", state.use_cow, state.focused == CopyDialogField::UseCow, normal, highlight);

    // y=12: symlink mode dropdown
    let sym_focused = state.focused == CopyDialogField::SymlinkMode;
    let sym_label = state.symlink_mode.label();
    let sym_prefix = "With symlinks:    ";
    let sym_pad = cw.saturating_sub(sym_prefix.len() + sym_label.len());
    render_line(
        frame,
        content,
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
    render_separator(frame, area, inner.y + 13, t.dialog_border_style());

    // y=14: Use filter checkbox
    render_checkbox(frame, content, 14, "Use filter", state.use_filter, state.focused == CopyDialogField::UseFilter, normal, highlight);

    // y=15: separator
    render_separator(frame, area, inner.y + 15, t.dialog_border_style());

    // y=16: buttons — centered
    let copy_focused = state.focused == CopyDialogField::ButtonCopy;
    let cancel_focused = state.focused == CopyDialogField::ButtonCancel;
    let btn_label = if state.is_move { "Move" } else { "Copy" };
    let btn_text = format!("{{ {} }}", btn_label);
    let buttons_len = btn_text.len() + 4 + 10; // btn + gap + "[ Cancel ]"
    let left_pad = cw.saturating_sub(buttons_len) / 2;
    let right_pad = cw.saturating_sub(buttons_len + left_pad);
    render_line(
        frame,
        content,
        16,
        Line::from(vec![
            Span::styled(" ".repeat(left_pad), normal),
            Span::styled(
                &btn_text,
                if copy_focused { highlight } else { normal },
            ),
            Span::styled("    ", normal),
            Span::styled(
                "[ Cancel ]",
                if cancel_focused { highlight } else { normal },
            ),
            Span::styled(" ".repeat(right_pad), normal),
        ]),
    );
    // y=17: empty padding row

    // Blinking cursor in destination input
    if dest_focused {
        let cursor_x = content.x + state.cursor as u16;
        let cursor_y = content.y + 2;
        if cursor_x < content.x + content.width {
            frame.set_cursor_position((cursor_x, cursor_y));
        }
    }

    outer
}

fn render_line(frame: &mut Frame, content: Rect, y_off: u16, line: Line) {
    let rect = Rect::new(content.x, content.y + y_off, content.width, 1);
    frame.render_widget(Paragraph::new(line), rect);
}

fn render_checkbox(
    frame: &mut Frame,
    content: Rect,
    y_off: u16,
    label: &str,
    checked: bool,
    focused: bool,
    normal: Style,
    highlight: Style,
) {
    let check = if checked { "x" } else { " " };
    let style = if focused { highlight } else { normal };
    let cw = content.width as usize;
    let text = format!(
        "[{}] {:<width$}",
        check,
        label,
        width = cw.saturating_sub(4)
    );
    render_line(frame, content, y_off, Line::from(Span::styled(text, style)));
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
