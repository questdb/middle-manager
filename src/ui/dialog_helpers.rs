use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::theme::theme;

const PAD: u16 = 2; // horizontal padding inside border
const MARGIN: u16 = 2; // outer margin left/right
const MARGIN_V: u16 = 1; // outer margin top/bottom

pub struct DialogLayout {
    pub outer: Rect,
    pub area: Rect,
    pub inner: Rect,
    pub content: Rect,
    pub cw: usize,
}

/// Render the common dialog frame: outer margin, border box, inner padding.
/// Returns layout rects for placing content.
pub fn render_dialog_frame(
    frame: &mut Frame,
    title: &str,
    dialog_width: u16,
    dialog_height: u16,
) -> DialogLayout {
    let t = theme();

    let outer_w = (dialog_width + MARGIN * 2).min(frame.area().width.saturating_sub(2));
    let outer_h = (dialog_height + MARGIN_V * 2).min(frame.area().height.saturating_sub(2));
    let outer = centered_rect(outer_w, outer_h, frame.area());

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

    let area = Rect::new(
        outer.x + MARGIN,
        outer.y + MARGIN_V,
        outer.width.saturating_sub(MARGIN * 2),
        outer.height.saturating_sub(MARGIN_V * 2),
    );

    let block = Block::default()
        .title(Span::styled(title, t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let content = Rect::new(
        inner.x + PAD,
        inner.y,
        inner.width.saturating_sub(PAD * 2),
        inner.height,
    );
    let cw = content.width as usize;

    // Store content area for click detection
    super::set_dialog_content(content);

    DialogLayout {
        outer,
        area,
        inner,
        content,
        cw,
    }
}

/// Common dialog styles: (normal, highlight, input_normal)
pub fn dialog_styles() -> (Style, Style, Style) {
    let t = theme();
    let dbg = t.dialog_bg;
    let normal = Style::default().fg(t.dialog_text_fg).bg(dbg);
    let highlight = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg);
    let input_normal = Style::default()
        .fg(t.dialog_input_fg)
        .bg(dbg)
        .add_modifier(Modifier::BOLD);
    (normal, highlight, input_normal)
}

pub fn render_line(frame: &mut Frame, content: Rect, y_off: u16, line: Line) {
    let rect = Rect::new(content.x, content.y + y_off, content.width, 1);
    frame.render_widget(Paragraph::new(line), rect);
}

pub fn render_separator(frame: &mut Frame, outer: Rect, y: u16, style: Style) {
    let mut s = String::with_capacity(outer.width as usize);
    s.push('\u{251c}');
    for _ in 0..outer.width.saturating_sub(2) {
        s.push('\u{2500}');
    }
    s.push('\u{2524}');
    let rect = Rect::new(outer.x, y, outer.width, 1);
    frame.render_widget(Paragraph::new(Span::styled(s, style)), rect);
}

#[allow(clippy::too_many_arguments)]
pub fn render_checkbox(
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

pub fn render_buttons(
    frame: &mut Frame,
    content: Rect,
    y_off: u16,
    buttons: &[(&str, bool)],
    normal: Style,
    highlight: Style,
) {
    let total_len: usize =
        buttons.iter().map(|(l, _)| l.len()).sum::<usize>() + (buttons.len().saturating_sub(1)) * 4; // 4-char gap between buttons
    let cw = content.width as usize;
    let left_pad = cw.saturating_sub(total_len) / 2;

    let mut spans = vec![Span::styled(" ".repeat(left_pad), normal)];
    for (i, (label, focused)) in buttons.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("    ", normal));
        }
        spans.push(Span::styled(
            *label,
            if *focused { highlight } else { normal },
        ));
    }
    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < cw {
        spans.push(Span::styled(" ".repeat(cw - used), normal));
    }

    render_line(frame, content, y_off, Line::from(spans));
}

/// Render a TextInput field with horizontal scrolling, selection highlighting,
/// and cursor positioning. Shared across all dialogs.
pub fn render_text_input(
    frame: &mut Frame,
    content: Rect,
    y_off: u16,
    input: &crate::text_input::TextInput,
    focused: bool,
    style: Style,
    cw: usize,
) {
    let t = theme();
    let text = &input.text;
    let cursor = input.cursor;

    // Compute visible window using char counts (safe for multi-byte UTF-8)
    let before_cursor = &text[..cursor];
    let before_chars = before_cursor.chars().count();
    let total_chars = text.chars().count();

    let skip_chars = if before_chars >= cw {
        before_chars - cw + 1
    } else {
        0
    };

    // Convert char offset to byte offset for slicing
    let visible_start = text
        .char_indices()
        .nth(skip_chars)
        .map(|(i, _)| i)
        .unwrap_or(0);
    let visible_end_char = (skip_chars + cw).min(total_chars);
    let visible_end = text
        .char_indices()
        .nth(visible_end_char)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    let visible_text = &text[visible_start..visible_end];
    let cursor_in_view = before_chars.saturating_sub(skip_chars);

    // Check for active selection
    if let Some((sel_start, sel_end)) = input.selection_range() {
        if sel_start != sel_end {
            let sel_style = t.input_selection_style();
            // Clamp selection to visible window (byte offsets relative to visible_text)
            let vis_sel_start = sel_start.max(visible_start) - visible_start;
            let vis_sel_end = sel_end.max(visible_start) - visible_start;
            let vis_sel_start = vis_sel_start.min(visible_text.len());
            let vis_sel_end = vis_sel_end.min(visible_text.len());

            let before = &visible_text[..vis_sel_start];
            let selected = &visible_text[vis_sel_start..vis_sel_end];
            let after = &visible_text[vis_sel_end..];
            let visible_chars = visible_text.chars().count();
            let remaining = cw.saturating_sub(visible_chars);

            let mut spans = vec![
                Span::styled(before.to_string(), style),
                Span::styled(selected.to_string(), sel_style),
                Span::styled(after.to_string(), style),
            ];
            if remaining > 0 {
                spans.push(Span::styled(" ".repeat(remaining), style));
            }
            render_line(frame, content, y_off, Line::from(spans));

            if focused {
                let cursor_x = content.x + cursor_in_view as u16;
                crate::ui::set_cursor(cursor_x, content.y + y_off);
            }
            return;
        }
    }

    // No selection — render visible portion of text
    let visible_chars = visible_text.chars().count();
    let remaining = cw.saturating_sub(visible_chars);
    let display = if remaining > 0 {
        format!("{}{}", visible_text, " ".repeat(remaining))
    } else {
        visible_text.to_string()
    };
    render_line(
        frame,
        content,
        y_off,
        Line::from(Span::styled(display, style)),
    );

    if focused {
        let cursor_x = content.x + cursor_in_view as u16;
        crate::ui::set_cursor(cursor_x, content.y + y_off);
    }
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}
