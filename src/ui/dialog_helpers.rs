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

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}
