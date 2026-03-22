use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::hex_viewer::{HexViewerState, BYTES_PER_ROW};
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, hex: &mut HexViewerState) {
    let t = theme();

    let size_display = format_size(hex.file_size);
    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [row {}/{}] ({}) ",
                hex.path.to_string_lossy(),
                hex.scroll_offset + 1,
                hex.total_rows(),
                size_display,
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    // -2: one for header, one for hint bar
    hex.visible_rows = inner.height.saturating_sub(2) as usize;

    let rows_data = hex.visible_rows_data();

    let inner_width = inner.width as usize;
    let bg_style = Style::default().bg(t.bg);

    let mut lines: Vec<Line> = Vec::with_capacity(hex.visible_rows + 1);

    // Header row
    let mut header = build_header(t.bg);
    let used: usize = header.spans.iter().map(|s| s.width()).sum();
    if used < inner_width {
        header.spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
    }
    lines.push(header);

    // Data rows
    for (offset, bytes) in &rows_data {
        let mut row = build_data_row(*offset, bytes, t.bg);
        let used: usize = row.spans.iter().map(|s| s.width()).sum();
        if used < inner_width {
            row.spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
        }
        lines.push(row);
    }

    // Fill empty lines below content
    while lines.len() < (hex.visible_rows + 1) {
        lines.push(Line::from(Span::styled(" ".repeat(inner_width), bg_style)));
    }

    // Fill every cell to prevent artifacts
    {
        let buf = frame.buffer_mut();
        for y in area.top()..area.bottom() {
            for x in area.left()..area.right() {
                if let Some(cell) = buf.cell_mut((x, y)) {
                    cell.set_symbol(" ");
                    cell.set_style(bg_style);
                }
            }
        }
    }

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);

    // Footer hint
    let hint = " Scroll: Arrows/PgUp/Dn | Home/End | g: Go to | F4/Tab: Text | q/Esc: Close ";
    let hint_area = Rect::new(
        area.x,
        area.y + area.height.saturating_sub(1),
        (hint.len() as u16).min(area.width),
        1,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            hint,
            Style::default().fg(t.viewer_hint_fg).bg(t.viewer_hint_bg),
        )),
        hint_area,
    );
}

fn build_header(bg: ratatui::style::Color) -> Line<'static> {
    let header_style = Style::default()
        .fg(ratatui::style::Color::Yellow)
        .bg(bg)
        .add_modifier(Modifier::BOLD);
    let dim_style = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);

    let mut spans = Vec::new();
    spans.push(Span::styled(" Offset   ", header_style));

    for i in 0..BYTES_PER_ROW {
        if i == 8 {
            spans.push(Span::styled(" ", dim_style));
        }
        spans.push(Span::styled(format!("{:02X} ", i), header_style));
    }

    spans.push(Span::styled("  ", dim_style));
    spans.push(Span::styled("Decoded Text", header_style));

    Line::from(spans)
}

fn build_data_row(offset: u64, bytes: &[u8], bg: ratatui::style::Color) -> Line<'static> {
    let offset_style = Style::default().fg(ratatui::style::Color::Yellow).bg(bg);
    let hex_style = Style::default().fg(ratatui::style::Color::LightCyan).bg(bg);
    let zero_style = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);
    let ascii_printable = Style::default()
        .fg(ratatui::style::Color::LightGreen)
        .bg(bg);
    let ascii_dot = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);
    let sep_style = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);

    let mut spans = Vec::new();

    // Offset column
    spans.push(Span::styled(format!(" {:08X}  ", offset), offset_style));

    // Hex bytes
    for i in 0..BYTES_PER_ROW {
        if i == 8 {
            spans.push(Span::styled(" ", sep_style));
        }
        if i < bytes.len() {
            let b = bytes[i];
            let style = if b == 0 { zero_style } else { hex_style };
            spans.push(Span::styled(format!("{:02X} ", b), style));
        } else {
            spans.push(Span::styled("   ", sep_style));
        }
    }

    // Separator
    spans.push(Span::styled("  ", sep_style));

    // ASCII decode
    for i in 0..BYTES_PER_ROW {
        if i < bytes.len() {
            let b = bytes[i];
            if b >= 0x20 && b <= 0x7E {
                spans.push(Span::styled(String::from(b as char), ascii_printable));
            } else {
                spans.push(Span::styled(".", ascii_dot));
            }
        }
    }

    Line::from(spans)
}

fn format_size(size: u64) -> String {
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.1} KB", size as f64 / 1024.0)
    } else if size < 1024 * 1024 * 1024 {
        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", size as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
