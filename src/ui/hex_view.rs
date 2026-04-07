use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use std::sync::LazyLock;

use crate::hex_viewer::{HexViewerState, BYTES_PER_ROW};
use crate::theme::theme;

/// Pre-computed "XX " strings for all 256 byte values — avoids format!() per byte per frame.
static HEX_BYTE_STRINGS: LazyLock<Vec<String>> =
    LazyLock::new(|| (0..256).map(|b| format!("{:02X} ", b)).collect());

/// Pre-computed ASCII display strings for each byte value (printable or '.').
static ASCII_STRINGS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    // All printable ASCII chars as &'static str, non-printable = "."
    static CHARS: [u8; 95] = {
        let mut arr = [0u8; 95];
        let mut i = 0u8;
        while i < 95 {
            arr[i as usize] = 0x20 + i;
            i += 1;
        }
        arr
    };
    (0..256u16)
        .map(|b| {
            if (0x20..=0x7E).contains(&b) {
                // Safety: single ASCII byte is valid UTF-8
                unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                        &CHARS[(b - 0x20) as usize],
                        1,
                    ))
                }
            } else {
                "."
            }
        })
        .collect()
});

pub fn render(frame: &mut Frame, area: Rect, hex: &mut HexViewerState) {
    let t = theme();

    let size_display = format_size(hex.file_size);
    let modified_marker = if hex.modified { " [modified]" } else { "" };
    let side = if hex.editing_ascii { "ASCII" } else { "HEX" };
    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [0x{:08X}] ({}) {}{} ",
                hex.path.to_string_lossy(),
                hex.cursor_offset,
                size_display,
                side,
                modified_marker,
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
        header
            .spans
            .push(Span::styled(" ".repeat(inner_width - used), bg_style));
    }
    lines.push(header);

    // Data rows
    for (offset, bytes) in &rows_data {
        let mut row = build_data_row(*offset, bytes, t.bg, hex);
        let used: usize = row.spans.iter().map(|s| s.width()).sum();
        if used < inner_width {
            row.spans
                .push(Span::styled(" ".repeat(inner_width - used), bg_style));
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

    // Status message, selection info, or hint bar
    let hint_text = if let Some(ref msg) = hex.status_msg {
        format!(" {} ", msg)
    } else if let Some(info) = hex.selection_info() {
        format!(" {} ", info)
    } else {
        " \u{2190}\u{2191}\u{2193}\u{2192}: Move | 0-F: Edit | Tab: Hex/ASCII | g: Goto | F7: Search | n/N: Next/Prev | Ctrl+S: Save | Ctrl+Z: Undo | q: Close ".to_string()
    };
    let hint_area = Rect::new(
        area.x,
        area.y + area.height.saturating_sub(1),
        (hint_text.len() as u16).min(area.width),
        1,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            hint_text,
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
        spans.push(Span::styled(HEX_BYTE_STRINGS[i].clone(), header_style));
    }

    spans.push(Span::styled("  ", dim_style));
    spans.push(Span::styled("Decoded Text", header_style));

    Line::from(spans)
}

fn build_data_row(
    offset: u64,
    bytes: &[u8],
    bg: ratatui::style::Color,
    hex: &HexViewerState,
) -> Line<'static> {
    let offset_style = Style::default().fg(ratatui::style::Color::Yellow).bg(bg);
    let hex_style = Style::default().fg(ratatui::style::Color::LightCyan).bg(bg);
    let zero_style = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);
    let modified_style = Style::default().fg(ratatui::style::Color::LightRed).bg(bg);
    let selected_style = Style::default()
        .fg(ratatui::style::Color::White)
        .bg(ratatui::style::Color::Blue);
    let search_style = Style::default()
        .fg(ratatui::style::Color::Black)
        .bg(ratatui::style::Color::Yellow);
    let cursor_hex_style = Style::default()
        .fg(ratatui::style::Color::Black)
        .bg(ratatui::style::Color::LightCyan);
    // Dimmer highlight for the non-active side tracking the cursor position
    let track_hex_style = Style::default()
        .fg(ratatui::style::Color::LightCyan)
        .bg(ratatui::style::Color::DarkGray);
    let ascii_printable = Style::default()
        .fg(ratatui::style::Color::LightGreen)
        .bg(bg);
    let ascii_dot = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);
    let cursor_ascii_style = Style::default()
        .fg(ratatui::style::Color::Black)
        .bg(ratatui::style::Color::LightGreen);
    let track_ascii_style = Style::default()
        .fg(ratatui::style::Color::LightGreen)
        .bg(ratatui::style::Color::DarkGray);
    let sep_style = Style::default().fg(ratatui::style::Color::DarkGray).bg(bg);

    let search_range = hex.search_match;
    let selection = hex.selection_range();

    let mut spans = Vec::new();

    // Offset column
    spans.push(Span::styled(format!(" {:08X}  ", offset), offset_style));

    // Hex bytes
    for i in 0..BYTES_PER_ROW {
        if i == 8 {
            spans.push(Span::styled(" ", sep_style));
        }
        if i < bytes.len() {
            let abs_offset = offset + i as u64;
            let b = bytes[i];
            let at_cursor = abs_offset == hex.cursor_offset;
            let is_cursor = at_cursor && !hex.editing_ascii;
            let is_tracking = at_cursor && hex.editing_ascii;
            let is_modified = hex.modifications.contains_key(&abs_offset);
            let is_search = search_range
                .map(|(start, len)| abs_offset >= start && abs_offset < start + len as u64)
                .unwrap_or(false);
            let is_selected = selection
                .map(|(s, e)| abs_offset >= s && abs_offset <= e)
                .unwrap_or(false);

            let style = if is_cursor {
                cursor_hex_style
            } else if is_tracking {
                track_hex_style
            } else if is_selected {
                selected_style
            } else if is_search {
                search_style
            } else if is_modified {
                modified_style
            } else if b == 0 {
                zero_style
            } else {
                hex_style
            };

            if is_cursor && !hex.editing_ascii {
                // Highlight individual nibbles — index into pre-computed string
                let hex_str = &HEX_BYTE_STRINGS[b as usize];
                let high: String = hex_str[..1].to_string();
                let low: String = hex_str[1..2].to_string();
                let nibble_active = Style::default()
                    .fg(ratatui::style::Color::Black)
                    .bg(ratatui::style::Color::White);
                if hex.cursor_nibble == 0 {
                    spans.push(Span::styled(high, nibble_active));
                    spans.push(Span::styled(low, cursor_hex_style));
                } else {
                    spans.push(Span::styled(high, cursor_hex_style));
                    spans.push(Span::styled(low, nibble_active));
                }
                spans.push(Span::styled(" ", style));
            } else {
                spans.push(Span::styled(HEX_BYTE_STRINGS[b as usize].clone(), style));
            }
        } else {
            spans.push(Span::styled("   ", sep_style));
        }
    }

    // Separator
    spans.push(Span::styled("  ", sep_style));

    // ASCII decode
    for i in 0..BYTES_PER_ROW {
        if i < bytes.len() {
            let abs_offset = offset + i as u64;
            let b = bytes[i];
            let at_cursor = abs_offset == hex.cursor_offset;
            let is_cursor = at_cursor && hex.editing_ascii;
            let is_tracking = at_cursor && !hex.editing_ascii;
            let is_modified = hex.modifications.contains_key(&abs_offset);
            let is_search = search_range
                .map(|(start, len)| abs_offset >= start && abs_offset < start + len as u64)
                .unwrap_or(false);
            let is_selected = selection
                .map(|(s, e)| abs_offset >= s && abs_offset <= e)
                .unwrap_or(false);

            let style = if is_cursor {
                cursor_ascii_style
            } else if is_tracking {
                track_ascii_style
            } else if is_selected {
                selected_style
            } else if is_search {
                search_style
            } else if is_modified {
                modified_style
            } else if (0x20..=0x7E).contains(&b) {
                ascii_printable
            } else {
                ascii_dot
            };

            spans.push(Span::styled(ASCII_STRINGS[b as usize], style));
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
