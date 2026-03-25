use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::parquet_viewer::{format_size, ItemKind, ParquetViewerState, ViewMode};
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut ParquetViewerState) {
    match state.view_mode {
        ViewMode::Tree => render_tree(frame, area, state),
        ViewMode::Table => render_table(frame, area, state),
    }
}

// ---------------------------------------------------------------------------
// Tree view
// ---------------------------------------------------------------------------

fn render_tree(frame: &mut Frame, area: Rect, state: &mut ParquetViewerState) {
    let t = theme();

    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [Parquet] ({}) ",
                state.path.to_string_lossy(),
                format_size(state.file_size),
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    // -1 for hint bar
    state.tree_visible = inner.height.saturating_sub(1) as usize;

    let inner_width = inner.width as usize;
    let bg_style = Style::default().bg(t.bg);

    let mut lines: Vec<Line> = Vec::with_capacity(state.tree_visible);

    for i in 0..state.tree_visible {
        let idx = state.tree_scroll + i;
        if idx >= state.tree_items.len() {
            lines.push(Line::from(Span::styled(
                " ".repeat(inner_width),
                bg_style,
            )));
            continue;
        }

        let item = &state.tree_items[idx];
        let is_cursor = idx == state.tree_cursor;

        // Build indentation
        let indent = "  ".repeat(item.depth);

        // Build icon
        let icon = if item.expandable {
            if let Some(ref nid) = item.node_id {
                if state.expanded.contains(nid) {
                    "[-] "
                } else {
                    "[+] "
                }
            } else {
                "    "
            }
        } else {
            "    "
        };

        let prefix = format!("{}{}", indent, icon);
        let text = &item.text;

        // Choose color by kind
        let fg = match item.kind {
            ItemKind::Header => Color::White,
            ItemKind::Property => Color::LightCyan,
            ItemKind::SchemaField => Color::LightGreen,
            ItemKind::RowGroupHeader => Color::Yellow,
            ItemKind::ColumnInfo => Color::Cyan,
            ItemKind::DataHeader => Color::Yellow,
            ItemKind::DataCell => Color::LightCyan,
            ItemKind::Error => Color::LightRed,
        };

        let style = if is_cursor {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(fg).bg(t.bg)
        };

        // Build the line with truncation
        let full = format!("{}{}", prefix, text);
        let display = if full.chars().count() > inner_width {
            let truncated: String = full.chars().take(inner_width.saturating_sub(3)).collect();
            format!("{}...", truncated)
        } else {
            full
        };

        let mut spans = vec![Span::styled(display, style)];

        // Fill rest of line
        let used: usize = spans.iter().map(|s| s.width()).sum();
        if used < inner_width {
            spans.push(Span::styled(
                " ".repeat(inner_width - used),
                if is_cursor {
                    style
                } else {
                    bg_style
                },
            ));
        }

        lines.push(Line::from(spans));
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

    // Hint bar
    let hint = " Up/Dn: Navigate | Enter/Right: Expand | Left: Collapse | Tab: Table View | g: Go to | q/Esc: Close ";
    render_hint(frame, area, hint);
}

// ---------------------------------------------------------------------------
// Table view
// ---------------------------------------------------------------------------

fn render_table(frame: &mut Frame, area: Rect, state: &mut ParquetViewerState) {
    let t = theme();

    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [Table] row {}/{} ",
                state.path.to_string_lossy(),
                state.table_scroll_row + 1,
                state.table_total_rows,
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    // -3 for header + separator + hint
    state.table_visible_rows = inner.height.saturating_sub(3) as usize;

    let inner_width = inner.width as usize;
    let bg_style = Style::default().bg(t.bg);

    // Calculate visible columns based on scroll position and available width
    let col_widths = &state.table_column_widths;
    let col_start = state.table_scroll_col;
    let mut visible_end = col_start;
    let mut used_width = 0;
    for i in col_start..col_widths.len() {
        let w = col_widths[i] + 3; // " | " separator
        if used_width + w > inner_width + 3 {
            break;
        }
        used_width += w;
        visible_end = i + 1;
    }
    state.table_visible_cols = visible_end - col_start;

    let mut lines: Vec<Line> = Vec::with_capacity(state.table_visible_rows + 2);

    // Header row
    let header_style = Style::default()
        .fg(Color::Yellow)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(Color::DarkGray).bg(t.bg);

    let header_spans = build_row_spans(
        &state.table_columns,
        col_widths,
        col_start,
        visible_end,
        inner_width,
        header_style,
        sep_style,
        bg_style,
    );
    lines.push(Line::from(header_spans));

    // Separator
    let sep_line = build_separator(col_widths, col_start, visible_end, inner_width, sep_style, bg_style);
    lines.push(Line::from(sep_line));

    // Data rows
    let data_style = Style::default().fg(Color::LightCyan).bg(t.bg);
    let null_style = Style::default().fg(Color::DarkGray).bg(t.bg);

    for i in 0..state.table_visible_rows {
        let global_row = state.table_scroll_row + i;
        if global_row >= state.table_total_rows {
            lines.push(Line::from(Span::styled(
                " ".repeat(inner_width),
                bg_style,
            )));
            continue;
        }

        if let Some(row) = state.table_row(global_row) {
            let row_spans = build_data_row_spans(
                row,
                col_widths,
                col_start,
                visible_end,
                inner_width,
                data_style,
                null_style,
                sep_style,
                bg_style,
            );
            lines.push(Line::from(row_spans));
        } else {
            // Row not loaded
            let mut spans = vec![Span::styled(
                "  ...loading...",
                Style::default().fg(Color::DarkGray).bg(t.bg),
            )];
            let used: usize = spans.iter().map(|s| s.width()).sum();
            if used < inner_width {
                spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
            }
            lines.push(Line::from(spans));
        }
    }

    // Fill every cell
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

    let hint = " Up/Dn: Scroll | Left/Right: Pan cols | Home/End | Tab: Tree View | g: Go to | q/Esc: Close ";
    render_hint(frame, area, hint);
}

// ---------------------------------------------------------------------------
// Span builders
// ---------------------------------------------------------------------------

fn build_row_spans(
    values: &[String],
    col_widths: &[usize],
    col_start: usize,
    col_end: usize,
    inner_width: usize,
    value_style: Style,
    sep_style: Style,
    bg_style: Style,
) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled(" ", bg_style));
    for i in col_start..col_end {
        if i > col_start {
            spans.push(Span::styled(" | ", sep_style));
        }
        let w = col_widths.get(i).copied().unwrap_or(8);
        let val = values.get(i).map(|s| s.as_str()).unwrap_or("");
        spans.push(Span::styled(fit(val, w), value_style));
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < inner_width {
        spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
    }
    spans
}

fn build_data_row_spans(
    values: &[String],
    col_widths: &[usize],
    col_start: usize,
    col_end: usize,
    inner_width: usize,
    data_style: Style,
    null_style: Style,
    sep_style: Style,
    bg_style: Style,
) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled(" ", bg_style));
    for i in col_start..col_end {
        if i > col_start {
            spans.push(Span::styled(" | ", sep_style));
        }
        let w = col_widths.get(i).copied().unwrap_or(8);
        let val = values.get(i).map(|s| s.as_str()).unwrap_or("");
        let style = if val == "null" { null_style } else { data_style };
        spans.push(Span::styled(fit(val, w), style));
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < inner_width {
        spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
    }
    spans
}

fn build_separator(
    col_widths: &[usize],
    col_start: usize,
    col_end: usize,
    inner_width: usize,
    sep_style: Style,
    bg_style: Style,
) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled("-", sep_style));
    for i in col_start..col_end {
        if i > col_start {
            spans.push(Span::styled("-+-", sep_style));
        }
        let w = col_widths.get(i).copied().unwrap_or(8);
        spans.push(Span::styled("-".repeat(w), sep_style));
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < inner_width {
        spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
    }
    spans
}

/// Fit a string to exactly `width` display chars: truncate or right-pad with spaces.
fn fit(s: &str, width: usize) -> String {
    let char_count = s.chars().count();
    if char_count > width {
        let truncated: String = s.chars().take(width).collect();
        truncated
    } else {
        format!("{:<w$}", s, w = width)
    }
}

fn render_hint(frame: &mut Frame, area: Rect, hint: &str) {
    let t = theme();
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
