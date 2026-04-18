use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use unicode_width::UnicodeWidthChar;
use unicode_width::UnicodeWidthStr;

use crate::parquet_viewer::{
    format_size, format_with_thousands, sanitize_for_line, Alignment, DetailPopup, ItemKind,
    ParquetViewerState, TableLayout, ViewMode,
};
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
            lines.push(Line::from(Span::styled(" ".repeat(inner_width), bg_style)));
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
                if is_cursor { style } else { bg_style },
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

    let total_cols = state.table_columns.len().max(1);
    let sel_count = state.selected_count();
    let hidden_count = (0..state.table_columns.len())
        .filter(|i| state.is_col_hidden(*i))
        .count();
    let mut title_text = format!(
        " {} [Table] row {}/{} col {}/{} ",
        state.path.to_string_lossy(),
        state.table_cursor_row + 1,
        state.table_total_rows,
        state.table_cursor_col + 1,
        total_cols,
    );
    if hidden_count > 0 {
        title_text.push_str(&format!(" {} hidden ", hidden_count));
    }
    if sel_count > 0 {
        title_text.push_str(&format!(" {} selected ", sel_count));
    }
    if let Some((c, asc)) = state.sort_order {
        let col_name = state.table_columns.get(c).cloned().unwrap_or_default();
        let arrow = if asc { "↑" } else { "↓" };
        title_text.push_str(&format!(" sort {} {} ", arrow, col_name));
    }
    // Active search indicator (committed, not while typing).
    if let Some(ref s) = state.search {
        if !s.input_open && !s.query().is_empty() {
            // Truncate long queries in the title so they don't push the
            // more important metadata off-screen.
            let q: String = s.query().chars().take(40).collect();
            let suffix = if s.query().chars().count() > 40 {
                "…"
            } else {
                ""
            };
            title_text.push_str(&format!(" /{}{}/ ", q, suffix));
        }
    }
    let block = Block::default()
        .title(Span::styled(title_text, t.title_style()))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    // -3 for header + separator + hint
    state.table_visible_rows = inner.height.saturating_sub(3) as usize;

    let inner_width = inner.width as usize;
    let bg_style = Style::default().bg(t.bg);

    // Row-number prefix column ("#    │ "): width = digits(total) + 1 padding.
    // The gutter is always visible and does not pan with column scrolling.
    // Min width 2 leaves room for the `◄` scroll indicator in the header.
    let raw_digits = if state.table_total_rows <= 1 {
        1
    } else {
        (state.table_total_rows as f64).log10().floor() as usize + 1
    };
    let num_digits = raw_digits.max(2);
    let gutter_w = num_digits + 3; // digits + " │ "
    let content_width = inner_width.saturating_sub(gutter_w);

    // Calculate visible columns based on scroll position and available content width.
    // If a column is frozen AND the user has scrolled past it, the frozen column
    // is rendered to the left of the scrolled range and costs (width + 3) cols
    // (value + " ║ " separator).
    let col_widths = &state.table_column_widths;
    let col_start = state.table_scroll_col;
    // Treat hidden frozen columns as un-frozen (renderer would otherwise try
    // to draw a column that isn't supposed to appear).
    let effective_frozen: Option<usize> = state
        .frozen_col
        .filter(|&fc| fc < col_start && fc < col_widths.len() && !state.is_col_hidden(fc));
    let freeze_overhead = match effective_frozen {
        Some(fc) => col_widths.get(fc).copied().unwrap_or(0) + 3,
        None => 0,
    };
    let scroll_content_width = content_width.saturating_sub(freeze_overhead);
    let mut visible_end = col_start;
    let mut used_width = 0;
    let mut visible_count = 0usize;
    for (i, &cw) in col_widths.iter().enumerate().skip(col_start) {
        if state.is_col_hidden(i) {
            // Hidden columns don't consume width, but they still count
            // toward visible_end so the data-row loop iterates past them.
            visible_end = i + 1;
            continue;
        }
        let w = cw + 3; // " | " separator
        if used_width + w > scroll_content_width + 3 {
            break;
        }
        used_width += w;
        visible_end = i + 1;
        visible_count += 1;
    }
    state.table_visible_cols = visible_count;

    // --- Layout snapshot for click_at ------------------------------------
    // Walk the same x-positions the span builders produce so a click can be
    // mapped back to a column. Header is at inner.y; separator at inner.y+1;
    // data rows start at inner.y+2.
    {
        let base_x: u16 = inner.x + gutter_w as u16;
        let mut x = base_x;
        let mut col_hits: Vec<(usize, u16, u16)> = Vec::new();
        if let Some(fc) = effective_frozen {
            let fw = col_widths.get(fc).copied().unwrap_or(0) as u16;
            col_hits.push((fc, x, x + fw));
            x += fw + 3; // " ║ " divider
        }
        // The span builders prepend a single leading space before the first
        // scrolled cell.
        x += 1;
        let mut first = true;
        for i in col_start..visible_end {
            if state.is_col_hidden(i) {
                continue;
            }
            if !first {
                x += 3; // " | " separator
            }
            first = false;
            let w = col_widths.get(i).copied().unwrap_or(8) as u16;
            col_hits.push((i, x, x + w));
            x += w;
        }
        let data_start_y = inner.y + 2; // header + separator
        let data_end_y = data_start_y + state.table_visible_rows as u16;
        state.last_layout = Some(TableLayout {
            data_start_y,
            data_end_y,
            scroll_row: state.table_scroll_row,
            col_hits,
        });
    }

    let mut lines: Vec<Line> = Vec::with_capacity(state.table_visible_rows + 2);

    // Header row
    let header_style = Style::default()
        .fg(Color::Yellow)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);
    let ts = TableStyles {
        sep: Style::default().fg(Color::DarkGray).bg(t.bg),
        bg: bg_style,
        data: Style::default().fg(Color::LightCyan).bg(t.bg),
        null: Style::default().fg(Color::DarkGray).bg(t.bg),
    };

    // Header with gutter. If there are more columns to the left/right of
    // the visible range, we replace the first/last character of the gutter
    // with a ◄/► indicator.
    let gutter_label_style = Style::default()
        .fg(t.parquet_gutter_fg)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);
    let has_left = col_start > 0;
    let has_right = visible_end < state.table_columns.len();
    let indicator_style = Style::default().fg(t.parquet_indicator_fg).bg(t.bg);

    // Precompute a hidden bitmap for quick lookups in the span builders.
    let hidden: Vec<bool> = (0..state.table_columns.len())
        .map(|i| state.is_col_hidden(i))
        .collect();

    let mut header_spans = Vec::<Span<'static>>::with_capacity(8);
    // Left indicator lives in the first cell of the gutter; '#' label fills
    // the rest.
    if has_left {
        header_spans.push(Span::styled("◄", indicator_style));
        header_spans.push(Span::styled(
            format!("{:>w$} │ ", "#", w = num_digits.saturating_sub(1).max(1)),
            gutter_label_style,
        ));
    } else {
        header_spans.push(Span::styled(
            format!("{:>w$} │ ", "#", w = num_digits),
            gutter_label_style,
        ));
    }
    // Frozen column for the header row.
    if let Some(fc) = effective_frozen {
        let fw = col_widths.get(fc).copied().unwrap_or(8);
        let name = state
            .table_columns
            .get(fc)
            .map(|s| s.as_str())
            .unwrap_or("");
        header_spans.push(Span::styled(fit(name, fw), header_style));
        header_spans.push(Span::styled(" ║ ", ts.sep));
    }
    // Header for the scrolled range; still reserve a column for `►` if needed.
    let header_target_width = if has_right {
        scroll_content_width.saturating_sub(1)
    } else {
        scroll_content_width
    };
    let header_layout = ColLayout {
        col_widths,
        hidden: &hidden,
        col_start,
        col_end: visible_end,
        inner_width: header_target_width,
    };
    header_spans.extend(build_row_spans(
        &state.table_columns,
        &header_layout,
        header_style,
        &ts,
    ));
    if has_right {
        header_spans.push(Span::styled("►", indicator_style));
    }
    lines.push(Line::from(header_spans));

    // Separator (horizontal rule under the header).
    let mut sep_spans = Vec::<Span<'static>>::with_capacity(8);
    sep_spans.push(Span::styled(
        format!("{} ┼ ", "─".repeat(num_digits)),
        ts.sep,
    ));
    if let Some(fc) = effective_frozen {
        let fw = col_widths.get(fc).copied().unwrap_or(8);
        sep_spans.push(Span::styled("─".repeat(fw), ts.sep));
        sep_spans.push(Span::styled("─╫─", ts.sep));
    }
    sep_spans.extend(build_separator(
        col_widths,
        &hidden,
        col_start,
        visible_end,
        scroll_content_width,
        &ts,
    ));
    lines.push(Line::from(sep_spans));

    let cursor_row = state.table_cursor_row;
    let cursor_col = state.table_cursor_col;
    let gutter_row_style = Style::default().fg(t.parquet_gutter_fg).bg(t.bg);
    let gutter_cursor_style = Style::default()
        .fg(t.parquet_cursor_gutter_fg)
        .bg(t.parquet_cursor_row_bg)
        .add_modifier(Modifier::BOLD);
    // Compile the search regex once per render rather than per row: the
    // per-`TableSearch` cache keeps the result across renders, but hoisting
    // also avoids one RefCell borrow per visible row.
    let search_re = state.search_regex();
    for i in 0..state.table_visible_rows {
        let global_row = state.table_scroll_row + i;
        if global_row >= state.table_total_rows {
            lines.push(Line::from(Span::styled(" ".repeat(inner_width), bg_style)));
            continue;
        }

        let is_cursor_row = global_row == cursor_row;
        let is_selected = state.is_row_selected(global_row);
        // When a sort is active, the visual position (`global_row`) doesn't
        // equal the canonical row number — look up the original one.
        let display_row = state.canonical_row_id(global_row);
        // Use `●` for selected rows (replaces the rightmost digit separator)
        // so the number width stays constant.
        let gutter_text = if is_selected {
            format!("{:>w$} ● ", display_row + 1, w = num_digits)
        } else {
            format!("{:>w$} │ ", display_row + 1, w = num_digits)
        };
        let gutter_style = if is_cursor_row {
            gutter_cursor_style
        } else if is_selected {
            Style::default()
                .fg(t.parquet_selected_row_fg)
                .bg(t.bg)
                .add_modifier(Modifier::BOLD)
        } else {
            gutter_row_style
        };
        let gutter_prefix = Span::styled(gutter_text, gutter_style);

        if let Some(row) = state.table_row(global_row) {
            let mut all_spans = Vec::<Span<'static>>::with_capacity(16);
            all_spans.push(gutter_prefix);

            // Render the frozen column (if any) before the scrolled range.
            if let Some(fc) = effective_frozen {
                let fw = col_widths.get(fc).copied().unwrap_or(8);
                let val = row.get(fc).map(|s| s.as_str()).unwrap_or("");
                let is_cursor_cell = is_cursor_row && cursor_col == fc;
                let fg_style = if val == "null" { ts.null } else { ts.data };
                let cell_bg = if is_cursor_cell {
                    t.parquet_cursor_cell_bg
                } else if is_cursor_row {
                    t.parquet_cursor_row_bg
                } else {
                    ts.bg.bg.unwrap_or(Color::Reset)
                };
                let align = if val == "null" {
                    Alignment::Left
                } else {
                    state
                        .table_column_aligns
                        .get(fc)
                        .copied()
                        .unwrap_or(Alignment::Left)
                };
                let display_val: String =
                    if state.thousands_separators && align == Alignment::Right && val != "null" {
                        format_with_thousands(val)
                    } else {
                        val.to_string()
                    };
                let fitted = fit_aligned(&display_val, fw, align);
                let cell_spans = split_with_matches(
                    &fitted,
                    search_re.as_ref(),
                    fg_style.bg(cell_bg),
                    Style::default().fg(t.search_label_fg).bg(t.search_label_bg),
                );
                all_spans.extend(cell_spans);
                // Vertical double-bar divider between the frozen column and
                // the scrolled area.
                let sep_bg = if is_cursor_row {
                    t.parquet_cursor_row_bg
                } else {
                    ts.bg.bg.unwrap_or(Color::Reset)
                };
                all_spans.push(Span::styled(" ║ ", ts.sep.bg(sep_bg)));
            }

            let data_layout = ColLayout {
                col_widths,
                hidden: &hidden,
                col_start,
                col_end: visible_end,
                inner_width: scroll_content_width,
            };
            let data_ctx = DataRowCtx {
                col_aligns: &state.table_column_aligns,
                is_cursor_row,
                cursor_col: if is_cursor_row {
                    Some(cursor_col)
                } else {
                    None
                },
                search: search_re.as_ref(),
                thousands_separators: state.thousands_separators,
            };
            all_spans.extend(build_data_row_spans(row, &data_layout, &ts, &data_ctx));
            lines.push(Line::from(all_spans));
        } else {
            let mut spans = vec![
                gutter_prefix,
                Span::styled(
                    "  ...loading...",
                    Style::default().fg(Color::DarkGray).bg(t.bg),
                ),
            ];
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

    // Hint / status / search input bar, in order of precedence.
    let hint_text = if state.search_is_input_open() {
        let q = state.search.as_ref().map(|s| s.query()).unwrap_or("");
        format!(" /{} (Enter=find, Esc=cancel) ", q)
    } else if let Some(ref msg) = state.status {
        format!(" {} ", msg)
    } else if state.popup_is_open() {
        " Up/Dn: Scroll | Enter/Esc: Close | y: Copy ".to_string()
    } else if let Some(sum) = state.current_column_summary() {
        let type_part = if sum.logical.is_empty() {
            sum.physical
        } else {
            format!("{}/{}", sum.physical, sum.logical)
        };
        let null_part = match sum.null_count {
            Some(n) if sum.num_values > 0 => {
                let pct = 100.0 * (n as f64) / (sum.num_values as f64);
                format!(" nulls: {} ({:.1}%)", n, pct)
            }
            Some(n) => format!(" nulls: {}", n),
            None => String::new(),
        };
        format!(
            " {} [{}]{}  |  Enter: Cell  /: Search  Space: Sel  e: Export  y/c: Copy  ?: Help ",
            sum.name, type_part, null_part
        )
    } else {
        " Arrows: Move  Enter: Cell  /: Search  Space: Select  y: Cell  Y: Row  c: Column  e/E: Export  [/]: RG  Tab: Tree "
            .to_string()
    };
    render_hint(frame, area, &hint_text);

    // Popup overlay
    if state.popup_is_open() {
        render_row_detail(frame, area, state);
    }
}

// ---------------------------------------------------------------------------
// Row detail popup
// ---------------------------------------------------------------------------

fn render_row_detail(frame: &mut Frame, area: Rect, state: &mut ParquetViewerState) {
    let t = theme();

    let Some(popup) = &state.popup else {
        return;
    };

    // Snapshot the popup data so we can mutate `state.popup` later for the
    // clamped scroll.
    let (title, rendered) = match popup {
        DetailPopup::Row { row_idx, pairs, .. } => {
            let pw = area.width.saturating_sub(6).max(20) as usize;
            let ph = area.height.saturating_sub(4).max(10) as usize;
            let title = format!(" Row {} details ", row_idx + 1);
            let rendered = render_row_pairs_lines(pairs, pw - 2, ph);
            (title, rendered)
        }
        DetailPopup::Cell {
            row_idx,
            col_idx,
            column_name,
            value,
            ..
        } => {
            let pw = area.width.saturating_sub(6).max(20) as usize;
            let title = format!(
                " Row {}, col {} [{}] ",
                row_idx + 1,
                col_idx + 1,
                column_name
            );
            let rendered = render_cell_lines(value, pw - 2);
            (title, rendered)
        }
        DetailPopup::ColumnInfo {
            col_idx,
            column_name,
            pairs,
            ..
        } => {
            let pw = area.width.saturating_sub(6).max(20) as usize;
            let ph = area.height.saturating_sub(4).max(10) as usize;
            let title = format!(" Column {} info [{}] ", col_idx + 1, column_name);
            let rendered = render_row_pairs_lines(pairs, pw - 2, ph);
            (title, rendered)
        }
    };

    // Size: ~80% of area. `render_dialog_frame` adds its own outer margins,
    // so subtract a bit to leave room for them.
    let pw = area.width.saturating_sub(10).max(18);
    let ph = area.height.saturating_sub(6).max(8);
    let layout = crate::ui::dialog_helpers::render_dialog_frame(frame, &title, pw, ph);
    let inner = layout.inner;

    let inner_width = inner.width as usize;
    let inner_height = inner.height as usize;
    if inner_width == 0 || inner_height == 0 {
        return;
    }

    // Clamp scroll and write back.
    let total = rendered.len();
    let max_scroll = total.saturating_sub(inner_height);
    let current_scroll = state.popup.as_ref().map(|p| p.scroll()).unwrap_or(0);
    let scroll = current_scroll.min(max_scroll);
    if let Some(p) = state.popup.as_mut() {
        *p.scroll_mut() = scroll;
    }

    let bg = t.dialog_bg_style();
    let name_style = Style::default()
        .fg(t.dialog_title_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);
    let value_style = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let null_style = Style::default().fg(t.dialog_hint_fg).bg(t.dialog_bg);
    let cont_style = Style::default().fg(t.parquet_popup_cont_fg).bg(t.dialog_bg);

    let mut lines: Vec<Line> = Vec::with_capacity(inner_height);
    for i in 0..inner_height {
        let idx = scroll + i;
        if idx >= rendered.len() {
            lines.push(Line::from(Span::styled(" ".repeat(inner_width), bg)));
            continue;
        }
        let (role, text) = &rendered[idx];
        let style = match role {
            RenderRole::Name => name_style,
            RenderRole::Value => value_style,
            RenderRole::Null => null_style,
            RenderRole::Continuation => cont_style,
        };
        let display: String = if UnicodeWidthStr::width(text.as_str()) > inner_width {
            truncate_chars(text, inner_width)
        } else {
            text.clone()
        };
        let used = UnicodeWidthStr::width(display.as_str());
        let mut spans = vec![Span::styled(display, style)];
        if used < inner_width {
            spans.push(Span::styled(" ".repeat(inner_width - used), bg));
        }
        lines.push(Line::from(spans));
    }

    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, inner);

    // Tiny scroll indicator in the bottom-right of the popup.
    if total > inner_height {
        let indicator = format!(" {}/{} ", scroll + 1, max_scroll + 1);
        let iw = indicator.chars().count() as u16;
        if layout.area.width > iw + 2 {
            let ix = layout.area.right().saturating_sub(iw + 1);
            let iy = layout.area.bottom().saturating_sub(1);
            let ind_area = Rect::new(ix, iy, iw, 1);
            frame.render_widget(
                Paragraph::new(Span::styled(
                    indicator,
                    Style::default().fg(t.viewer_hint_fg).bg(t.viewer_hint_bg),
                )),
                ind_area,
            );
        }
    }
}

#[derive(Clone, Copy)]
enum RenderRole {
    Name,
    Value,
    Null,
    Continuation,
}

fn render_row_pairs_lines(
    pairs: &[(String, String)],
    inner_width: usize,
    _inner_height: usize,
) -> Vec<(RenderRole, String)> {
    let name_w = pairs
        .iter()
        .map(|(k, _)| UnicodeWidthStr::width(k.as_str()))
        .max()
        .unwrap_or(0)
        .min(inner_width.saturating_sub(4).max(1));
    let value_w = inner_width.saturating_sub(name_w + 2).max(1);

    let mut rendered: Vec<(RenderRole, String)> = Vec::new();
    for (name, value) in pairs {
        let is_null = value == "null";
        let wrapped = wrap_value(value, value_w);
        let mut iter = wrapped.into_iter();
        let first = iter.next().unwrap_or_default();
        let header = format!(
            "{}{}  {}",
            truncate_chars(name, name_w),
            pad_to_width(name, name_w),
            first
        );
        rendered.push((
            if is_null {
                RenderRole::Null
            } else {
                RenderRole::Name
            },
            header,
        ));
        let indent = " ".repeat(name_w + 2);
        for seg in iter {
            rendered.push((RenderRole::Continuation, format!("{}{}", indent, seg)));
        }
        rendered.push((RenderRole::Value, String::new()));
    }
    rendered
}

fn render_cell_lines(value: &str, inner_width: usize) -> Vec<(RenderRole, String)> {
    let is_null = value == "null";
    // If the cell parses as JSON and has structure (object/array), render a
    // pretty-printed copy. Primitives (string/number/bool) aren't worth
    // pretty-printing — they'd just be the same text. Invalid JSON falls
    // back to the raw wrapped value.
    let source = maybe_pretty_json(value).unwrap_or_else(|| value.to_string());
    let role = if is_null {
        RenderRole::Null
    } else {
        RenderRole::Value
    };
    wrap_value(&source, inner_width.max(1))
        .into_iter()
        .map(|line| (role, line))
        .collect()
}

/// Returns a pretty-printed JSON version of `s` if it parses as a non-trivial
/// JSON value (object or array). Returns `None` for primitives and for
/// invalid JSON so the caller shows the raw value unchanged.
fn maybe_pretty_json(s: &str) -> Option<String> {
    let trimmed = s.trim();
    let starts_ok = trimmed.starts_with('{') || trimmed.starts_with('[');
    if !starts_ok {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    if !parsed.is_object() && !parsed.is_array() {
        return None;
    }
    serde_json::to_string_pretty(&parsed).ok()
}

/// Right-pad `name` with spaces so the combined display width equals `width`.
/// `truncate_chars` in the caller has already trimmed if needed.
fn pad_to_width(name: &str, width: usize) -> String {
    let already = UnicodeWidthStr::width(truncate_chars(name, width).as_str());
    if already >= width {
        String::new()
    } else {
        " ".repeat(width - already)
    }
}

/// Wrap `value` into lines each up to `width` characters. Preserves the input
/// order of characters; does not break on word boundaries (the cell contents
/// are arbitrary, so naive fixed-width wrap is fine).
fn wrap_value(value: &str, width: usize) -> Vec<String> {
    if width == 0 || value.is_empty() {
        return vec![value.to_string()];
    }
    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut count = 0usize;
    for ch in value.chars() {
        if ch == '\n' {
            out.push(std::mem::take(&mut current));
            count = 0;
            continue;
        }
        current.push(ch);
        count += 1;
        if count >= width {
            out.push(std::mem::take(&mut current));
            count = 0;
        }
    }
    if !current.is_empty() || out.is_empty() {
        out.push(current);
    }
    out
}

fn truncate_chars(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        return s.to_string();
    }
    s.chars().take(width).collect()
}

// ---------------------------------------------------------------------------
// Span builders
// ---------------------------------------------------------------------------

struct TableStyles {
    sep: Style,
    bg: Style,
    data: Style,
    null: Style,
}

/// Column-layout view shared between the header row, separator, and data
/// rows. Groups the "which columns are visible and how wide" parameters so
/// the span builders don't need 6+ positional args.
struct ColLayout<'a> {
    col_widths: &'a [usize],
    hidden: &'a [bool],
    col_start: usize,
    col_end: usize,
    inner_width: usize,
}

/// Per-row context for `build_data_row_spans`. Factors out cursor, search,
/// and display-format state that the header row doesn't need.
struct DataRowCtx<'a> {
    col_aligns: &'a [Alignment],
    is_cursor_row: bool,
    cursor_col: Option<usize>,
    search: Option<&'a regex::Regex>,
    thousands_separators: bool,
}

fn build_row_spans(
    values: &[String],
    layout: &ColLayout,
    value_style: Style,
    ts: &TableStyles,
) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled(" ", ts.bg));
    let mut first_visible = true;
    for i in layout.col_start..layout.col_end {
        if layout.hidden.get(i).copied().unwrap_or(false) {
            continue;
        }
        if !first_visible {
            spans.push(Span::styled(" | ", ts.sep));
        }
        first_visible = false;
        let w = layout.col_widths.get(i).copied().unwrap_or(8);
        let val = values.get(i).map(|s| s.as_str()).unwrap_or("");
        spans.push(Span::styled(fit(val, w), value_style));
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < layout.inner_width {
        spans.push(Span::styled(" ".repeat(layout.inner_width - used), ts.bg));
    }
    spans
}

fn build_data_row_spans(
    values: &[String],
    layout: &ColLayout,
    ts: &TableStyles,
    ctx: &DataRowCtx,
) -> Vec<Span<'static>> {
    // Row highlight is BG only (bold widens glyphs and breaks alignment).
    // Cell highlight is a brighter BG overlaid only on the cursor column.
    let t = theme();
    let row_bg = t.parquet_cursor_row_bg;
    let cell_bg = t.parquet_cursor_cell_bg;
    let match_fg = t.search_label_fg;
    let match_bg = t.search_label_bg;
    let base_bg = if ctx.is_cursor_row {
        row_bg
    } else {
        ts.bg.bg.unwrap_or(Color::Reset)
    };
    let row_bg_style = Style::default().bg(base_bg);

    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled(" ", row_bg_style));
    let mut first_visible = true;
    for i in layout.col_start..layout.col_end {
        if layout.hidden.get(i).copied().unwrap_or(false) {
            continue;
        }
        let is_cursor_cell = ctx.is_cursor_row && ctx.cursor_col == Some(i);
        let col_bg = if is_cursor_cell { cell_bg } else { base_bg };
        if !first_visible {
            spans.push(Span::styled(" | ", ts.sep.bg(base_bg)));
        }
        first_visible = false;
        let w = layout.col_widths.get(i).copied().unwrap_or(8);
        let val = values.get(i).map(|s| s.as_str()).unwrap_or("");
        let fg_style = if val == "null" { ts.null } else { ts.data };
        // Null renders left-aligned regardless of column type so "null" reads
        // uniformly down a column.
        let align = if val == "null" {
            Alignment::Left
        } else {
            ctx.col_aligns.get(i).copied().unwrap_or(Alignment::Left)
        };
        // Apply thousands separators only for right-aligned (numeric) cells
        // when the viewer's global toggle is on. Non-integer values pass
        // through `format_with_thousands` unchanged.
        let display_val: String =
            if ctx.thousands_separators && align == Alignment::Right && val != "null" {
                format_with_thousands(val)
            } else {
                val.to_string()
            };
        let fitted = fit_aligned(&display_val, w, align);
        // If a search pattern matches within the fitted cell text, split the
        // span on match boundaries and paint the matching bytes in a high-
        // contrast yellow background.
        let mut cell_spans = split_with_matches(
            &fitted,
            ctx.search,
            fg_style.bg(col_bg),
            Style::default().fg(match_fg).bg(match_bg),
        );
        spans.append(&mut cell_spans);
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < layout.inner_width {
        spans.push(Span::styled(
            " ".repeat(layout.inner_width - used),
            row_bg_style,
        ));
    }
    spans
}

/// Split `text` on regex match boundaries and return one `Span` per segment.
/// Non-matching segments get `normal_style`; matching segments get
/// `match_style`. If `re` is `None`, returns a single span with `normal_style`.
fn split_with_matches(
    text: &str,
    re: Option<&regex::Regex>,
    normal_style: Style,
    match_style: Style,
) -> Vec<Span<'static>> {
    let re = match re {
        Some(r) => r,
        None => return vec![Span::styled(text.to_string(), normal_style)],
    };
    let mut out = Vec::new();
    let mut last = 0usize;
    for m in re.find_iter(text) {
        if m.start() > last {
            out.push(Span::styled(
                text[last..m.start()].to_string(),
                normal_style,
            ));
        }
        if m.start() < m.end() {
            out.push(Span::styled(
                text[m.start()..m.end()].to_string(),
                match_style,
            ));
        } else {
            // Zero-width match (e.g. regex "a*" on empty): advance past it to
            // avoid infinite loop; don't emit a highlight span.
        }
        last = m.end();
        // Avoid infinite loops on zero-width matches like `a*` by advancing
        // past the match position if we haven't consumed any bytes.
        if m.start() == m.end() {
            break;
        }
    }
    if last < text.len() {
        out.push(Span::styled(text[last..].to_string(), normal_style));
    }
    if out.is_empty() {
        out.push(Span::styled(text.to_string(), normal_style));
    }
    out
}

fn build_separator(
    col_widths: &[usize],
    hidden: &[bool],
    col_start: usize,
    col_end: usize,
    inner_width: usize,
    ts: &TableStyles,
) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(Span::styled("-", ts.sep));
    let mut first_visible = true;
    for i in col_start..col_end {
        if hidden.get(i).copied().unwrap_or(false) {
            continue;
        }
        if !first_visible {
            spans.push(Span::styled("-+-", ts.sep));
        }
        first_visible = false;
        let w = col_widths.get(i).copied().unwrap_or(8);
        spans.push(Span::styled("-".repeat(w), ts.sep));
    }

    let used: usize = spans.iter().map(|s| s.width()).sum();
    if used < inner_width {
        spans.push(Span::styled(" ".repeat(inner_width - used), ts.bg));
    }
    spans
}

/// Fit a string to exactly `width` terminal columns, left-aligned.
fn fit(s: &str, width: usize) -> String {
    fit_aligned(s, width, Alignment::Left)
}

/// Fit a string to exactly `width` terminal columns with the given alignment.
/// Sanitizes newlines/tabs, then truncates or pads using Unicode display width
/// (so CJK, accented chars, emoji don't break alignment). On truncation we
/// always keep the leading characters — for right-aligned numeric values the
/// most-significant digits are what the user wants to see.
fn fit_aligned(s: &str, width: usize, align: Alignment) -> String {
    let clean = sanitize_for_line(s);
    let total = UnicodeWidthStr::width(clean.as_str());
    if total <= width {
        let pad = width - total;
        let mut out = String::with_capacity(clean.len() + pad);
        match align {
            Alignment::Left => {
                out.push_str(&clean);
                out.extend(std::iter::repeat_n(' ', pad));
            }
            Alignment::Right => {
                out.extend(std::iter::repeat_n(' ', pad));
                out.push_str(&clean);
            }
        }
        return out;
    }
    // Truncate char-by-char, counting display width so we don't split a
    // double-wide glyph in half.
    let mut kept = String::with_capacity(width);
    let mut used = 0usize;
    for c in clean.chars() {
        let cw = UnicodeWidthChar::width(c).unwrap_or(0);
        if used + cw > width {
            break;
        }
        kept.push(c);
        used += cw;
    }
    if used < width {
        // Rare: a wide glyph doesn't fit at the truncation boundary; pad
        // the remaining slot.
        let pad = width - used;
        match align {
            Alignment::Left => kept.extend(std::iter::repeat_n(' ', pad)),
            Alignment::Right => {
                let mut prefix = String::with_capacity(pad + kept.len());
                prefix.extend(std::iter::repeat_n(' ', pad));
                prefix.push_str(&kept);
                kept = prefix;
            }
        }
    }
    kept
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_object_is_pretty_printed() {
        let out = maybe_pretty_json(r#"{"a":1,"b":[2,3]}"#).unwrap();
        // Pretty-printed JSON spans multiple lines.
        assert!(out.contains('\n'));
        assert!(out.contains("\"a\""));
        assert!(out.contains("  ")); // indentation
    }

    #[test]
    fn json_array_is_pretty_printed() {
        let out = maybe_pretty_json(r#"[1,2,3]"#).unwrap();
        assert!(out.contains('\n'));
    }

    #[test]
    fn json_primitive_returns_none() {
        assert!(maybe_pretty_json("42").is_none());
        assert!(maybe_pretty_json("\"hello\"").is_none());
        assert!(maybe_pretty_json("true").is_none());
    }

    #[test]
    fn invalid_json_returns_none() {
        assert!(maybe_pretty_json("{oops").is_none());
        assert!(maybe_pretty_json("not json at all").is_none());
    }

    #[test]
    fn fit_pads_ascii_to_exact_width() {
        assert_eq!(fit("hi", 5), "hi   ");
        assert_eq!(fit("", 3), "   ");
    }

    #[test]
    fn fit_aligned_right_puts_padding_on_left() {
        assert_eq!(fit_aligned("42", 5, Alignment::Right), "   42");
        assert_eq!(fit_aligned("42", 2, Alignment::Right), "42");
    }

    #[test]
    fn fit_aligned_right_pads_empty_string() {
        assert_eq!(fit_aligned("", 3, Alignment::Right), "   ");
    }

    #[test]
    fn fit_truncates_without_splitting_wide_glyphs() {
        // "你好" is 2 CJK glyphs, each width 2. Fit to width 3 should give
        // just "你" plus one space (the second glyph doesn't fit).
        let out = fit("你好", 3);
        assert_eq!(UnicodeWidthStr::width(out.as_str()), 3);
        // The second glyph must not leak through partially.
        assert!(out.starts_with('你'));
    }
}
