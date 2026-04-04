use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

use crate::diff_viewer::{DiffKind, DiffSide, DiffViewerState};
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut DiffViewerState) {
    let t = theme();

    // Title with stats
    let mut stats = Vec::new();
    if state.added_count > 0 {
        stats.push(format!("+{}", state.added_count));
    }
    if state.deleted_count > 0 {
        stats.push(format!("-{}", state.deleted_count));
    }
    if state.changed_count > 0 {
        stats.push(format!("~{}", state.changed_count));
    }
    let stats_str = if stats.is_empty() {
        "no changes".to_string()
    } else {
        stats.join(" ")
    };

    let title = format!(
        " Diff: {} ({}) — ↑↓ scroll, n/N change, q close ",
        state.path, stats_str
    );

    let block = Block::default()
        .title(Span::styled(title, t.title_style()))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let height = inner.height as usize;
    state.visible_lines = height.saturating_sub(1); // leave room for hint line

    let total_width = inner.width as usize;
    // Split: left half | divider (1 char) | right half
    let half_width = total_width.saturating_sub(1) / 2;
    if half_width < 5 {
        return; // too narrow to render
    }

    // Line number width from cached max
    let num_width = digit_count(state.max_line_num).max(3);
    let text_width = half_width.saturating_sub(num_width + 2); // num + space + text + space

    // Styles
    let bg_style = Style::default().bg(t.bg);
    let line_num_style = Style::default().fg(t.viewer_line_num_fg).bg(t.bg);
    let added_num_style = Style::default()
        .fg(t.viewer_line_num_fg)
        .bg(t.diff_added_bg);
    let deleted_num_style = Style::default()
        .fg(t.viewer_line_num_fg)
        .bg(t.diff_deleted_bg);
    let changed_left_num_style = Style::default()
        .fg(t.viewer_line_num_fg)
        .bg(t.diff_changed_old_bg);
    let changed_right_num_style = Style::default()
        .fg(t.viewer_line_num_fg)
        .bg(t.diff_changed_new_bg);
    let divider_style = Style::default().fg(t.border).bg(t.bg);
    let empty_num = " ".repeat(num_width + 1);

    // Selection and search info
    let selection = state.selection_range();
    let search_highlight_style = Style::default().fg(t.search_label_fg).bg(t.search_label_bg);

    let visible_lines = state.visible_lines;
    let mut lines_out: Vec<Line> = Vec::with_capacity(height);

    // Track cursor screen position for terminal caret
    let mut caret_pos: Option<(u16, u16)> = None;
    let left_text_x = inner.x as usize; // start of left num column
                                        // right panel starts after: left_num + text + pad + divider
    let right_panel_x = inner.x as usize + half_width + 1;

    for (i, pair) in state
        .lines
        .iter()
        .enumerate()
        .skip(state.scroll)
        .take(visible_lines)
    {
        let is_cursor = i == state.cursor;

        // Background colors from diff kind
        let (left_bg, right_bg, left_num_st, right_num_st, left_empty_st, right_empty_st) =
            match pair.kind {
                DiffKind::Equal => (
                    t.bg,
                    t.bg,
                    line_num_style,
                    line_num_style,
                    bg_style,
                    bg_style,
                ),
                DiffKind::Added => (
                    t.diff_added_bg,
                    t.diff_added_bg,
                    added_num_style,
                    added_num_style,
                    Style::default().bg(t.diff_added_bg),
                    Style::default().bg(t.diff_added_bg),
                ),
                DiffKind::Deleted => (
                    t.diff_deleted_bg,
                    t.diff_deleted_bg,
                    deleted_num_style,
                    deleted_num_style,
                    Style::default().bg(t.diff_deleted_bg),
                    Style::default().bg(t.diff_deleted_bg),
                ),
                DiffKind::Changed => (
                    t.diff_changed_old_bg,
                    t.diff_changed_new_bg,
                    changed_left_num_style,
                    changed_right_num_style,
                    Style::default().bg(t.diff_changed_old_bg),
                    Style::default().bg(t.diff_changed_new_bg),
                ),
            };

        let left_text_style = Style::default().fg(t.viewer_text_fg).bg(left_bg);
        let right_text_style = Style::default().fg(t.viewer_text_fg).bg(right_bg);

        // Left side
        let (left_num_str, left_num_style_final) = match &pair.left {
            Some(n) => (
                format!("{:>width$} ", n.num, width = num_width),
                left_num_st,
            ),
            None => (empty_num.clone(), left_empty_st),
        };
        let left_text = match &pair.left {
            Some(n) => truncate_to_width(&n.text, text_width),
            None => String::new(),
        };
        let left_pad = text_width.saturating_sub(display_width(&left_text));

        // Right side
        let (right_num_str, right_num_style_final) = match &pair.right {
            Some(n) => (
                format!("{:>width$} ", n.num, width = num_width),
                right_num_st,
            ),
            None => (empty_num.clone(), right_empty_st),
        };
        let right_text = match &pair.right {
            Some(n) => truncate_to_width(&n.text, text_width),
            None => String::new(),
        };
        let right_pad = text_width.saturating_sub(display_width(&right_text));

        // Compute caret screen position
        if is_cursor {
            let screen_y = inner.y as usize + (i - state.scroll);
            // Convert cursor_col (original char index) to display width
            let orig_text = match state.cursor_side {
                DiffSide::Left => pair.left.as_ref().map(|n| n.text.as_str()).unwrap_or(""),
                DiffSide::Right => pair.right.as_ref().map(|n| n.text.as_str()).unwrap_or(""),
            };
            let display_col = orig_col_to_display_width(orig_text, state.cursor_col);
            let screen_x = match state.cursor_side {
                DiffSide::Left => left_text_x + num_width + display_col.min(text_width),
                DiffSide::Right => right_panel_x + num_width + display_col.min(text_width),
            };
            if (screen_x as u16) < inner.x + inner.width
                && (screen_y as u16) < inner.y + inner.height.saturating_sub(1)
            {
                caret_pos = Some((screen_x as u16, screen_y as u16));
            }
        }

        // Get syntax colors for this line
        let left_line_num = pair.left.as_ref().map(|n| n.num);
        let right_line_num = pair.right.as_ref().map(|n| n.num);
        let left_colors = left_line_num.and_then(|num| state.syntax_colors_left.get(num));
        let right_colors = right_line_num.and_then(|num| state.syntax_colors_right.get(num));

        // Build left text spans with syntax + selection + search highlighting
        let left_spans = build_diff_spans(
            &left_text,
            pair.left.as_ref().map(|n| n.text.as_str()).unwrap_or(""),
            left_colors,
            left_bg,
            left_text_style,
            selection.as_ref(),
            i,
            DiffSide::Left,
            &state.search_matches,
            search_highlight_style,
            text_width,
            t.viewer_text_fg,
        );

        // Build right text spans
        let right_spans = build_diff_spans(
            &right_text,
            pair.right.as_ref().map(|n| n.text.as_str()).unwrap_or(""),
            right_colors,
            right_bg,
            right_text_style,
            selection.as_ref(),
            i,
            DiffSide::Right,
            &state.search_matches,
            search_highlight_style,
            text_width,
            t.viewer_text_fg,
        );

        let mut line_spans = Vec::new();
        line_spans.push(Span::styled(left_num_str, left_num_style_final));
        line_spans.extend(left_spans);
        line_spans.push(Span::styled(" ".repeat(left_pad), left_text_style));
        line_spans.push(Span::styled("\u{2502}", divider_style)); // │
        line_spans.push(Span::styled(right_num_str, right_num_style_final));
        line_spans.extend(right_spans);
        line_spans.push(Span::styled(" ".repeat(right_pad), right_text_style));

        lines_out.push(Line::from(line_spans));
    }

    // Fill remaining height
    while lines_out.len() < height.saturating_sub(1) {
        lines_out.push(Line::from(Span::styled(" ".repeat(total_width), bg_style)));
    }

    // Bottom hint line
    let search_info = if let Some(ref q) = state.search_query {
        let count = state.search_matches.len();
        format!(" | search: \"{}\" ({} matches)", q, count)
    } else {
        String::new()
    };
    let hint = format!(
        " {}/{} | ↑↓ PgUp/PgDn | n/N next/prev | Ctrl+F search | F4 edit | q close{}",
        state.cursor + 1,
        state.lines.len(),
        search_info
    );
    let hint_style = Style::default().fg(t.viewer_hint_fg).bg(t.viewer_hint_bg);
    let hint_pad = total_width.saturating_sub(display_width(&hint));
    lines_out.push(Line::from(vec![
        Span::styled(&hint, hint_style),
        Span::styled(" ".repeat(hint_pad), hint_style),
    ]));

    frame.render_widget(Paragraph::new(lines_out), inner);

    // Place terminal caret (unless search input is active)
    if state.search_input.is_none() {
        if let Some((x, y)) = caret_pos {
            crate::ui::set_cursor(x, y);
        }
    }

    // Render search input overlay
    if let Some(ref input) = state.search_input {
        render_search_input(frame, input);
    }
}

/// Render the inline search prompt overlay.
fn render_search_input(frame: &mut Frame, input: &str) {
    let t = theme();
    let width: u16 = 40;
    let height: u16 = 3;
    let area = frame.area();
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    let rect = Rect::new(x, y, width.min(area.width), height.min(area.height));

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(
            " Search (Esc cancel) ",
            t.dialog_title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let prompt = Line::from(vec![
        Span::styled(
            "> ",
            Style::default().fg(t.dialog_prompt_fg).bg(t.dialog_bg),
        ),
        Span::styled(
            input,
            Style::default()
                .fg(t.dialog_input_fg)
                .bg(t.dialog_bg)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(Paragraph::new(vec![prompt]), inner);

    // Place cursor at end of input
    let cx = inner.x + 2 + input.len() as u16;
    let cy = inner.y;
    if cx < inner.x + inner.width && cy < inner.y + inner.height {
        crate::ui::set_cursor(cx, cy);
    }
}

/// Build syntax-colored spans for one side of a diff line, with selection and search highlighting.
#[allow(clippy::too_many_arguments)]
fn build_diff_spans<'a>(
    display_text: &str,
    orig_text: &str,
    line_colors: Option<&Vec<Color>>,
    diff_bg: Color,
    fallback_style: Style,
    selection: Option<&(usize, usize, usize, usize, DiffSide)>,
    pair_idx: usize,
    side: DiffSide,
    search_matches: &[crate::diff_viewer::SearchMatch],
    search_style: Style,
    _text_width: usize,
    default_fg: Color,
) -> Vec<Span<'a>> {
    if display_text.is_empty() {
        return vec![Span::styled(String::new(), fallback_style)];
    }

    // Pre-filter search matches for this line+side (avoids O(N*M) per char)
    let line_matches: Vec<&crate::diff_viewer::SearchMatch> = search_matches
        .iter()
        .filter(|m| m.pair_idx == pair_idx && m.side == side)
        .collect();

    // Build display-to-orig char mapping (handling tab expansion)
    let orig_chars: Vec<char> = orig_text.chars().collect();
    let mut display_to_orig: Vec<usize> = Vec::with_capacity(display_text.chars().count());
    let mut orig_idx = 0;
    while orig_idx < orig_chars.len() && display_to_orig.len() < display_text.chars().count() {
        let ch = orig_chars[orig_idx];
        if ch == '\t' {
            let dcol = display_to_orig.len();
            let tab_width = 4 - (dcol % 4);
            for _ in 0..tab_width {
                display_to_orig.push(orig_idx);
            }
            orig_idx += 1;
        } else if ch.is_control() {
            orig_idx += 1;
        } else {
            display_to_orig.push(orig_idx);
            orig_idx += 1;
        }
    }

    let display_chars: Vec<char> = display_text.chars().collect();

    // For each display char, determine: fg color, bg color, any style override
    let mut spans: Vec<Span<'a>> = Vec::new();
    let mut current_fg = default_fg;
    let mut current_bg = diff_bg;
    let mut current_text = String::new();

    for (disp_idx, &ch) in display_chars.iter().enumerate() {
        let orig_char_idx = display_to_orig.get(disp_idx).copied().unwrap_or(0);

        // Base fg from syntax colors
        let base_fg = line_colors
            .and_then(|c| c.get(orig_char_idx))
            .copied()
            .unwrap_or(default_fg);

        let mut fg = base_fg;
        let mut bg = diff_bg;

        // Check if this char is in selection
        if let Some(&(sl, sc, el, ec, sel_side)) = selection {
            if sel_side == side && pair_idx >= sl && pair_idx <= el {
                let in_sel = if sl == el {
                    // Single line selection
                    orig_char_idx >= sc && orig_char_idx < ec
                } else if pair_idx == sl {
                    orig_char_idx >= sc
                } else if pair_idx == el {
                    orig_char_idx < ec
                } else {
                    true
                };
                if in_sel {
                    let t = theme();
                    fg = t.highlight_fg;
                    bg = t.highlight_bg;
                }
            }
        }

        // Check if this char is in a search match (overrides selection)
        for m in &line_matches {
            if orig_char_idx >= m.col && orig_char_idx < m.col + m.len {
                fg = search_style.fg.unwrap_or(fg);
                bg = search_style.bg.unwrap_or(bg);
                break;
            }
        }

        // Group consecutive chars with same style
        if (fg != current_fg || bg != current_bg) && !current_text.is_empty() {
            spans.push(Span::styled(
                std::mem::take(&mut current_text),
                Style::default().fg(current_fg).bg(current_bg),
            ));
        }
        current_fg = fg;
        current_bg = bg;
        current_text.push(ch);
    }

    if !current_text.is_empty() {
        spans.push(Span::styled(
            current_text,
            Style::default().fg(current_fg).bg(current_bg),
        ));
    }

    if spans.is_empty() {
        spans.push(Span::styled(display_text.to_owned(), fallback_style));
    }

    spans
}

/// Convert an original char column to display width, accounting for tabs.
fn orig_col_to_display_width(text: &str, orig_col: usize) -> usize {
    let mut display_w = 0;
    for (i, c) in text.chars().enumerate() {
        if i >= orig_col {
            break;
        }
        if c == '\t' {
            display_w += 4 - (display_w % 4);
        } else {
            display_w += c.width().unwrap_or(0);
        }
    }
    display_w
}

pub fn digit_count(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    ((n as f64).log10().floor() as usize) + 1
}

fn display_width(s: &str) -> usize {
    s.chars()
        .map(|c| if c == '\t' { 4 } else { c.width().unwrap_or(0) })
        .sum()
}

fn truncate_to_width(s: &str, max_width: usize) -> String {
    let mut width = 0;
    let mut result = String::new();
    for c in s.chars() {
        let cw = if c == '\t' { 4 } else { c.width().unwrap_or(0) };
        if width + cw > max_width {
            break;
        }
        if c == '\t' {
            result.push_str("    ");
        } else {
            result.push(c);
        }
        width += cw;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- digit_count tests ---

    #[test]
    fn digit_count_zero() {
        assert_eq!(digit_count(0), 1);
    }

    #[test]
    fn digit_count_single_digits() {
        assert_eq!(digit_count(1), 1);
        assert_eq!(digit_count(9), 1);
    }

    #[test]
    fn digit_count_multi_digits() {
        assert_eq!(digit_count(10), 2);
        assert_eq!(digit_count(99), 2);
        assert_eq!(digit_count(100), 3);
        assert_eq!(digit_count(999), 3);
        assert_eq!(digit_count(1000), 4);
        assert_eq!(digit_count(99999), 5);
    }

    // --- display_width tests ---

    #[test]
    fn display_width_ascii() {
        assert_eq!(display_width("hello"), 5);
    }

    #[test]
    fn display_width_empty() {
        assert_eq!(display_width(""), 0);
    }

    #[test]
    fn display_width_tab() {
        assert_eq!(display_width("\t"), 4);
        assert_eq!(display_width("a\tb"), 6); // 1 + 4 + 1
    }

    #[test]
    fn display_width_wide_chars() {
        // CJK characters are width 2
        assert_eq!(display_width("你好"), 4);
        assert_eq!(display_width("a你b"), 4); // 1 + 2 + 1
    }

    // --- truncate_to_width tests ---

    #[test]
    fn truncate_short_string_unchanged() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
    }

    #[test]
    fn truncate_exact_width() {
        assert_eq!(truncate_to_width("hello", 5), "hello");
    }

    #[test]
    fn truncate_cuts_at_limit() {
        assert_eq!(truncate_to_width("hello world", 5), "hello");
    }

    #[test]
    fn truncate_empty() {
        assert_eq!(truncate_to_width("", 10), "");
    }

    #[test]
    fn truncate_zero_width() {
        assert_eq!(truncate_to_width("hello", 0), "");
    }

    #[test]
    fn truncate_tab_expansion() {
        // Tab expands to 4 spaces
        assert_eq!(truncate_to_width("\thello", 8), "    hell");
        assert_eq!(truncate_to_width("\t", 4), "    ");
        assert_eq!(truncate_to_width("\t", 3), ""); // tab is 4 wide, doesn't fit in 3
    }

    #[test]
    fn truncate_wide_char_boundary() {
        // CJK char is width 2, shouldn't be split
        assert_eq!(truncate_to_width("a你好b", 3), "a你");
        assert_eq!(truncate_to_width("a你好b", 2), "a"); // 你 needs 2 but only 1 left
    }

    // --- orig_col_to_display_width tests ---

    #[test]
    fn display_width_col_zero() {
        assert_eq!(orig_col_to_display_width("hello", 0), 0);
    }

    #[test]
    fn display_width_ascii_col() {
        assert_eq!(orig_col_to_display_width("hello", 3), 3);
        assert_eq!(orig_col_to_display_width("hello", 5), 5);
    }

    #[test]
    fn display_width_with_tab() {
        // Tab at col 0 expands to 4 spaces
        assert_eq!(orig_col_to_display_width("\thello", 1), 4);
        // "a\t" — tab at col 1 expands to 3 (4 - 1%4 = 3)
        assert_eq!(orig_col_to_display_width("a\thello", 2), 4);
    }

    #[test]
    fn display_width_with_wide_chars() {
        // CJK char is width 2
        assert_eq!(orig_col_to_display_width("你好", 1), 2);
        assert_eq!(orig_col_to_display_width("a你b", 2), 3); // 'a'=1 + '你'=2
    }

    #[test]
    fn display_width_past_end() {
        assert_eq!(orig_col_to_display_width("abc", 10), 3); // clamps to string length
    }
}
