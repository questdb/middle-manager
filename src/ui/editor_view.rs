use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

use crate::editor::EditorState;
use crate::syntax::SyntaxHighlighter;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, editor: &mut EditorState) {
    let t = theme();

    // Compute line number width dynamically based on total lines
    let total_lines = editor.total_virtual_lines().max(1);
    let num_digits = ((total_lines as f64).log10().floor() as usize) + 1;
    let num_digits = num_digits.max(4); // minimum 4 digits
    let line_num_width = num_digits + 2; // digits + space + separator

    let modified_indicator = if editor.modified { " [Modified]" } else { "" };
    let title = format!(" {} {}", editor.path.to_string_lossy(), modified_indicator);

    let block = Block::default()
        .title(Span::styled(title, t.title_style()))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    editor.visible_lines = inner.height.saturating_sub(1) as usize;
    editor.visible_cols = (inner.width as usize).saturating_sub(line_num_width);
    editor.viewport_x = inner.x;
    editor.viewport_y = inner.y;
    editor.line_num_width = line_num_width;

    let sel_range = editor.selection_range();
    let content = editor.visible_content();

    let num_style = Style::default().fg(Color::Yellow).bg(t.bg);
    let sep_style = Style::default().fg(Color::DarkGray).bg(t.bg);
    let default_text_style = Style::default().fg(Color::LightCyan).bg(t.bg);
    let sel_style = Style::default().fg(Color::Black).bg(Color::LightCyan);

    // Build syntax-highlighted color map.
    // Small files (< 10MB): use cached full-file parse — always accurate.
    // Large files: feed tree-sitter context lines before viewport.
    let syntax_colors = if editor.syntax.has_full_parse() {
        build_syntax_colors_from_cache(&content, &editor.syntax)
    } else {
        const CONTEXT_LINES: usize = 200;
        let context_start = editor.scroll_y.saturating_sub(CONTEXT_LINES);
        let context = editor.get_lines_range(context_start, editor.scroll_y);
        let context_line_count = context.len();
        build_syntax_colors(&context, &content, &mut editor.syntax, context_line_count)
    };

    let inner_width = inner.width as usize;
    let mut lines: Vec<Line> = Vec::with_capacity(editor.visible_lines);
    let bg_style = Style::default().bg(t.bg);

    for (line_idx, (vline, text)) in content.iter().enumerate() {
        let line_num = vline + 1;

        let display_text = truncate_to_width(text, editor.scroll_x, editor.visible_cols);

        // Convert selection columns from original-char-space to display-space
        let display_sel = sel_range.map(|((sl, sc), (el, ec))| {
            let dsc = if *vline == sl {
                orig_col_to_display_col(text, sc, editor.scroll_x)
            } else {
                sc
            };
            let dec = if *vline == el {
                orig_col_to_display_col(text, ec, editor.scroll_x)
            } else {
                ec
            };
            ((sl, dsc), (el, dec))
        });

        // Get per-char colors for this line (in original char space)
        let line_colors = syntax_colors.get(line_idx);

        let text_spans = build_highlighted_spans(
            &display_text,
            text,
            *vline,
            editor.scroll_x,
            display_sel,
            line_colors,
            default_text_style,
            sel_style,
            t.bg,
        );

        let mut spans = vec![
            Span::styled(format!("{:>width$} ", line_num, width = num_digits), num_style),
            Span::styled("\u{2502}", sep_style),
        ];
        spans.extend(text_spans);

        let used: usize = spans.iter().map(|s| s.width()).sum();
        if used < inner_width {
            spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
        }

        lines.push(Line::from(spans));
    }

    while lines.len() < editor.visible_lines {
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

    // Status bar
    let status_y = inner.y + inner.height.saturating_sub(1);
    let status_text = if let Some(ref msg) = editor.status_msg {
        format!(
            " Ln {}, Col {} | {} ",
            editor.cursor_line + 1,
            editor.cursor_col + 1,
            msg
        )
    } else {
        format!(
            " Ln {}, Col {} | F7: Search | Shift+F7: Find Next | Ctrl+C: Copy | Ctrl+A: Select All | F2: Save | Esc: Close ",
            editor.cursor_line + 1,
            editor.cursor_col + 1,
        )
    };
    let status_area = Rect::new(inner.x, status_y, inner.width, 1);
    frame.render_widget(
        Paragraph::new(Span::styled(
            status_text,
            Style::default()
                .fg(t.viewer_hint_fg)
                .bg(t.viewer_hint_bg)
                .add_modifier(Modifier::BOLD),
        )),
        status_area,
    );

    // Terminal cursor — also needs display-space conversion
    let cursor_display_col = content
        .iter()
        .find(|(vl, _)| *vl == editor.cursor_line)
        .map(|(_, text)| orig_col_to_display_col(text, editor.cursor_col, editor.scroll_x))
        .unwrap_or(0);
    let cursor_screen_x = inner.x as usize + line_num_width + cursor_display_col;
    let cursor_screen_y = inner.y as usize + editor.cursor_line.saturating_sub(editor.scroll_y);

    if cursor_screen_x < (inner.x + inner.width) as usize
        && cursor_screen_y < (inner.y + inner.height.saturating_sub(1)) as usize
    {
        frame.set_cursor_position((cursor_screen_x as u16, cursor_screen_y as u16));
        // Apply cursor shape from theme
        let _ = crossterm::execute!(std::io::stdout(), t.editor_cursor);
    }
}

/// Convert an original column position to a display column position,
/// accounting for tab expansion and scroll offset.
fn orig_col_to_display_col(text: &str, orig_col: usize, scroll_x: usize) -> usize {
    let mut display_col = 0;
    for (i, ch) in text.chars().enumerate() {
        if i < scroll_x {
            continue;
        }
        if i >= orig_col {
            break;
        }
        if ch == '\t' {
            display_col += 4 - (display_col % 4);
        } else if ch.is_control() {
            // Control chars are stripped in display, zero width
            continue;
        } else {
            display_col += UnicodeWidthChar::width(ch).unwrap_or(1);
        }
    }
    display_col
}

/// Expand tabs to spaces and truncate to fit `max_width` display cells,
/// skipping `scroll_x` characters first.
fn truncate_to_width(text: &str, scroll_x: usize, max_width: usize) -> String {
    let mut result = String::new();
    let mut width = 0;
    for ch in text.chars().skip(scroll_x) {
        if ch == '\t' {
            let spaces = 4 - (width % 4);
            for _ in 0..spaces {
                if width >= max_width {
                    break;
                }
                result.push(' ');
                width += 1;
            }
        } else if ch.is_control() {
            // Skip control chars (\r, \x1b, etc.) — they corrupt terminal output
            continue;
        } else {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(1);
            if width + ch_width > max_width {
                break;
            }
            result.push(ch);
            width += ch_width;
        }
    }
    result
}

/// Split a display line into spans, highlighting the selected region.
/// Columns are expected in display-space (tabs already accounted for).
#[allow(dead_code)]
fn build_text_spans<'a>(
    display_text: &str,
    vline: usize,
    scroll_x: usize,
    sel_range: Option<((usize, usize), (usize, usize))>,
    normal: Style,
    selected: Style,
) -> Vec<Span<'a>> {
    let ((sl, sc), (el, ec)) = match sel_range {
        Some(r) => r,
        None => return vec![Span::styled(display_text.to_owned(), normal)],
    };

    if vline < sl || vline > el {
        return vec![Span::styled(display_text.to_owned(), normal)];
    }

    let chars: Vec<char> = display_text.chars().collect();
    let len = chars.len();
    let continues = vline != el;

    let sel_start = if vline == sl {
        sc.saturating_sub(scroll_x).min(len)
    } else {
        0
    };
    let sel_end = if vline == el {
        ec.saturating_sub(scroll_x).min(len)
    } else {
        len
    };

    if sel_start >= sel_end && !continues {
        return vec![Span::styled(display_text.to_owned(), normal)];
    }

    let mut spans = Vec::new();

    if sel_start > 0 {
        spans.push(Span::styled(
            chars[..sel_start].iter().collect::<String>(),
            normal,
        ));
    }
    if sel_end > sel_start {
        spans.push(Span::styled(
            chars[sel_start..sel_end].iter().collect::<String>(),
            selected,
        ));
    }
    if continues {
        spans.push(Span::styled(" ", selected));
    }
    if sel_end < len {
        spans.push(Span::styled(
            chars[sel_end..].iter().collect::<String>(),
            normal,
        ));
    }

    spans
}

/// Build per-line syntax colors from the cached full-file parse.
/// Uses stored line byte offsets to look up the right spans.
fn build_syntax_colors_from_cache(
    content: &[(usize, String)],
    syntax: &SyntaxHighlighter,
) -> Vec<Vec<Color>> {
    let default_color = Color::LightCyan;

    if !syntax.is_active() || content.is_empty() {
        return content
            .iter()
            .map(|(_, text)| vec![default_color; text.chars().count()])
            .collect();
    }

    let mut result: Vec<Vec<Color>> = content
        .iter()
        .map(|(_, text)| vec![default_color; text.chars().count()])
        .collect();

    for (vis_idx, (vline, text)) in content.iter().enumerate() {
        let line_start_byte = syntax.line_byte_offset(*vline);
        let line_end_byte = line_start_byte + text.len();

        let spans = syntax.get_cached_spans(line_start_byte, line_end_byte);
        for &(span_start, span_end, color) in spans {
            let byte_start_in_line = span_start.saturating_sub(line_start_byte);
            let byte_end_in_line = (span_end - line_start_byte).min(text.len());

            let char_start = text[..byte_start_in_line].chars().count();
            let char_end = text[..byte_end_in_line].chars().count();

            for i in char_start..char_end.min(result[vis_idx].len()) {
                result[vis_idx][i] = color;
            }
        }
    }

    result
}

/// Build per-line syntax color arrays from tree-sitter highlighting.
/// `context` contains lines BEFORE the visible area (for multi-line constructs).
/// `content` contains the visible lines.
/// `context_line_count` is how many context lines were prepended.
/// Returns a Vec (one per visible line) of Vec<Color> (one per original char).
fn build_syntax_colors(
    context: &[String],
    content: &[(usize, String)],
    syntax: &mut SyntaxHighlighter,
    context_line_count: usize,
) -> Vec<Vec<Color>> {
    let default_color = Color::LightCyan;

    if !syntax.is_active() || content.is_empty() {
        return content
            .iter()
            .map(|(_, text)| vec![default_color; text.chars().count()])
            .collect();
    }

    // Join context + visible lines into a single string for tree-sitter
    let mut all_lines: Vec<&str> = Vec::with_capacity(context.len() + content.len());
    for line in context {
        all_lines.push(line.as_str());
    }
    for (_, text) in content {
        all_lines.push(text.as_str());
    }
    let joined = all_lines.join("\n");

    let highlights = syntax.highlight_text(&joined);

    // Build byte-offset boundaries for ALL lines (context + visible)
    let total_lines = context_line_count + content.len();
    let mut line_byte_starts = Vec::with_capacity(total_lines);
    let mut offset = 0;
    for &line_text in &all_lines {
        line_byte_starts.push(offset);
        offset += line_text.len() + 1;
    }

    // Only populate colors for the visible lines (skip context lines)
    let mut result: Vec<Vec<Color>> = content
        .iter()
        .map(|(_, text)| vec![default_color; text.chars().count()])
        .collect();

    for (span_start, span_end, color) in &highlights {
        let first_line = line_byte_starts
            .partition_point(|&start| start <= *span_start)
            .saturating_sub(1);

        for all_line_idx in first_line..total_lines {
            // Skip context lines — we only color visible lines
            if all_line_idx < context_line_count {
                let line_end_byte = line_byte_starts[all_line_idx] + all_lines[all_line_idx].len();
                if *span_end <= line_end_byte {
                    break;
                }
                continue;
            }

            let visible_idx = all_line_idx - context_line_count;
            if visible_idx >= content.len() {
                break;
            }

            let line_start_byte = line_byte_starts[all_line_idx];
            let line_text = all_lines[all_line_idx];
            let line_end_byte = line_start_byte + line_text.len();

            if *span_start >= line_end_byte {
                continue;
            }
            if *span_end <= line_start_byte {
                break;
            }

            let byte_start_in_line = span_start.saturating_sub(line_start_byte);
            let byte_end_in_line = (*span_end - line_start_byte).min(line_text.len());

            let char_start = line_text[..byte_start_in_line].chars().count();
            let char_end = line_text[..byte_end_in_line].chars().count();

            for i in char_start..char_end.min(result[visible_idx].len()) {
                result[visible_idx][i] = *color;
            }
        }
    }

    result
}

/// Build highlighted spans for a display line, combining syntax colors and selection.
#[allow(clippy::too_many_arguments)]
fn build_highlighted_spans<'a>(
    display_text: &str,
    orig_text: &str,
    vline: usize,
    scroll_x: usize,
    sel_range: Option<((usize, usize), (usize, usize))>,
    line_colors: Option<&Vec<Color>>,
    default_style: Style,
    sel_style: Style,
    bg: Color,
) -> Vec<Span<'a>> {
    let ((sl, sc), (el, ec)) = match sel_range {
        Some(r) => r,
        None => {
            // No selection — just apply syntax colors
            return build_colored_spans(display_text, orig_text, scroll_x, line_colors, default_style, bg);
        }
    };

    if vline < sl || vline > el {
        return build_colored_spans(display_text, orig_text, scroll_x, line_colors, default_style, bg);
    }

    let display_chars: Vec<char> = display_text.chars().collect();
    let len = display_chars.len();
    let continues = vline != el;

    let sel_start = if vline == sl { sc.min(len) } else { 0 };
    let sel_end = if vline == el { ec.min(len) } else { len };

    if sel_start >= sel_end && !continues {
        return build_colored_spans(display_text, orig_text, scroll_x, line_colors, default_style, bg);
    }

    // Build with selection overlay
    let mut spans = Vec::new();

    // Before selection
    if sel_start > 0 {
        let segment: String = display_chars[..sel_start].iter().collect();
        let sub_spans = build_colored_spans(&segment, orig_text, scroll_x, line_colors, default_style, bg);
        spans.extend(sub_spans);
    }

    // Selected region
    if sel_end > sel_start {
        let segment: String = display_chars[sel_start..sel_end].iter().collect();
        spans.push(Span::styled(segment, sel_style));
    }

    // Continuation marker for multi-line selection
    if continues {
        spans.push(Span::styled(" ", sel_style));
    }

    // After selection
    if sel_end < len {
        let segment: String = display_chars[sel_end..].iter().collect();
        // For the after-selection part, we need to offset the color lookup
        let offset_colors: Option<Vec<Color>> = line_colors.map(|colors| {
            let orig_char_start = display_col_to_orig_char(orig_text, sel_end, scroll_x);
            if orig_char_start < colors.len() {
                colors[orig_char_start..].to_vec()
            } else {
                vec![]
            }
        });
        let sub_spans = build_colored_spans_with_offset(
            &segment,
            offset_colors.as_ref(),
            default_style,
            bg,
        );
        spans.extend(sub_spans);
    }

    spans
}

/// Build spans with syntax colors applied (no selection).
/// Maps display characters back to original char positions for correct
/// color lookup, handling tab expansion and control char stripping.
fn build_colored_spans<'a>(
    display_text: &str,
    orig_text: &str,
    scroll_x: usize,
    line_colors: Option<&Vec<Color>>,
    default_style: Style,
    bg: Color,
) -> Vec<Span<'a>> {
    let colors = match line_colors {
        Some(c) if !c.is_empty() => c,
        _ => return vec![Span::styled(display_text.to_owned(), default_style)],
    };

    // Collect original chars once (avoids O(n²) .chars().count()/.nth() calls)
    let orig_chars: Vec<char> = orig_text.chars().collect();

    // Build a mapping: for each display char, which original char index does it
    // correspond to? Tabs expand to multiple display chars that all map to the
    // same original index.
    let mut display_to_orig: Vec<usize> = Vec::with_capacity(display_text.chars().count());
    let mut orig_idx = scroll_x;
    while orig_idx < orig_chars.len() {
        let ch = orig_chars[orig_idx];
        if ch == '\t' {
            let dcol = display_to_orig.len();
            let tab_width = 4 - (dcol % 4);
            for _ in 0..tab_width {
                display_to_orig.push(orig_idx); // all tab spaces → same orig index
            }
            orig_idx += 1;
        } else if ch.is_control() {
            // Control chars are stripped from display, skip
            orig_idx += 1;
        } else {
            display_to_orig.push(orig_idx);
            orig_idx += 1;
        }
        if display_to_orig.len() >= display_text.chars().count() {
            break;
        }
    }

    // Build spans by grouping consecutive chars with the same color
    let mut spans = Vec::new();
    let mut current_color = Color::LightCyan;
    let mut current_text = String::new();

    for (disp_idx, ch) in display_text.chars().enumerate() {
        let color = display_to_orig
            .get(disp_idx)
            .and_then(|&oi| colors.get(oi))
            .copied()
            .unwrap_or(Color::LightCyan);

        if color != current_color && !current_text.is_empty() {
            spans.push(Span::styled(
                std::mem::take(&mut current_text),
                Style::default().fg(current_color).bg(bg),
            ));
        }
        current_color = color;
        current_text.push(ch);
    }

    if !current_text.is_empty() {
        spans.push(Span::styled(
            current_text,
            Style::default().fg(current_color).bg(bg),
        ));
    }

    if spans.is_empty() {
        spans.push(Span::styled(display_text.to_owned(), default_style));
    }

    spans
}

/// Build colored spans with pre-offset color array (for after-selection segments).
fn build_colored_spans_with_offset<'a>(
    text: &str,
    colors: Option<&Vec<Color>>,
    default_style: Style,
    bg: Color,
) -> Vec<Span<'a>> {
    let colors = match colors {
        Some(c) if !c.is_empty() => c,
        _ => return vec![Span::styled(text.to_owned(), default_style)],
    };

    let mut spans = Vec::new();
    let mut current_color = Color::LightCyan;
    let mut current_text = String::new();

    for (i, ch) in text.chars().enumerate() {
        let color = if i < colors.len() { colors[i] } else { Color::LightCyan };
        if color != current_color && !current_text.is_empty() {
            spans.push(Span::styled(
                std::mem::take(&mut current_text),
                Style::default().fg(current_color).bg(bg),
            ));
        }
        current_color = color;
        current_text.push(ch);
    }

    if !current_text.is_empty() {
        spans.push(Span::styled(
            current_text,
            Style::default().fg(current_color).bg(bg),
        ));
    }

    spans
}

/// Convert a display column to an original char index (for color lookup).
fn display_col_to_orig_char(text: &str, display_col: usize, scroll_x: usize) -> usize {
    let mut dcol = 0;
    for (i, ch) in text.chars().enumerate() {
        if i < scroll_x { continue; }
        if dcol >= display_col { return i; }
        if ch == '\t' {
            dcol += 4 - (dcol % 4);
        } else if ch.is_control() {
            continue;
        } else {
            dcol += 1;
        }
    }
    text.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::Color;

    fn normal() -> Style {
        Style::default().fg(Color::White)
    }
    fn sel() -> Style {
        Style::default().fg(Color::Black).bg(Color::Cyan)
    }

    fn span_texts<'a>(spans: &'a [Span<'a>]) -> Vec<(&'a str, bool)> {
        spans
            .iter()
            .map(|s| (s.content.as_ref(), s.style == sel()))
            .collect()
    }

    // --- build_text_spans tests (display-space columns) ---

    #[test]
    fn no_selection() {
        let spans = build_text_spans("hello world", 5, 0, None, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("hello world", false)]);
    }

    #[test]
    fn line_outside_selection() {
        let range = Some(((2, 0), (4, 5)));
        let spans = build_text_spans("hello", 0, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("hello", false)]);
    }

    #[test]
    fn single_line_selection_middle() {
        let range = Some(((3, 2), (3, 5)));
        let spans = build_text_spans("hello world", 3, 0, range, normal(), sel());
        assert_eq!(
            span_texts(&spans),
            vec![("he", false), ("llo", true), (" world", false)]
        );
    }

    #[test]
    fn multi_line_start_line() {
        let range = Some(((3, 5), (6, 2)));
        let spans = build_text_spans("hello world", 3, 0, range, normal(), sel());
        assert_eq!(
            span_texts(&spans),
            vec![("hello", false), (" world", true), (" ", true)]
        );
    }

    #[test]
    fn multi_line_middle_line() {
        let range = Some(((3, 5), (6, 2)));
        let spans = build_text_spans("entire line", 4, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("entire line", true), (" ", true)]);
    }

    #[test]
    fn multi_line_end_line() {
        let range = Some(((3, 5), (6, 2)));
        let spans = build_text_spans("hello world", 6, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("he", true), ("llo world", false)]);
    }

    #[test]
    fn cursor_at_end_of_short_line_up_selection() {
        let range = Some(((499, 5), (500, 10)));
        let spans = build_text_spans("short", 499, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("short", false), (" ", true)]);
    }

    #[test]
    fn cursor_at_end_of_short_line_middle() {
        let range = Some(((498, 0), (500, 10)));
        let spans = build_text_spans("hi", 499, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("hi", true), (" ", true)]);
    }

    #[test]
    fn empty_line_in_selection() {
        let range = Some(((3, 0), (6, 5)));
        let spans = build_text_spans("", 4, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![(" ", true)]);
    }

    #[test]
    fn selection_with_horizontal_scroll() {
        // Columns are now in display-space, scroll_x already handled by caller
        let range = Some(((5, 0), (5, 5)));
        let spans = build_text_spans("lo world", 5, 0, range, normal(), sel());
        assert_eq!(span_texts(&spans), vec![("lo wo", true), ("rld", false)]);
    }

    // --- orig_col_to_display_col tests ---

    #[test]
    fn display_col_no_tabs() {
        assert_eq!(orig_col_to_display_col("hello world", 5, 0), 5);
        assert_eq!(orig_col_to_display_col("hello world", 0, 0), 0);
        assert_eq!(orig_col_to_display_col("hello world", 11, 0), 11);
    }

    #[test]
    fn display_col_with_tabs() {
        // "a\tb" -> tab at col 1 expands to 3 spaces (next 4-col boundary)
        // display: "a   b" (cols: a=0, spaces=1-3, b=4)
        assert_eq!(orig_col_to_display_col("a\tb", 0, 0), 0); // before 'a'
        assert_eq!(orig_col_to_display_col("a\tb", 1, 0), 1); // after 'a', before tab
        assert_eq!(orig_col_to_display_col("a\tb", 2, 0), 4); // after tab (expanded to col 4)
    }

    #[test]
    fn display_col_tab_at_start() {
        // "\thello" -> tab at col 0 expands to 4 spaces
        // display: "    hello"
        assert_eq!(orig_col_to_display_col("\thello", 0, 0), 0);
        assert_eq!(orig_col_to_display_col("\thello", 1, 0), 4); // after tab
        assert_eq!(orig_col_to_display_col("\thello", 2, 0), 5); // after 'h'
    }

    #[test]
    fn display_col_with_scroll() {
        // "hello world", scroll_x=3 -> displaying "lo world"
        assert_eq!(orig_col_to_display_col("hello world", 5, 3), 2); // 'o' is at display col 2
        assert_eq!(orig_col_to_display_col("hello world", 3, 3), 0); // 'l' is at display col 0
    }
}
