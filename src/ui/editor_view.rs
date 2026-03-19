use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

use crate::editor::EditorState;
use crate::theme::theme;

const LINE_NUM_WIDTH: usize = 7; // "  1234 " = 7 chars

pub fn render(frame: &mut Frame, area: Rect, editor: &mut EditorState) {
    let t = theme();

    let modified_indicator = if editor.modified { " [Modified]" } else { "" };
    let title = format!(" {} {}", editor.path.to_string_lossy(), modified_indicator);

    let block = Block::default()
        .title(Span::styled(title, t.title_style()))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    editor.visible_lines = inner.height.saturating_sub(1) as usize;
    editor.visible_cols = (inner.width as usize).saturating_sub(LINE_NUM_WIDTH);

    let sel_range = editor.selection_range();
    let content = editor.visible_content();

    let num_style = Style::default().fg(Color::Yellow).bg(t.bg);
    let sep_style = Style::default().fg(Color::DarkGray).bg(t.bg);
    let text_style = Style::default().fg(Color::LightCyan).bg(t.bg);
    let sel_style = Style::default().fg(Color::Black).bg(Color::LightCyan);

    let inner_width = inner.width as usize;
    let mut lines: Vec<Line> = Vec::with_capacity(editor.visible_lines);
    let bg_style = Style::default().bg(t.bg);

    for (vline, text) in &content {
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

        let text_spans = build_text_spans(
            &display_text,
            *vline,
            0, // scroll already handled by the column conversion
            display_sel,
            text_style,
            sel_style,
        );

        let mut spans = vec![
            Span::styled(format!("{:>5} ", line_num), num_style),
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
    let cursor_screen_x = inner.x as usize + LINE_NUM_WIDTH + cursor_display_col;
    let cursor_screen_y = inner.y as usize + editor.cursor_line.saturating_sub(editor.scroll_y);

    if cursor_screen_x < (inner.x + inner.width) as usize
        && cursor_screen_y < (inner.y + inner.height.saturating_sub(1)) as usize
    {
        frame.set_cursor_position((cursor_screen_x as u16, cursor_screen_y as u16));
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
