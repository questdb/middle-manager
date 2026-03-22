use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::theme::theme;
use crate::viewer::ViewerState;

const GUTTER_WIDTH: usize = 7; // "{:>6} " = 7 chars

pub fn render(frame: &mut Frame, area: Rect, viewer: &mut ViewerState) {
    let t = theme();

    let mode_label = if viewer.wrap_mode { "Wrap" } else { "NoWrap" };
    let col_info = if !viewer.wrap_mode && viewer.scroll_x > 0 {
        format!(" col {}+", viewer.scroll_x + 1)
    } else {
        String::new()
    };

    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [line {}/{}] [{}]{} ",
                viewer.path.to_string_lossy(),
                viewer.scroll_offset + 1,
                viewer.total_lines_display(),
                mode_label,
                col_info,
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    let viewport_height = inner.height.saturating_sub(1) as usize; // minus hint bar
    let inner_width = inner.width as usize;
    let text_width = inner_width.saturating_sub(GUTTER_WIDTH);
    let bg_style = Style::default().bg(t.bg);
    let num_style = Style::default().fg(t.viewer_line_num_fg).bg(t.bg);
    let text_style = Style::default().fg(t.viewer_text_fg).bg(t.bg);

    viewer.visible_lines = viewport_height;

    // Fetch file lines — in wrap mode fetch extra since one file line may span multiple rows.
    let fetch_count = if viewer.wrap_mode {
        viewport_height * 3
    } else {
        viewport_height
    };
    let visible = viewer.visible_line_iter(fetch_count);

    let mut lines: Vec<Line> = Vec::with_capacity(viewport_height);

    if viewer.wrap_mode {
        for (line_num, text) in &visible {
            let remaining_rows = viewport_height - lines.len();
            if remaining_rows == 0 {
                break;
            }
            // Only expand enough to fill remaining viewport rows + 1 (for partial last row).
            let max_cols = text_width * (remaining_rows + 1);
            let expanded = expand_tabs_scrolled(text, 0, max_cols);
            let chunks = wrap_line(&expanded, text_width);
            for (i, chunk) in chunks.into_iter().enumerate() {
                if lines.len() >= viewport_height {
                    break;
                }
                let gutter = if i == 0 {
                    format!("{:>6} ", line_num + 1)
                } else {
                    " ".repeat(GUTTER_WIDTH)
                };
                let mut spans = vec![
                    Span::styled(gutter, num_style),
                    Span::styled(chunk, text_style),
                ];
                let used: usize = spans.iter().map(|s| s.width()).sum();
                if used < inner_width {
                    spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
                }
                lines.push(Line::from(spans));
            }
        }
    } else {
        for (line_num, text) in &visible {
            let expanded = expand_tabs_scrolled(text, viewer.scroll_x, text_width);
            let mut spans = vec![
                Span::styled(format!("{:>6} ", line_num + 1), num_style),
                Span::styled(expanded, text_style),
            ];
            let used: usize = spans.iter().map(|s| s.width()).sum();
            if used < inner_width {
                spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
            }
            lines.push(Line::from(spans));
        }
    }

    // Pad remaining viewport rows with blank lines.
    while lines.len() < viewport_height {
        lines.push(Line::from(Span::styled(
            " ".repeat(inner_width),
            bg_style,
        )));
    }

    // Fill the entire area to prevent artifacts from previous frames.
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

    let hint = if viewer.wrap_mode {
        " Up/Dn/PgUp/PgDn | Home/End | g: Go to | w: Unwrap | F4/Tab: Hex | q/Esc: Close "
    } else {
        " Up/Dn/PgUp/PgDn | \u{2190}\u{2192}: HScroll | Home/End | g: Go to | w: Wrap | F4/Tab: Hex | q/Esc: Close "
    };
    let hint_area = Rect::new(
        area.x,
        area.y + area.height.saturating_sub(1),
        (UnicodeWidthStr::width(hint) as u16).min(area.width),
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

/// Expand tabs to spaces (4-space tab stops), strip control characters,
/// skip `scroll_x` display columns, then take up to `max_width` display columns.
/// For full expansion without scrolling or truncation, pass `(text, 0, usize::MAX)`.
fn expand_tabs_scrolled(text: &str, scroll_x: usize, max_width: usize) -> String {
    let mut result = String::new();
    let mut col: usize = 0; // absolute display column
    let mut visible: usize = 0; // columns emitted so far
    for ch in text.chars() {
        if ch == '\t' {
            let spaces = 4 - (col % 4);
            for _ in 0..spaces {
                if col >= scroll_x {
                    if visible >= max_width {
                        return result;
                    }
                    result.push(' ');
                    visible += 1;
                }
                col += 1;
            }
        } else if ch.is_control() {
            continue;
        } else {
            let w = UnicodeWidthChar::width(ch).unwrap_or(1);
            if col >= scroll_x {
                if visible + w > max_width {
                    break;
                }
                result.push(ch);
                visible += w;
            }
            col += w;
        }
    }
    result
}

/// Split an expanded (no tabs) line into chunks of at most `width` display columns.
fn wrap_line(text: &str, width: usize) -> Vec<String> {
    if width == 0 || text.is_empty() {
        return vec![text.to_string()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut col = 0;
    for ch in text.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(1);
        if col + w > width {
            chunks.push(current);
            current = String::new();
            col = 0;
        }
        current.push(ch);
        col += w;
    }
    chunks.push(current);
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- expand_tabs_scrolled ---

    #[test]
    fn no_tabs_no_scroll() {
        assert_eq!(expand_tabs_scrolled("hello", 0, 100), "hello");
    }

    #[test]
    fn tabs_at_start() {
        // "\thello" -> 4 spaces + hello
        assert_eq!(expand_tabs_scrolled("\thello", 0, 100), "    hello");
    }

    #[test]
    fn tab_alignment() {
        // "a\tb" -> "a" at col 0, tab expands to 3 spaces (next 4-col boundary), "b" at col 4
        assert_eq!(expand_tabs_scrolled("a\tb", 0, 100), "a   b");
    }

    #[test]
    fn consecutive_tabs() {
        assert_eq!(expand_tabs_scrolled("\t\t", 0, 100), "        ");
    }

    #[test]
    fn scroll_past_tab() {
        // "\thello" -> "    hello", scroll_x=4 -> "hello"
        assert_eq!(expand_tabs_scrolled("\thello", 4, 100), "hello");
    }

    #[test]
    fn scroll_into_tab() {
        // "\thello" -> "    hello", scroll_x=2 -> "  hello"
        assert_eq!(expand_tabs_scrolled("\thello", 2, 100), "  hello");
    }

    #[test]
    fn max_width_truncates() {
        assert_eq!(expand_tabs_scrolled("hello world", 0, 5), "hello");
    }

    #[test]
    fn scroll_and_truncate() {
        // scroll_x=6, max_width=5 -> "world"
        assert_eq!(expand_tabs_scrolled("hello world", 6, 5), "world");
    }

    #[test]
    fn control_chars_stripped() {
        // Only the ESC (\x1b) is a control char; "[31m" are regular printable characters.
        assert_eq!(expand_tabs_scrolled("a\x1b[31mb", 0, 100), "a[31mb");
        // \r is a control char
        assert_eq!(expand_tabs_scrolled("a\rb", 0, 100), "ab");
    }

    #[test]
    fn empty_string() {
        assert_eq!(expand_tabs_scrolled("", 0, 100), "");
    }

    #[test]
    fn scroll_past_end() {
        assert_eq!(expand_tabs_scrolled("hi", 10, 100), "");
    }

    #[test]
    fn zero_max_width() {
        assert_eq!(expand_tabs_scrolled("hello", 0, 0), "");
        assert_eq!(expand_tabs_scrolled("\thello", 0, 0), "");
    }

    #[test]
    fn scroll_into_tab_boundary() {
        // "ab\tcd" -> "ab  cd" (tab at col 2 expands to 2 spaces, next stop at col 4)
        // scroll_x=3: skip cols 0-2, show " cd" (space at col 3, c at col 4, d at col 5)
        assert_eq!(expand_tabs_scrolled("ab\tcd", 3, 100), " cd");
        // scroll_x=4: skip the entire tab, show "cd"
        assert_eq!(expand_tabs_scrolled("ab\tcd", 4, 100), "cd");
    }

    #[test]
    fn full_expansion_no_limit() {
        // Used in wrap mode: scroll_x=0, max_width=usize::MAX
        assert_eq!(
            expand_tabs_scrolled("\ta\tb", 0, usize::MAX),
            "    a   b"
        );
    }

    // --- wrap_line ---

    #[test]
    fn wrap_short_line() {
        assert_eq!(wrap_line("hello", 10), vec!["hello"]);
    }

    #[test]
    fn wrap_exact_width() {
        assert_eq!(wrap_line("12345", 5), vec!["12345"]);
    }

    #[test]
    fn wrap_splits_at_boundary() {
        assert_eq!(wrap_line("1234567890", 5), vec!["12345", "67890"]);
    }

    #[test]
    fn wrap_three_chunks() {
        assert_eq!(
            wrap_line("123456789012345", 5),
            vec!["12345", "67890", "12345"]
        );
    }

    #[test]
    fn wrap_empty_line() {
        assert_eq!(wrap_line("", 10), vec![""]);
    }

    #[test]
    fn wrap_single_char_width() {
        assert_eq!(wrap_line("abc", 1), vec!["a", "b", "c"]);
    }
}
