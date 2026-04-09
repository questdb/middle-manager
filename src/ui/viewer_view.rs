use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

use crate::theme::theme;
use crate::viewer::ViewerState;

pub fn render(frame: &mut Frame, area: Rect, viewer: &mut ViewerState) {
    let t = theme();

    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [line {}/{}] ",
                viewer.path.to_string_lossy(),
                viewer.scroll_offset + 1,
                viewer.total_lines_display(),
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    viewer.visible_lines = inner.height.saturating_sub(1) as usize;

    let visible = viewer.visible_line_iter();

    let inner_width = inner.width as usize;
    let bg_style = Style::default().bg(t.bg);
    let line_num_style = Style::default().fg(t.viewer_line_num_fg).bg(t.bg);
    let text_style = Style::default().fg(t.viewer_text_fg).bg(t.bg);
    let match_style = Style::default().fg(t.search_label_fg).bg(t.search_label_bg);

    let search_info = viewer.search.as_ref().and_then(|s| {
        if s.query.is_empty() {
            None
        } else {
            Some((s.query.clone(), s.case_sensitive))
        }
    });

    let line_num_width: usize = 7; // "{:>6} " = 7 chars
    let text_width = inner_width.saturating_sub(line_num_width);

    let mut lines: Vec<Line> = Vec::with_capacity(viewer.visible_lines);
    for (line_num, text) in visible.iter() {
        let num_span = Span::styled(format!("{:>6} ", line_num + 1), line_num_style);
        let display_text = sanitize_and_truncate(text, text_width);

        let mut spans = vec![num_span];
        if let Some((ref query, case_sensitive)) = search_info {
            spans.extend(highlight_matches(&display_text, query, case_sensitive, text_style, match_style));
        } else {
            spans.push(Span::styled(display_text, text_style));
        }

        let used: usize = spans.iter().map(|s| s.width()).sum();
        if used < inner_width {
            spans.push(Span::styled(" ".repeat(inner_width - used), bg_style));
        }

        lines.push(Line::from(spans));
    }

    while lines.len() < viewer.visible_lines {
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

    let hint =
        " Arrows/PgUp/Dn | Home/End | g:Goto | f:Find n/b:Next/Prev | F4/Tab:Hex | q/Esc:Close ";
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

/// Build spans for a line, highlighting all occurrences of `query`.
fn highlight_matches(
    text: &str,
    query: &str,
    case_sensitive: bool,
    normal_style: Style,
    match_style: Style,
) -> Vec<Span<'static>> {
    if text.is_empty() {
        return vec![Span::styled(String::new(), normal_style)];
    }

    let (search_text, search_query) = if case_sensitive {
        (text.to_owned(), query.to_owned())
    } else {
        (text.to_lowercase(), query.to_lowercase())
    };

    let mut spans = Vec::new();
    let mut last_end = 0;
    let mut pos = 0;

    while pos < search_text.len() {
        if let Some(offset) = search_text[pos..].find(&search_query) {
            let match_start = pos + offset;
            let match_end = match_start + search_query.len();

            // Clamp to original text length (lowering can change byte length)
            let ms = match_start.min(text.len());
            let me = match_end.min(text.len());
            let le = last_end.min(text.len());

            if ms > le {
                spans.push(Span::styled(text[le..ms].to_owned(), normal_style));
            }
            if me > ms {
                spans.push(Span::styled(text[ms..me].to_owned(), match_style));
            }

            last_end = match_end;
            pos = match_end;
        } else {
            break;
        }
    }

    let le = last_end.min(text.len());
    if le < text.len() {
        spans.push(Span::styled(text[le..].to_owned(), normal_style));
    }

    if spans.is_empty() {
        spans.push(Span::styled(text.to_owned(), normal_style));
    }

    spans
}

/// Expand tabs to spaces, strip control characters, and truncate to `max_width` display cells.
fn sanitize_and_truncate(text: &str, max_width: usize) -> String {
    let mut result = String::new();
    let mut width = 0;
    for ch in text.chars() {
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
