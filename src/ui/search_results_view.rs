use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::file_search::{SearchItem, SearchState};
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut SearchState, is_active: bool) {
    let t = theme();

    let status = if state.searching {
        format!(
            " Searching: \"{}\" — {} matches in {} files... ",
            state.query,
            state.total_matches,
            state.files.len()
        )
    } else {
        format!(
            " Results: \"{}\" — {} matches in {} files ",
            state.query,
            state.total_matches,
            state.files.len()
        )
    };

    let title_style = if is_active {
        Style::default()
            .fg(t.path_active_fg)
            .bg(t.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(t.path_inactive_fg).bg(t.bg)
    };

    let block = Block::default()
        .title(Span::styled(status, title_style))
        .borders(Borders::ALL)
        .border_style(t.border_style(is_active))
        .style(t.bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let visible_height = inner.height as usize;
    if visible_height == 0 {
        return;
    }

    state.scroll_to_selected(visible_height);

    let items = state.visible_items();
    let highlight = if is_active {
        t.highlight_style()
    } else {
        t.file_style()
    };

    let file_style = Style::default()
        .fg(t.dir_fg)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);
    let match_count_style = Style::default().fg(t.size_fg).bg(t.bg);
    let line_num_style = Style::default().fg(t.viewer_line_num_fg).bg(t.bg);
    let match_text_style = Style::default().fg(t.file_fg).bg(t.bg);
    let search_match_style = Style::default()
        .fg(t.header_fg)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);

    let mut lines: Vec<Line> = Vec::with_capacity(visible_height);
    let query_lower = state.query.to_lowercase();

    for (idx, item) in items
        .iter()
        .enumerate()
        .skip(state.scroll)
        .take(visible_height)
    {
        let is_sel = idx == state.selected;

        match item {
            SearchItem::File(fi) => {
                let file = &state.files[*fi];
                let arrow = if file.expanded {
                    "\u{25bc}"
                } else {
                    "\u{25b6}"
                };
                let count = format!(" ({})", file.matches.len());

                if is_sel {
                    let text = format!(" {} {} {}", arrow, file.rel_path, count);
                    lines.push(Line::from(Span::styled(text, highlight)));
                } else {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!(" {} ", arrow),
                            Style::default().fg(t.border).bg(t.bg),
                        ),
                        Span::styled(file.rel_path.clone(), file_style),
                        Span::styled(count, match_count_style),
                    ]));
                }
            }
            SearchItem::Match(fi, mi) => {
                let file = &state.files[*fi];
                let m = &file.matches[*mi];

                if is_sel {
                    let text = format!("   {:>5}: {}", m.line_number, m.text);
                    lines.push(Line::from(Span::styled(text, highlight)));
                } else {
                    // Highlight the matching text within the line
                    let spans = highlight_match(
                        &m.text,
                        &query_lower,
                        m.line_number,
                        line_num_style,
                        match_text_style,
                        search_match_style,
                    );
                    lines.push(Line::from(spans));
                }
            }
        }
    }

    // Fill remaining space
    let bg_style = Style::default().bg(t.bg);
    while lines.len() < visible_height {
        lines.push(Line::from(Span::styled(" ", bg_style)));
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

/// Build spans for a match line with the query highlighted.
fn highlight_match<'a>(
    text: &str,
    query_lower: &str,
    line_number: u64,
    line_num_style: Style,
    text_style: Style,
    match_style: Style,
) -> Vec<Span<'a>> {
    let mut spans = vec![Span::styled(
        format!("   {:>5}: ", line_number),
        line_num_style,
    )];

    if query_lower.is_empty() {
        spans.push(Span::styled(text.to_string(), text_style));
        return spans;
    }

    let text_lower = text.to_lowercase();
    let mut pos = 0;

    // Find and highlight all occurrences of the query
    while let Some(start) = text_lower[pos..].find(query_lower) {
        let abs_start = pos + start;
        let abs_end = abs_start + query_lower.len();

        // Text before the match
        if abs_start > pos {
            spans.push(Span::styled(text[pos..abs_start].to_string(), text_style));
        }

        // The match itself (use original case from the text)
        spans.push(Span::styled(
            text[abs_start..abs_end].to_string(),
            match_style,
        ));

        pos = abs_end;
    }

    // Remaining text after last match
    if pos < text.len() {
        spans.push(Span::styled(text[pos..].to_string(), text_style));
    }

    spans
}
