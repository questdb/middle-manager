use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Row, Table};
use ratatui::Frame;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::app::{FuzzySearchState, GotoPathState};
use crate::panel::github::PrCheckStatus;
use crate::panel::sort::SortField;
use crate::panel::Panel;
use crate::theme::{theme, Theme};

pub fn render_with_overlays(
    frame: &mut Frame,
    area: Rect,
    panel: &mut Panel,
    is_active: bool,
    goto_path: Option<&GotoPathState>,
    fuzzy_search: Option<&FuzzySearchState>,
) {
    // Determine which overlay is active (at most one at a time)
    let has_overlay = goto_path.is_some() || fuzzy_search.is_some();

    if has_overlay {
        let [input_area, panel_area] =
            Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(area);

        render(frame, panel_area, panel, is_active);

        if let Some(state) = goto_path {
            render_goto_path_input(frame, input_area, state);
            // Render completions as an overlay directly below input (full border)
            if state.completions.len() > 1 {
                let comp_rows = (state.completions.len() as u16).min(8);
                let border_h = comp_rows + 2; // +2 for top/bottom border
                let available_h = panel_area.height;
                let total_h = border_h.min(available_h);
                let comp_area = Rect::new(
                    area.x,
                    input_area.y + input_area.height,
                    area.width,
                    total_h,
                );
                render_completions(frame, comp_area, state);
            }
        } else if let Some(state) = fuzzy_search {
            render_fuzzy_input(frame, input_area, state);
            if !state.results.is_empty() {
                let result_rows = (state.results.len() as u16).min(8);
                let border_h = result_rows + 2;
                let available_h = panel_area.height;
                let total_h = border_h.min(available_h);
                let comp_area = Rect::new(
                    area.x,
                    input_area.y + input_area.height,
                    area.width,
                    total_h,
                );
                render_fuzzy_results(frame, comp_area, state);
            }
        }
    } else {
        render(frame, area, panel, is_active);
    }
}

/// Compute the horizontal-scroll byte offset and the cursor's display-column
/// offset within the visible window, given `text`, a byte cursor position,
/// and the `available` number of terminal columns.
///
/// Returns `(visible_start_byte, cursor_col_in_view)`. The cursor is kept
/// inside the viewport, leaving one column for the caret itself.
fn compute_scroll(text: &str, cursor: usize, available: usize) -> (usize, usize) {
    let before = &text[..cursor];
    let before_w = UnicodeWidthStr::width(before);

    if available == 0 {
        return (cursor, 0);
    }

    // Need the cursor to fit: width(before[visible_start..]) + 1 <= available.
    if before_w < available {
        return (0, before_w);
    }

    let need_to_trim = before_w + 1 - available;
    let mut trimmed = 0usize;
    let mut visible_start = cursor;
    for (i, ch) in before.char_indices() {
        if trimmed >= need_to_trim {
            visible_start = i;
            break;
        }
        trimmed += UnicodeWidthChar::width(ch).unwrap_or(0);
        visible_start = i + ch.len_utf8();
    }
    let visible_before_w = UnicodeWidthStr::width(&text[visible_start..cursor]);
    (visible_start, visible_before_w)
}

/// Shared scrollable text-input bar with prompt, selection, and cursor positioning.
/// `suffix_spans` are appended after the input text (e.g. result count).
fn render_input_bar(
    frame: &mut Frame,
    area: Rect,
    input: &crate::text_input::TextInput,
    prompt: &str,
    suffix_spans: Vec<Span<'_>>,
) {
    let t = theme();
    let prompt_style = Style::default()
        .fg(t.dialog_title_fg)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);
    let input_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg);
    let sel_style = t.input_selection_style();

    let text = &input.text;
    let cursor = input.cursor;
    let after = &text[cursor..];

    // Widths and offsets are computed in terminal columns (display width),
    // not bytes or chars, so multi-byte chars (e.g. `●`, 3 bytes / 1 col)
    // and wide chars (e.g. CJK / emoji, 1 char / 2 cols) both position the
    // cursor correctly.
    let prompt_w = UnicodeWidthStr::width(prompt);
    let available = (area.width as usize).saturating_sub(prompt_w);
    let (visible_start, visible_before_w) = compute_scroll(text, cursor, available);

    let mut spans = vec![Span::styled(prompt, prompt_style)];

    if let Some((sel_start, sel_end)) = input.selection_range() {
        if sel_start != sel_end {
            let vis_text = &text[visible_start..];
            let vis_sel_start = sel_start.saturating_sub(visible_start).min(vis_text.len());
            let vis_sel_end = sel_end.saturating_sub(visible_start).min(vis_text.len());

            spans.push(Span::styled(
                vis_text[..vis_sel_start].to_string(),
                input_style,
            ));
            spans.push(Span::styled(
                vis_text[vis_sel_start..vis_sel_end].to_string(),
                sel_style,
            ));
            spans.push(Span::styled(
                vis_text[vis_sel_end..].to_string(),
                input_style,
            ));
            spans.extend(suffix_spans);

            frame.render_widget(Clear, area);
            frame.render_widget(
                ratatui::widgets::Paragraph::new(Line::from(spans))
                    .style(Style::default().bg(t.dialog_input_bg)),
                area,
            );

            let cursor_x = area.x + prompt_w as u16 + visible_before_w as u16;
            crate::ui::set_cursor(cursor_x, area.y);
            return;
        }
    }

    let visible_before = &text[visible_start..cursor];
    spans.push(Span::styled(visible_before.to_string(), input_style));
    if !after.is_empty() {
        spans.push(Span::styled(after.to_string(), input_style));
    }
    spans.extend(suffix_spans);

    frame.render_widget(Clear, area);
    frame.render_widget(
        ratatui::widgets::Paragraph::new(Line::from(spans))
            .style(Style::default().bg(t.dialog_input_bg)),
        area,
    );

    let cursor_x = area.x + prompt_w as u16 + visible_before_w as u16;
    crate::ui::set_cursor(cursor_x, area.y);
}

fn render_goto_path_input(frame: &mut Frame, area: Rect, state: &GotoPathState) {
    render_input_bar(frame, area, &state.input, " Go: ", vec![]);
}

fn render_fuzzy_input(frame: &mut Frame, area: Rect, state: &FuzzySearchState) {
    let suffix = if !state.input.text.is_empty() {
        let t = theme();
        vec![Span::styled(
            format!(" {}", state.results.len()),
            Style::default().fg(t.dialog_title_fg).bg(t.dialog_input_bg),
        )]
    } else {
        vec![]
    };
    render_input_bar(frame, area, &state.input, " Find: ", suffix);
}

/// Shared dropdown-list rendering (bordered, scrollable, highlighted selection).
fn render_dropdown_list<F>(
    frame: &mut Frame,
    area: Rect,
    total: usize,
    selected: usize,
    scroll_offset: usize,
    render_item: F,
) where
    F: Fn(usize) -> Option<(String, Style)>,
{
    let t = theme();
    let border_style = Style::default().fg(t.dialog_border_fg).bg(t.dialog_bg);

    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .style(Style::default().bg(t.dialog_bg));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let max_visible = inner.height as usize;
    let scroll = if selected >= scroll_offset + max_visible {
        selected - max_visible + 1
    } else {
        scroll_offset
    };

    for i in 0..max_visible {
        let idx = scroll + i;
        let y = inner.y + i as u16;
        if y >= inner.y + inner.height || idx >= total {
            break;
        }
        let row_area = Rect::new(inner.x, y, inner.width, 1);
        if let Some((text, style)) = render_item(idx) {
            let line = Line::from(Span::styled(text, style));
            frame.render_widget(
                ratatui::widgets::Paragraph::new(line).style(style),
                row_area,
            );
        }
    }
}

fn render_completions(frame: &mut Frame, area: Rect, state: &GotoPathState) {
    let t = theme();
    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let highlight = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);
    let selected = state.comp_index.unwrap_or(0);
    let scroll = if selected >= 8 { selected - 7 } else { 0 };

    render_dropdown_list(
        frame,
        area,
        state.completions.len(),
        selected,
        scroll,
        |idx| {
            let style = if state.comp_index == Some(idx) {
                highlight
            } else {
                normal
            };
            Some((format!(" /{}", state.completions[idx]), style))
        },
    );
}

fn render_fuzzy_results(frame: &mut Frame, area: Rect, state: &FuzzySearchState) {
    let t = theme();
    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let highlight = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);

    render_dropdown_list(frame, area, state.results.len(), state.selected, 0, |idx| {
        let &(path_idx, _score) = state.results.get(idx)?;
        let style = if state.selected == idx {
            highlight
        } else {
            normal
        };
        Some((format!(" {}", state.all_paths[path_idx]), style))
    });
}

pub fn render(frame: &mut Frame, area: Rect, panel: &mut Panel, is_active: bool) {
    let t = theme();

    // Record usable data rows (minus 2 border rows and 1 header row) so
    // PageUp/PageDown can step by the actual viewport height.
    panel.visible_rows = (area.height as usize).saturating_sub(3);

    let title_spans = build_panel_title(panel, &t, is_active, area.width as usize);
    let block = Block::default()
        .title(Line::from(title_spans))
        .borders(Borders::ALL)
        .border_style(t.border_style(is_active))
        .style(t.bg_style());

    // Build header row with sort indicator
    let mut header_cells: Vec<Span> = Vec::with_capacity(5);
    if panel.git_info.is_some() {
        header_cells.push(Span::styled("G", t.header_style()));
    }
    header_cells.push(header_cell(
        "Name",
        panel.sort_field,
        SortField::Name,
        panel.sort_ascending,
    ));
    header_cells.push(header_cell(
        "Size",
        panel.sort_field,
        SortField::Size,
        panel.sort_ascending,
    ));
    header_cells.push(header_cell(
        "Date",
        panel.sort_field,
        SortField::Date,
        panel.sort_ascending,
    ));
    header_cells.push(Span::styled("Perm", t.header_style()));
    let header = Row::new(header_cells).style(t.bg_style()).height(1);

    // Build rows
    let has_git = panel.git_info.is_some();
    let git_statuses = panel.git_info.as_ref().map(|gi| &gi.statuses);

    let rows: Vec<Row> = panel
        .entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let is_selected = panel.selected_indices.contains(&idx);
            let git_status = git_statuses.and_then(|s| s.get(&entry.name)).copied();

            let name_style = if is_selected {
                t.selected_style()
            } else if let Some(status) = git_status {
                if entry.name != ".." {
                    Style::default().fg(t.git_status_color(status)).bg(t.bg)
                } else {
                    t.dir_style()
                }
            } else if entry.is_dir {
                t.dir_style()
            } else if entry.is_symlink {
                t.symlink_style()
            } else if entry.permissions & 0o111 != 0 {
                t.exec_style()
            } else {
                t.file_style()
            };

            let meta_style = if is_selected {
                t.selected_style()
            } else {
                Style::default().fg(t.size_fg).bg(t.bg)
            };

            let name_display = if entry.is_dir && entry.name != ".." {
                format!("/{}", entry.name)
            } else if entry.is_symlink && entry.is_dir {
                format!("~{}", entry.name)
            } else {
                entry.name.clone()
            };

            let mut cells = Vec::with_capacity(5);

            // Git status column (only when in a git repo)
            if has_git {
                let (marker, marker_style) = if let Some(status) = git_status {
                    (
                        status.marker(),
                        Style::default().fg(t.git_status_color(status)).bg(t.bg),
                    )
                } else {
                    (" ", Style::default().bg(t.bg))
                };
                cells.push(Cell::from(marker).style(marker_style));
            }

            cells.push(Cell::from(name_display).style(name_style));
            cells.push(Cell::from(panel.display_size(entry)).style(meta_style));
            cells.push(Cell::from(entry.formatted_date()).style(if is_selected {
                t.selected_style()
            } else {
                Style::default().fg(t.date_fg).bg(t.bg)
            }));
            cells.push(
                Cell::from(entry.formatted_permissions()).style(if is_selected {
                    t.selected_style()
                } else {
                    Style::default().fg(t.perm_fg).bg(t.bg)
                }),
            );

            Row::new(cells)
        })
        .collect();

    let widths: Vec<Constraint> = if has_git {
        vec![
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(9),
        ]
    } else {
        vec![
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(9),
        ]
    };

    let mut table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .highlight_symbol("  ");

    if is_active {
        let cursor_idx = panel.table_state.selected().unwrap_or(0);
        let cursor_is_selected = panel.selected_indices.contains(&cursor_idx);
        if cursor_is_selected {
            table = table.row_highlight_style(t.selected_highlight_style());
        } else {
            table = table.row_highlight_style(t.highlight_style());
        }
    }

    frame.render_stateful_widget(table, area, &mut panel.table_state);

    // Render quick search overlay if active
    if let Some(ref query) = panel.quick_search {
        let search_line = Line::from(vec![
            Span::styled(
                " Search: ",
                Style::default().fg(t.search_label_fg).bg(t.search_label_bg),
            ),
            Span::styled(
                query.as_str(),
                Style::default()
                    .fg(t.search_text_fg)
                    .bg(t.bg)
                    .add_modifier(Modifier::BOLD),
            ),
        ]);
        let search_area = Rect::new(
            area.x + 1,
            area.y + area.height.saturating_sub(2),
            area.width.saturating_sub(2).min(30),
            1,
        );
        frame.render_widget(ratatui::widgets::Paragraph::new(search_line), search_area);
    }

    // Render error if present
    if let Some(ref error) = panel.error {
        let error_line = Line::from(Span::styled(
            error.as_str(),
            Style::default()
                .fg(t.error_fg)
                .bg(t.bg)
                .add_modifier(Modifier::BOLD),
        ));
        let error_area = Rect::new(
            area.x + 1,
            area.y + area.height / 2,
            area.width.saturating_sub(2),
            1,
        );
        frame.render_widget(ratatui::widgets::Paragraph::new(error_line), error_area);
    }
}

/// Build the panel border title: " /path  ⎇ branch  ● 6 ? 1 "
fn build_panel_title(
    panel: &Panel,
    t: &Theme,
    is_active: bool,
    panel_width: usize,
) -> Vec<Span<'static>> {
    let mut spans = Vec::with_capacity(12);
    let title_style = if is_active {
        Style::default()
            .fg(t.path_active_fg)
            .bg(t.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(t.path_inactive_fg).bg(t.bg)
    };
    // SFTP prefix if this is a remote panel
    let sftp_prefix = panel.source.label();
    let sftp_prefix_len = sftp_prefix.as_ref().map(|s| s.len() + 2).unwrap_or(0); // " SFTP: user@host | "

    // Build the git suffix first to know how much space the path gets
    let git_suffix = if panel.source.is_remote() {
        vec![] // No git info for SFTP panels
    } else {
        build_git_suffix(panel, t)
    };
    let git_len: usize = git_suffix.iter().map(|s| s.width()).sum();
    // 4 = " " prefix + " " suffix + 2 border chars
    let path_budget = panel_width.saturating_sub(git_len + sftp_prefix_len + 4);

    // Shorten path: replace home dir with ~, then truncate from left
    let path = shorten_path(&panel.current_dir.to_string_lossy(), path_budget);

    spans.push(Span::styled(" ", title_style));
    if let Some(label) = sftp_prefix {
        let sftp_style = Style::default()
            .fg(t.git_branch_fg)
            .bg(t.bg)
            .add_modifier(Modifier::BOLD);
        spans.push(Span::styled(label, sftp_style));
        spans.push(Span::styled(" ", title_style));
    }
    spans.push(Span::styled(path, title_style));

    spans.extend(git_suffix);
    spans.push(Span::styled(" ", title_style));
    spans
}

/// Build the git portion of the title (branch + ahead/behind + status counts).
fn build_git_suffix(panel: &Panel, t: &Theme) -> Vec<Span<'static>> {
    let gi = match &panel.git_info {
        Some(gi) => gi,
        None => return vec![],
    };

    let sep = Style::default().fg(t.border).bg(t.bg);
    let branch_style = Style::default()
        .fg(t.git_branch_fg)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = Vec::with_capacity(10);

    // Branch with ⎇ glyph (U+2387)
    spans.push(Span::styled("  \u{2387} ", sep));
    spans.push(Span::styled(gi.branch.clone(), branch_style));

    // Ahead/behind
    if gi.ahead > 0 || gi.behind > 0 {
        spans.push(Span::styled("  ", sep));
        spans.push(Span::styled(
            format!("\u{2191}{}", gi.ahead),
            Style::default().fg(t.git_added_fg).bg(t.bg),
        ));
        spans.push(Span::styled(" ", sep));
        spans.push(Span::styled(
            format!("\u{2193}{}", gi.behind),
            Style::default().fg(t.git_deleted_fg).bg(t.bg),
        ));
    }

    // File status summary
    let counts = [
        ("\u{25CF}", gi.total_modified, t.git_modified_fg),
        ("+", gi.total_added, t.git_added_fg),
        ("-", gi.total_deleted, t.git_deleted_fg),
        ("\u{2192}", gi.total_renamed, t.git_renamed_fg),
        ("!", gi.total_conflict, t.git_conflict_fg),
        ("?", gi.total_untracked, t.git_untracked_fg),
    ];

    let has_any = counts.iter().any(|(_, n, _)| *n > 0);
    if has_any {
        spans.push(Span::styled("  ", sep));
        let mut first = true;
        for (marker, count, color) in &counts {
            if *count > 0 {
                if !first {
                    spans.push(Span::styled(" ", sep));
                }
                spans.push(Span::styled(
                    format!("{} {}", marker, count),
                    Style::default().fg(*color).bg(t.bg),
                ));
                first = false;
            }
        }
    }

    // PR status
    if let Some(ref pr) = gi.pr {
        spans.push(Span::styled("  ", sep));

        if pr.state == "MERGED" {
            // Merged PR — show ● in magenta, ignore check status
            spans.push(Span::styled(
                format!("PR #{} \u{25cf}", pr.number),
                Style::default().fg(ratatui::style::Color::Magenta).bg(t.bg),
            ));
        } else if pr.state == "CLOSED" {
            // Closed without merge
            spans.push(Span::styled(
                format!("PR #{} \u{2718}", pr.number),
                Style::default().fg(t.git_deleted_fg).bg(t.bg),
            ));
        } else {
            // Open PR — show check status
            let (pr_color, check_marker) = match pr.checks {
                PrCheckStatus::Pass => (t.git_added_fg, pr.checks.marker()),
                PrCheckStatus::Fail => (t.git_deleted_fg, pr.checks.marker()),
                PrCheckStatus::Pending => (ratatui::style::Color::Yellow, pr.checks.marker()),
                PrCheckStatus::None => (t.git_branch_fg, ""),
            };
            spans.push(Span::styled(
                format!("PR #{}", pr.number),
                Style::default().fg(t.git_branch_fg).bg(t.bg),
            ));
            if !check_marker.is_empty() {
                spans.push(Span::styled(
                    format!(" {}", check_marker),
                    Style::default().fg(pr_color).bg(t.bg),
                ));
            }
        }
    }

    spans
}

/// Shorten a path: replace home dir with ~, then truncate from the left if too long.
fn shorten_path(path: &str, max_width: usize) -> String {
    // Replace home directory with ~
    let shortened = if let Some(home) = std::env::var_os("HOME") {
        let home = home.to_string_lossy();
        if path.starts_with(home.as_ref()) {
            format!("~{}", &path[home.len()..])
        } else {
            path.to_string()
        }
    } else {
        path.to_string()
    };

    if shortened.len() <= max_width {
        shortened
    } else if max_width > 3 {
        format!("...{}", &shortened[shortened.len() - (max_width - 3)..])
    } else {
        shortened[..max_width].to_string()
    }
}

fn header_cell(
    name: &str,
    current_sort: SortField,
    this_field: SortField,
    ascending: bool,
) -> Span<'static> {
    let t = theme();
    let style = t.header_style();

    if current_sort == this_field {
        let arrow = if ascending { " \u{25b2}" } else { " \u{25bc}" };
        Span::styled(format!("{}{}", name, arrow), style)
    } else {
        Span::styled(name.to_string(), style)
    }
}

#[cfg(test)]
mod compute_scroll_tests {
    use super::compute_scroll;

    // When text fits entirely, no scrolling and the cursor column equals
    // the display width of the text before the cursor.
    #[test]
    fn ascii_fits_no_scroll() {
        let (start, col) = compute_scroll("hello", 5, 20);
        assert_eq!(start, 0);
        assert_eq!(col, 5);
    }

    #[test]
    fn ascii_cursor_mid_string() {
        let (start, col) = compute_scroll("hello", 2, 20);
        assert_eq!(start, 0);
        assert_eq!(col, 2);
    }

    // Regression for the reported bug: bullet `●` is 3 bytes, 1 column.
    // Cursor col must be 1 per bullet, not 3.
    #[test]
    fn multibyte_narrow_char_bullet() {
        let text = "●";
        let cursor = text.len(); // 3 bytes
        let (start, col) = compute_scroll(text, cursor, 20);
        assert_eq!(start, 0);
        assert_eq!(col, 1, "cursor after `●` must be at column 1, not 3");
    }

    #[test]
    fn multibyte_narrow_run() {
        let text = "●●●●●";
        let cursor = text.len(); // 15 bytes, 5 cols
        let (start, col) = compute_scroll(text, cursor, 20);
        assert_eq!(start, 0);
        assert_eq!(col, 5);
    }

    #[test]
    fn cursor_between_multibyte_chars() {
        // cursor after the second `●` (6 bytes in)
        let text = "●●●";
        let cursor = 6;
        let (_, col) = compute_scroll(text, cursor, 20);
        assert_eq!(col, 2);
    }

    // Wide chars (e.g. CJK) are 2 columns each, even though `.chars()` is 1.
    #[test]
    fn wide_cjk_char_takes_two_columns() {
        let text = "中文";
        let cursor = text.len(); // 6 bytes, 4 cols
        let (start, col) = compute_scroll(text, cursor, 20);
        assert_eq!(start, 0);
        assert_eq!(col, 4);
    }

    // Mixed: ASCII + multi-byte narrow + wide
    #[test]
    fn mixed_ascii_bullet_cjk() {
        // "a●中" = 1 + 3 + 3 = 7 bytes; widths 1 + 1 + 2 = 4 cols
        let text = "a●中";
        assert_eq!(text.len(), 7);
        let (_, col) = compute_scroll(text, text.len(), 20);
        assert_eq!(col, 4);
    }

    // Scrolling: text overflows available width, so visible_start moves
    // forward and the cursor stays on the last visible column.
    #[test]
    fn scrolls_when_ascii_overflows() {
        // 10 chars, width available 5 → last 4 + caret
        let text = "abcdefghij";
        let (start, col) = compute_scroll(text, text.len(), 5);
        assert!(start > 0, "must scroll when text overflows");
        // Cursor is inside viewport, leaving 1 col for caret.
        assert!(col < 5, "cursor col {} must be < available 5", col);
    }

    #[test]
    fn scrolls_with_multibyte_chars() {
        // 10 bullets (10 cols wide, 30 bytes). available = 5.
        let text = "●●●●●●●●●●";
        let (start, col) = compute_scroll(text, text.len(), 5);
        assert!(start > 0);
        assert_eq!(start % 3, 0, "scroll must land on a char boundary");
        assert!(col < 5);
    }

    // Returned byte offset must always be on a char boundary so slicing is safe.
    #[test]
    fn visible_start_is_always_char_boundary() {
        let text = "●abc●def●";
        let cursor = text.len();
        for available in 1..=12 {
            let (start, _) = compute_scroll(text, cursor, available);
            assert!(
                text.is_char_boundary(start),
                "visible_start {} not a char boundary for available={} text={:?}",
                start,
                available,
                text,
            );
        }
    }

    #[test]
    fn cursor_at_zero() {
        let (start, col) = compute_scroll("●●●", 0, 10);
        assert_eq!(start, 0);
        assert_eq!(col, 0);
    }

    // available = 0 is a degenerate case; ensure we don't panic.
    #[test]
    fn zero_available_does_not_panic() {
        let (_start, col) = compute_scroll("abc", 2, 0);
        assert_eq!(col, 0);
    }

    // Empty text — cursor is always at column 0.
    #[test]
    fn empty_text() {
        let (start, col) = compute_scroll("", 0, 10);
        assert_eq!(start, 0);
        assert_eq!(col, 0);
    }
}
