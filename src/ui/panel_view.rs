use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};
use ratatui::Frame;

use crate::panel::sort::SortField;
use crate::panel::Panel;
use crate::theme::{theme, Theme};

pub fn render(frame: &mut Frame, area: Rect, panel: &mut Panel, is_active: bool) {
    let t = theme();

    let title_spans = build_panel_title(panel, t, is_active, area.width as usize);
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
    let git_statuses = panel
        .git_info
        .as_ref()
        .map(|gi| &gi.statuses);

    let rows: Vec<Row> = panel
        .entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let is_selected = panel.selected_indices.contains(&idx);
            let git_status = git_statuses
                .and_then(|s| s.get(&entry.name))
                .copied();

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
            cells.push(Cell::from(entry.formatted_size()).style(meta_style));
            cells.push(Cell::from(entry.formatted_date()).style(if is_selected {
                t.selected_style()
            } else {
                Style::default().fg(t.date_fg).bg(t.bg)
            }));
            cells.push(Cell::from(entry.formatted_permissions()).style(if is_selected {
                t.selected_style()
            } else {
                Style::default().fg(t.perm_fg).bg(t.bg)
            }));

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
        table = table.row_highlight_style(t.highlight_style());
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
fn build_panel_title(panel: &Panel, t: &Theme, is_active: bool, panel_width: usize) -> Vec<Span<'static>> {
    let mut spans = Vec::with_capacity(12);
    let title_style = if is_active {
        Style::default()
            .fg(t.path_active_fg)
            .bg(t.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(t.path_inactive_fg).bg(t.bg)
    };
    // Build the git suffix first to know how much space the path gets
    let git_suffix = build_git_suffix(panel, t);
    let git_len: usize = git_suffix.iter().map(|s| s.width()).sum();
    // 4 = " " prefix + " " suffix + 2 border chars
    let path_budget = panel_width.saturating_sub(git_len + 4);

    // Shorten path: replace home dir with ~, then truncate from left
    let path = shorten_path(&panel.current_dir.to_string_lossy(), path_budget);

    spans.push(Span::styled(" ", title_style));
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
