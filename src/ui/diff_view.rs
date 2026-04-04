use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::pr_diff::{DiffFileStatus, DiffTreeItem, DiffView, PrDiffPanel};
use crate::theme::{theme, Theme};

pub fn render(frame: &mut Frame, area: Rect, panel: &mut PrDiffPanel, is_active: bool) {
    let t = theme();

    let title = match &panel.view {
        DiffView::Tree { files, .. } => {
            let total = files.len();
            let (added, modified, deleted) = panel.status_counts();
            let pr_label = panel
                .pr_number
                .map(|n| format!(" PR #{}", n))
                .unwrap_or_default();
            let mut parts = Vec::new();
            if added > 0 {
                parts.push(format!("+{}", added));
            }
            if modified > 0 {
                parts.push(format!("~{}", modified));
            }
            if deleted > 0 {
                parts.push(format!("-{}", deleted));
            }
            if parts.is_empty() {
                format!(" Diff{} ({}) ", pr_label, total)
            } else {
                format!(" Diff{} ({}) ", pr_label, parts.join(" "))
            }
        }
        DiffView::Loading(msg) => format!(" {} ", msg),
        DiffView::Error(_) => " Diff ".to_string(),
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
        .title(Span::styled(title, title_style))
        .borders(Borders::ALL)
        .border_style(t.border_style(is_active))
        .style(t.bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    panel.visible_height = inner.height as usize;

    match &panel.view {
        DiffView::Tree {
            items,
            selected,
            scroll,
            ..
        } => {
            render_tree(frame, inner, items, *selected, *scroll, is_active, t);
        }
        DiffView::Loading(_) => {}
        DiffView::Error(msg) => {
            let line = Line::from(Span::styled(
                msg.as_str(),
                Style::default().fg(t.error_fg).bg(t.bg),
            ));
            frame.render_widget(Paragraph::new(line), inner);
        }
    }

    // Quick search overlay
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
        frame.render_widget(Paragraph::new(search_line), search_area);
    }
}

fn render_tree(
    frame: &mut Frame,
    area: Rect,
    items: &[DiffTreeItem],
    selected: usize,
    scroll: usize,
    is_active: bool,
    t: &Theme,
) {
    let visible_height = area.height as usize;
    let highlight = if is_active {
        t.highlight_style()
    } else {
        t.file_style()
    };

    let mut lines: Vec<Line> = Vec::with_capacity(visible_height);

    for (i, item) in items.iter().enumerate().skip(scroll).take(visible_height) {
        let is_sel = i == selected;
        let text_style = if is_sel { highlight } else { t.file_style() };

        match item {
            DiffTreeItem::Dir {
                name,
                expanded,
                depth,
                ..
            } => {
                let indent = "  ".repeat(*depth);
                let arrow = if *expanded {
                    "\u{25bc}" // ▼
                } else {
                    "\u{25b6}" // ▶
                };
                let arrow_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(t.border).bg(t.bg)
                };
                let dir_style = if is_sel {
                    highlight
                } else {
                    Style::default()
                        .fg(t.dir_fg)
                        .bg(t.bg)
                        .add_modifier(Modifier::BOLD)
                };

                lines.push(Line::from(vec![
                    Span::styled(format!(" {}{} ", indent, arrow), arrow_style),
                    Span::styled(format!("{}/", name), dir_style),
                ]));
            }
            DiffTreeItem::File {
                name,
                status,
                depth,
                ..
            } => {
                let indent = "  ".repeat(*depth);
                let marker = status.marker();
                let marker_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(status_color(*status, t)).bg(t.bg)
                };
                let name_style = if is_sel { highlight } else { t.file_style() };

                lines.push(Line::from(vec![
                    Span::styled(format!(" {} ", indent), text_style),
                    Span::styled(format!("{} ", marker), marker_style),
                    Span::styled(name.as_str(), name_style),
                ]));
            }
        }
    }

    let fill = t.bg_style();
    while lines.len() < visible_height {
        lines.push(Line::from(Span::styled(" ", fill)));
    }

    frame.render_widget(Paragraph::new(lines), area);
}

fn status_color(status: DiffFileStatus, t: &Theme) -> ratatui::style::Color {
    match status {
        DiffFileStatus::Added => t.git_added_fg,
        DiffFileStatus::Modified => t.git_modified_fg,
        DiffFileStatus::Deleted => t.git_deleted_fg,
        DiffFileStatus::Renamed => t.git_renamed_fg,
        DiffFileStatus::Copied => t.git_untracked_fg,
    }
}
