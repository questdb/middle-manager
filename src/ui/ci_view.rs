use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::ci::{CiPanel, CiStatus, CiView, TreeItem};
use crate::theme::{theme, Theme};

pub fn render(frame: &mut Frame, area: Rect, ci: &mut CiPanel, is_active: bool) {
    let t = theme();

    let title = if let Some(ref dl) = ci.download {
        format!(" {} ", dl.progress_text(ci.spinner_tick))
    } else {
        match &ci.view {
            CiView::Tree { checks, .. } => {
                let total = checks.len();
                let failed = checks
                    .iter()
                    .filter(|c| c.status == CiStatus::Failure)
                    .count();
                if failed > 0 {
                    format!(" CI Checks ({} failed / {}) ", failed, total)
                } else {
                    format!(" CI Checks ({}) ", total)
                }
            }
            CiView::Loading(msg) => format!(" {} ", msg),
            CiView::Error(_) => " CI ".to_string(),
        }
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

    ci.visible_height = inner.height as usize;

    match &ci.view {
        CiView::Tree {
            items,
            selected,
            scroll,
            ..
        } => {
            render_tree(frame, inner, t, is_active, items, *selected, *scroll);
        }
        CiView::Loading(_) => {}
        CiView::Error(msg) => {
            let line = Line::from(Span::styled(
                msg.as_str(),
                Style::default().fg(t.error_fg).bg(t.bg),
            ));
            frame.render_widget(Paragraph::new(line), inner);
        }
    }
}

fn render_tree(
    frame: &mut Frame,
    area: Rect,
    t: &Theme,
    is_active: bool,
    items: &[TreeItem],
    selected: usize,
    scroll: usize,
) {
    let visible_height = area.height as usize;
    let highlight = if is_active {
        t.highlight_style()
    } else {
        t.file_style()
    };

    let mut lines: Vec<Line> = Vec::with_capacity(visible_height);

    for i in scroll..items.len().min(scroll + visible_height) {
        let is_sel = i == selected;
        let item = &items[i];

        let text_style = if is_sel { highlight } else { t.file_style() };

        match item {
            TreeItem::Check {
                check, expanded, ..
            } => {
                let arrow = if *expanded { "\u{25bc}" } else { "\u{25b6}" }; // ▼ / ▶
                let marker = check.status.marker();
                let marker_style = if is_sel {
                    highlight
                } else {
                    Style::default()
                        .fg(status_color(check.status, t))
                        .bg(t.bg)
                };
                let arrow_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(t.border).bg(t.bg)
                };

                lines.push(Line::from(vec![
                    Span::styled(format!(" {} ", marker), marker_style),
                    Span::styled(format!("{} ", arrow), arrow_style),
                    Span::styled(check.display_name(), text_style),
                ]));
            }
            TreeItem::Step { step, .. } => {
                let marker = step.status.marker();
                let marker_style = if is_sel {
                    highlight
                } else {
                    Style::default()
                        .fg(status_color(step.status, t))
                        .bg(t.bg)
                };

                lines.push(Line::from(vec![
                    Span::styled("     ", text_style), // indent
                    Span::styled(format!("{} ", marker), marker_style),
                    Span::styled(&step.name, text_style),
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

fn status_color(status: CiStatus, t: &Theme) -> ratatui::style::Color {
    match status {
        CiStatus::Success => t.git_added_fg,
        CiStatus::Failure | CiStatus::Cancelled => t.git_deleted_fg,
        CiStatus::Pending => t.git_modified_fg,
        CiStatus::Skipped => t.git_untracked_fg,
    }
}
