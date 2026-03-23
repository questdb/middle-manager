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
                let pr_label = ci
                    .pr_number
                    .map(|n| format!(" PR #{}", n))
                    .unwrap_or_default();
                if failed > 0 {
                    format!(" CI{} ({} failed / {}) ", pr_label, failed, total)
                } else {
                    format!(" CI{} ({}) ", pr_label, total)
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
            render_tree(
                frame,
                inner,
                items,
                &TreeRenderCtx {
                    t,
                    is_active,
                    selected: *selected,
                    scroll: *scroll,
                    spinner_tick: ci.spinner_tick,
                },
            );
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

struct TreeRenderCtx<'a> {
    t: &'a Theme,
    is_active: bool,
    selected: usize,
    scroll: usize,
    spinner_tick: usize,
}

fn render_tree(frame: &mut Frame, area: Rect, items: &[TreeItem], ctx: &TreeRenderCtx) {
    let visible_height = area.height as usize;
    let highlight = if ctx.is_active {
        ctx.t.highlight_style()
    } else {
        ctx.t.file_style()
    };

    let mut lines: Vec<Line> = Vec::with_capacity(visible_height);

    for (i, item) in items
        .iter()
        .enumerate()
        .skip(ctx.scroll)
        .take(visible_height)
    {
        let is_sel = i == ctx.selected;

        let t = ctx.t;
        let text_style = if is_sel { highlight } else { t.file_style() };

        match item {
            TreeItem::Check {
                check,
                expanded,
                loading,
                ..
            } => {
                let arrow = if *expanded {
                    "\u{25bc}" // ▼
                } else {
                    "\u{25b6}" // ▶
                };
                let marker = check.status.marker();
                let marker_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(status_color(check.status, t)).bg(t.bg)
                };
                let arrow_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(t.border).bg(t.bg)
                };

                let mut spans = vec![
                    Span::styled(format!(" {} ", marker), marker_style),
                    Span::styled(format!("{} ", arrow), arrow_style),
                    Span::styled(check.display_name(), text_style),
                ];

                if *loading {
                    let braille = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
                    let c = braille[ctx.spinner_tick % braille.len()];
                    let loading_style = Style::default().fg(t.header_fg).bg(t.bg);
                    spans.push(Span::styled(format!("  {} loading...", c), loading_style));
                }

                lines.push(Line::from(spans));
            }
            TreeItem::Step { step, .. } => {
                let marker = step.status.marker();
                let marker_style = if is_sel {
                    highlight
                } else {
                    Style::default().fg(status_color(step.status, t)).bg(t.bg)
                };

                lines.push(Line::from(vec![
                    Span::styled("     ", text_style), // indent
                    Span::styled(format!("{} ", marker), marker_style),
                    Span::styled(&step.name, text_style),
                ]));
            }
        }
    }

    let fill = ctx.t.bg_style();
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
