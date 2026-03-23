use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::terminal::TerminalPanel;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, tp: &TerminalPanel, is_active: bool) {
    let t = theme();

    let title_style = if is_active {
        Style::default()
            .fg(t.path_active_fg)
            .bg(t.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(t.path_inactive_fg).bg(t.bg)
    };

    let block = Block::default()
        .title(Span::styled(&tp.title, title_style))
        .borders(Borders::ALL)
        .border_style(t.border_style(is_active))
        .style(Style::default().bg(Color::Reset));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let screen = tp.screen();
    let mut lines: Vec<Line> = Vec::with_capacity(inner.height as usize);

    for row in 0..inner.height as u16 {
        let mut spans: Vec<Span> = Vec::new();
        let mut current_text = String::new();
        let mut current_style = Style::default();
        let mut first = true;

        for col in 0..inner.width as u16 {
            let cell = screen.cell(row, col);
            let style = match cell {
                Some(cell) => vt100_cell_style(cell),
                None => Style::default(),
            };

            if first {
                current_style = style;
                first = false;
            }

            if style != current_style {
                if !current_text.is_empty() {
                    spans.push(Span::styled(
                        std::mem::take(&mut current_text),
                        current_style,
                    ));
                }
                current_style = style;
            }

            // Append cell content directly into the accumulator (no per-cell allocation)
            match cell {
                Some(cell) => {
                    let contents = cell.contents();
                    if contents.is_empty() {
                        current_text.push(' ');
                    } else {
                        current_text.push_str(&contents);
                    }
                }
                None => current_text.push(' '),
            }
        }
        if !current_text.is_empty() {
            spans.push(Span::styled(current_text, current_style));
        }
        lines.push(Line::from(spans));
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

fn vt100_cell_style(cell: &vt100::Cell) -> Style {
    let mut style = Style::default();
    style = style.fg(vt100_color(cell.fgcolor()));
    style = style.bg(vt100_color(cell.bgcolor()));
    if cell.bold() {
        style = style.add_modifier(Modifier::BOLD);
    }
    if cell.italic() {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if cell.underline() {
        style = style.add_modifier(Modifier::UNDERLINED);
    }
    if cell.inverse() {
        style = style.add_modifier(Modifier::REVERSED);
    }
    style
}

fn vt100_color(color: vt100::Color) -> Color {
    match color {
        vt100::Color::Default => Color::Reset,
        vt100::Color::Idx(i) => Color::Indexed(i),
        vt100::Color::Rgb(r, g, b) => Color::Rgb(r, g, b),
    }
}
