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
    let width = inner.width as u16;
    let mut lines: Vec<Line> = Vec::with_capacity(inner.height as usize);
    // Pre-allocate reusable buffers
    let mut current_text = String::with_capacity(width as usize * 4);
    let mut spans: Vec<Span> = Vec::with_capacity(width as usize);

    for row in 0..inner.height as u16 {
        spans.clear();
        current_text.clear();
        let mut current_style = Style::default();
        let mut first = true;

        for col in 0..width {
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
                    // Clone text into span, keeping current_text's buffer for reuse
                    spans.push(Span::styled(current_text.clone(), current_style));
                    current_text.clear();
                }
                current_style = style;
            }

            match cell {
                Some(cell) if cell.has_contents() => {
                    current_text.push_str(cell.contents());
                }
                _ => current_text.push(' '),
            }
        }
        if !current_text.is_empty() {
            spans.push(Span::styled(current_text.clone(), current_style));
            current_text.clear();
        }
        lines.push(Line::from(spans.clone()));
        spans.clear();
    }

    frame.render_widget(Paragraph::new(lines), inner);

    // Show hardware cursor only for terminals that want it (shells), not TUI apps (Claude Code)
    if is_active && tp.show_cursor {
        let screen = tp.screen();
        let (cursor_row, cursor_col) = screen.cursor_position();
        // Only show if not in scrollback (scrollback == 0 means live view)
        if screen.scrollback() == 0 {
            let x = inner.x + cursor_col;
            let y = inner.y + cursor_row;
            if x < inner.x + inner.width && y < inner.y + inner.height {
                frame.set_cursor_position((x, y));
            }
        }
    }
}

fn vt100_cell_style(cell: &vt100::Cell) -> Style {
    let mut mods = Modifier::empty();
    if cell.bold() {
        mods |= Modifier::BOLD;
    }
    if cell.italic() {
        mods |= Modifier::ITALIC;
    }
    if cell.underline() {
        mods |= Modifier::UNDERLINED;
    }
    if cell.inverse() {
        mods |= Modifier::REVERSED;
    }

    Style::default()
        .fg(vt100_color(cell.fgcolor()))
        .bg(vt100_color(cell.bgcolor()))
        .add_modifier(mods)
}

fn vt100_color(color: vt100::Color) -> Color {
    match color {
        vt100::Color::Default => Color::Reset,
        vt100::Color::Idx(i) => Color::Indexed(i),
        vt100::Color::Rgb(r, g, b) => Color::Rgb(r, g, b),
    }
}
