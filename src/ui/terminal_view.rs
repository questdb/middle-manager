use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders};
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

    // Write directly to ratatui's Buffer — no intermediate Lines/Spans/Paragraph.
    // One pass: iterate vt screen rows → write cells to buffer positions.
    let screen = tp.screen();
    let buf = frame.buffer_mut();
    let width = inner.width as usize;

    for screen_row in 0..inner.height {
        let y = inner.y + screen_row;
        let row_cells = screen.visible_row(screen_row);

        for screen_col in 0..inner.width {
            let x = inner.x + screen_col;
            let col = screen_col as usize;

            let buf_cell = match buf.cell_mut((x, y)) {
                Some(c) => c,
                None => continue,
            };

            let selected = tp.is_selected(screen_row, screen_col);
            let vt_cell = row_cells.and_then(|r| r.get(col));

            match vt_cell {
                Some(cell) if cell.is_wide_continuation() => {
                    // ratatui handles wide chars via the first cell's width;
                    // set continuation cell to empty so it doesn't overwrite.
                    buf_cell.set_symbol("");
                    let mut style = cell_style(cell);
                    if selected {
                        style = style.add_modifier(Modifier::REVERSED);
                    }
                    buf_cell.set_style(style);
                }
                Some(cell) if cell.has_contents() => {
                    buf_cell.set_symbol(cell.contents());
                    let mut style = cell_style(cell);
                    if selected {
                        style = style.add_modifier(Modifier::REVERSED);
                    }
                    buf_cell.set_style(style);
                }
                Some(cell) => {
                    buf_cell.set_symbol(" ");
                    let mut style = cell_style(cell);
                    if selected {
                        style = style.add_modifier(Modifier::REVERSED);
                    }
                    buf_cell.set_style(style);
                }
                None => {
                    if col < width {
                        buf_cell.set_symbol(" ");
                        let style = if selected {
                            Style::default().add_modifier(Modifier::REVERSED)
                        } else {
                            Style::default()
                        };
                        buf_cell.set_style(style);
                    }
                }
            }
        }
    }

    // Show hardware cursor only for terminals that want it (shells), not TUI apps (Claude Code)
    if is_active && tp.show_cursor {
        let (cursor_row, cursor_col) = screen.cursor_position();
        if screen.scrollback() == 0 {
            let x = inner.x + cursor_col;
            let y = inner.y + cursor_row;
            if x < inner.x + inner.width && y < inner.y + inner.height {
                crate::ui::set_cursor(x, y);
            }
        }
    }
}

#[inline]
fn cell_style(cell: &crate::vt::Cell) -> Style {
    let mut mods = Modifier::empty();
    if cell.bold() {
        mods |= Modifier::BOLD;
    }
    if cell.italic() {
        mods |= Modifier::ITALIC;
    }
    if cell.dim() {
        mods |= Modifier::DIM;
    }
    if cell.underline() {
        mods |= Modifier::UNDERLINED;
    }
    if cell.inverse() {
        mods |= Modifier::REVERSED;
    }

    Style::default()
        .fg(map_color(cell.fgcolor()))
        .bg(map_color(cell.bgcolor()))
        .add_modifier(mods)
}

#[inline]
fn map_color(color: crate::vt::Color) -> Color {
    match color {
        crate::vt::Color::Default => Color::Reset,
        crate::vt::Color::Idx(i) => Color::Indexed(i),
        crate::vt::Color::Rgb(r, g, b) => Color::Rgb(r, g, b),
    }
}
