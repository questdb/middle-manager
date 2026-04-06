use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::theme::{self, theme};

pub fn render(frame: &mut Frame, selected: usize) -> Rect {
    let t = theme();
    let area = frame.area();

    let width = 50u16.min(area.width.saturating_sub(8));
    let height = 10u16.min(area.height.saturating_sub(4));
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let rect = Rect::new(x, y, width, height);

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(
            " Settings — Shift+F1/Esc to close ",
            t.dialog_title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let highlight = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);
    let value_style = Style::default()
        .fg(t.dialog_title_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);

    // Setting 0: Theme
    let theme_name = theme::current_theme_name();
    let is_selected = selected == 0;
    let style = if is_selected { highlight } else { normal };

    let label = "  Theme: ";
    let value = theme_name.label();
    let hint = if is_selected { "  Space/← → to change" } else { "" };

    let line = Line::from(vec![
        Span::styled(label, style),
        Span::styled(value, if is_selected { value_style } else { normal }),
        Span::styled(hint, Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg).add_modifier(Modifier::DIM)),
    ]);

    let line_rect = Rect::new(inner.x, inner.y + 1, inner.width, 1);
    frame.render_widget(Paragraph::new(line), line_rect);

    // Footer hint
    let hint_rect = Rect::new(inner.x + 1, inner.y + inner.height.saturating_sub(1), inner.width.saturating_sub(2), 1);
    let hint_style = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg).add_modifier(Modifier::DIM);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled("Up/Down: select  Space/Left/Right: change  Esc: close", hint_style))),
        hint_rect,
    );

    rect
}
