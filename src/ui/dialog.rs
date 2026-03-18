use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::DialogState;
use crate::theme::theme;

pub fn render(frame: &mut Frame, dialog: &DialogState) -> Rect {
    let t = theme();
    let area = centered_rect(50, 7, frame.area());

    frame.render_widget(Clear, area);

    let block = Block::default()
        .title(Span::styled(
            format!(" {} ", dialog.title),
            t.dialog_title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(inner);

    frame.render_widget(
        Paragraph::new(dialog.message.as_str())
            .style(t.dialog_text_style())
            .wrap(Wrap { trim: true }),
        chunks[0],
    );

    if dialog.has_input {
        let input_line = Line::from(vec![
            Span::styled(
                "> ",
                Style::default().fg(t.dialog_prompt_fg).bg(t.dialog_bg),
            ),
            Span::styled(
                &dialog.input,
                Style::default()
                    .fg(t.dialog_input_fg)
                    .bg(t.dialog_bg)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("_", Style::default().fg(t.dialog_cursor_fg).bg(t.dialog_bg)),
        ]);
        frame.render_widget(Paragraph::new(input_line), chunks[2]);
    }

    let hints = if dialog.has_input {
        "Enter: Confirm  |  Esc: Cancel"
    } else {
        "Enter: Yes  |  Esc: No"
    };
    frame.render_widget(
        Paragraph::new(hints).style(Style::default().fg(t.dialog_hint_fg).bg(t.dialog_bg)),
        chunks[3],
    );

    area
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}
