use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, _app: &App) {
    let t = theme();
    let line = Line::from(Span::styled(
        " ".repeat(area.width as usize),
        Style::default().bg(t.bg),
    ));
    frame.render_widget(Paragraph::new(line), area);
}
