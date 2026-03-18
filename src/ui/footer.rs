use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect) {
    let t = theme();
    let key_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: Vec<(&str, &str)> = vec![
        ("1", "Help"),
        ("2", "UsrMn"),
        ("3", "View"),
        ("4", "Edit"),
        ("5", "Copy"),
        ("6", "RnMov"),
        ("7", "MkFld"),
        ("8", "Del"),
        ("9", "Sort"),
        ("10", "Quit"),
    ];

    let mut spans = Vec::new();
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" ", sep_style));
        }
        spans.push(Span::styled(format!("{}", key), key_style));
        spans.push(Span::styled(format!("{}", label), key_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}
