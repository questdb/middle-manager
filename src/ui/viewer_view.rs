use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::theme::theme;
use crate::viewer::ViewerState;

pub fn render(frame: &mut Frame, area: Rect, viewer: &mut ViewerState) {
    let t = theme();

    let block = Block::default()
        .title(Span::styled(
            format!(
                " {} [line {}/{}] ",
                viewer.path.to_string_lossy(),
                viewer.scroll_offset + 1,
                viewer.total_lines_display(),
            ),
            t.title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.border_style(true))
        .style(t.bg_style());

    let inner = block.inner(area);
    viewer.visible_lines = inner.height.saturating_sub(1) as usize;

    let visible = viewer.visible_line_iter();

    let lines: Vec<Line> = visible
        .iter()
        .map(|(line_num, text)| {
            Line::from(vec![
                Span::styled(
                    format!("{:>6} ", line_num + 1),
                    Style::default().fg(t.viewer_line_num_fg).bg(t.bg),
                ),
                Span::styled(
                    text.as_str(),
                    Style::default().fg(t.viewer_text_fg).bg(t.bg),
                ),
            ])
        })
        .collect();

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);

    let hint = " Scroll: Arrows/PgUp/Dn | Home/End | g: Go to | F4/Tab: Hex | q/Esc: Close ";
    let hint_area = Rect::new(
        area.x,
        area.y + area.height.saturating_sub(1),
        (hint.len() as u16).min(area.width),
        1,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            hint,
            Style::default().fg(t.viewer_hint_fg).bg(t.viewer_hint_bg),
        )),
        hint_area,
    );
}
