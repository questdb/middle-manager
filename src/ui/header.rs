use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let t = theme();
    let half_width = area.width / 2;

    let left_path = app.panels[0].current_dir.to_string_lossy();
    let right_path = app.panels[1].current_dir.to_string_lossy();

    let active_style = Style::default()
        .fg(t.path_active_fg)
        .bg(t.bg)
        .add_modifier(Modifier::BOLD);
    let inactive_style = Style::default().fg(t.path_inactive_fg).bg(t.bg);

    let left_style = if app.active_panel == 0 {
        active_style
    } else {
        inactive_style
    };
    let right_style = if app.active_panel == 1 {
        active_style
    } else {
        inactive_style
    };

    let left_display = truncate_path(&left_path, half_width as usize);
    let right_display = truncate_path(&right_path, (area.width - half_width) as usize);

    let left_padded = format!("{:<width$}", left_display, width = half_width as usize);
    let right_padded = format!(
        "{:<width$}",
        right_display,
        width = (area.width - half_width) as usize
    );

    let line = Line::from(vec![
        Span::styled(left_padded, left_style),
        Span::styled(right_padded, right_style),
    ]);

    frame.render_widget(Paragraph::new(line), area);
}

fn truncate_path(path: &str, max_width: usize) -> String {
    if path.len() <= max_width {
        path.to_string()
    } else if max_width > 3 {
        format!("...{}", &path[path.len() - (max_width - 3)..])
    } else {
        path[..max_width].to_string()
    }
}
