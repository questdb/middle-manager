use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};
use ratatui::Frame;

use crate::panel::sort::SortField;
use crate::panel::Panel;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect, panel: &mut Panel, is_active: bool) {
    let t = theme();

    let title = format!(" {} ", panel.current_dir.to_string_lossy());
    let block = Block::default()
        .title(Span::styled(title, t.title_style()))
        .borders(Borders::ALL)
        .border_style(t.border_style(is_active))
        .style(t.bg_style());

    // Build header row with sort indicator
    let header_cells = [
        header_cell(
            "Name",
            panel.sort_field,
            SortField::Name,
            panel.sort_ascending,
        ),
        header_cell(
            "Size",
            panel.sort_field,
            SortField::Size,
            panel.sort_ascending,
        ),
        header_cell(
            "Date",
            panel.sort_field,
            SortField::Date,
            panel.sort_ascending,
        ),
        Span::styled("Perm", t.header_style()),
    ];
    let header = Row::new(header_cells).style(t.bg_style()).height(1);

    // Build rows
    let rows: Vec<Row> = panel
        .entries
        .iter()
        .map(|entry| {
            let name_style = if entry.is_dir {
                t.dir_style()
            } else if entry.is_symlink {
                t.symlink_style()
            } else if entry.permissions & 0o111 != 0 {
                t.exec_style()
            } else {
                t.file_style()
            };

            let name_display = if entry.is_dir && entry.name != ".." {
                format!("/{}", entry.name)
            } else if entry.is_symlink && entry.is_dir {
                format!("~{}", entry.name)
            } else {
                entry.name.clone()
            };

            Row::new(vec![
                Cell::from(name_display).style(name_style),
                Cell::from(entry.formatted_size()).style(Style::default().fg(t.size_fg).bg(t.bg)),
                Cell::from(entry.formatted_date()).style(Style::default().fg(t.date_fg).bg(t.bg)),
                Cell::from(entry.formatted_permissions())
                    .style(Style::default().fg(t.perm_fg).bg(t.bg)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Fill(1),
        Constraint::Length(8),
        Constraint::Length(16),
        Constraint::Length(9),
    ];

    let mut table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .highlight_symbol("  ");

    if is_active {
        table = table.row_highlight_style(t.highlight_style());
    }

    frame.render_stateful_widget(table, area, &mut panel.table_state);

    // Render quick search overlay if active
    if let Some(ref query) = panel.quick_search {
        let search_line = Line::from(vec![
            Span::styled(
                " Search: ",
                Style::default().fg(t.search_label_fg).bg(t.search_label_bg),
            ),
            Span::styled(
                query.as_str(),
                Style::default()
                    .fg(t.search_text_fg)
                    .bg(t.bg)
                    .add_modifier(Modifier::BOLD),
            ),
        ]);
        let search_area = Rect::new(
            area.x + 1,
            area.y + area.height.saturating_sub(2),
            area.width.saturating_sub(2).min(30),
            1,
        );
        frame.render_widget(ratatui::widgets::Paragraph::new(search_line), search_area);
    }

    // Render error if present
    if let Some(ref error) = panel.error {
        let error_line = Line::from(Span::styled(
            error.as_str(),
            Style::default()
                .fg(t.error_fg)
                .bg(t.bg)
                .add_modifier(Modifier::BOLD),
        ));
        let error_area = Rect::new(
            area.x + 1,
            area.y + area.height / 2,
            area.width.saturating_sub(2),
            1,
        );
        frame.render_widget(ratatui::widgets::Paragraph::new(error_line), error_area);
    }
}

fn header_cell(
    name: &str,
    current_sort: SortField,
    this_field: SortField,
    ascending: bool,
) -> Span<'static> {
    let t = theme();
    let style = t.header_style();

    if current_sort == this_field {
        let arrow = if ascending { " \u{25b2}" } else { " \u{25bc}" };
        Span::styled(format!("{}{}", name, arrow), style)
    } else {
        Span::styled(name.to_string(), style)
    }
}
