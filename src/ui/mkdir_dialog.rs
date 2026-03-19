use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{MkdirDialogField, MkdirDialogState};
use crate::theme::theme;

const DIALOG_WIDTH: u16 = 66;
const DIALOG_HEIGHT: u16 = 11;

const PAD: usize = 2;    // horizontal padding inside border
const MARGIN: u16 = 2;   // outer margin around the border (left/right)
const MARGIN_V: u16 = 1; // outer margin top/bottom

pub fn render(frame: &mut Frame, state: &MkdirDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;

    // Outer area includes margin; the border box sits inside it
    let outer_w = (DIALOG_WIDTH + MARGIN * 2).min(frame.area().width.saturating_sub(2));
    let outer_h = (DIALOG_HEIGHT + MARGIN_V * 2).min(frame.area().height.saturating_sub(2));
    let outer = centered_rect(outer_w, outer_h, frame.area());

    // Clear the full outer area (creates the gap around the border)
    frame.render_widget(Clear, outer);
    // Fill outer margin with dialog background
    let bg_style = t.dialog_bg_style();
    let buf = frame.buffer_mut();
    for y in outer.top()..outer.bottom() {
        for x in outer.left()..outer.right() {
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_symbol(" ");
                cell.set_style(bg_style);
            }
        }
    }

    // Border box inside the margin
    let area = Rect::new(
        outer.x + MARGIN,
        outer.y + MARGIN_V,
        outer.width.saturating_sub(MARGIN * 2),
        outer.height.saturating_sub(MARGIN_V * 2),
    );

    let block = Block::default()
        .title(Span::styled(" Make folder ", t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let normal = Style::default().fg(t.dialog_text_fg).bg(dbg);
    let highlight = Style::default().fg(t.dialog_input_fg_focused).bg(t.dialog_input_bg);
    let input_highlight = highlight;
    let input_normal = Style::default()
        .fg(t.dialog_input_fg)
        .bg(dbg)
        .add_modifier(Modifier::BOLD);

    // Padded content area
    let content = Rect::new(
        inner.x + PAD as u16,
        inner.y,
        inner.width.saturating_sub(PAD as u16 * 2),
        inner.height,
    );
    let cw = content.width as usize;

    // y=0: empty padding row
    // y=1: "Create the folder"
    render_line(
        frame,
        content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", "Create the folder", width = cw),
            normal,
        )),
    );

    // y=2: input field
    let input_focused = state.focused == MkdirDialogField::Input;
    let input_style = if input_focused { input_highlight } else { input_normal };
    let input_text = format!("{:<width$}", state.input, width = cw);
    render_line(
        frame,
        content,
        2,
        Line::from(Span::styled(input_text, input_style)),
    );

    // y=3: empty padding row
    // y=4: separator (full width, uses outer area)
    render_separator(frame, area, inner.y + 4, t.dialog_border_style());

    // y=5: "Process multiple names" checkbox
    let pm_focused = state.focused == MkdirDialogField::ProcessMultiple;
    let check = if state.process_multiple { "x" } else { " " };
    let pm_style = if pm_focused { highlight } else { normal };
    let pm_text = format!(
        "[{}] Process multiple names{:<width$}",
        check,
        "",
        width = cw.saturating_sub(28)
    );
    render_line(
        frame,
        content,
        5,
        Line::from(Span::styled(pm_text, pm_style)),
    );

    // y=6: separator
    render_separator(frame, area, inner.y + 6, t.dialog_border_style());

    // y=7: buttons — centered
    let ok_focused = state.focused == MkdirDialogField::ButtonOk;
    let cancel_focused = state.focused == MkdirDialogField::ButtonCancel;
    let buttons_len = 6 + 4 + 10; // "{ OK }" + gap + "[ Cancel ]"
    let left_pad = cw.saturating_sub(buttons_len) / 2;
    let right_pad = cw.saturating_sub(buttons_len + left_pad);
    render_line(
        frame,
        content,
        7,
        Line::from(vec![
            Span::styled(" ".repeat(left_pad), normal),
            Span::styled(
                "{ OK }",
                if ok_focused { highlight } else { normal },
            ),
            Span::styled("    ", normal),
            Span::styled(
                "[ Cancel ]",
                if cancel_focused { highlight } else { normal },
            ),
            Span::styled(" ".repeat(right_pad), normal),
        ]),
    );
    // y=8: empty padding row

    // Blinking cursor in input field
    if input_focused {
        let cursor_x = content.x + state.cursor as u16;
        let cursor_y = content.y + 2;
        if cursor_x < content.x + content.width {
            frame.set_cursor_position((cursor_x, cursor_y));
        }
    }

    outer
}

fn render_line(frame: &mut Frame, content: Rect, y_off: u16, line: Line) {
    let rect = Rect::new(content.x, content.y + y_off, content.width, 1);
    frame.render_widget(Paragraph::new(line), rect);
}

fn render_separator(frame: &mut Frame, outer: Rect, y: u16, style: Style) {
    let mut s = String::with_capacity(outer.width as usize);
    s.push('\u{251c}'); // ├
    for _ in 0..outer.width.saturating_sub(2) {
        s.push('\u{2500}'); // ─
    }
    s.push('\u{2524}'); // ┤
    let rect = Rect::new(outer.x, y, outer.width, 1);
    frame.render_widget(Paragraph::new(Span::styled(s, style)), rect);
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}
