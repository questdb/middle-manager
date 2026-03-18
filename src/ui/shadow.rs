use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::Frame;

/// Render a transparent drop shadow to the right (2 cells) and below (1 row)
/// of the given dialog area. Darkens existing content behind the shadow.
pub fn render_shadow(frame: &mut Frame, dialog_area: Rect) {
    let buf = frame.buffer_mut();
    let term = buf.area;

    // Right shadow: 2 chars wide, offset 1 row down from dialog top
    let rs_x_end = (dialog_area.right() + 2).min(term.right());
    let rs_y_end = (dialog_area.bottom() + 1).min(term.bottom());
    for y in (dialog_area.top() + 1)..rs_y_end {
        for x in dialog_area.right()..rs_x_end {
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_bg(Color::Black);
                cell.set_fg(Color::DarkGray);
            }
        }
    }

    // Bottom shadow: 1 row tall, offset 2 chars right from dialog left
    let bs_y = dialog_area.bottom();
    if bs_y < term.bottom() {
        let bs_x_end = (dialog_area.right() + 2).min(term.right());
        for x in (dialog_area.left() + 2)..bs_x_end {
            if let Some(cell) = buf.cell_mut((x, bs_y)) {
                cell.set_bg(Color::Black);
                cell.set_fg(Color::DarkGray);
            }
        }
    }
}
