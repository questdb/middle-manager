use std::collections::VecDeque;

use crate::vt::attrs::Attrs;
use crate::vt::cell::Cell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pos {
    pub row: u16,
    pub col: u16,
}

/// A grid of cells with scrollback, scroll regions, and cursor management.
/// Uses VecDeque for rows to enable O(1) scroll when the scroll region
/// covers the full screen.
pub struct Grid {
    size: Size,
    pos: Pos,
    saved_pos: Pos,
    saved_attrs: Attrs,
    pub(crate) rows: VecDeque<Vec<Cell>>,
    scrollback: VecDeque<Vec<Cell>>,
    scrollback_len: usize,
    scrollback_offset: usize,
    scroll_top: u16,
    scroll_bottom: u16,
    origin_mode: bool,
    auto_wrap: bool,
    pub pending_wrap: bool,
}

impl Grid {
    pub fn new(size: Size, scrollback_len: usize) -> Self {
        let rows = (0..size.rows).map(|_| new_row(size.cols)).collect();
        Self {
            size,
            pos: Pos::default(),
            saved_pos: Pos::default(),
            saved_attrs: Attrs::default(),
            rows,
            scrollback: VecDeque::new(),
            scrollback_len,
            scrollback_offset: 0,
            scroll_top: 0,
            scroll_bottom: size.rows.saturating_sub(1),
            origin_mode: false,
            auto_wrap: true,
            pending_wrap: false,
        }
    }

    pub fn size(&self) -> Size {
        self.size
    }

    pub fn pos(&self) -> Pos {
        self.pos
    }

    pub fn auto_wrap(&self) -> bool {
        self.auto_wrap
    }

    pub fn set_auto_wrap(&mut self, on: bool) {
        self.auto_wrap = on;
    }

    pub fn set_origin_mode(&mut self, on: bool) {
        self.origin_mode = on;
        self.pos = Pos::default();
        self.pending_wrap = false;
    }

    #[allow(dead_code)]
    pub fn scroll_top(&self) -> u16 {
        self.scroll_top
    }

    #[allow(dead_code)]
    pub fn scroll_bottom(&self) -> u16 {
        self.scroll_bottom
    }

    // --- Scrollback ---

    pub fn scrollback(&self) -> usize {
        self.scrollback_offset
    }

    pub fn scrollback_capacity(&self) -> usize {
        self.scrollback_len
    }

    pub fn set_scrollback(&mut self, offset: usize) {
        self.scrollback_offset = offset.min(self.scrollback.len());
    }

    pub fn clear_scrollback(&mut self) {
        self.scrollback.clear();
        self.scrollback_offset = 0;
    }

    // --- Cell access ---

    pub fn visible_cell(&self, row: u16, col: u16) -> Option<&Cell> {
        self.visible_row_slice(row)
            .and_then(|r| r.get(col as usize))
    }

    pub fn visible_row_slice(&self, row: u16) -> Option<&Vec<Cell>> {
        let row = row as usize;
        let screen_rows = self.rows.len();
        if self.scrollback_offset == 0 {
            return self.rows.get(row);
        }
        let sb_len = self.scrollback.len();
        let offset = self.scrollback_offset.min(sb_len);
        let sb_start = sb_len - offset;
        let sb_visible = offset.min(screen_rows);
        if row < sb_visible {
            self.scrollback.get(sb_start + row)
        } else {
            let screen_row = row - sb_visible;
            if screen_row < screen_rows.saturating_sub(offset) {
                self.rows.get(screen_row)
            } else {
                None
            }
        }
    }

    pub fn drawing_cell(&self, row: u16, col: u16) -> Option<&Cell> {
        self.rows
            .get(row as usize)
            .and_then(|r| r.get(col as usize))
    }

    pub fn drawing_cell_mut(&mut self, row: u16, col: u16) -> Option<&mut Cell> {
        self.rows
            .get_mut(row as usize)
            .and_then(|r| r.get_mut(col as usize))
    }

    // --- Cursor movement ---

    pub fn set_pos(&mut self, row: u16, col: u16) {
        self.pending_wrap = false;
        if self.origin_mode {
            self.pos.row = (self.scroll_top + row).min(self.scroll_bottom);
        } else {
            self.pos.row = row.min(self.size.rows.saturating_sub(1));
        }
        self.pos.col = col.min(self.size.cols.saturating_sub(1));
    }

    pub fn set_col(&mut self, col: u16) {
        self.pending_wrap = false;
        self.pos.col = col.min(self.size.cols.saturating_sub(1));
    }

    pub fn set_row(&mut self, row: u16) {
        self.pending_wrap = false;
        self.pos.row = row.min(self.size.rows.saturating_sub(1));
    }

    pub fn move_right(&mut self, n: u16) {
        self.pending_wrap = false;
        self.pos.col = (self.pos.col + n).min(self.size.cols.saturating_sub(1));
    }

    pub fn move_left(&mut self, n: u16) {
        self.pending_wrap = false;
        self.pos.col = self.pos.col.saturating_sub(n);
    }

    pub fn move_up(&mut self, n: u16) {
        self.pending_wrap = false;
        self.pos.row = self.pos.row.saturating_sub(n);
    }

    pub fn move_down(&mut self, n: u16) {
        self.pending_wrap = false;
        self.pos.row = (self.pos.row + n).min(self.size.rows.saturating_sub(1));
    }

    pub fn move_to_col0(&mut self) {
        self.pending_wrap = false;
        self.pos.col = 0;
    }

    pub fn tab(&mut self) {
        self.pending_wrap = false;
        let next = ((self.pos.col / 8) + 1) * 8;
        self.pos.col = next.min(self.size.cols.saturating_sub(1));
    }

    pub fn do_wrap(&mut self) -> bool {
        if self.pending_wrap {
            self.pending_wrap = false;
            self.pos.col = 0;
            if self.pos.row == self.scroll_bottom {
                self.scroll_up(1);
            } else if self.pos.row < self.size.rows - 1 {
                self.pos.row += 1;
            }
            return true;
        }
        false
    }

    pub fn advance_cursor(&mut self, width: u16) {
        let new_col = self.pos.col + width;
        if new_col >= self.size.cols {
            if self.auto_wrap {
                self.pending_wrap = true;
            }
            self.pos.col = self.size.cols - 1;
        } else {
            self.pos.col = new_col;
        }
    }

    // --- Line feed / reverse index ---

    pub fn line_feed(&mut self) {
        self.pending_wrap = false;
        if self.pos.row == self.scroll_bottom {
            self.scroll_up(1);
        } else if self.pos.row < self.size.rows - 1 {
            self.pos.row += 1;
        }
    }

    pub fn reverse_index(&mut self) {
        self.pending_wrap = false;
        if self.pos.row == self.scroll_top {
            self.scroll_down(1);
        } else if self.pos.row > 0 {
            self.pos.row -= 1;
        }
    }

    // --- Scrolling ---

    pub fn scroll_up(&mut self, n: u16) {
        let top = self.scroll_top as usize;
        let bottom = self.scroll_bottom as usize;
        let full_screen = self.scroll_top == 0 && self.scroll_bottom == self.size.rows - 1;

        for _ in 0..n {
            if top >= self.rows.len() || bottom >= self.rows.len() {
                break;
            }
            if full_screen {
                // O(1) path: rotate the deque and reuse the row
                if let Some(removed) = self.rows.pop_front() {
                    if self.scrollback_len > 0 {
                        if self.scrollback.len() >= self.scrollback_len {
                            self.scrollback.pop_front();
                        }
                        self.scrollback.push_back(removed);
                    }
                    let mut blank = new_row_or_reuse(None, self.size.cols);
                    clear_row(&mut blank, self.size.cols);
                    self.rows.push_back(blank);
                }
            } else {
                // Partial scroll region: must remove/insert at arbitrary positions
                if let Some(removed) = self.rows.remove(top) {
                    // Don't push partial-region rows to scrollback
                    drop(removed);
                }
                let mut blank = new_row(self.size.cols);
                clear_row(&mut blank, self.size.cols);
                self.rows.insert(bottom, blank);
            }
        }
    }

    pub fn scroll_down(&mut self, n: u16) {
        let top = self.scroll_top as usize;
        let bottom = self.scroll_bottom as usize;
        let full_screen = self.scroll_top == 0 && self.scroll_bottom == self.size.rows - 1;

        for _ in 0..n {
            if top >= self.rows.len() || bottom >= self.rows.len() {
                break;
            }
            if full_screen {
                self.rows.pop_back();
                let mut blank = new_row(self.size.cols);
                clear_row(&mut blank, self.size.cols);
                self.rows.push_front(blank);
            } else {
                self.rows.remove(bottom);
                let mut blank = new_row(self.size.cols);
                clear_row(&mut blank, self.size.cols);
                self.rows.insert(top, blank);
            }
        }
    }

    // --- Scroll region ---

    pub fn set_scroll_region(&mut self, top: u16, bottom: u16) {
        let max = self.size.rows.saturating_sub(1);
        self.scroll_top = top.min(max);
        self.scroll_bottom = bottom.min(max);
        if self.scroll_top >= self.scroll_bottom {
            self.scroll_top = 0;
            self.scroll_bottom = max;
        }
        self.set_pos(0, 0);
    }

    // --- Insert / Delete ---

    pub fn insert_lines(&mut self, n: u16) {
        let row = self.pos.row as usize;
        let bottom = self.scroll_bottom as usize;
        for _ in 0..n {
            if row > bottom || bottom >= self.rows.len() {
                break;
            }
            self.rows.remove(bottom);
            self.rows.insert(row, new_row(self.size.cols));
        }
    }

    pub fn delete_lines(&mut self, n: u16) {
        let row = self.pos.row as usize;
        let bottom = self.scroll_bottom as usize;
        for _ in 0..n {
            if row > bottom || bottom >= self.rows.len() {
                break;
            }
            self.rows.remove(row);
            self.rows.insert(bottom, new_row(self.size.cols));
        }
    }

    pub fn insert_cells(&mut self, n: u16) {
        let row = &mut self.rows[self.pos.row as usize];
        let col = self.pos.col as usize;
        let cols = self.size.cols as usize;
        if col >= cols {
            return;
        }
        let count = (n as usize).min(cols - col);
        row.truncate(cols - count);
        let blanks = std::iter::repeat_with(Cell::default).take(count);
        row.splice(col..col, blanks);
    }

    pub fn delete_cells(&mut self, n: u16) {
        let row = &mut self.rows[self.pos.row as usize];
        let col = self.pos.col as usize;
        let cols = self.size.cols as usize;
        if col >= row.len() {
            return;
        }
        let count = (n as usize).min(row.len() - col);
        row.drain(col..col + count);
        row.resize_with(cols, Cell::default);
    }

    // --- Erase ---

    pub fn erase_display_forward(&mut self, attrs: Attrs) {
        self.erase_row_forward(attrs);
        for r in (self.pos.row + 1) as usize..self.rows.len() {
            for cell in &mut self.rows[r] {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_display_backward(&mut self, attrs: Attrs) {
        self.erase_row_backward(attrs);
        for r in 0..self.pos.row as usize {
            for cell in &mut self.rows[r] {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_display_all(&mut self, attrs: Attrs) {
        for row in &mut self.rows {
            for cell in row.iter_mut() {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_row_forward(&mut self, attrs: Attrs) {
        if let Some(row) = self.rows.get_mut(self.pos.row as usize) {
            for cell in &mut row[self.pos.col as usize..] {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_row_backward(&mut self, attrs: Attrs) {
        if let Some(row) = self.rows.get_mut(self.pos.row as usize) {
            if row.is_empty() {
                return;
            }
            let end = (self.pos.col as usize).min(row.len() - 1);
            for cell in &mut row[..=end] {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_row_all(&mut self, attrs: Attrs) {
        if let Some(row) = self.rows.get_mut(self.pos.row as usize) {
            for cell in row.iter_mut() {
                cell.clear(attrs);
            }
        }
    }

    pub fn erase_cells(&mut self, n: u16, attrs: Attrs) {
        if let Some(row) = self.rows.get_mut(self.pos.row as usize) {
            let start = self.pos.col as usize;
            let end = (start + n as usize).min(row.len());
            for cell in &mut row[start..end] {
                cell.clear(attrs);
            }
        }
    }

    // --- Save / Restore cursor ---

    pub fn save_cursor(&mut self, attrs: Attrs) {
        self.saved_pos = self.pos;
        self.saved_attrs = attrs;
    }

    pub fn restore_cursor(&mut self) -> Attrs {
        self.pos = self.saved_pos;
        self.pending_wrap = false;
        self.clamp_cursor();
        self.saved_attrs
    }

    fn clamp_cursor(&mut self) {
        self.pos.row = self.pos.row.min(self.size.rows.saturating_sub(1));
        self.pos.col = self.pos.col.min(self.size.cols.saturating_sub(1));
    }

    // --- Resize ---

    pub fn set_size(&mut self, size: Size) {
        if size == self.size {
            return;
        }
        for row in &mut self.rows {
            row.resize_with(size.cols as usize, Cell::default);
        }
        while self.rows.len() < size.rows as usize {
            self.rows.push_back(new_row(size.cols));
        }
        while self.rows.len() > size.rows as usize {
            self.rows.pop_back();
        }
        self.size = size;
        self.scroll_top = 0;
        self.scroll_bottom = size.rows.saturating_sub(1);
        self.clamp_cursor();
    }

    // --- Clear ---

    pub fn clear(&mut self) {
        for row in &mut self.rows {
            for cell in row.iter_mut() {
                cell.clear(Attrs::default());
            }
        }
        self.pos = Pos::default();
        self.pending_wrap = false;
    }

    pub fn allocate_rows(&mut self) {
        while self.rows.len() < self.size.rows as usize {
            self.rows.push_back(new_row(self.size.cols));
        }
    }
}

fn new_row(cols: u16) -> Vec<Cell> {
    vec![Cell::default(); cols as usize]
}

/// Reuse an existing row if available, otherwise allocate.
fn new_row_or_reuse(existing: Option<Vec<Cell>>, cols: u16) -> Vec<Cell> {
    match existing {
        Some(mut row) => {
            row.resize_with(cols as usize, Cell::default);
            row
        }
        None => new_row(cols),
    }
}

/// Clear all cells in a row in-place (no allocation).
fn clear_row(row: &mut [Cell], _cols: u16) {
    let attrs = Attrs::default();
    for cell in row.iter_mut() {
        cell.clear(attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_grid(rows: u16, cols: u16) -> Grid {
        Grid::new(Size { rows, cols }, 100)
    }

    #[test]
    fn new_grid_dimensions() {
        let g = make_grid(5, 10);
        assert_eq!(g.size(), Size { rows: 5, cols: 10 });
        assert_eq!(g.pos(), Pos { row: 0, col: 0 });
    }

    #[test]
    fn cursor_movement() {
        let mut g = make_grid(10, 20);
        g.move_right(5);
        assert_eq!(g.pos().col, 5);
        g.move_down(3);
        assert_eq!(g.pos().row, 3);
        g.move_left(2);
        assert_eq!(g.pos().col, 3);
        g.move_up(1);
        assert_eq!(g.pos().row, 2);
    }

    #[test]
    fn cursor_clamp() {
        let mut g = make_grid(5, 10);
        g.move_right(100);
        assert_eq!(g.pos().col, 9);
        g.move_down(100);
        assert_eq!(g.pos().row, 4);
    }

    #[test]
    fn set_pos_basic() {
        let mut g = make_grid(10, 20);
        g.set_pos(5, 10);
        assert_eq!(g.pos(), Pos { row: 5, col: 10 });
    }

    #[test]
    fn scroll_up_adds_to_scrollback() {
        let mut g = make_grid(3, 5);
        g.rows[0][0].set('A', Attrs::default(), false);
        g.scroll_up(1);
        assert_eq!(g.scrollback.len(), 1);
        assert_eq!(g.scrollback[0][0].contents(), "A");
        // Row 0 should now be what was row 1 (empty)
        assert!(!g.rows[0][0].has_contents());
    }

    #[test]
    fn scroll_region() {
        let mut g = make_grid(5, 10);
        g.set_scroll_region(1, 3);
        assert_eq!(g.scroll_top(), 1);
        assert_eq!(g.scroll_bottom(), 3);
    }

    #[test]
    fn auto_wrap_on() {
        let mut g = make_grid(3, 5);
        assert!(g.auto_wrap());
        g.pos.col = 4;
        g.advance_cursor(1);
        assert!(g.pending_wrap);
        g.do_wrap();
        assert_eq!(g.pos().row, 1);
        assert_eq!(g.pos().col, 0);
    }

    #[test]
    fn auto_wrap_off() {
        let mut g = make_grid(3, 5);
        g.set_auto_wrap(false);
        g.pos.col = 4;
        g.advance_cursor(1);
        assert!(!g.pending_wrap);
        assert_eq!(g.pos().col, 4);
    }

    #[test]
    fn line_feed_at_bottom_scrolls() {
        let mut g = make_grid(3, 5);
        g.rows[0][0].set('A', Attrs::default(), false);
        g.pos.row = 2;
        g.line_feed();
        assert_eq!(g.scrollback.len(), 1);
    }

    #[test]
    fn scrollback_visible() {
        let mut g = make_grid(3, 5);
        g.rows[0][0].set('A', Attrs::default(), false);
        g.scroll_up(1);
        assert_eq!(g.scrollback.len(), 1);
        g.set_scrollback(1);
        let cell = g.visible_cell(0, 0).unwrap();
        assert_eq!(cell.contents(), "A");
    }

    #[test]
    fn resize() {
        let mut g = make_grid(3, 5);
        g.set_size(Size { rows: 5, cols: 10 });
        assert_eq!(g.size(), Size { rows: 5, cols: 10 });
        assert_eq!(g.rows.len(), 5);
        assert_eq!(g.rows[0].len(), 10);
    }

    #[test]
    fn erase_display_all() {
        let mut g = make_grid(3, 5);
        g.rows[1][2].set('X', Attrs::default(), false);
        g.erase_display_all(Attrs::default());
        assert!(!g.rows[1][2].has_contents());
    }

    #[test]
    fn tab_stops() {
        let mut g = make_grid(1, 40);
        assert_eq!(g.pos().col, 0);
        g.tab();
        assert_eq!(g.pos().col, 8);
        g.tab();
        assert_eq!(g.pos().col, 16);
    }

    #[test]
    fn save_restore_cursor() {
        let mut g = make_grid(10, 20);
        g.set_pos(3, 7);
        g.save_cursor(Attrs::default());
        g.set_pos(0, 0);
        g.restore_cursor();
        assert_eq!(g.pos(), Pos { row: 3, col: 7 });
    }

    #[test]
    fn full_screen_scroll_is_o1() {
        // Verify that full-screen scroll uses deque rotation (no remove/insert)
        let mut g = make_grid(3, 5);
        g.rows[0][0].set('A', Attrs::default(), false);
        g.rows[1][0].set('B', Attrs::default(), false);
        g.rows[2][0].set('C', Attrs::default(), false);
        g.scroll_up(1);
        // A went to scrollback, B is now row 0, C is row 1, blank is row 2
        assert_eq!(g.rows[0][0].contents(), "B");
        assert_eq!(g.rows[1][0].contents(), "C");
        assert!(!g.rows[2][0].has_contents());
    }

    #[test]
    fn partial_scroll_region_no_scrollback() {
        let mut g = make_grid(5, 5);
        g.set_scroll_region(1, 3);
        g.rows[0][0].set('T', Attrs::default(), false); // outside region
        g.rows[1][0].set('A', Attrs::default(), false);
        g.rows[2][0].set('B', Attrs::default(), false);
        g.rows[3][0].set('C', Attrs::default(), false);

        g.pos.row = 3;
        g.scroll_up(1);

        // Row 0 untouched
        assert_eq!(g.rows[0][0].contents(), "T");
        // Region scrolled: A removed, B->row1, C->row2, blank->row3
        assert_eq!(g.rows[1][0].contents(), "B");
        assert_eq!(g.rows[2][0].contents(), "C");
        assert!(!g.rows[3][0].has_contents());
        // No scrollback for partial region
        assert_eq!(g.scrollback.len(), 0);
    }
}
