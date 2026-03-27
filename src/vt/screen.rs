use crate::vt::attrs::Attrs;
use crate::vt::cell::Cell;
use crate::vt::color::Color;
use crate::vt::grid::{Grid, Size};

const MODE_ALTERNATE_SCREEN: u8 = 0x01;
const MODE_HIDE_CURSOR: u8 = 0x02;
const MODE_APPLICATION_CURSOR: u8 = 0x04;
const MODE_APPLICATION_KEYPAD: u8 = 0x08;
const MODE_BRACKETED_PASTE: u8 = 0x10;

/// Terminal screen state: manages grids, attributes, and all terminal operations.
pub struct Screen {
    grid: Grid,
    alternate_grid: Grid,
    pub(crate) attrs: Attrs,
    modes: u8,
}

impl Screen {
    pub fn new(rows: u16, cols: u16, scrollback_len: usize) -> Self {
        let size = Size { rows, cols };
        Self {
            grid: Grid::new(size, scrollback_len),
            alternate_grid: Grid::new(size, 0),
            attrs: Attrs::default(),
            modes: 0,
        }
    }

    // --- Public API (used by terminal.rs and terminal_view.rs) ---

    pub fn set_size(&mut self, rows: u16, cols: u16) {
        let size = Size { rows, cols };
        self.grid.set_size(size);
        self.alternate_grid.set_size(size);
    }

    pub fn size(&self) -> (u16, u16) {
        let s = self.active_grid().size();
        (s.rows, s.cols)
    }

    pub fn scrollback(&self) -> usize {
        self.active_grid().scrollback()
    }

    pub fn set_scrollback(&mut self, offset: usize) {
        self.active_grid_mut().set_scrollback(offset);
    }

    pub fn cursor_position(&self) -> (u16, u16) {
        let p = self.active_grid().pos();
        (p.row, p.col)
    }

    pub fn cell(&self, row: u16, col: u16) -> Option<&Cell> {
        self.active_grid().visible_cell(row, col)
    }

    /// Get an entire visible row for efficient iteration during rendering.
    pub fn visible_row(&self, row: u16) -> Option<&[Cell]> {
        self.active_grid()
            .visible_row_slice(row)
            .map(|v| v.as_slice())
    }

    pub fn alternate_screen(&self) -> bool {
        self.modes & MODE_ALTERNATE_SCREEN != 0
    }

    #[allow(dead_code)]
    pub fn hide_cursor(&self) -> bool {
        self.modes & MODE_HIDE_CURSOR != 0
    }

    // --- Grid access ---

    fn active_grid(&self) -> &Grid {
        if self.alternate_screen() {
            &self.alternate_grid
        } else {
            &self.grid
        }
    }

    fn active_grid_mut(&mut self) -> &mut Grid {
        if self.alternate_screen() {
            &mut self.alternate_grid
        } else {
            &mut self.grid
        }
    }

    // --- Character output ---

    pub(crate) fn text(&mut self, c: char) {
        let width = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0) as u16;
        let attrs = self.attrs; // copy to avoid borrow conflicts

        if width == 0 {
            let grid = self.active_grid_mut();
            let pos = grid.pos();
            if pos.col > 0 {
                if let Some(cell) = grid.drawing_cell_mut(pos.row, pos.col - 1) {
                    cell.append(c);
                }
            }
            return;
        }

        let grid = self.active_grid_mut();
        let cols = grid.size().cols;

        grid.do_wrap();

        let pos = grid.pos();

        // Wide character at end of line (or screen too narrow for wide char)
        if width == 2 && pos.col >= cols.saturating_sub(1) {
            if cols < 2 {
                // Screen too narrow for a wide character — skip it
                return;
            }
            if grid.auto_wrap() {
                if let Some(cell) = grid.drawing_cell_mut(pos.row, pos.col) {
                    cell.clear(attrs);
                }
                grid.pending_wrap = true;
                grid.do_wrap();
            } else {
                let col = cols - 2;
                if let Some(cell) = grid.drawing_cell_mut(pos.row, col) {
                    cell.set(c, attrs, true);
                }
                if let Some(cell) = grid.drawing_cell_mut(pos.row, col + 1) {
                    cell.set_continuation(attrs);
                }
                return;
            }
        }

        let pos = grid.pos();

        // Clear existing wide char being overwritten
        if let Some(existing) = grid.drawing_cell(pos.row, pos.col) {
            if existing.is_wide_continuation() && pos.col > 0 {
                // Overwriting right half — clear left half
                if let Some(left) = grid.drawing_cell_mut(pos.row, pos.col - 1) {
                    left.clear(attrs);
                }
            } else if existing.is_wide() && pos.col + 1 < cols {
                // Overwriting left half — clear right half (continuation)
                if let Some(right) = grid.drawing_cell_mut(pos.row, pos.col + 1) {
                    right.clear(attrs);
                }
            }
        }

        // Write the character
        if let Some(cell) = grid.drawing_cell_mut(pos.row, pos.col) {
            cell.set(c, attrs, width == 2);
        }

        // For wide characters, set continuation cell
        if width == 2 && pos.col + 1 < cols {
            let next_is_wide = grid
                .drawing_cell(pos.row, pos.col + 1)
                .is_some_and(|cell| cell.is_wide());
            if next_is_wide && pos.col + 2 < cols {
                if let Some(cont) = grid.drawing_cell_mut(pos.row, pos.col + 2) {
                    cont.clear(attrs);
                }
            }
            if let Some(cell) = grid.drawing_cell_mut(pos.row, pos.col + 1) {
                cell.set_continuation(attrs);
            }
        }

        grid.advance_cursor(width);
    }

    // --- Control characters ---

    pub(crate) fn bs(&mut self) {
        self.active_grid_mut().move_left(1);
    }

    pub(crate) fn tab(&mut self) {
        self.active_grid_mut().tab();
    }

    pub(crate) fn lf(&mut self) {
        self.active_grid_mut().line_feed();
    }

    pub(crate) fn cr(&mut self) {
        self.active_grid_mut().move_to_col0();
    }

    // --- Escape sequences ---

    pub(crate) fn decsc(&mut self) {
        let attrs = self.attrs;
        self.active_grid_mut().save_cursor(attrs);
    }

    pub(crate) fn decrc(&mut self) {
        self.attrs = self.active_grid_mut().restore_cursor();
    }

    pub(crate) fn ri(&mut self) {
        self.active_grid_mut().reverse_index();
    }

    pub(crate) fn ris(&mut self) {
        *self = Self::new(
            self.grid.size().rows,
            self.grid.size().cols,
            self.grid.scrollback_capacity(),
        );
    }

    pub(crate) fn deckpam(&mut self) {
        self.modes |= MODE_APPLICATION_KEYPAD;
    }

    pub(crate) fn deckpnm(&mut self) {
        self.modes &= !MODE_APPLICATION_KEYPAD;
    }

    // --- CSI sequences ---

    /// CUU — Cursor Up
    pub(crate) fn cuu(&mut self, n: u16) {
        self.active_grid_mut().move_up(n);
    }

    /// CUD — Cursor Down
    pub(crate) fn cud(&mut self, n: u16) {
        self.active_grid_mut().move_down(n);
    }

    /// CUF — Cursor Forward
    pub(crate) fn cuf(&mut self, n: u16) {
        self.active_grid_mut().move_right(n);
    }

    /// CUB — Cursor Back
    pub(crate) fn cub(&mut self, n: u16) {
        self.active_grid_mut().move_left(n);
    }

    /// CNL — Cursor Next Line
    pub(crate) fn cnl(&mut self, n: u16) {
        let g = self.active_grid_mut();
        g.move_down(n);
        g.move_to_col0();
    }

    /// CPL — Cursor Previous Line
    pub(crate) fn cpl(&mut self, n: u16) {
        let g = self.active_grid_mut();
        g.move_up(n);
        g.move_to_col0();
    }

    /// CHA — Cursor Horizontal Absolute
    pub(crate) fn cha(&mut self, col: u16) {
        self.active_grid_mut().set_col(col.saturating_sub(1));
    }

    /// CUP — Cursor Position (also HVP via CSI f)
    pub(crate) fn cup(&mut self, row: u16, col: u16) {
        self.active_grid_mut()
            .set_pos(row.saturating_sub(1), col.saturating_sub(1));
    }

    /// VPA — Vertical Position Absolute
    pub(crate) fn vpa(&mut self, row: u16) {
        self.active_grid_mut().set_row(row.saturating_sub(1));
    }

    /// ED — Erase in Display
    pub(crate) fn ed(&mut self, mode: u16) {
        let attrs = self.attrs;
        match mode {
            0 => self.active_grid_mut().erase_display_forward(attrs),
            1 => self.active_grid_mut().erase_display_backward(attrs),
            2 => self.active_grid_mut().erase_display_all(attrs),
            3 => {
                let g = self.active_grid_mut();
                g.erase_display_all(attrs);
                g.clear_scrollback();
            }
            _ => {}
        }
    }

    /// EL — Erase in Line
    pub(crate) fn el(&mut self, mode: u16) {
        let attrs = self.attrs;
        match mode {
            0 => self.active_grid_mut().erase_row_forward(attrs),
            1 => self.active_grid_mut().erase_row_backward(attrs),
            2 => self.active_grid_mut().erase_row_all(attrs),
            _ => {}
        }
    }

    /// ICH — Insert Characters
    pub(crate) fn ich(&mut self, n: u16) {
        self.active_grid_mut().insert_cells(n);
    }

    /// DCH — Delete Characters
    pub(crate) fn dch(&mut self, n: u16) {
        self.active_grid_mut().delete_cells(n);
    }

    /// IL — Insert Lines
    pub(crate) fn il(&mut self, n: u16) {
        self.active_grid_mut().insert_lines(n);
    }

    /// DL — Delete Lines
    pub(crate) fn dl(&mut self, n: u16) {
        self.active_grid_mut().delete_lines(n);
    }

    /// SU — Scroll Up
    pub(crate) fn su(&mut self, n: u16) {
        self.active_grid_mut().scroll_up(n);
    }

    /// SD — Scroll Down
    pub(crate) fn sd(&mut self, n: u16) {
        self.active_grid_mut().scroll_down(n);
    }

    /// ECH — Erase Characters
    pub(crate) fn ech(&mut self, n: u16) {
        let attrs = self.attrs;
        self.active_grid_mut().erase_cells(n, attrs);
    }

    /// DECSTBM — Set Scrolling Region
    pub(crate) fn decstbm(&mut self, top: u16, bottom: u16) {
        self.active_grid_mut()
            .set_scroll_region(top.saturating_sub(1), bottom.saturating_sub(1));
    }

    /// SGR — Select Graphic Rendition
    pub(crate) fn sgr(&mut self, params: &vte::Params) {
        if params.is_empty() {
            self.attrs = Attrs::default();
            return;
        }

        let mut iter = params.iter();

        loop {
            let param = match iter.next() {
                Some(p) => p,
                None => return,
            };

            match param {
                [0] => self.attrs = Attrs::default(),
                [1] => self.attrs.set_bold(),
                [2] => self.attrs.set_dim(),
                [3] => self.attrs.set_italic(true),
                [4] => self.attrs.set_underline(true),
                [7] => self.attrs.set_inverse(true),
                [9] => self.attrs.set_strikethrough(true),
                [22] => self.attrs.set_normal_intensity(),
                [23] => self.attrs.set_italic(false),
                [24] => self.attrs.set_underline(false),
                [27] => self.attrs.set_inverse(false),
                [29] => self.attrs.set_strikethrough(false),
                // Standard foreground colors (30-37)
                [n] if (30..=37).contains(n) => {
                    self.attrs.fgcolor = Color::Idx((*n as u8) - 30);
                }
                // 256-color / RGB foreground (colon-separated subparams)
                [38, 2, r, g, b] => {
                    self.attrs.fgcolor = Color::Rgb(*r as u8, *g as u8, *b as u8);
                }
                [38, 5, i] => {
                    self.attrs.fgcolor = Color::Idx(*i as u8);
                }
                // 256-color / RGB foreground (semicolon-separated params)
                [38] => match iter.next() {
                    Some([2]) => {
                        let r = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        let g = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        let b = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        self.attrs.fgcolor = Color::Rgb(r, g, b);
                    }
                    Some([5]) => {
                        let i = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        self.attrs.fgcolor = Color::Idx(i);
                    }
                    _ => {} // skip unknown, continue processing remaining SGR codes
                },
                [39] => self.attrs.fgcolor = Color::Default,
                // Standard background colors (40-47)
                [n] if (40..=47).contains(n) => {
                    self.attrs.bgcolor = Color::Idx((*n as u8) - 40);
                }
                // 256-color / RGB background (colon-separated subparams)
                [48, 2, r, g, b] => {
                    self.attrs.bgcolor = Color::Rgb(*r as u8, *g as u8, *b as u8);
                }
                [48, 5, i] => {
                    self.attrs.bgcolor = Color::Idx(*i as u8);
                }
                // 256-color / RGB background (semicolon-separated params)
                [48] => match iter.next() {
                    Some([2]) => {
                        let r = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        let g = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        let b = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        self.attrs.bgcolor = Color::Rgb(r, g, b);
                    }
                    Some([5]) => {
                        let i = iter.next().and_then(|p| p.first().copied()).unwrap_or(0) as u8;
                        self.attrs.bgcolor = Color::Idx(i);
                    }
                    _ => {} // skip unknown, continue processing remaining SGR codes
                },
                [49] => self.attrs.bgcolor = Color::Default,
                // Bright foreground colors (90-97)
                [n] if (90..=97).contains(n) => {
                    self.attrs.fgcolor = Color::Idx((*n as u8) - 82);
                }
                // Bright background colors (100-107)
                [n] if (100..=107).contains(n) => {
                    self.attrs.bgcolor = Color::Idx((*n as u8) - 92);
                }
                _ => {} // ignore unknown SGR codes
            }
        }
    }

    /// DECSET — DEC Private Mode Set
    pub(crate) fn decset(&mut self, params: &vte::Params) {
        for param in params {
            match param {
                [1] => self.modes |= MODE_APPLICATION_CURSOR,
                [6] => self.active_grid_mut().set_origin_mode(true),
                [7] => self.active_grid_mut().set_auto_wrap(true),
                [25] => self.modes &= !MODE_HIDE_CURSOR,
                [47] => self.enter_alternate(),
                [1049] => {
                    self.decsc();
                    self.alternate_grid.clear();
                    self.enter_alternate();
                }
                [2004] => self.modes |= MODE_BRACKETED_PASTE,
                // Mouse modes — accept silently (we don't forward mouse to PTY)
                [9] | [1000] | [1002] | [1003] | [1005] | [1006] | [1015] => {}
                // Synchronized output — accept silently
                [2026] => {}
                _ => {} // ignore unknown
            }
        }
    }

    /// DECRST — DEC Private Mode Reset
    pub(crate) fn decrst(&mut self, params: &vte::Params) {
        for param in params {
            match param {
                [1] => self.modes &= !MODE_APPLICATION_CURSOR,
                [6] => self.active_grid_mut().set_origin_mode(false),
                [7] => self.active_grid_mut().set_auto_wrap(false),
                [25] => self.modes |= MODE_HIDE_CURSOR,
                [47] => self.exit_alternate(),
                [1049] => {
                    self.exit_alternate();
                    self.decrc();
                }
                [2004] => self.modes &= !MODE_BRACKETED_PASTE,
                // Mouse modes — accept silently
                [9] | [1000] | [1002] | [1003] | [1005] | [1006] | [1015] => {}
                // Synchronized output — accept silently
                [2026] => {}
                _ => {}
            }
        }
    }

    // --- Alternate screen ---

    fn enter_alternate(&mut self) {
        self.active_grid_mut().set_scrollback(0);
        self.modes |= MODE_ALTERNATE_SCREEN;
        self.alternate_grid.allocate_rows();
    }

    fn exit_alternate(&mut self) {
        self.modes &= !MODE_ALTERNATE_SCREEN;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_screen(rows: u16, cols: u16) -> Screen {
        Screen::new(rows, cols, 100)
    }

    #[test]
    fn new_screen() {
        let s = make_screen(24, 80);
        assert_eq!(s.size(), (24, 80));
        assert_eq!(s.cursor_position(), (0, 0));
    }

    #[test]
    fn text_basic() {
        let mut s = make_screen(3, 10);
        s.text('H');
        s.text('i');
        assert_eq!(s.cell(0, 0).unwrap().contents(), "H");
        assert_eq!(s.cell(0, 1).unwrap().contents(), "i");
        assert_eq!(s.cursor_position(), (0, 2));
    }

    #[test]
    fn text_wide_char() {
        let mut s = make_screen(3, 10);
        s.text('日');
        assert_eq!(s.cell(0, 0).unwrap().contents(), "日");
        assert!(s.cell(0, 0).unwrap().is_wide());
        assert!(s.cell(0, 1).unwrap().is_wide_continuation());
        assert_eq!(s.cursor_position(), (0, 2));
    }

    #[test]
    fn text_wraps_at_end() {
        let mut s = make_screen(3, 5);
        for c in "ABCDE".chars() {
            s.text(c);
        }
        assert_eq!(s.cursor_position(), (0, 4));
        s.text('F');
        assert_eq!(s.cell(1, 0).unwrap().contents(), "F");
    }

    #[test]
    fn no_wrap_mode() {
        let mut s = make_screen(3, 5);
        s.active_grid_mut().set_auto_wrap(false);
        for c in "ABCDEFGH".chars() {
            s.text(c);
        }
        assert_eq!(s.cursor_position(), (0, 4));
        assert_eq!(s.cell(0, 4).unwrap().contents(), "H");
    }

    #[test]
    fn cup_positions_cursor() {
        let mut s = make_screen(10, 20);
        s.cup(5, 10);
        assert_eq!(s.cursor_position(), (4, 9));
    }

    #[test]
    fn sgr_colors() {
        let mut s = make_screen(3, 10);
        let mut attrs = Attrs::default();
        attrs.fgcolor = Color::Idx(1);
        s.attrs = attrs;
        s.text('R');
        assert_eq!(s.cell(0, 0).unwrap().fgcolor(), Color::Idx(1));
    }

    #[test]
    fn erase_display() {
        let mut s = make_screen(3, 5);
        s.text('A');
        s.text('B');
        s.ed(2);
        assert!(!s.cell(0, 0).unwrap().has_contents());
        assert!(!s.cell(0, 1).unwrap().has_contents());
    }

    #[test]
    fn decsc_decrc() {
        let mut s = make_screen(10, 20);
        s.cup(3, 7);
        s.attrs.set_bold();
        s.decsc();
        s.cup(1, 1);
        s.attrs = Attrs::default();
        s.decrc();
        assert_eq!(s.cursor_position(), (2, 6));
        assert!(s.attrs.bold());
    }

    #[test]
    fn sgr_strikethrough() {
        let mut s = make_screen(3, 10);
        s.attrs.set_strikethrough(true);
        s.text('X');
        assert!(s.cell(0, 0).unwrap().strikethrough());
    }
}
