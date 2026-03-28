use crate::vt::screen::Screen;

/// VT100 terminal parser. Wraps a `vte::Parser` and a `Screen`.
pub struct Parser {
    vte_parser: vte::Parser,
    screen: Screen,
}

impl Parser {
    pub fn new(rows: u16, cols: u16, scrollback_len: usize) -> Self {
        Self {
            vte_parser: vte::Parser::new(),
            screen: Screen::new(rows, cols, scrollback_len),
        }
    }

    /// Feed raw bytes from the PTY into the parser.
    pub fn process(&mut self, bytes: &[u8]) {
        let screen = &mut self.screen as *mut Screen;
        // SAFETY: we need a mutable reference to both the vte parser and the screen
        // simultaneously, but they don't alias. We use a raw pointer to work around
        // the borrow checker since vte::Parser::advance takes &mut self and the
        // Perform impl needs &mut Screen.
        let mut performer = Performer {
            screen: unsafe { &mut *screen },
        };
        self.vte_parser.advance(&mut performer, bytes);
    }

    pub fn screen(&self) -> &Screen {
        &self.screen
    }

    pub fn screen_mut(&mut self) -> &mut Screen {
        &mut self.screen
    }
}

struct Performer<'a> {
    screen: &'a mut Screen,
}

impl vte::Perform for Performer<'_> {
    fn print(&mut self, c: char) {
        if c == '\u{fffd}' || ('\u{80}'..'\u{a0}').contains(&c) {
            // Replacement character or C1 control codes — ignore
        } else {
            self.screen.text(c);
        }
    }

    fn execute(&mut self, b: u8) {
        match b {
            7 => {} // BEL — accept silently (no audio in TUI)
            8 => self.screen.bs(),
            9 => self.screen.tab(),
            10 => self.screen.lf(), // LF
            11 => self.screen.lf(), // VT — treated as LF in modern terminals
            12 => self.screen.lf(), // FF — treated as LF in modern terminals
            13 => self.screen.cr(),
            // SI/SO (charset switching) — ignore
            14 | 15 => {}
            _ => {} // ignore other control chars
        }
    }

    fn esc_dispatch(&mut self, intermediates: &[u8], _ignore: bool, b: u8) {
        if intermediates.is_empty() {
            match b {
                b'7' => self.screen.decsc(),
                b'8' => self.screen.decrc(),
                b'=' => self.screen.deckpam(),
                b'>' => self.screen.deckpnm(),
                b'M' => self.screen.ri(),
                b'c' => self.screen.ris(),
                _ => {} // ignore unknown
            }
        }
    }

    fn csi_dispatch(&mut self, params: &vte::Params, intermediates: &[u8], _ignore: bool, c: char) {
        match intermediates.first() {
            None => {
                let p1 = param1(params, 1);
                let p0 = param1(params, 0);
                match c {
                    '@' => self.screen.ich(p1),
                    'A' => self.screen.cuu(p1),
                    'B' => self.screen.cud(p1),
                    'C' => self.screen.cuf(p1),
                    'D' => self.screen.cub(p1),
                    'E' => self.screen.cnl(p1),
                    'F' => self.screen.cpl(p1),
                    'G' => self.screen.cha(p1),
                    'H' | 'f' => {
                        // CUP and HVP — both position the cursor
                        let (row, col) = param2(params, 1, 1);
                        self.screen.cup(row, col);
                    }
                    'J' => self.screen.ed(p0),
                    'K' => self.screen.el(p0),
                    'L' => self.screen.il(p1),
                    'M' => self.screen.dl(p1),
                    'P' => self.screen.dch(p1),
                    'S' => self.screen.su(p1),
                    'T' => self.screen.sd(p1),
                    'X' => self.screen.ech(p1),
                    'd' => self.screen.vpa(p1),
                    'm' => self.screen.sgr(params),
                    'r' => {
                        let (rows, _) = self.screen.size();
                        let (top, bottom) = param2(params, 1, rows);
                        self.screen.decstbm(top, bottom);
                    }
                    't' => {} // window manipulation — accept silently
                    _ => {}   // ignore unknown CSI
                }
            }
            Some(b'?') => match c {
                'h' => self.screen.decset(params),
                'l' => self.screen.decrst(params),
                'J' => self.screen.ed(param1(params, 0)), // DECSED
                'K' => self.screen.el(param1(params, 0)), // DECSEL
                _ => {}                                   // ignore unknown DEC private
            },
            _ => {} // ignore unknown intermediates
        }
    }

    fn osc_dispatch(&mut self, _params: &[&[u8]], _bel_terminated: bool) {
        // We don't need window title or clipboard callbacks
    }
}

/// Extract first parameter with a default value.
fn param1(params: &vte::Params, default: u16) -> u16 {
    params
        .iter()
        .next()
        .and_then(|p| p.first().copied())
        .map(|v| if v == 0 { default } else { v })
        .unwrap_or(default)
}

/// Extract first two parameters with default values.
fn param2(params: &vte::Params, d1: u16, d2: u16) -> (u16, u16) {
    let mut iter = params.iter();
    let a = iter
        .next()
        .and_then(|p| p.first().copied())
        .map(|v| if v == 0 { d1 } else { v })
        .unwrap_or(d1);
    let b = iter
        .next()
        .and_then(|p| p.first().copied())
        .map(|v| if v == 0 { d2 } else { v })
        .unwrap_or(d2);
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Parser {
        let mut p = Parser::new(24, 80, 100);
        p.process(input.as_bytes());
        p
    }

    #[test]
    fn basic_text() {
        let p = parse("Hello");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "H");
        assert_eq!(p.screen().cell(0, 4).unwrap().contents(), "o");
        assert_eq!(p.screen().cursor_position(), (0, 5));
    }

    #[test]
    fn cursor_position_cup() {
        let p = parse("\x1b[5;10H");
        assert_eq!(p.screen().cursor_position(), (4, 9));
    }

    #[test]
    fn cursor_position_hvp() {
        let p = parse("\x1b[5;10f");
        assert_eq!(p.screen().cursor_position(), (4, 9));
    }

    #[test]
    fn cursor_home() {
        let p = parse("ABC\x1b[H");
        assert_eq!(p.screen().cursor_position(), (0, 0));
    }

    #[test]
    fn sgr_red_fg() {
        let p = parse("\x1b[31mR");
        let cell = p.screen().cell(0, 0).unwrap();
        assert_eq!(cell.contents(), "R");
        assert_eq!(cell.fgcolor(), crate::vt::color::Color::Idx(1));
    }

    #[test]
    fn sgr_256_color() {
        let p = parse("\x1b[38;5;200mX");
        let cell = p.screen().cell(0, 0).unwrap();
        assert_eq!(cell.fgcolor(), crate::vt::color::Color::Idx(200));
    }

    #[test]
    fn sgr_rgb_color() {
        let p = parse("\x1b[38;2;100;150;200mX");
        let cell = p.screen().cell(0, 0).unwrap();
        assert_eq!(cell.fgcolor(), crate::vt::color::Color::Rgb(100, 150, 200));
    }

    #[test]
    fn sgr_bold() {
        let p = parse("\x1b[1mB");
        assert!(p.screen().cell(0, 0).unwrap().bold());
    }

    #[test]
    fn sgr_strikethrough() {
        let p = parse("\x1b[9mS\x1b[29mN");
        assert!(p.screen().cell(0, 0).unwrap().strikethrough());
        assert!(!p.screen().cell(0, 1).unwrap().strikethrough());
    }

    #[test]
    fn sgr_reset() {
        let p = parse("\x1b[1;31mA\x1b[mB");
        let a = p.screen().cell(0, 0).unwrap();
        assert!(a.bold());
        assert_eq!(a.fgcolor(), crate::vt::color::Color::Idx(1));
        let b = p.screen().cell(0, 1).unwrap();
        assert!(!b.bold());
        assert_eq!(b.fgcolor(), crate::vt::color::Color::Default);
    }

    #[test]
    fn sgr_bright_colors() {
        let p = parse("\x1b[91mX");
        // bright red = index 9 (91 - 82)
        assert_eq!(
            p.screen().cell(0, 0).unwrap().fgcolor(),
            crate::vt::color::Color::Idx(9)
        );
    }

    #[test]
    fn erase_display() {
        let p = parse("HELLO\x1b[2J");
        assert!(!p.screen().cell(0, 0).unwrap().has_contents());
    }

    #[test]
    fn erase_line() {
        let p = parse("ABCDE\x1b[3G\x1b[K");
        // Erase from col 2 to end
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        assert_eq!(p.screen().cell(0, 1).unwrap().contents(), "B");
        assert!(!p.screen().cell(0, 2).unwrap().has_contents());
    }

    #[test]
    fn alternate_screen() {
        let mut p = Parser::new(3, 10, 100);
        p.process(b"PRIMARY");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "P");

        // Enter alternate screen
        p.process(b"\x1b[?1049h");
        assert!(p.screen().alternate_screen());
        assert!(!p.screen().cell(0, 0).unwrap().has_contents());

        p.process(b"ALT");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");

        // Exit alternate screen
        p.process(b"\x1b[?1049l");
        assert!(!p.screen().alternate_screen());
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "P");
    }

    #[test]
    fn auto_wrap_disable() {
        let mut p = Parser::new(3, 5, 0);
        p.process(b"\x1b[?7l"); // disable auto-wrap
        p.process(b"ABCDEFGH");
        // Should stay on row 0, last column overwritten
        assert_eq!(p.screen().cursor_position().0, 0);
        assert_eq!(p.screen().cell(0, 4).unwrap().contents(), "H");
        // Row 1 should be empty
        assert!(!p.screen().cell(1, 0).unwrap().has_contents());
    }

    #[test]
    fn synchronized_output_accepted() {
        // Should not crash
        let mut p = Parser::new(3, 10, 0);
        p.process(b"\x1b[?2026h");
        p.process(b"OK");
        p.process(b"\x1b[?2026l");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "O");
    }

    #[test]
    fn scroll_region() {
        let mut p = Parser::new(5, 10, 0);
        p.process(b"\x1b[2;4r"); // set scroll region rows 2-4 (1-based)
        p.process(b"\x1b[4;1H"); // move to row 4 (inside region)
        p.process(b"\n"); // LF should scroll within region
    }

    #[test]
    fn decsc_decrc() {
        let mut p = Parser::new(10, 20, 0);
        p.process(b"\x1b[5;10H"); // move to (4,9)
        p.process(b"\x1b7"); // save
        p.process(b"\x1b[1;1H"); // move to (0,0)
        p.process(b"\x1b8"); // restore
        assert_eq!(p.screen().cursor_position(), (4, 9));
    }

    #[test]
    fn newline_cr_lf() {
        let mut p = Parser::new(5, 10, 0);
        p.process(b"A\r\nB");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        assert_eq!(p.screen().cell(1, 0).unwrap().contents(), "B");
    }

    #[test]
    fn tab() {
        let mut p = Parser::new(1, 40, 0);
        p.process(b"\t");
        assert_eq!(p.screen().cursor_position(), (0, 8));
        p.process(b"\t");
        assert_eq!(p.screen().cursor_position(), (0, 16));
    }

    #[test]
    fn cursor_movement() {
        let mut p = Parser::new(10, 20, 0);
        p.process(b"\x1b[5;10H"); // (4,9)
        p.process(b"\x1b[2A"); // up 2 -> (2,9)
        assert_eq!(p.screen().cursor_position(), (2, 9));
        p.process(b"\x1b[3B"); // down 3 -> (5,9)
        assert_eq!(p.screen().cursor_position(), (5, 9));
        p.process(b"\x1b[4C"); // right 4 -> (5,13)
        assert_eq!(p.screen().cursor_position(), (5, 13));
        p.process(b"\x1b[2D"); // left 2 -> (5,11)
        assert_eq!(p.screen().cursor_position(), (5, 11));
    }

    #[test]
    fn scrollback() {
        let mut p = Parser::new(3, 10, 100);
        // Write 5 lines in a 3-row screen — should push 2 into scrollback
        p.process(b"A\r\nB\r\nC\r\nD\r\nE");
        assert!(p.screen().scrollback() == 0);
        p.screen_mut().set_scrollback(2);
        assert_eq!(p.screen().scrollback(), 2);
        // First visible row should be from scrollback
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
    }

    #[test]
    fn resize() {
        let mut p = Parser::new(3, 10, 0);
        p.process(b"AB");
        p.screen_mut().set_size(5, 20);
        assert_eq!(p.screen().size(), (5, 20));
        // Content preserved
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
    }

    #[test]
    fn wide_char() {
        let p = parse("日");
        assert!(p.screen().cell(0, 0).unwrap().is_wide());
        assert!(p.screen().cell(0, 1).unwrap().is_wide_continuation());
        assert_eq!(p.screen().cursor_position(), (0, 2));
    }

    #[test]
    fn hide_cursor() {
        let mut p = Parser::new(3, 10, 0);
        assert!(!p.screen().hide_cursor());
        p.process(b"\x1b[?25l");
        assert!(p.screen().hide_cursor());
        p.process(b"\x1b[?25h");
        assert!(!p.screen().hide_cursor());
    }

    #[test]
    fn insert_delete_chars() {
        let mut p = Parser::new(1, 10, 0);
        p.process(b"ABCDE");
        p.process(b"\x1b[2G"); // col 1
        p.process(b"\x1b[2P"); // delete 2 chars
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        assert_eq!(p.screen().cell(0, 1).unwrap().contents(), "D");
    }

    #[test]
    fn btop_startup_sequence() {
        let mut p = Parser::new(25, 80, 100);
        p.process(b"\x1b[?1049h");
        p.process(b"\x1b[?25l");
        p.process(b"\x1b[?1002h");
        p.process(b"\x1b[?1015h"); // urxvt mouse — accepted silently
        p.process(b"\x1b[?1006h");
        p.process(b"\x1b[2J");
        p.process(b"\x1b[0;0f"); // HVP

        assert!(p.screen().alternate_screen());
        assert!(p.screen().hide_cursor());
        assert_eq!(p.screen().cursor_position(), (0, 0));

        p.process(b"\x1b[38;2;100;200;50mHello btop");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "H");
        assert_eq!(
            p.screen().cell(0, 0).unwrap().fgcolor(),
            crate::vt::color::Color::Rgb(100, 200, 50)
        );
    }

    // ---------------------------------------------------------------
    // Edge case tests for verified fixes
    // ---------------------------------------------------------------

    // Fix #4: ris() preserves scrollback capacity
    #[test]
    fn ris_preserves_scrollback_capacity() {
        let mut p = Parser::new(3, 10, 500);
        // Write some lines to push into scrollback
        for _ in 0..10 {
            p.process(b"line\r\n");
        }
        // Full reset
        p.process(b"\x1bc");
        // Write more lines — scrollback should still work
        for _ in 0..10 {
            p.process(b"after\r\n");
        }
        // Should have scrollback available (capacity was preserved at 500)
        p.screen_mut().set_scrollback(5);
        assert!(p.screen().scrollback() > 0);
    }

    // Fix #7: SGR malformed subparam doesn't kill remaining codes
    #[test]
    fn sgr_malformed_fg_continues() {
        let mut p = Parser::new(3, 10, 0);
        // ESC[38;9m — unknown subparam after 38, then ESC[1m — bold
        // The 38;9 should be skipped, but 1m should still apply
        p.process(b"\x1b[38;9m\x1b[1mB");
        assert!(p.screen().cell(0, 0).unwrap().bold());
    }

    #[test]
    fn sgr_malformed_bg_continues() {
        let mut p = Parser::new(3, 10, 0);
        // ESC[48;99m — unknown subparam after 48
        // Then ESC[3m — italic. Should still apply.
        p.process(b"\x1b[48;99m\x1b[3mI");
        assert!(p.screen().cell(0, 0).unwrap().italic());
    }

    // Fix #8: ED mode 3 clears scrollback
    #[test]
    fn ed_mode3_clears_scrollback() {
        let mut p = Parser::new(3, 10, 100);
        // Push lines into scrollback
        for _ in 0..10 {
            p.process(b"line\r\n");
        }
        // Verify scrollback exists
        p.screen_mut().set_scrollback(5);
        assert!(p.screen().scrollback() > 0);
        p.screen_mut().set_scrollback(0);

        // ESC[3J — erase display and scrollback
        p.process(b"\x1b[3J");

        // Scrollback should be empty
        p.screen_mut().set_scrollback(100); // try to scroll back as far as possible
        assert_eq!(p.screen().scrollback(), 0); // clamped to 0 — nothing in scrollback
    }

    #[test]
    fn ed_mode2_preserves_scrollback() {
        let mut p = Parser::new(3, 10, 100);
        for _ in 0..10 {
            p.process(b"line\r\n");
        }
        // ESC[2J — erase display only (not scrollback)
        p.process(b"\x1b[2J");

        // Scrollback should still have content
        p.screen_mut().set_scrollback(100);
        assert!(p.screen().scrollback() > 0);
    }

    // Fix #10: insert/delete cells batch
    #[test]
    fn insert_cells_batch() {
        let mut p = Parser::new(1, 10, 0);
        p.process(b"ABCDEFGHIJ"); // fill row
        p.process(b"\x1b[3G"); // cursor to col 2
        p.process(b"\x1b[3@"); // insert 3 cells
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        assert_eq!(p.screen().cell(0, 1).unwrap().contents(), "B");
        assert!(!p.screen().cell(0, 2).unwrap().has_contents()); // inserted blank
        assert!(!p.screen().cell(0, 3).unwrap().has_contents()); // inserted blank
        assert!(!p.screen().cell(0, 4).unwrap().has_contents()); // inserted blank
        assert_eq!(p.screen().cell(0, 5).unwrap().contents(), "C");
        // Last 3 chars (H, I, J) should be pushed off the end
    }

    #[test]
    fn delete_cells_batch() {
        let mut p = Parser::new(1, 10, 0);
        p.process(b"ABCDEFGHIJ");
        p.process(b"\x1b[2G"); // cursor to col 1
        p.process(b"\x1b[3P"); // delete 3 cells
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        assert_eq!(p.screen().cell(0, 1).unwrap().contents(), "E");
        assert_eq!(p.screen().cell(0, 6).unwrap().contents(), "J");
        // Last 3 cells should be blank
        assert!(!p.screen().cell(0, 7).unwrap().has_contents());
    }

    // Fix #11: Wide char on 1-column screen
    #[test]
    fn wide_char_1col_screen() {
        let mut p = Parser::new(3, 1, 0);
        p.process("日".as_bytes()); // wide char, won't fit
                                    // Should not panic, char is skipped
        assert!(!p.screen().cell(0, 0).unwrap().has_contents());
        // Narrow chars should still work
        p.process(b"A");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
    }

    #[test]
    fn wide_char_2col_screen() {
        let mut p = Parser::new(3, 2, 0);
        p.process("日".as_bytes());
        assert!(p.screen().cell(0, 0).unwrap().is_wide());
        assert!(p.screen().cell(0, 1).unwrap().is_wide_continuation());
    }

    // Fix #12: Missing CSI sequences
    #[test]
    fn window_manipulation_accepted() {
        let mut p = Parser::new(3, 10, 0);
        // CSI 8;25;80 t — resize window (should be silently accepted)
        p.process(b"\x1b[8;25;80t");
        p.process(b"OK");
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "O");
    }

    #[test]
    fn decsed_decsel() {
        let mut p = Parser::new(3, 10, 0);
        p.process(b"HELLO");
        // CSI ? 2 J — DEC selective erase (treated as ED)
        p.process(b"\x1b[?2J");
        assert!(!p.screen().cell(0, 0).unwrap().has_contents());
    }

    // Fix #13: BEL accepted
    #[test]
    fn bel_accepted() {
        let mut p = Parser::new(3, 10, 0);
        p.process(b"\x07OK"); // BEL followed by text
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "O");
    }

    // Fix #14: erase_row_backward with empty row (was a potential panic)
    #[test]
    fn erase_backward_edge() {
        let mut p = Parser::new(3, 5, 0);
        p.process(b"ABC");
        p.process(b"\x1b[1K"); // erase from cursor backward
                               // Should erase cols 0..=2 (cursor is at col 3 after writing ABC)
        assert!(!p.screen().cell(0, 0).unwrap().has_contents());
        assert!(!p.screen().cell(0, 1).unwrap().has_contents());
        assert!(!p.screen().cell(0, 2).unwrap().has_contents());
    }

    // Additional edge tests
    #[test]
    fn cursor_home_no_params() {
        let p = parse("\x1b[H");
        assert_eq!(p.screen().cursor_position(), (0, 0));
    }

    #[test]
    fn cursor_zero_params_default() {
        // ESC[0A should behave like ESC[1A (0 treated as default)
        let mut p = Parser::new(10, 10, 0);
        p.process(b"\x1b[5;5H"); // (4,4)
        p.process(b"\x1b[0A"); // up 0 → treated as up 1
        assert_eq!(p.screen().cursor_position(), (3, 4));
    }

    #[test]
    fn decstbm_reset_no_params() {
        let mut p = Parser::new(10, 10, 0);
        p.process(b"\x1b[3;7r"); // set region 3-7
        p.process(b"\x1b[r"); // reset to full screen
                              // Cursor should be at home after DECSTBM
        assert_eq!(p.screen().cursor_position(), (0, 0));
    }

    #[test]
    fn wrap_writes_char_on_next_line() {
        let mut p = Parser::new(3, 5, 0);
        p.process(b"ABCDE"); // fill row 0 — cursor at (0,4), pending wrap
        p.process(b"F"); // triggers wrap, F goes to (1,0)
        assert_eq!(p.screen().cell(0, 4).unwrap().contents(), "E");
        assert_eq!(p.screen().cell(1, 0).unwrap().contents(), "F");
    }

    #[test]
    fn wide_char_wrap_at_end() {
        let mut p = Parser::new(3, 5, 0);
        p.process(b"ABCD"); // cursor at (0,4)
        p.process("日".as_bytes()); // wide char needs 2 cols, doesn't fit at col 4
                                    // Col 4 should be blank (cleared), wide char on next line
        assert!(!p.screen().cell(0, 4).unwrap().has_contents());
        assert!(p.screen().cell(1, 0).unwrap().is_wide());
        assert_eq!(p.screen().cell(1, 0).unwrap().contents(), "日");
    }

    #[test]
    fn combining_char_append() {
        let mut p = Parser::new(3, 10, 0);
        p.process("e\u{0301}".as_bytes()); // e + combining acute
        let cell = p.screen().cell(0, 0).unwrap();
        assert!(cell.has_contents());
        assert_eq!(cell.contents().chars().count(), 2); // base + combining
    }

    #[test]
    fn scroll_region_scroll() {
        let mut p = Parser::new(5, 10, 0);
        p.process(b"\x1b[2;4r"); // scroll region rows 2-4
        p.process(b"\x1b[1;1H"); // home
        p.process(b"\x1b[1;1HTOP"); // row 0 outside region
        p.process(b"\x1b[4;1HIN_REGION"); // row 3 inside region
        p.process(b"\n"); // LF at bottom of region — should scroll region
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "T"); // row 0 unaffected
    }

    #[test]
    fn insert_lines() {
        let mut p = Parser::new(5, 5, 0);
        p.process(b"\x1b[1;1HA\r\nB\r\nC\r\nD\r\nE");
        p.process(b"\x1b[2;1H"); // row 1
        p.process(b"\x1b[2L"); // insert 2 lines
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A"); // unchanged
        assert!(!p.screen().cell(1, 0).unwrap().has_contents()); // inserted blank
        assert!(!p.screen().cell(2, 0).unwrap().has_contents()); // inserted blank
        assert_eq!(p.screen().cell(3, 0).unwrap().contents(), "B"); // shifted down
    }

    #[test]
    fn delete_lines() {
        let mut p = Parser::new(5, 5, 0);
        p.process(b"\x1b[1;1HA\r\nB\r\nC\r\nD\r\nE");
        p.process(b"\x1b[2;1H"); // row 1
        p.process(b"\x1b[1M"); // delete 1 line
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A"); // unchanged
        assert_eq!(p.screen().cell(1, 0).unwrap().contents(), "C"); // was row 2
        assert_eq!(p.screen().cell(2, 0).unwrap().contents(), "D"); // was row 3
    }

    #[test]
    fn multiple_sgr_in_one_sequence() {
        // ESC[1;31;42m — bold + red fg + green bg in one call
        let p = parse("\x1b[1;31;42mX");
        let cell = p.screen().cell(0, 0).unwrap();
        assert!(cell.bold());
        assert_eq!(cell.fgcolor(), crate::vt::color::Color::Idx(1));
        assert_eq!(cell.bgcolor(), crate::vt::color::Color::Idx(2));
    }

    #[test]
    fn overwrite_wide_char_left_half() {
        let mut p = Parser::new(3, 10, 0);
        p.process("日".as_bytes()); // cols 0-1
        p.process(b"\x1b[1G"); // back to col 0
        p.process(b"A"); // overwrite left half
        assert_eq!(p.screen().cell(0, 0).unwrap().contents(), "A");
        // Right half (continuation) should be cleared
        assert!(!p.screen().cell(0, 1).unwrap().is_wide_continuation());
    }

    #[test]
    fn overwrite_wide_char_right_half() {
        let mut p = Parser::new(3, 10, 0);
        p.process("日".as_bytes()); // cols 0-1
        p.process(b"\x1b[2G"); // col 1 (right half)
        p.process(b"B"); // overwrite right half
                         // Left half should be cleared since we broke the wide char
        assert!(!p.screen().cell(0, 0).unwrap().is_wide());
        assert_eq!(p.screen().cell(0, 1).unwrap().contents(), "B");
    }
}
