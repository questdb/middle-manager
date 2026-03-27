use crate::vt::attrs::Attrs;
use crate::vt::color::Color;

const FLAG_WIDE: u8 = 0x80;
const FLAG_WIDE_CONT: u8 = 0x40;
const LEN_MASK: u8 = 0x3F;
const MAX_CONTENT: usize = 24;

/// A single terminal cell containing a character (possibly multi-byte) and attributes.
#[derive(Clone, Debug)]
pub struct Cell {
    /// Inline content buffer (UTF-8 encoded).
    content: [u8; MAX_CONTENT],
    /// Low 6 bits = byte length, bit 7 = wide, bit 6 = wide continuation.
    len_flags: u8,
    /// Visual attributes (colors, bold, etc.).
    pub(crate) attrs: Attrs,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            content: [0; MAX_CONTENT],
            len_flags: 0,
            attrs: Attrs::default(),
        }
    }
}

impl Cell {
    fn len(&self) -> usize {
        (self.len_flags & LEN_MASK) as usize
    }

    /// The text content of this cell (a grapheme cluster).
    pub fn contents(&self) -> &str {
        let n = self.len();
        // SAFETY: we only ever store valid UTF-8 from `set` and `append`.
        unsafe { std::str::from_utf8_unchecked(&self.content[..n]) }
    }

    /// Whether this cell has any visible content.
    pub fn has_contents(&self) -> bool {
        self.len() > 0
    }

    /// Whether this cell is a wide (double-width) character.
    pub fn is_wide(&self) -> bool {
        self.len_flags & FLAG_WIDE != 0
    }

    /// Whether this cell is the continuation (right half) of a wide character.
    pub fn is_wide_continuation(&self) -> bool {
        self.len_flags & FLAG_WIDE_CONT != 0
    }

    pub fn bold(&self) -> bool {
        self.attrs.bold()
    }
    pub fn dim(&self) -> bool {
        self.attrs.dim()
    }
    pub fn italic(&self) -> bool {
        self.attrs.italic()
    }
    pub fn underline(&self) -> bool {
        self.attrs.underline()
    }
    pub fn inverse(&self) -> bool {
        self.attrs.inverse()
    }
    #[allow(dead_code)]
    pub fn strikethrough(&self) -> bool {
        self.attrs.strikethrough()
    }
    pub fn fgcolor(&self) -> Color {
        self.attrs.fgcolor
    }
    pub fn bgcolor(&self) -> Color {
        self.attrs.bgcolor
    }

    /// Set the cell to a single character with given attributes and width.
    pub(crate) fn set(&mut self, c: char, attrs: Attrs, wide: bool) {
        let mut buf = [0u8; 4];
        let s = c.encode_utf8(&mut buf);
        let n = s.len().min(MAX_CONTENT);
        self.content[..n].copy_from_slice(&buf[..n]);
        self.len_flags = n as u8;
        if wide {
            self.len_flags |= FLAG_WIDE;
        }
        self.attrs = attrs;
    }

    /// Mark this cell as a wide-continuation placeholder.
    pub(crate) fn set_continuation(&mut self, attrs: Attrs) {
        self.content[0] = 0;
        self.len_flags = FLAG_WIDE_CONT;
        self.attrs = attrs;
    }

    /// Append a combining character to the existing content.
    pub(crate) fn append(&mut self, c: char) {
        let mut buf = [0u8; 4];
        let s = c.encode_utf8(&mut buf);
        let cur = self.len();
        let new_len = cur + s.len();
        if new_len <= MAX_CONTENT {
            self.content[cur..new_len].copy_from_slice(s.as_bytes());
            self.len_flags = (self.len_flags & !LEN_MASK) | new_len as u8;
        }
    }

    /// Clear the cell, setting it to empty with the given attributes.
    pub(crate) fn clear(&mut self, attrs: Attrs) {
        self.len_flags = 0;
        self.attrs = attrs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cell() {
        let c = Cell::default();
        assert!(!c.has_contents());
        assert_eq!(c.contents(), "");
        assert!(!c.is_wide());
        assert!(!c.is_wide_continuation());
    }

    #[test]
    fn set_ascii() {
        let mut c = Cell::default();
        c.set('A', Attrs::default(), false);
        assert_eq!(c.contents(), "A");
        assert!(c.has_contents());
        assert!(!c.is_wide());
    }

    #[test]
    fn set_wide() {
        let mut c = Cell::default();
        c.set('日', Attrs::default(), true);
        assert_eq!(c.contents(), "日");
        assert!(c.is_wide());
    }

    #[test]
    fn continuation() {
        let mut c = Cell::default();
        c.set_continuation(Attrs::default());
        assert!(c.is_wide_continuation());
        assert!(!c.has_contents());
    }

    #[test]
    fn append_combining() {
        let mut c = Cell::default();
        c.set('e', Attrs::default(), false);
        c.append('\u{0301}'); // combining acute accent
        assert_eq!(c.contents(), "e\u{0301}"); // NFD form (decomposed)
        assert!(c.has_contents());
        assert_eq!(c.contents().chars().count(), 2); // base + combining
    }

    #[test]
    fn clear_cell() {
        let mut c = Cell::default();
        c.set('X', Attrs::default(), false);
        c.clear(Attrs::default());
        assert!(!c.has_contents());
        assert_eq!(c.contents(), "");
    }

    #[test]
    fn attr_delegation() {
        let mut attrs = Attrs::default();
        attrs.set_bold();
        attrs.fgcolor = crate::vt::color::Color::Idx(1);
        let mut c = Cell::default();
        c.set('A', attrs, false);
        assert!(c.bold());
        assert_eq!(c.fgcolor(), crate::vt::color::Color::Idx(1));
    }
}
