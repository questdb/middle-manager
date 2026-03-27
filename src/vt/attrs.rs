use crate::vt::color::Color;

const INTENSITY_MASK: u8 = 0b0000_0011;
const BOLD: u8 = 0b0000_0001;
const DIM: u8 = 0b0000_0010;
const ITALIC: u8 = 0b0000_0100;
const UNDERLINE: u8 = 0b0000_1000;
const INVERSE: u8 = 0b0001_0000;
const STRIKETHROUGH: u8 = 0b0010_0000;

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct Attrs {
    pub fgcolor: Color,
    pub bgcolor: Color,
    mode: u8,
}

impl Attrs {
    pub fn bold(&self) -> bool {
        self.mode & BOLD != 0
    }

    pub fn dim(&self) -> bool {
        self.mode & DIM != 0
    }

    pub fn set_bold(&mut self) {
        self.mode &= !INTENSITY_MASK;
        self.mode |= BOLD;
    }

    pub fn set_dim(&mut self) {
        self.mode &= !INTENSITY_MASK;
        self.mode |= DIM;
    }

    pub fn set_normal_intensity(&mut self) {
        self.mode &= !INTENSITY_MASK;
    }

    pub fn italic(&self) -> bool {
        self.mode & ITALIC != 0
    }

    pub fn set_italic(&mut self, on: bool) {
        if on {
            self.mode |= ITALIC;
        } else {
            self.mode &= !ITALIC;
        }
    }

    pub fn underline(&self) -> bool {
        self.mode & UNDERLINE != 0
    }

    pub fn set_underline(&mut self, on: bool) {
        if on {
            self.mode |= UNDERLINE;
        } else {
            self.mode &= !UNDERLINE;
        }
    }

    pub fn inverse(&self) -> bool {
        self.mode & INVERSE != 0
    }

    pub fn set_inverse(&mut self, on: bool) {
        if on {
            self.mode |= INVERSE;
        } else {
            self.mode &= !INVERSE;
        }
    }

    #[allow(dead_code)]
    pub fn strikethrough(&self) -> bool {
        self.mode & STRIKETHROUGH != 0
    }

    pub fn set_strikethrough(&mut self, on: bool) {
        if on {
            self.mode |= STRIKETHROUGH;
        } else {
            self.mode &= !STRIKETHROUGH;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_attrs() {
        let a = Attrs::default();
        assert!(!a.bold());
        assert!(!a.dim());
        assert!(!a.italic());
        assert!(!a.underline());
        assert!(!a.inverse());
        assert!(!a.strikethrough());
        assert_eq!(a.fgcolor, Color::Default);
        assert_eq!(a.bgcolor, Color::Default);
    }

    #[test]
    fn bold_dim_mutual_exclusion() {
        let mut a = Attrs::default();
        a.set_bold();
        assert!(a.bold());
        assert!(!a.dim());
        a.set_dim();
        assert!(!a.bold());
        assert!(a.dim());
        a.set_normal_intensity();
        assert!(!a.bold());
        assert!(!a.dim());
    }

    #[test]
    fn independent_flags() {
        let mut a = Attrs::default();
        a.set_italic(true);
        a.set_underline(true);
        a.set_inverse(true);
        a.set_strikethrough(true);
        assert!(a.italic());
        assert!(a.underline());
        assert!(a.inverse());
        assert!(a.strikethrough());
        a.set_italic(false);
        assert!(!a.italic());
        assert!(a.underline()); // others unchanged
    }
}
