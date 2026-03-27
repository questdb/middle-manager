/// Foreground or background color for terminal cells.
#[derive(Eq, PartialEq, Debug, Copy, Clone, Default)]
pub enum Color {
    /// The default terminal color.
    #[default]
    Default,
    /// An indexed color (0-255).
    Idx(u8),
    /// A 24-bit RGB color.
    Rgb(u8, u8, u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_default() {
        assert_eq!(Color::default(), Color::Default);
    }

    #[test]
    fn equality() {
        assert_eq!(Color::Idx(5), Color::Idx(5));
        assert_ne!(Color::Idx(5), Color::Idx(6));
        assert_eq!(Color::Rgb(1, 2, 3), Color::Rgb(1, 2, 3));
        assert_ne!(Color::Default, Color::Idx(0));
    }
}
