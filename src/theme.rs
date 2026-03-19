use ratatui::style::{Color, Modifier, Style};

pub struct Theme {
    pub bg: Color,
    pub border: Color,
    pub border_inactive: Color,
    pub title: Color,
    pub header_fg: Color,
    pub dir_fg: Color,
    pub file_fg: Color,
    pub symlink_fg: Color,
    pub exec_fg: Color,
    pub size_fg: Color,
    pub date_fg: Color,
    pub perm_fg: Color,
    pub highlight_bg: Color,
    pub highlight_fg: Color,
    pub error_fg: Color,
    pub search_label_fg: Color,
    pub search_label_bg: Color,
    pub search_text_fg: Color,
    pub path_active_fg: Color,
    pub path_inactive_fg: Color,
    pub footer_key_fg: Color,
    pub footer_key_bg: Color,
    pub footer_sep_fg: Color,
    pub footer_sep_bg: Color,
    pub dialog_bg: Color,
    pub dialog_border_fg: Color,
    pub dialog_title_fg: Color,
    pub dialog_text_fg: Color,
    pub dialog_input_fg: Color,
    pub dialog_prompt_fg: Color,
    pub dialog_cursor_fg: Color,
    pub dialog_input_fg_focused: Color,
    pub dialog_input_bg: Color,
    #[allow(dead_code)]
    pub dialog_hint_fg: Color,
    pub selected_fg: Color,
    pub viewer_line_num_fg: Color,
    pub viewer_text_fg: Color,
    pub viewer_hint_fg: Color,
    pub viewer_hint_bg: Color,
}

impl Theme {
    pub fn far_manager() -> Self {
        let bg = Color::Rgb(0, 0, 128);
        Self {
            bg,
            border: Color::Cyan,
            border_inactive: Color::Cyan,
            title: Color::Cyan,
            header_fg: Color::Yellow,
            dir_fg: Color::White,
            file_fg: Color::LightCyan,
            symlink_fg: Color::LightCyan,
            exec_fg: Color::LightGreen,
            size_fg: Color::LightCyan,
            date_fg: Color::LightCyan,
            perm_fg: Color::Cyan,
            highlight_bg: Color::Cyan,
            highlight_fg: Color::Black,
            error_fg: Color::LightRed,
            search_label_fg: Color::Black,
            search_label_bg: Color::Yellow,
            search_text_fg: Color::White,
            path_active_fg: Color::White,
            path_inactive_fg: Color::Cyan,
            footer_key_fg: Color::Black,
            footer_key_bg: Color::Cyan,
            footer_sep_fg: Color::Cyan,
            footer_sep_bg: Color::Black,
            dialog_bg: Color::Rgb(192, 192, 192),
            dialog_border_fg: Color::Black,
            dialog_title_fg: Color::Black,
            dialog_text_fg: Color::Black,
            dialog_input_fg: Color::Black,
            dialog_prompt_fg: Color::Yellow,
            dialog_cursor_fg: Color::Black,
            dialog_input_fg_focused: Color::White,
            dialog_input_bg: Color::Rgb(0, 128, 128),
            dialog_hint_fg: Color::DarkGray,
            selected_fg: Color::Yellow,
            viewer_line_num_fg: Color::Yellow,
            viewer_text_fg: Color::LightCyan,
            viewer_hint_fg: Color::Black,
            viewer_hint_bg: Color::Cyan,
        }
    }
}

// Convenience style builders
impl Theme {
    pub fn bg_style(&self) -> Style {
        Style::default().bg(self.bg)
    }

    pub fn border_style(&self, active: bool) -> Style {
        let color = if active {
            self.border
        } else {
            self.border_inactive
        };
        Style::default().fg(color).bg(self.bg)
    }

    pub fn title_style(&self) -> Style {
        Style::default().fg(self.title).bg(self.bg)
    }

    pub fn header_style(&self) -> Style {
        Style::default()
            .fg(self.header_fg)
            .bg(self.bg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn highlight_style(&self) -> Style {
        Style::default()
            .bg(self.highlight_bg)
            .fg(self.highlight_fg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn dir_style(&self) -> Style {
        Style::default()
            .fg(self.dir_fg)
            .bg(self.bg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn file_style(&self) -> Style {
        Style::default().fg(self.file_fg).bg(self.bg)
    }

    pub fn symlink_style(&self) -> Style {
        Style::default().fg(self.symlink_fg).bg(self.bg)
    }

    pub fn exec_style(&self) -> Style {
        Style::default().fg(self.exec_fg).bg(self.bg)
    }

    pub fn selected_style(&self) -> Style {
        Style::default()
            .fg(self.selected_fg)
            .bg(self.bg)
            .add_modifier(Modifier::BOLD)
    }

    pub fn dialog_bg_style(&self) -> Style {
        Style::default().bg(self.dialog_bg)
    }

    pub fn dialog_border_style(&self) -> Style {
        Style::default()
            .fg(self.dialog_border_fg)
            .bg(self.dialog_bg)
    }

    pub fn dialog_title_style(&self) -> Style {
        Style::default()
            .fg(self.dialog_title_fg)
            .bg(self.dialog_bg)
            .add_modifier(Modifier::BOLD)
    }

    #[allow(dead_code)]
    pub fn dialog_text_style(&self) -> Style {
        Style::default().fg(self.dialog_text_fg).bg(self.dialog_bg)
    }
}

/// Global theme accessor. Call `theme()` from any UI module.
pub fn theme() -> &'static Theme {
    use std::sync::OnceLock;
    static THEME: OnceLock<Theme> = OnceLock::new();
    THEME.get_or_init(Theme::far_manager)
}
