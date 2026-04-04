use crossterm::cursor::SetCursorStyle;
use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeName {
    FarManager,
    QuestdbDark,
}

impl ThemeName {
    pub fn label(&self) -> &'static str {
        match self {
            Self::FarManager => "Far Manager (Classic)",
            Self::QuestdbDark => "QuestDB Dark",
        }
    }

    #[allow(dead_code)]
    pub fn all() -> &'static [ThemeName] {
        &[ThemeName::FarManager, ThemeName::QuestdbDark]
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "questdb_dark" => Self::QuestdbDark,
            _ => Self::FarManager,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            Self::FarManager => "far_manager",
            Self::QuestdbDark => "questdb_dark",
        }
    }

    pub fn next(&self) -> Self {
        match self {
            Self::FarManager => Self::QuestdbDark,
            Self::QuestdbDark => Self::FarManager,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            Self::FarManager => Self::QuestdbDark,
            Self::QuestdbDark => Self::FarManager,
        }
    }
}

#[derive(Clone)]
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
    pub footer_fkey_fg: Color,
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
    // Syntax highlighting
    pub syn_keyword: Color,
    pub syn_function: Color,
    pub syn_type: Color,
    pub syn_string: Color,
    pub syn_number: Color,
    pub syn_comment: Color,
    pub syn_variable: Color,
    pub syn_constant: Color,
    pub syn_operator: Color,
    pub syn_punctuation: Color,
    pub syn_attribute: Color,
    pub syn_tag: Color,
    pub syn_property: Color,
    pub syn_escape: Color,
    pub syn_constructor: Color,
    // Editor / dialog cursor
    pub editor_text_fg: Color,
    pub editor_cursor: SetCursorStyle,
    // Diff viewer backgrounds
    pub diff_added_bg: Color,
    pub diff_deleted_bg: Color,
    pub diff_changed_old_bg: Color,
    pub diff_changed_new_bg: Color,
    // Git
    pub git_modified_fg: Color,
    pub git_added_fg: Color,
    pub git_deleted_fg: Color,
    pub git_untracked_fg: Color,
    pub git_conflict_fg: Color,
    pub git_renamed_fg: Color,
    pub git_branch_fg: Color,
}

impl Theme {
    pub fn for_name(name: ThemeName) -> Self {
        match name {
            ThemeName::FarManager => Self::far_manager(),
            ThemeName::QuestdbDark => Self::questdb_dark(),
        }
    }

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
            footer_fkey_fg: Color::Rgb(0, 0, 128),
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
            syn_keyword: Color::Yellow,
            syn_function: Color::LightBlue,
            syn_type: Color::Magenta,
            syn_string: Color::LightGreen,
            syn_number: Color::Cyan,
            syn_comment: Color::DarkGray,
            syn_variable: Color::White,
            syn_constant: Color::Cyan,
            syn_operator: Color::White,
            syn_punctuation: Color::White,
            syn_attribute: Color::Yellow,
            syn_tag: Color::LightRed,
            syn_property: Color::LightCyan,
            syn_escape: Color::LightRed,
            syn_constructor: Color::Yellow,
            diff_added_bg: Color::Rgb(0, 50, 0),
            diff_deleted_bg: Color::Rgb(80, 0, 0),
            diff_changed_old_bg: Color::Rgb(80, 40, 0),
            diff_changed_new_bg: Color::Rgb(0, 50, 30),
            editor_text_fg: Color::White,
            editor_cursor: SetCursorStyle::BlinkingBar,
            git_modified_fg: Color::Yellow,
            git_added_fg: Color::LightGreen,
            git_deleted_fg: Color::LightRed,
            git_untracked_fg: Color::DarkGray,
            git_conflict_fg: Color::LightRed,
            git_renamed_fg: Color::Magenta,
            git_branch_fg: Color::LightGreen,
        }
    }

    pub fn questdb_dark() -> Self {
        let bg = Color::Rgb(26, 21, 32);          // #1a1520 deep purple-black
        let surface = Color::Rgb(36, 30, 46);      // #241e2e panel surfaces
        let rose = Color::Rgb(209, 70, 113);        // #d14671 QuestDB primary
        let magenta = Color::Rgb(137, 44, 108);     // #892c6c QuestDB deep
        let pink_light = Color::Rgb(232, 121, 154); // #e8799a soft pink
        let text = Color::Rgb(232, 228, 237);        // #e8e4ed cool light grey
        let text_dim = Color::Rgb(154, 143, 176);    // #9a8fb0 muted lavender
        let text_very_dim = Color::Rgb(107, 90, 132);// #6b5a84 dim purple
        let selection = Color::Rgb(46, 39, 64);       // #2e2740 selection bg
        let green = Color::Rgb(130, 210, 150);        // #82d296 soft green
        let blue = Color::Rgb(130, 160, 230);          // #82a0e6 soft blue
        let red = Color::Rgb(240, 100, 100);           // #f06464 soft red
        let purple_light = Color::Rgb(195, 140, 220); // #c38cdc light purple

        Self {
            bg,
            border: rose,
            border_inactive: Color::Rgb(74, 56, 96), // #4a3860
            title: rose,
            header_fg: pink_light,
            dir_fg: text,
            file_fg: Color::Rgb(186, 178, 201),       // #bab2c9 light lavender
            symlink_fg: text_dim,
            exec_fg: green,
            size_fg: text_dim,
            date_fg: text_dim,
            perm_fg: text_very_dim,
            highlight_bg: magenta,
            highlight_fg: Color::White,
            error_fg: red,
            search_label_fg: Color::White,
            search_label_bg: rose,
            search_text_fg: text,
            path_active_fg: text,
            path_inactive_fg: text_very_dim,
            footer_key_fg: Color::Rgb(26, 21, 32),         // dark bg text on pink buttons
            footer_key_bg: Color::Rgb(209, 70, 113),        // rose button background
            footer_fkey_fg: Color::Rgb(255, 230, 240),       // #ffe6f0 near-white pink for "F1" etc.
            footer_sep_fg: Color::Rgb(137, 44, 108),         // magenta separator
            footer_sep_bg: Color::Rgb(18, 14, 24),           // #120e18 very dark bar background
            dialog_bg: surface,
            dialog_border_fg: rose,
            dialog_title_fg: pink_light,
            dialog_text_fg: text,
            dialog_input_fg: text,
            dialog_prompt_fg: rose,
            dialog_cursor_fg: text,
            dialog_input_fg_focused: Color::White,
            dialog_input_bg: selection,
            dialog_hint_fg: text_very_dim,
            selected_fg: pink_light,
            viewer_line_num_fg: magenta,
            viewer_text_fg: text,
            viewer_hint_fg: bg,
            viewer_hint_bg: rose,
            syn_keyword: rose,
            syn_function: blue,
            syn_type: Color::Rgb(195, 65, 112),       // #c34170 QuestDB pink alt
            syn_string: green,
            syn_number: pink_light,
            syn_comment: text_very_dim,
            syn_variable: text,
            syn_constant: purple_light,
            syn_operator: Color::Rgb(186, 178, 201),
            syn_punctuation: text_dim,
            syn_attribute: pink_light,
            syn_tag: red,
            syn_property: Color::Rgb(186, 178, 201),
            syn_escape: red,
            syn_constructor: pink_light,
            editor_text_fg: text,
            editor_cursor: SetCursorStyle::BlinkingBar,
            diff_added_bg: Color::Rgb(0, 50, 0),
            diff_deleted_bg: Color::Rgb(50, 0, 0),
            diff_changed_old_bg: Color::Rgb(80, 40, 0),
            diff_changed_new_bg: Color::Rgb(0, 50, 30),
            git_modified_fg: pink_light,
            git_added_fg: green,
            git_deleted_fg: red,
            git_untracked_fg: text_very_dim,
            git_conflict_fg: red,
            git_renamed_fg: purple_light,
            git_branch_fg: green,
        }
    }
}

// Convenience style builders
impl Theme {
    pub fn bg_style(&self) -> Style {
        Style::default().bg(self.bg)
    }

    pub fn border_style(&self, active: bool) -> Style {
        let color = if active { self.border } else { self.border_inactive };
        Style::default().fg(color).bg(self.bg)
    }

    pub fn title_style(&self) -> Style {
        Style::default().fg(self.title).bg(self.bg)
    }

    pub fn header_style(&self) -> Style {
        Style::default().fg(self.header_fg).bg(self.bg).add_modifier(Modifier::BOLD)
    }

    pub fn highlight_style(&self) -> Style {
        Style::default().bg(self.highlight_bg).fg(self.highlight_fg).add_modifier(Modifier::BOLD)
    }

    pub fn selected_highlight_style(&self) -> Style {
        Style::default().bg(self.highlight_bg).fg(Color::Rgb(100, 60, 0)).add_modifier(Modifier::BOLD)
    }

    pub fn dir_style(&self) -> Style {
        Style::default().fg(self.dir_fg).bg(self.bg).add_modifier(Modifier::BOLD)
    }

    pub fn input_selection_style(&self) -> Style {
        Style::default().fg(Color::White).bg(Color::Black)
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
        Style::default().fg(self.selected_fg).bg(self.bg).add_modifier(Modifier::BOLD)
    }

    pub fn git_status_color(&self, status: crate::panel::git::GitFileStatus) -> Color {
        use crate::panel::git::GitFileStatus;
        match status {
            GitFileStatus::Modified => self.git_modified_fg,
            GitFileStatus::Added => self.git_added_fg,
            GitFileStatus::Deleted => self.git_deleted_fg,
            GitFileStatus::Renamed => self.git_renamed_fg,
            GitFileStatus::Untracked => self.git_untracked_fg,
            GitFileStatus::Conflict => self.git_conflict_fg,
        }
    }

    pub fn dialog_bg_style(&self) -> Style {
        Style::default().bg(self.dialog_bg)
    }

    pub fn dialog_border_style(&self) -> Style {
        Style::default().fg(self.dialog_border_fg).bg(self.dialog_bg)
    }

    pub fn dialog_title_style(&self) -> Style {
        Style::default().fg(self.dialog_title_fg).bg(self.dialog_bg).add_modifier(Modifier::BOLD)
    }

    #[allow(dead_code)]
    pub fn dialog_text_style(&self) -> Style {
        Style::default().fg(self.dialog_text_fg).bg(self.dialog_bg)
    }
}

// --- Global theme state ---

use std::sync::RwLock;

static THEME: RwLock<Option<Theme>> = RwLock::new(None);
static THEME_NAME: RwLock<ThemeName> = RwLock::new(ThemeName::FarManager);
static THEME_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the active theme.
pub fn set_theme(name: ThemeName) {
    let t = Theme::for_name(name);
    *THEME_NAME.write().unwrap() = name;
    *THEME.write().unwrap() = Some(t);
    THEME_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Get the current theme name.
pub fn current_theme_name() -> ThemeName {
    *THEME_NAME.read().unwrap()
}

/// Get a clone of the active theme.
/// Uses a thread-local cache to avoid RwLock acquisition on every call within the same frame.
pub fn theme() -> Theme {
    use std::cell::RefCell;
    use std::sync::atomic::Ordering;

    thread_local! {
        static CACHED: RefCell<(u64, Option<Theme>)> = RefCell::new((0, None));
    }

    let gen = THEME_GENERATION.load(Ordering::Relaxed);
    let cached = CACHED.with(|c| {
        let borrow = c.borrow();
        if borrow.0 == gen {
            borrow.1.clone()
        } else {
            None
        }
    });

    if let Some(t) = cached {
        return t;
    }

    // Cache miss -- read from RwLock
    let t = {
        let guard = THEME.read().unwrap();
        if let Some(ref t) = *guard {
            t.clone()
        } else {
            drop(guard); // drop read lock before set_theme acquires write lock
            set_theme(ThemeName::FarManager);
            THEME.read().unwrap().as_ref().unwrap().clone()
        }
    };

    // Re-read generation (set_theme may have incremented it)
    let current_gen = THEME_GENERATION.load(Ordering::Relaxed);
    CACHED.with(|c| {
        *c.borrow_mut() = (current_gen, Some(t.clone()));
    });
    t
}
