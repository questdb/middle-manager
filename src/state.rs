use serde::{Deserialize, Serialize};
use std::path::PathBuf;

fn default_true() -> bool {
    true
}

fn default_split() -> u16 {
    60
}

/// Persistent application state saved to ~/.config/middle-manager/state.json
#[derive(Debug, Serialize, Deserialize)]
pub struct AppState {
    /// Color theme name.
    #[serde(default)]
    pub theme: String,

    /// Last editor search parameters.
    #[serde(default)]
    pub search_query: String,
    #[serde(default)]
    pub search_direction_forward: bool,
    #[serde(default)]
    pub search_case_sensitive: bool,

    /// Last file content search parameters.
    #[serde(default)]
    pub file_search_term: String,
    #[serde(default)]
    pub file_search_replace: String,
    #[serde(default)]
    pub file_search_filter: String,
    #[serde(default)]
    pub file_search_file_type: String,
    #[serde(default)]
    pub file_search_type_exclude: String,
    #[serde(default)]
    pub file_search_regex: bool,
    #[serde(default)]
    pub file_search_case_insensitive: bool,
    #[serde(default)]
    pub file_search_smart_case: bool,
    #[serde(default)]
    pub file_search_whole_word: bool,
    #[serde(default)]
    pub file_search_whole_line: bool,
    #[serde(default)]
    pub file_search_invert_match: bool,
    #[serde(default)]
    pub file_search_multiline: bool,
    #[serde(default)]
    pub file_search_multiline_dotall: bool,
    #[serde(default)]
    pub file_search_crlf: bool,
    #[serde(default)]
    pub file_search_hidden: bool,
    #[serde(default)]
    pub file_search_follow_symlinks: bool,
    #[serde(default)]
    pub file_search_no_gitignore: bool,
    #[serde(default)]
    pub file_search_binary: bool,
    #[serde(default)]
    pub file_search_search_zip: bool,
    #[serde(default)]
    pub file_search_glob_case_insensitive: bool,
    #[serde(default)]
    pub file_search_one_file_system: bool,
    #[serde(default)]
    pub file_search_trim: bool,
    #[serde(default)]
    pub file_search_before_context: String,
    #[serde(default)]
    pub file_search_after_context: String,
    #[serde(default)]
    pub file_search_max_depth: String,
    #[serde(default)]
    pub file_search_max_count: String,
    #[serde(default)]
    pub file_search_max_filesize: String,
    #[serde(default)]
    pub file_search_encoding: String,

    /// Last panel paths.
    #[serde(default)]
    pub left_panel_path: Option<String>,
    #[serde(default)]
    pub right_panel_path: Option<String>,

    /// Bottom panel split sizes (percentage for file panel, per type).
    #[serde(default = "default_split")]
    pub split_pct_ci: u16,
    #[serde(default = "default_split")]
    pub split_pct_shell: u16,
    #[serde(default = "default_split")]
    pub split_pct_claude: u16,

    /// Open bottom panels per side: "ci", "shell", "claude", or empty.
    /// Multiple can be open; stored as comma-separated.
    #[serde(default)]
    pub left_bottom_panels: String,
    #[serde(default)]
    pub right_bottom_panels: String,

    /// Directory Claude was spawned in (may differ from the panel's current dir).
    #[serde(default)]
    pub claude_dir_left: Option<String>,
    #[serde(default)]
    pub claude_dir_right: Option<String>,

    /// SSH host names for restoration.
    #[serde(default)]
    pub ssh_host_left: Option<String>,
    #[serde(default)]
    pub ssh_host_right: Option<String>,
    #[serde(default = "default_split")]
    pub split_pct_ssh: u16,

    /// Panel sort preferences.
    #[serde(default)]
    pub left_sort_field: u8,
    #[serde(default = "default_true")]
    pub left_sort_ascending: bool,
    #[serde(default)]
    pub right_sort_field: u8,
    #[serde(default = "default_true")]
    pub right_sort_ascending: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            theme: String::new(),
            search_query: String::new(),
            search_direction_forward: false,
            search_case_sensitive: false,
            file_search_term: String::new(),
            file_search_replace: String::new(),
            file_search_filter: String::new(),
            file_search_file_type: String::new(),
            file_search_type_exclude: String::new(),
            file_search_regex: false,
            file_search_case_insensitive: false,
            file_search_smart_case: false,
            file_search_whole_word: false,
            file_search_whole_line: false,
            file_search_invert_match: false,
            file_search_multiline: false,
            file_search_multiline_dotall: false,
            file_search_crlf: false,
            file_search_hidden: false,
            file_search_follow_symlinks: false,
            file_search_no_gitignore: false,
            file_search_binary: false,
            file_search_search_zip: false,
            file_search_glob_case_insensitive: false,
            file_search_one_file_system: false,
            file_search_trim: false,
            file_search_before_context: String::new(),
            file_search_after_context: String::new(),
            file_search_max_depth: String::new(),
            file_search_max_count: String::new(),
            file_search_max_filesize: String::new(),
            file_search_encoding: String::new(),
            left_panel_path: None,
            right_panel_path: None,
            split_pct_ci: 60,
            split_pct_shell: 60,
            split_pct_claude: 60,
            left_bottom_panels: String::new(),
            right_bottom_panels: String::new(),
            claude_dir_left: None,
            claude_dir_right: None,
            ssh_host_left: None,
            ssh_host_right: None,
            split_pct_ssh: 60,
            left_sort_field: 0,
            left_sort_ascending: true,
            right_sort_field: 0,
            right_sort_ascending: true,
        }
    }
}

impl AppState {
    /// Load state from disk. Returns default if file doesn't exist or is corrupt.
    pub fn load() -> Self {
        let path = state_file_path();
        match std::fs::read_to_string(&path) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Save state to disk. Errors are silently ignored.
    pub fn save(&self) {
        let path = state_file_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write(&path, json);
        }
    }
}

fn state_file_path() -> PathBuf {
    crate::remote_fs::config_dir().join("state.json")
}
