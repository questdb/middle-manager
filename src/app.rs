use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

const DOUBLE_CLICK_MS: u128 = 400;

use crate::action::Action;
use crate::ci::CiPanel;
use crate::editor::EditorState;
use crate::fs_ops;
use crate::hex_viewer::HexViewerState;
use crate::panel::git::GitCache;
use crate::panel::sort::SortField;
use crate::panel::Panel;
use crate::state::AppState;
use crate::terminal::TerminalPanel;
use crate::watcher::DirWatcher;

fn sort_field_from_u8(v: u8) -> SortField {
    match v {
        1 => SortField::Size,
        2 => SortField::Date,
        _ => SortField::Name,
    }
}

fn sort_field_to_u8(f: SortField) -> u8 {
    match f {
        SortField::Name => 0,
        SortField::Size => 1,
        SortField::Date => 2,
    }
}
use crate::viewer::ViewerState;

pub struct App {
    pub panels: [Panel; 2],
    pub active_panel: usize,
    pub mode: AppMode,
    pub should_quit: bool,
    pub status_message: Option<String>,
    pub panel_areas: [Rect; 2],
    pub ci_panel_areas: [Option<Rect>; 2],
    last_click: Option<(Instant, u16, u16)>,
    /// Go-to-line prompt state. When Some, an input overlay is shown.
    pub goto_line_input: Option<String>,
    /// Set to true when the UI needs a full terminal clear (e.g. leaving full-screen mode).
    pub needs_clear: bool,
    /// Search dialog overlay (shown on top of editor).
    pub search_dialog: Option<SearchDialogState>,
    /// Unsaved changes confirmation dialog overlay.
    pub unsaved_dialog: Option<UnsavedDialogField>,
    /// Quit confirmation dialog: true = Quit focused, false = Cancel focused.
    pub quit_confirm: Option<bool>,
    /// Shared git status cache across panels.
    git_cache: GitCache,
    /// Persistent state (search, paths, sort, etc.)
    pub persisted: AppState,
    /// Filesystem watcher for auto-refresh on external changes.
    dir_watcher: Option<DirWatcher>,
    /// CI panels (one per file panel side, independently togglable).
    pub ci_panels: [Option<CiPanel>; 2],
    /// Which CI panel has focus (None = file panel has focus).
    pub ci_focused: Option<usize>,
    /// Go-to-path input per panel side. When Some, a path editor is shown at the top of the panel.
    pub goto_path: [Option<GotoPathState>; 2],
    /// Fuzzy file search per panel side.
    pub fuzzy_search: [Option<FuzzySearchState>; 2],
    /// Help dialog scroll offset.
    pub help_scroll: Option<usize>,
    /// Embedded terminal panel (runs `claude`).
    pub terminal_panel: Option<TerminalPanel>,
    /// Which panel side the terminal occupies (0=left, 1=right).
    pub terminal_side: usize,
    /// Whether the terminal panel has focus.
    pub terminal_focused: bool,
    /// Wakeup sender for the event loop (given to terminal reader threads).
    wakeup_sender: Option<crate::event::WakeupSender>,
}

pub struct GotoPathState {
    pub input: String,
    pub cursor: usize,
    /// Tab-completion candidates (directory names).
    pub completions: Vec<String>,
    /// Currently highlighted completion index.
    pub comp_index: Option<usize>,
    /// The prefix that was being completed (used to detect when input changes).
    pub comp_base: Option<String>,
}

/// Pre-computed data for each file path to avoid per-keystroke allocation.
struct FileEntry {
    /// Original relative path.
    path: String,
    /// Lowercase chars (pre-computed once).
    lower_chars: Vec<char>,
    /// Original chars (for word boundary checks).
    chars: Vec<char>,
    /// Char index where the filename starts (after last '/').
    filename_start: usize,
}

pub struct FuzzySearchState {
    pub input: String,
    pub cursor: usize,
    /// Pre-computed file entries.
    entries: Vec<FileEntry>,
    /// Filtered + ranked results: (index into entries, score).
    pub results: Vec<(usize, i64)>,
    /// Currently highlighted result index.
    pub selected: usize,
    /// Public access to paths for rendering.
    pub all_paths: Vec<String>,
}

impl FuzzySearchState {
    fn new(paths: Vec<String>) -> Self {
        let entries: Vec<FileEntry> = paths
            .iter()
            .map(|p| {
                let chars: Vec<char> = p.chars().collect();
                let lower_chars: Vec<char> = p.to_lowercase().chars().collect();
                let filename_start = chars
                    .iter()
                    .rposition(|&c| c == '/')
                    .map(|i| i + 1)
                    .unwrap_or(0);
                FileEntry {
                    path: p.clone(),
                    lower_chars,
                    chars,
                    filename_start,
                }
            })
            .collect();

        let mut state = Self {
            input: String::new(),
            cursor: 0,
            entries,
            results: Vec::new(),
            selected: 0,
            all_paths: paths,
        };
        state.update_results();
        state
    }

    fn update_results(&mut self) {
        self.results.clear();
        if self.input.is_empty() {
            self.results = (0..self.entries.len().min(100)).map(|i| (i, 0)).collect();
        } else {
            // Pre-compute query chars once
            let query_chars: Vec<char> = self.input.to_lowercase().chars().collect();
            for (i, entry) in self.entries.iter().enumerate() {
                if let Some(score) = fuzzy_score_precomputed(&query_chars, entry) {
                    self.results.push((i, score));
                }
            }
            self.results.sort_unstable_by(|a, b| b.1.cmp(&a.1));
            self.results.truncate(100);
        }
        self.selected = 0;
    }
}

/// Fuzzy match against pre-computed entry data. Zero allocation.
fn fuzzy_score_precomputed(query_chars: &[char], entry: &FileEntry) -> Option<i64> {
    if query_chars.is_empty() {
        return Some(0);
    }
    // Quick reject: query longer than candidate
    if query_chars.len() > entry.lower_chars.len() {
        return None;
    }

    let mut score: i64 = 0;
    let mut qi = 0;
    let mut prev_match: Option<usize> = None;

    for (ci, &cc) in entry.lower_chars.iter().enumerate() {
        if qi < query_chars.len() && cc == query_chars[qi] {
            score += 1;

            // Consecutive match bonus
            if let Some(prev) = prev_match {
                if ci == prev + 1 {
                    score += 5;
                }
            }

            // Word boundary bonus
            if ci == 0
                || ci == entry.filename_start
                || matches!(
                    entry.chars.get(ci.wrapping_sub(1)),
                    Some('/' | '.' | '_' | '-' | ' ')
                )
            {
                score += 10;
            }

            // Filename match bonus
            if ci >= entry.filename_start {
                score += 3;
            }

            prev_match = Some(ci);
            qi += 1;
        }
    }

    if qi == query_chars.len() {
        score -= (entry.path.len() as i64) / 10;
        Some(score)
    } else {
        None
    }
}

fn collect_files_recursive(
    root: &std::path::Path,
    max_files: usize,
    max_depth: usize,
) -> Vec<String> {
    const SKIP_DIRS: &[&str] = &[
        ".git",
        "node_modules",
        "target",
        ".hg",
        "__pycache__",
        ".DS_Store",
        ".idea",
        ".vscode",
    ];
    let mut result = Vec::new();
    let mut stack: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];

    while let Some((dir, depth)) = stack.pop() {
        if depth > max_depth || result.len() >= max_files {
            break;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            if result.len() >= max_files {
                break;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if file_type.is_dir() {
                if !SKIP_DIRS.contains(&name_str.as_ref()) {
                    stack.push((entry.path(), depth + 1));
                }
            } else if file_type.is_file() {
                if let Ok(rel) = entry.path().strip_prefix(root) {
                    result.push(rel.to_string_lossy().to_string());
                }
            }
        }
    }
    result.sort_unstable();
    result
}

pub enum AppMode {
    Normal,
    QuickSearch,
    Dialog(DialogState),
    MkdirDialog(MkdirDialogState),
    CopyDialog(CopyDialogState),
    Viewing(Box<ViewerState>),
    HexViewing(Box<HexViewerState>),
    Editing(Box<EditorState>),
}

// --- Simple dialog (delete, mkdir, rename) ---

#[derive(Clone)]
pub struct DialogState {
    pub kind: DialogKind,
    pub title: String,
    pub message: String,
    pub input: String,
    pub cursor: usize,
    pub has_input: bool,
    pub focused: DialogField,
}

impl DialogState {
    pub fn insert_char(&mut self, c: char) {
        let byte_pos = self.byte_offset(self.cursor);
        self.input.insert(byte_pos, c);
        self.cursor += 1;
    }

    pub fn delete_char_backward(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            let byte_pos = self.byte_offset(self.cursor);
            self.input.remove(byte_pos);
        }
    }

    pub fn cursor_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn cursor_right(&mut self) {
        let len = self.input.chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.input.chars().count();
    }

    fn byte_offset(&self, char_pos: usize) -> usize {
        self.input
            .char_indices()
            .nth(char_pos)
            .map(|(i, _)| i)
            .unwrap_or(self.input.len())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum DialogField {
    Input,
    ButtonOk,
    ButtonCancel,
}

impl DialogField {
    pub fn next(self, has_input: bool) -> Self {
        match self {
            Self::Input => Self::ButtonOk,
            Self::ButtonOk => Self::ButtonCancel,
            Self::ButtonCancel => {
                if has_input {
                    Self::Input
                } else {
                    Self::ButtonOk
                }
            }
        }
    }
    pub fn prev(self, has_input: bool) -> Self {
        match self {
            Self::Input => Self::ButtonCancel,
            Self::ButtonOk => {
                if has_input {
                    Self::Input
                } else {
                    Self::ButtonCancel
                }
            }
            Self::ButtonCancel => Self::ButtonOk,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum DialogKind {
    ConfirmDelete,
    InputRename,
}

// --- Make folder dialog ---

#[derive(Clone)]
pub struct MkdirDialogState {
    pub input: String,
    pub cursor: usize,
    pub process_multiple: bool,
    pub focused: MkdirDialogField,
}

impl MkdirDialogState {
    pub fn new() -> Self {
        Self {
            input: String::new(),
            cursor: 0,
            process_multiple: false,
            focused: MkdirDialogField::Input,
        }
    }

    pub fn insert_char(&mut self, c: char) {
        let byte_pos = self.byte_offset(self.cursor);
        self.input.insert(byte_pos, c);
        self.cursor += 1;
    }

    pub fn delete_char_backward(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            let byte_pos = self.byte_offset(self.cursor);
            self.input.remove(byte_pos);
        }
    }

    pub fn cursor_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn cursor_right(&mut self) {
        let len = self.input.chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.input.chars().count();
    }

    fn byte_offset(&self, char_pos: usize) -> usize {
        self.input
            .char_indices()
            .nth(char_pos)
            .map(|(i, _)| i)
            .unwrap_or(self.input.len())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum MkdirDialogField {
    Input,
    ProcessMultiple,
    ButtonOk,
    ButtonCancel,
}

impl MkdirDialogField {
    pub fn next(self) -> Self {
        match self {
            Self::Input => Self::ProcessMultiple,
            Self::ProcessMultiple => Self::ButtonOk,
            Self::ButtonOk => Self::ButtonCancel,
            Self::ButtonCancel => Self::Input,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Input => Self::ButtonCancel,
            Self::ProcessMultiple => Self::Input,
            Self::ButtonOk => Self::ProcessMultiple,
            Self::ButtonCancel => Self::ButtonOk,
        }
    }
}

// --- Search dialog ---

#[derive(Clone, Copy, PartialEq)]
pub enum SearchDirection {
    Forward,
    Backward,
}

#[derive(Clone)]
pub struct SearchDialogState {
    pub query: String,
    pub cursor: usize,
    pub direction: SearchDirection,
    pub case_sensitive: bool,
    pub focused: SearchDialogField,
}

impl SearchDialogState {
    pub fn insert_char(&mut self, c: char) {
        let byte_pos = self.byte_offset(self.cursor);
        self.query.insert(byte_pos, c);
        self.cursor += 1;
    }

    pub fn delete_char_backward(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            let byte_pos = self.byte_offset(self.cursor);
            self.query.remove(byte_pos);
        }
    }

    pub fn cursor_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn cursor_right(&mut self) {
        let len = self.query.chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.query.chars().count();
    }

    fn byte_offset(&self, char_pos: usize) -> usize {
        self.query
            .char_indices()
            .nth(char_pos)
            .map(|(i, _)| i)
            .unwrap_or(self.query.len())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum SearchDialogField {
    Query,
    Direction,
    CaseSensitive,
    ButtonFind,
    ButtonCancel,
}

impl SearchDialogField {
    pub fn next(self) -> Self {
        match self {
            Self::Query => Self::Direction,
            Self::Direction => Self::CaseSensitive,
            Self::CaseSensitive => Self::ButtonFind,
            Self::ButtonFind => Self::ButtonCancel,
            Self::ButtonCancel => Self::Query,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Query => Self::ButtonCancel,
            Self::Direction => Self::Query,
            Self::CaseSensitive => Self::Direction,
            Self::ButtonFind => Self::CaseSensitive,
            Self::ButtonCancel => Self::ButtonFind,
        }
    }
}

// --- Unsaved changes dialog ---

#[derive(Clone, Copy, PartialEq)]
pub enum UnsavedDialogField {
    Save,
    Discard,
    Cancel,
}

impl UnsavedDialogField {
    pub fn next(self) -> Self {
        match self {
            Self::Save => Self::Discard,
            Self::Discard => Self::Cancel,
            Self::Cancel => Self::Save,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Save => Self::Cancel,
            Self::Discard => Self::Save,
            Self::Cancel => Self::Discard,
        }
    }
}

// --- Copy/Move dialog ---

#[derive(Clone)]
pub struct CopyDialogState {
    pub source_name: String,
    pub source_paths: Vec<PathBuf>,
    pub destination: String,
    pub cursor: usize,
    pub is_move: bool,
    pub overwrite_mode: OverwriteMode,
    pub process_multiple: bool,
    pub copy_access_mode: bool,
    pub copy_extended_attrs: bool,
    pub disable_write_cache: bool,
    pub produce_sparse: bool,
    pub use_cow: bool,
    pub symlink_mode: SymlinkMode,
    pub use_filter: bool,
    pub focused: CopyDialogField,
}

impl CopyDialogState {
    pub fn new(
        source_name: String,
        source_paths: Vec<PathBuf>,
        destination: String,
        is_move: bool,
    ) -> Self {
        let cursor = destination.chars().count();
        Self {
            source_name,
            source_paths,
            cursor,
            destination,
            is_move,
            overwrite_mode: OverwriteMode::Ask,
            process_multiple: false,
            copy_access_mode: true,
            copy_extended_attrs: false,
            disable_write_cache: false,
            produce_sparse: false,
            use_cow: false,
            symlink_mode: SymlinkMode::Smart,
            use_filter: false,
            focused: CopyDialogField::Destination,
        }
    }

    pub fn insert_char(&mut self, c: char) {
        let byte_pos = self.byte_offset(self.cursor);
        self.destination.insert(byte_pos, c);
        self.cursor += 1;
    }

    pub fn delete_char_backward(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            let byte_pos = self.byte_offset(self.cursor);
            self.destination.remove(byte_pos);
        }
    }

    pub fn cursor_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn cursor_right(&mut self) {
        let len = self.destination.chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.destination.chars().count();
    }

    fn byte_offset(&self, char_pos: usize) -> usize {
        self.destination
            .char_indices()
            .nth(char_pos)
            .map(|(i, _)| i)
            .unwrap_or(self.destination.len())
    }

    pub fn toggle_focused(&mut self) {
        match self.focused {
            CopyDialogField::OverwriteMode => self.overwrite_mode = self.overwrite_mode.next(),
            CopyDialogField::ProcessMultiple => self.process_multiple = !self.process_multiple,
            CopyDialogField::CopyAccessMode => self.copy_access_mode = !self.copy_access_mode,
            CopyDialogField::CopyExtendedAttrs => {
                self.copy_extended_attrs = !self.copy_extended_attrs
            }
            CopyDialogField::DisableWriteCache => {
                self.disable_write_cache = !self.disable_write_cache
            }
            CopyDialogField::ProduceSparse => self.produce_sparse = !self.produce_sparse,
            CopyDialogField::UseCow => self.use_cow = !self.use_cow,
            CopyDialogField::SymlinkMode => self.symlink_mode = self.symlink_mode.next(),
            CopyDialogField::UseFilter => self.use_filter = !self.use_filter,
            _ => {}
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum OverwriteMode {
    Ask,
    Overwrite,
    Skip,
    Rename,
    Append,
}

impl OverwriteMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ask => "Ask",
            Self::Overwrite => "Overwrite",
            Self::Skip => "Skip",
            Self::Rename => "Rename",
            Self::Append => "Append",
        }
    }
    pub fn next(self) -> Self {
        match self {
            Self::Ask => Self::Overwrite,
            Self::Overwrite => Self::Skip,
            Self::Skip => Self::Rename,
            Self::Rename => Self::Append,
            Self::Append => Self::Ask,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum SymlinkMode {
    Smart,
    CopyContents,
    CopyAsLink,
}

impl SymlinkMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Smart => "Smartly copy link or target file",
            Self::CopyContents => "Copy symlink contents",
            Self::CopyAsLink => "Copy as symbolic link",
        }
    }
    pub fn next(self) -> Self {
        match self {
            Self::Smart => Self::CopyContents,
            Self::CopyContents => Self::CopyAsLink,
            Self::CopyAsLink => Self::Smart,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum CopyDialogField {
    Destination,
    OverwriteMode,
    ProcessMultiple,
    CopyAccessMode,
    CopyExtendedAttrs,
    DisableWriteCache,
    ProduceSparse,
    UseCow,
    SymlinkMode,
    UseFilter,
    ButtonCopy,
    ButtonCancel,
}

impl CopyDialogField {
    pub fn next(self) -> Self {
        match self {
            Self::Destination => Self::OverwriteMode,
            Self::OverwriteMode => Self::ProcessMultiple,
            Self::ProcessMultiple => Self::CopyAccessMode,
            Self::CopyAccessMode => Self::CopyExtendedAttrs,
            Self::CopyExtendedAttrs => Self::DisableWriteCache,
            Self::DisableWriteCache => Self::ProduceSparse,
            Self::ProduceSparse => Self::UseCow,
            Self::UseCow => Self::SymlinkMode,
            Self::SymlinkMode => Self::UseFilter,
            Self::UseFilter => Self::ButtonCopy,
            Self::ButtonCopy => Self::ButtonCancel,
            Self::ButtonCancel => Self::Destination,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Destination => Self::ButtonCancel,
            Self::OverwriteMode => Self::Destination,
            Self::ProcessMultiple => Self::OverwriteMode,
            Self::CopyAccessMode => Self::ProcessMultiple,
            Self::CopyExtendedAttrs => Self::CopyAccessMode,
            Self::DisableWriteCache => Self::CopyExtendedAttrs,
            Self::ProduceSparse => Self::DisableWriteCache,
            Self::UseCow => Self::ProduceSparse,
            Self::SymlinkMode => Self::UseCow,
            Self::UseFilter => Self::SymlinkMode,
            Self::ButtonCopy => Self::UseFilter,
            Self::ButtonCancel => Self::ButtonCopy,
        }
    }
}

// ============================================================
// App implementation
// ============================================================

impl App {
    pub fn new() -> Self {
        let persisted = AppState::load();
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));

        // Restore panel paths from saved state, fall back to cwd
        let left_path = persisted
            .left_panel_path
            .as_ref()
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
            .unwrap_or_else(|| cwd.clone());
        let right_path = persisted
            .right_panel_path
            .as_ref()
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
            .unwrap_or_else(|| cwd.clone());

        let mut panels = [Panel::new(left_path), Panel::new(right_path)];

        // Restore sort preferences
        panels[0].sort_field = sort_field_from_u8(persisted.left_sort_field);
        panels[0].sort_ascending = persisted.left_sort_ascending;
        panels[0].apply_sort();
        panels[1].sort_field = sort_field_from_u8(persisted.right_sort_field);
        panels[1].sort_ascending = persisted.right_sort_ascending;
        panels[1].apply_sort();

        let mut git_cache = GitCache::new();
        panels[0].refresh_git(&mut git_cache);
        panels[1].refresh_git(&mut git_cache);

        let mut dir_watcher = DirWatcher::new();
        if let Some(ref mut w) = dir_watcher {
            w.watch_dirs([&panels[0].current_dir, &panels[1].current_dir]);
        }

        Self {
            panels,
            active_panel: 0,
            mode: AppMode::Normal,
            should_quit: false,
            status_message: None,
            panel_areas: [Rect::default(); 2],
            ci_panel_areas: [None, None],
            last_click: None,
            goto_line_input: None,
            needs_clear: false,
            search_dialog: None,
            unsaved_dialog: None,
            git_cache,
            persisted,
            dir_watcher,
            ci_panels: [None, None],
            ci_focused: None,
            quit_confirm: None,
            goto_path: [None, None],
            fuzzy_search: [None, None],
            help_scroll: None,
            terminal_panel: None,
            terminal_side: 1,
            terminal_focused: false,
            wakeup_sender: None,
        }
    }

    /// Set the wakeup sender (called from main after creating the event handler).
    pub fn set_wakeup_sender(&mut self, sender: crate::event::WakeupSender) {
        self.wakeup_sender = Some(sender);
    }

    /// Save current state to disk.
    pub fn save_state(&mut self) {
        self.persisted.left_panel_path =
            Some(self.panels[0].current_dir.to_string_lossy().to_string());
        self.persisted.right_panel_path =
            Some(self.panels[1].current_dir.to_string_lossy().to_string());
        self.persisted.left_sort_field = sort_field_to_u8(self.panels[0].sort_field);
        self.persisted.left_sort_ascending = self.panels[0].sort_ascending;
        self.persisted.right_sort_field = sort_field_to_u8(self.panels[1].sort_field);
        self.persisted.right_sort_ascending = self.panels[1].sort_ascending;
        self.persisted.save();
    }

    /// Reload both panels and refresh git status.
    pub fn reload_panels(&mut self) {
        self.panels[0].reload();
        self.panels[1].reload();
        self.git_cache.invalidate(&self.panels[0].current_dir);
        self.git_cache.invalidate(&self.panels[1].current_dir);
        self.panels[0].refresh_git(&mut self.git_cache);
        self.panels[1].refresh_git(&mut self.git_cache);
        self.update_watched_dirs();
    }

    /// Close CI panels that are no longer relevant (branch changed or left the repo).
    fn check_ci_panels(&mut self) {
        for side in 0..2 {
            if let Some(ref ci) = self.ci_panels[side] {
                let still_valid = self.panels[side]
                    .git_info
                    .as_ref()
                    .map(|gi| gi.branch == ci.branch)
                    .unwrap_or(false);
                if !still_valid && ci.download.is_none() {
                    self.ci_panels[side] = None;
                    if self.ci_focused == Some(side) {
                        self.ci_focused = None;
                    }
                }
            }
        }
    }

    /// Update filesystem watcher to track current panel directories.
    fn update_watched_dirs(&mut self) {
        if let Some(ref mut w) = self.dir_watcher {
            w.watch_dirs([&self.panels[0].current_dir, &self.panels[1].current_dir]);
        }
    }

    pub fn take_edit_request(&mut self) -> Option<String> {
        if let Some(ref msg) = self.status_message {
            if msg.starts_with("__EDIT__") {
                let path = msg.trim_start_matches("__EDIT__").to_string();
                self.status_message = None;
                return Some(path);
            }
        }
        None
    }

    pub fn active_panel(&self) -> &Panel {
        &self.panels[self.active_panel]
    }

    pub fn active_panel_mut(&mut self) -> &mut Panel {
        &mut self.panels[self.active_panel]
    }

    pub fn inactive_panel(&self) -> &Panel {
        &self.panels[1 - self.active_panel]
    }

    // --- Key/mouse mapping ---

    pub fn map_mouse_to_action(&mut self, mouse: MouseEvent) -> Action {
        let col = mouse.column;
        let row = mouse.row;
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let now = Instant::now();
                if let Some((prev_time, prev_col, prev_row)) = self.last_click {
                    if now.duration_since(prev_time).as_millis() < DOUBLE_CLICK_MS
                        && col == prev_col
                        && row == prev_row
                    {
                        self.last_click = None;
                        return Action::MouseDoubleClick(col, row);
                    }
                }
                self.last_click = Some((now, col, row));
                Action::MouseClick(col, row)
            }
            MouseEventKind::ScrollUp => Action::MouseScrollUp(col, row),
            MouseEventKind::ScrollDown => Action::MouseScrollDown(col, row),
            _ => Action::None,
        }
    }

    pub fn map_key_to_action(&self, key: KeyEvent) -> Action {
        // Help dialog intercepts keys
        if self.help_scroll.is_some() {
            return match key.code {
                KeyCode::Esc | KeyCode::F(1) | KeyCode::Char('q') => Action::DialogCancel,
                KeyCode::Up => Action::MoveUp,
                KeyCode::Down => Action::MoveDown,
                KeyCode::PageUp => Action::PageUp,
                KeyCode::PageDown => Action::PageDown,
                KeyCode::Home => Action::MoveToTop,
                KeyCode::End => Action::MoveToBottom,
                _ => Action::None,
            };
        }

        // Goto-line prompt intercepts keys
        if self.goto_line_input.is_some() {
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Backspace => Action::DialogBackspace,
                KeyCode::Char(c) if c.is_ascii_digit() || c == ':' => Action::DialogInput(c),
                _ => Action::None,
            };
        }

        // Go-to-path input intercepts keys
        if self.goto_path[self.active_panel].is_some() {
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Backspace => Action::DialogBackspace,
                KeyCode::Left => Action::CursorLeft,
                KeyCode::Right => Action::CursorRight,
                KeyCode::Home => Action::CursorLineStart,
                KeyCode::End => Action::CursorLineEnd,
                KeyCode::Tab | KeyCode::Down => Action::MoveDown, // next completion
                KeyCode::BackTab | KeyCode::Up => Action::MoveUp, // prev completion
                KeyCode::Char(c) => Action::DialogInput(c),
                _ => Action::None,
            };
        }

        // Fuzzy file search input intercepts keys
        if self.fuzzy_search[self.active_panel].is_some() {
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Backspace => Action::DialogBackspace,
                KeyCode::Left => Action::CursorLeft,
                KeyCode::Right => Action::CursorRight,
                KeyCode::Home => Action::CursorLineStart,
                KeyCode::End => Action::CursorLineEnd,
                KeyCode::Tab | KeyCode::Down => Action::MoveDown,
                KeyCode::BackTab | KeyCode::Up => Action::MoveUp,
                KeyCode::Char(c) => Action::DialogInput(c),
                _ => Action::None,
            };
        }

        // Quit confirmation intercepts keys
        if self.quit_confirm.is_some() {
            return match key.code {
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Tab | KeyCode::Left | KeyCode::Right | KeyCode::BackTab => {
                    Action::SwitchPanel
                }
                _ => Action::None,
            };
        }

        // Unsaved dialog intercepts keys when active
        if self.unsaved_dialog.is_some() {
            return match key.code {
                KeyCode::Esc => Action::QuickSearchClear,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Tab | KeyCode::Right | KeyCode::Down => Action::MoveDown,
                KeyCode::BackTab | KeyCode::Left | KeyCode::Up => Action::MoveUp,
                _ => Action::None,
            };
        }

        // Terminal panel intercepts keys when focused
        if self.terminal_focused && self.terminal_panel.is_some() {
            return match key.code {
                // These keys are reserved for middle-manager
                KeyCode::F(5) => Action::TerminalOpenFile,
                KeyCode::F(12) => Action::ToggleTerminal,
                KeyCode::F(10) => Action::Quit,
                // F1 switches focus away from terminal (Tab forwarded to claude)
                KeyCode::F(1) => Action::SwitchPanel,
                // Everything else (including Tab) is forwarded to the terminal
                _ => Action::TerminalInput(crate::terminal::encode_key_event(key)),
            };
        }

        // CI panel intercepts keys when focused
        if self.ci_focused.is_some() {
            return match key.code {
                KeyCode::Up => Action::MoveUp,
                KeyCode::Down => Action::MoveDown,
                KeyCode::PageUp => Action::PageUp,
                KeyCode::PageDown => Action::PageDown,
                KeyCode::Home => Action::MoveToTop,
                KeyCode::End => Action::MoveToBottom,
                KeyCode::Enter => Action::Enter,
                KeyCode::Right => Action::CursorRight,
                KeyCode::Left => Action::GoUp,
                KeyCode::Char('o') => Action::OpenPr,
                KeyCode::Tab => Action::SwitchPanel,
                KeyCode::BackTab => Action::SwitchPanelReverse,
                KeyCode::F(2) => Action::ToggleCi,
                KeyCode::F(10) => Action::Quit,
                _ => Action::None,
            };
        }

        // Search dialog intercepts keys when active
        if let Some(ref state) = self.search_dialog {
            return Self::map_search_dialog_key(key, state.focused);
        }

        match &self.mode {
            AppMode::Normal => self.map_normal_key(key),
            AppMode::QuickSearch => self.map_quick_search_key(key),
            AppMode::Dialog(state) => Self::map_dialog_key(key, state.focused, state.has_input),
            AppMode::MkdirDialog(state) => Self::map_mkdir_dialog_key(key, state.focused),
            AppMode::CopyDialog(state) => Self::map_copy_dialog_key(key, state.focused),
            AppMode::Viewing(_) | AppMode::HexViewing(_) => self.map_viewer_key(key),
            AppMode::Editing(_) => Self::map_editor_key(key),
        }
    }

    fn map_normal_key(&self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Up if key.modifiers.contains(KeyModifiers::SHIFT) => Action::SelectMoveUp,
            KeyCode::Down if key.modifiers.contains(KeyModifiers::SHIFT) => Action::SelectMoveDown,
            KeyCode::Insert => Action::ToggleSelect,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Home | KeyCode::Left => Action::MoveToTop,
            KeyCode::End | KeyCode::Right => Action::MoveToBottom,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Enter => Action::Enter,
            KeyCode::Backspace => Action::GoUp,
            KeyCode::Tab => Action::SwitchPanel,
            KeyCode::BackTab => Action::SwitchPanelReverse,
            KeyCode::F(1) => Action::ShowHelp,
            KeyCode::F(3) => Action::ViewFile,
            KeyCode::F(4) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::EditFile // external $EDITOR
                } else {
                    Action::EditBuiltin
                }
            }
            KeyCode::F(5) => Action::Copy,
            KeyCode::F(6) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::Rename
                } else {
                    Action::Move
                }
            }
            KeyCode::F(7) => Action::CreateDir,
            KeyCode::F(8) => Action::Delete,
            KeyCode::F(9) => Action::CycleSort,
            KeyCode::F(10) => Action::Quit,
            KeyCode::F(2) => Action::ToggleCi,
            KeyCode::F(11) => Action::OpenPr,
            KeyCode::F(12) => Action::ToggleTerminal,
            KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::CopyName,
            KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::CopyPath,
            KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::GotoPathPrompt
            }
            KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::FuzzySearchPrompt
            }
            KeyCode::Char(c) if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' => {
                Action::QuickSearch(c)
            }
            // Esc does nothing in normal mode — use F10 to quit
            _ => Action::None,
        }
    }

    fn map_quick_search_key(&self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Esc => Action::QuickSearchClear,
            KeyCode::Enter => Action::Enter, // dismiss search + enter selected item
            KeyCode::Backspace => Action::DialogBackspace,
            KeyCode::Char(c) => Action::QuickSearch(c),
            _ => Action::QuickSearchClear,
        }
    }

    fn map_dialog_key(key: KeyEvent, focused: DialogField, has_input: bool) -> Action {
        let on_buttons = matches!(focused, DialogField::ButtonOk | DialogField::ButtonCancel);
        match key.code {
            KeyCode::Esc => Action::DialogCancel,
            KeyCode::Enter => Action::DialogConfirm,
            KeyCode::Tab => Action::MoveDown,
            KeyCode::BackTab => Action::MoveUp,
            KeyCode::Up if !has_input || on_buttons => Action::MoveUp,
            KeyCode::Down if on_buttons => Action::None,
            KeyCode::Down if !has_input || focused == DialogField::Input => Action::MoveDown,
            KeyCode::Left if on_buttons => Action::SwitchPanel,
            KeyCode::Right if on_buttons => Action::SwitchPanel,
            KeyCode::Char(c) if focused == DialogField::Input => Action::DialogInput(c),
            KeyCode::Backspace if focused == DialogField::Input => Action::DialogBackspace,
            KeyCode::Left if focused == DialogField::Input => Action::CursorLeft,
            KeyCode::Right if focused == DialogField::Input => Action::CursorRight,
            KeyCode::Home if focused == DialogField::Input => Action::CursorLineStart,
            KeyCode::End if focused == DialogField::Input => Action::CursorLineEnd,
            _ => Action::None,
        }
    }

    fn map_mkdir_dialog_key(key: KeyEvent, focused: MkdirDialogField) -> Action {
        let on_buttons = matches!(
            focused,
            MkdirDialogField::ButtonOk | MkdirDialogField::ButtonCancel
        );
        match key.code {
            KeyCode::Esc => Action::DialogCancel,
            KeyCode::Enter => Action::DialogConfirm,
            KeyCode::Tab => Action::MoveDown,
            KeyCode::BackTab => Action::MoveUp,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down if on_buttons => Action::None, // stay on button row
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left if on_buttons => Action::SwitchPanel, // swap between buttons
            KeyCode::Right if on_buttons => Action::SwitchPanel,
            KeyCode::Char(' ') if focused == MkdirDialogField::ProcessMultiple => Action::Toggle,
            KeyCode::Char(c) if focused == MkdirDialogField::Input => Action::DialogInput(c),
            KeyCode::Backspace if focused == MkdirDialogField::Input => Action::DialogBackspace,
            KeyCode::Left if focused == MkdirDialogField::Input => Action::CursorLeft,
            KeyCode::Right if focused == MkdirDialogField::Input => Action::CursorRight,
            KeyCode::Home if focused == MkdirDialogField::Input => Action::CursorLineStart,
            KeyCode::End if focused == MkdirDialogField::Input => Action::CursorLineEnd,
            _ => Action::None,
        }
    }

    fn map_copy_dialog_key(key: KeyEvent, focused: CopyDialogField) -> Action {
        let on_buttons = matches!(
            focused,
            CopyDialogField::ButtonCopy | CopyDialogField::ButtonCancel
        );
        match key.code {
            KeyCode::Esc => Action::DialogCancel,
            KeyCode::Enter => Action::DialogConfirm,
            KeyCode::Tab => Action::MoveDown,
            KeyCode::BackTab => Action::MoveUp,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down if on_buttons => Action::None,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left if on_buttons => Action::SwitchPanel,
            KeyCode::Right if on_buttons => Action::SwitchPanel,
            KeyCode::Char(' ') if focused != CopyDialogField::Destination => Action::Toggle,
            KeyCode::Char(c) if focused == CopyDialogField::Destination => Action::DialogInput(c),
            KeyCode::Backspace if focused == CopyDialogField::Destination => {
                Action::DialogBackspace
            }
            KeyCode::Left if focused == CopyDialogField::Destination => Action::CursorLeft,
            KeyCode::Right if focused == CopyDialogField::Destination => Action::CursorRight,
            KeyCode::Home if focused == CopyDialogField::Destination => Action::CursorLineStart,
            KeyCode::End if focused == CopyDialogField::Destination => Action::CursorLineEnd,
            _ => Action::None,
        }
    }

    fn map_search_dialog_key(key: KeyEvent, focused: SearchDialogField) -> Action {
        let on_buttons = matches!(
            focused,
            SearchDialogField::ButtonFind | SearchDialogField::ButtonCancel
        );
        match key.code {
            KeyCode::Esc => Action::DialogCancel,
            KeyCode::Enter => Action::DialogConfirm,
            KeyCode::Tab => Action::MoveDown,
            KeyCode::BackTab => Action::MoveUp,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down if on_buttons => Action::None,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left if on_buttons => Action::SwitchPanel,
            KeyCode::Right if on_buttons => Action::SwitchPanel,
            KeyCode::Char(' ')
                if matches!(
                    focused,
                    SearchDialogField::Direction | SearchDialogField::CaseSensitive
                ) =>
            {
                Action::Toggle
            }
            KeyCode::Char(c) if focused == SearchDialogField::Query => Action::DialogInput(c),
            KeyCode::Backspace if focused == SearchDialogField::Query => Action::DialogBackspace,
            KeyCode::Left if focused == SearchDialogField::Query => Action::CursorLeft,
            KeyCode::Right if focused == SearchDialogField::Query => Action::CursorRight,
            KeyCode::Home if focused == SearchDialogField::Query => Action::CursorLineStart,
            KeyCode::End if focused == SearchDialogField::Query => Action::CursorLineEnd,
            _ => Action::None,
        }
    }

    fn map_viewer_key(&self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down => Action::MoveDown,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Home => Action::MoveToTop,
            KeyCode::End => Action::MoveToBottom,
            KeyCode::Tab | KeyCode::F(4) => Action::Toggle, // switch text <-> hex
            KeyCode::Char('g') => Action::GotoLinePrompt,
            KeyCode::Char('q') | KeyCode::Esc => Action::DialogCancel,
            _ => Action::None,
        }
    }

    fn map_editor_key(key: KeyEvent) -> Action {
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl {
            return match key.code {
                KeyCode::Char('s') => Action::EditorSave,
                KeyCode::Char('k') | KeyCode::Char('y') => Action::EditorDeleteLine,
                KeyCode::Char('c') => Action::CopySelection,
                KeyCode::Char('a') => Action::SelectAll,
                KeyCode::Char('f') => Action::SearchPrompt,
                KeyCode::Char('z') if shift => Action::EditorRedo,
                KeyCode::Char('z') => Action::EditorUndo,
                KeyCode::Char('g') => Action::GotoLinePrompt,
                KeyCode::Char('q') => Action::DialogCancel,
                KeyCode::Left => Action::WordLeft, // Ctrl+Left (Linux)
                KeyCode::Right => Action::WordRight, // Ctrl+Right (Linux)
                KeyCode::Home | KeyCode::Up => Action::MoveToTop,
                KeyCode::End | KeyCode::Down => Action::MoveToBottom,
                _ => Action::None,
            };
        }

        let alt = key.modifiers.contains(KeyModifiers::ALT);

        match key.code {
            KeyCode::F(7) if shift => Action::FindNext,
            KeyCode::F(7) => Action::SearchPrompt,
            // Fn+Opt+Up/Down on Mac → Alt+PageUp/PageDown → top/bottom of file
            KeyCode::PageUp if alt => Action::MoveToTop,
            KeyCode::PageDown if alt => Action::MoveToBottom,
            // Opt+Left/Right on Mac → sends Alt+b/Alt+f (readline-style)
            KeyCode::Char('b') if alt => Action::WordLeft,
            KeyCode::Char('f') if alt => Action::WordRight,
            KeyCode::Up if shift => Action::SelectUp,
            KeyCode::Down if shift => Action::SelectDown,
            KeyCode::Left if shift => Action::SelectLeft,
            KeyCode::Right if shift => Action::SelectRight,
            KeyCode::Home if shift => Action::SelectLineStart,
            KeyCode::End if shift => Action::SelectLineEnd,
            KeyCode::PageUp if shift => Action::SelectPageUp,
            KeyCode::PageDown if shift => Action::SelectPageDown,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left => Action::CursorLeft,
            KeyCode::Right => Action::CursorRight,
            KeyCode::Home => Action::CursorLineStart,
            KeyCode::End => Action::CursorLineEnd,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Enter => Action::EditorNewline,
            KeyCode::Backspace => Action::DialogBackspace,
            KeyCode::Delete => Action::EditorDeleteForward,
            KeyCode::F(2) => Action::EditorSave,
            KeyCode::Esc => Action::DialogCancel,
            KeyCode::Char(c) => Action::DialogInput(c),
            _ => Action::None,
        }
    }

    // --- Action dispatch ---

    pub fn handle_action(&mut self, action: Action) {
        // Help dialog intercepts when active
        if self.help_scroll.is_some() {
            match action {
                Action::DialogCancel => self.help_scroll = None,
                Action::MoveUp => {
                    if let Some(ref mut s) = self.help_scroll {
                        *s = s.saturating_sub(1);
                    }
                }
                Action::MoveDown => {
                    if let Some(ref mut s) = self.help_scroll {
                        *s += 1;
                    }
                }
                Action::PageUp => {
                    if let Some(ref mut s) = self.help_scroll {
                        *s = s.saturating_sub(20);
                    }
                }
                Action::PageDown => {
                    if let Some(ref mut s) = self.help_scroll {
                        *s += 20;
                    }
                }
                Action::MoveToTop => self.help_scroll = Some(0),
                Action::MoveToBottom => self.help_scroll = Some(usize::MAX),
                _ => {}
            }
            return;
        }

        // Go-to-line prompt intercepts all input when active
        if self.goto_line_input.is_some() {
            self.handle_goto_line_action(action);
            return;
        }

        // Go-to-path input intercepts all input when active
        if self.goto_path[self.active_panel].is_some() {
            self.handle_goto_path_action(action);
            return;
        }

        // Fuzzy file search intercepts all input when active
        if self.fuzzy_search[self.active_panel].is_some() {
            self.handle_fuzzy_search_action(action);
            return;
        }

        // F10 Quit — always confirm, works from any panel/mode
        if matches!(action, Action::Quit)
            && self.unsaved_dialog.is_none()
            && self.quit_confirm.is_none()
        {
            let has_unsaved = matches!(self.mode, AppMode::Editing(ref e) if e.modified);
            if has_unsaved {
                self.unsaved_dialog = Some(UnsavedDialogField::Save);
            } else {
                self.quit_confirm = Some(true); // Quit button focused
            }
            return;
        }

        // Quit confirmation dialog
        if self.quit_confirm.is_some() {
            match action {
                Action::DialogConfirm => {
                    if self.quit_confirm == Some(true) {
                        self.should_quit = true;
                    } else {
                        self.quit_confirm = None;
                    }
                }
                Action::DialogCancel => {
                    self.quit_confirm = None;
                }
                Action::SwitchPanel | Action::SwitchPanelReverse => {
                    self.quit_confirm = Some(!self.quit_confirm.unwrap_or(true));
                }
                Action::None | Action::Tick | Action::Resize(_, _) => {}
                _ => {}
            }
            return;
        }

        // Unsaved changes dialog intercepts when active
        if self.unsaved_dialog.is_some() {
            match action {
                Action::DialogConfirm => {
                    let focused = self.unsaved_dialog.unwrap();
                    match focused {
                        UnsavedDialogField::Save => {
                            if let AppMode::Editing(ref mut e) = self.mode {
                                match e.save() {
                                    Ok(()) => {}
                                    Err(err) => {
                                        e.status_msg = Some(format!("Save failed: {}", err));
                                        self.unsaved_dialog = None;
                                        return;
                                    }
                                }
                            }
                            self.unsaved_dialog = None;
                            self.mode = AppMode::Normal;
                            self.needs_clear = true;
                            self.reload_panels();
                        }
                        UnsavedDialogField::Discard => {
                            self.unsaved_dialog = None;
                            self.mode = AppMode::Normal;
                            self.needs_clear = true;
                        }
                        UnsavedDialogField::Cancel => {
                            self.unsaved_dialog = None;
                        }
                    }
                }
                Action::MoveDown => {
                    if let Some(ref mut f) = self.unsaved_dialog {
                        *f = f.next();
                    }
                }
                Action::MoveUp => {
                    if let Some(ref mut f) = self.unsaved_dialog {
                        *f = f.prev();
                    }
                }
                Action::QuickSearchClear => {
                    // Esc — stay in editor
                    self.unsaved_dialog = None;
                }
                _ => {}
            }
            return;
        }

        // Search dialog overlay intercepts when active
        if self.search_dialog.is_some() {
            self.handle_search_dialog_action(action);
            return;
        }

        // CI panel intercepts when focused
        if self.ci_focused.is_some() {
            self.handle_ci_action(action);
            return;
        }

        // Terminal panel intercepts when focused
        if self.terminal_focused && self.terminal_panel.is_some() {
            self.handle_terminal_action(action);
            return;
        }

        // Dialog, mkdir dialog, copy dialog, and editor have their own dispatch
        if matches!(self.mode, AppMode::Dialog(_)) {
            self.handle_dialog_action(action);
            return;
        }
        if matches!(self.mode, AppMode::MkdirDialog(_)) {
            self.handle_mkdir_dialog_action(action);
            return;
        }
        if matches!(self.mode, AppMode::CopyDialog(_)) {
            self.handle_copy_dialog_action(action);
            return;
        }
        if matches!(self.mode, AppMode::Editing(_)) {
            self.handle_editor_action(action);
            return;
        }

        match action {
            Action::None => {}
            Action::Tick => {
                // Poll all CI panels for async results and downloads
                for ci in self.ci_panels.iter_mut().flatten() {
                    ci.poll();
                    if let Some(result) = ci.poll_download() {
                        match result {
                            Ok(path) => {
                                self.ci_focused = None;
                                self.mode = AppMode::Editing(Box::new(
                                    crate::editor::EditorState::open(path),
                                ));
                                return;
                            }
                            Err(e) => {
                                self.status_message = Some(format!("Download failed: {}", e));
                            }
                        }
                    }
                }
                // Poll terminal panel
                if let Some(ref mut tp) = self.terminal_panel {
                    tp.poll();
                    if tp.exited {
                        self.terminal_panel = None;
                        self.terminal_focused = false;
                    }
                }
                // Poll for async PR query results
                if self.git_cache.poll_pending() {
                    self.panels[0].refresh_git(&mut self.git_cache);
                    self.panels[1].refresh_git(&mut self.git_cache);
                }
                // Check for filesystem changes (kqueue/inotify — zero cost if idle)
                if let Some(ref w) = self.dir_watcher {
                    if w.has_changes() {
                        self.reload_panels();
                    }
                }
            }
            Action::Quit => self.should_quit = true,
            Action::Resize(_, _) => {
                self.resize_terminal();
            }
            Action::Toggle => self.handle_toggle_viewer(),
            Action::GotoLinePrompt => {
                // Only works in viewer/hex/editor modes
                if matches!(
                    self.mode,
                    AppMode::Viewing(_) | AppMode::HexViewing(_) | AppMode::Editing(_)
                ) {
                    self.goto_line_input = Some(String::new());
                }
            }
            Action::EditBuiltin => self.handle_edit_builtin(),
            Action::CursorLeft
            | Action::CursorRight
            | Action::CursorLineStart
            | Action::CursorLineEnd
            | Action::EditorSave
            | Action::EditorNewline
            | Action::EditorDeleteForward
            | Action::EditorDeleteLine
            | Action::SelectUp
            | Action::SelectDown
            | Action::SelectLeft
            | Action::SelectRight
            | Action::SelectLineStart
            | Action::SelectLineEnd
            | Action::SelectPageUp
            | Action::SelectPageDown
            | Action::SelectAll
            | Action::CopySelection
            | Action::WordLeft
            | Action::WordRight
            | Action::EditorUndo
            | Action::EditorRedo
            | Action::SearchPrompt
            | Action::FindNext => {}

            // Panel multi-file selection
            Action::ToggleSelect => self.active_panel_mut().toggle_select_current(),
            Action::SelectMoveUp => self.active_panel_mut().select_move_up(),
            Action::SelectMoveDown => self.active_panel_mut().select_move_down(),

            // Navigation
            Action::MoveUp => self.handle_move_up(),
            Action::MoveDown => self.handle_move_down(),
            Action::MoveToTop => self.handle_move_to_top(),
            Action::MoveToBottom => self.handle_move_to_bottom(),
            Action::PageUp => self.handle_page_up(),
            Action::PageDown => self.handle_page_down(),
            Action::Enter => self.handle_enter(),
            Action::GoUp => self.handle_go_up(),
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),

            // File operations
            Action::Copy => self.handle_copy(),
            Action::Move => self.handle_move(),
            Action::Rename => self.handle_rename(),
            Action::CreateDir => self.handle_create_dir(),
            Action::Delete => self.handle_delete(),
            Action::ViewFile => self.handle_view_file(),
            Action::EditFile => self.handle_edit_file(),

            // Clipboard
            Action::CopyName => {
                if let Some(entry) = self.active_panel().selected_entry() {
                    let name = entry.name.clone();
                    crate::editor::osc52_copy(&name);
                    self.status_message = Some(format!("Copied: {}", name));
                }
            }
            Action::CopyPath => {
                if let Some(entry) = self.active_panel().selected_entry() {
                    let path = entry.path.display().to_string();
                    crate::editor::osc52_copy(&path);
                    self.status_message = Some(format!("Copied: {}", path));
                }
            }

            // Go-to-path
            Action::GotoPathPrompt => {
                let path = self
                    .active_panel()
                    .current_dir
                    .to_string_lossy()
                    .to_string();
                let cursor = path.len();
                self.goto_path[self.active_panel] = Some(GotoPathState {
                    input: path,
                    cursor,
                    completions: Vec::new(),
                    comp_index: None,
                    comp_base: None,
                });
            }

            // Help
            Action::ShowHelp => {
                self.help_scroll = Some(0);
            }

            // Fuzzy file search
            Action::FuzzySearchPrompt => {
                let root = self.active_panel().current_dir.clone();
                let paths = collect_files_recursive(&root, 10_000, 20);
                self.fuzzy_search[self.active_panel] = Some(FuzzySearchState::new(paths));
            }

            // Sorting
            Action::CycleSort => {
                self.active_panel_mut().cycle_sort();
            }

            // CI / GitHub
            Action::ToggleCi => {
                let side = self.active_panel;
                if self.ci_panels[side].is_some() {
                    if self.ci_panels[side]
                        .as_ref()
                        .map(|ci| ci.download.is_some())
                        .unwrap_or(false)
                    {
                        self.status_message =
                            Some("Download in progress — wait for it to complete".to_string());
                    } else {
                        self.ci_panels[side] = None;
                        if self.ci_focused == Some(side) {
                            self.ci_focused = None;
                        }
                    }
                } else if let Some(ref gi) = self.active_panel().git_info {
                    let dir = self.active_panel().current_dir.clone();
                    self.ci_panels[side] = Some(CiPanel::for_branch(&dir, &gi.branch));
                    self.ci_focused = Some(side);
                } else {
                    self.status_message = Some("Not in a git repository".to_string());
                }
            }
            Action::OpenPr => {
                if let Some(ref gi) = self.active_panel().git_info {
                    if let Some(ref pr) = gi.pr {
                        crate::panel::github::open_url(&pr.url);
                    } else {
                        self.status_message = Some("No PR for this branch".to_string());
                    }
                }
            }

            // Terminal
            Action::ToggleTerminal => {
                if self.terminal_panel.is_some() {
                    self.terminal_panel = None;
                    self.terminal_focused = false;
                } else if let Some(ref wakeup) = self.wakeup_sender {
                    let spawn_side = 1 - self.active_panel;
                    let dir = self.panels[self.active_panel].current_dir.clone();
                    let area = self.panel_areas[spawn_side];
                    let cols = area.width.saturating_sub(2).max(1);
                    let rows = area.height.saturating_sub(2).max(1);
                    match TerminalPanel::spawn(&dir, cols, rows, wakeup.clone()) {
                        Ok(tp) => {
                            self.terminal_panel = Some(tp);
                            self.terminal_side = spawn_side;
                            self.terminal_focused = true;
                            self.ci_focused = None;
                        }
                        Err(e) => {
                            self.status_message = Some(format!("Failed to start terminal: {}", e));
                        }
                    }
                } else {
                    self.status_message = Some("Event loop not ready".to_string());
                }
            }

            // Mouse
            Action::MouseClick(col, row) => self.handle_mouse_click(col, row),
            Action::MouseDoubleClick(col, row) => self.handle_mouse_double_click(col, row),
            Action::MouseScrollUp(col, row) => self.handle_mouse_scroll(col, row, -3),
            Action::MouseScrollDown(col, row) => self.handle_mouse_scroll(col, row, 3),

            // Quick search
            Action::QuickSearch(c) => self.handle_quick_search(c),
            Action::QuickSearchClear => self.handle_quick_search_clear(),

            // These can still fire from non-dialog modes (viewer cancel, quick search backspace)
            Action::DialogCancel => self.handle_dialog_cancel(),
            Action::DialogBackspace => self.handle_dialog_backspace(),
            Action::DialogConfirm | Action::DialogInput(_) => {}
            Action::TerminalInput(_) | Action::TerminalOpenFile => {} // handled by intercepts above
        }
    }

    // --- Navigation handlers ---

    fn handle_move_up(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_up(1),
            AppMode::HexViewing(h) => h.scroll_up(1),
            _ => self.active_panel_mut().move_selection(-1),
        }
    }

    fn handle_move_down(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_down(1),
            AppMode::HexViewing(h) => h.scroll_down(1),
            _ => self.active_panel_mut().move_selection(1),
        }
    }

    fn handle_move_to_top(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_to_top(),
            AppMode::HexViewing(h) => h.scroll_to_top(),
            _ => self.active_panel_mut().move_to_top(),
        }
    }

    fn handle_move_to_bottom(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_to_bottom(),
            AppMode::HexViewing(h) => h.scroll_to_bottom(),
            _ => self.active_panel_mut().move_to_bottom(),
        }
    }

    fn handle_page_up(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => {
                let page = v.visible_lines.max(1);
                v.scroll_up(page);
            }
            AppMode::HexViewing(h) => {
                let page = h.visible_rows.max(1);
                h.scroll_up(page);
            }
            _ => self.active_panel_mut().move_selection(-20),
        }
    }

    fn handle_page_down(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => {
                let page = v.visible_lines.max(1);
                v.scroll_down(page);
            }
            AppMode::HexViewing(h) => {
                let page = h.visible_rows.max(1);
                h.scroll_down(page);
            }
            _ => self.active_panel_mut().move_selection(20),
        }
    }

    fn handle_enter(&mut self) {
        // Clear quick search if active
        self.panels[self.active_panel].quick_search = None;
        if matches!(self.mode, AppMode::QuickSearch) {
            self.mode = AppMode::Normal;
        }

        let panel = self.active_panel_mut();
        if let Some(entry) = panel.selected_entry().cloned() {
            if entry.is_dir {
                panel.navigate_into();
                self.panels[self.active_panel].refresh_git(&mut self.git_cache);
                self.update_watched_dirs();
                self.check_ci_panels();
            } else {
                self.open_file(entry.path);
            }
        }
    }

    fn handle_go_up(&mut self) {
        self.active_panel_mut().navigate_up();
        self.panels[self.active_panel].refresh_git(&mut self.git_cache);
        self.update_watched_dirs();
        self.check_ci_panels();
    }

    fn handle_switch_panel(&mut self) {
        self.handle_switch_panel_dir(false);
    }

    fn handle_switch_panel_reverse(&mut self) {
        self.handle_switch_panel_dir(true);
    }

    fn handle_switch_panel_dir(&mut self, reverse: bool) {
        // Focus targets: Panel(side), Ci(side), Terminal
        #[derive(PartialEq)]
        enum Target {
            Panel(usize),
            Ci(usize),
            Terminal,
        }

        let has_terminal = self.terminal_panel.is_some();

        // Build tab order: left, left_ci?, terminal?(if left side), right, right_ci?, terminal?(if right side)
        let mut order: Vec<Target> = Vec::with_capacity(6);

        // Left side: skip file panel if terminal occupies it
        if !(has_terminal && self.terminal_side == 0) {
            order.push(Target::Panel(0));
        }
        if self.ci_panels[0].is_some() {
            order.push(Target::Ci(0));
        }
        if has_terminal && self.terminal_side == 0 {
            order.push(Target::Terminal);
        }

        // Right side
        if !(has_terminal && self.terminal_side == 1) {
            order.push(Target::Panel(1));
        }
        if self.ci_panels[1].is_some() {
            order.push(Target::Ci(1));
        }
        if has_terminal && self.terminal_side == 1 {
            order.push(Target::Terminal);
        }

        if order.is_empty() {
            return;
        }

        // Simple case: only two file panels, no CI, no terminal
        if order.len() == 2
            && matches!(order[0], Target::Panel(_))
            && matches!(order[1], Target::Panel(_))
        {
            self.active_panel = 1 - self.active_panel;
            self.ci_focused = None;
            self.terminal_focused = false;
            return;
        }

        // Find current position
        let current = if self.terminal_focused {
            order.iter().position(|t| *t == Target::Terminal)
        } else if let Some(ci_side) = self.ci_focused {
            order.iter().position(|t| *t == Target::Ci(ci_side))
        } else {
            order
                .iter()
                .position(|t| *t == Target::Panel(self.active_panel))
        };

        let len = order.len();
        let next = current
            .map(|i| {
                if reverse {
                    (i + len - 1) % len
                } else {
                    (i + 1) % len
                }
            })
            .unwrap_or(0);

        match &order[next] {
            Target::Panel(side) => {
                self.active_panel = *side;
                self.ci_focused = None;
                self.terminal_focused = false;
            }
            Target::Ci(side) => {
                self.ci_focused = Some(*side);
                self.terminal_focused = false;
            }
            Target::Terminal => {
                self.terminal_focused = true;
                self.ci_focused = None;
            }
        }
    }

    fn handle_goto_line_action(&mut self, action: Action) {
        match action {
            Action::DialogCancel => {
                self.goto_line_input = None;
            }
            Action::DialogConfirm | Action::EditorNewline | Action::Enter => {
                if let Some(input) = self.goto_line_input.take() {
                    self.goto_line_col(&input);
                }
            }
            Action::DialogInput(c) if c.is_ascii_digit() || c == ':' => {
                if let Some(ref mut input) = self.goto_line_input {
                    input.push(c);
                }
            }
            Action::DialogBackspace => {
                if let Some(ref mut input) = self.goto_line_input {
                    input.pop();
                }
            }
            _ => {}
        }
    }

    fn handle_goto_path_action(&mut self, action: Action) {
        let side = self.active_panel;
        match action {
            Action::DialogCancel => {
                self.goto_path[side] = None;
            }
            Action::DialogConfirm => {
                // If completions are visible and one is selected, apply it
                if let Some(ref mut state) = self.goto_path[side] {
                    if let Some(idx) = state.comp_index {
                        if let Some(completion) = state.completions.get(idx).cloned() {
                            Self::apply_completion(state, &completion);
                            return;
                        }
                    }
                }
                if let Some(state) = self.goto_path[side].take() {
                    self.goto_path_navigate(side, &state.input);
                }
            }
            Action::DialogInput(c) => {
                if let Some(ref mut state) = self.goto_path[side] {
                    state.input.insert(state.cursor, c);
                    state.cursor += c.len_utf8();
                    state.completions.clear();
                    state.comp_index = None;
                    state.comp_base = None;
                }
            }
            Action::DialogBackspace => {
                if let Some(ref mut state) = self.goto_path[side] {
                    if state.cursor > 0 {
                        let prev = state.input[..state.cursor]
                            .char_indices()
                            .last()
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                        state.input.remove(prev);
                        state.cursor = prev;
                        state.completions.clear();
                        state.comp_index = None;
                        state.comp_base = None;
                    }
                }
            }
            Action::MoveDown => {
                // Tab: trigger or cycle completion
                if let Some(ref mut state) = self.goto_path[side] {
                    if state.completions.is_empty() {
                        Self::populate_completions(state);
                        if state.completions.len() == 1 {
                            let completion = state.completions[0].clone();
                            Self::apply_completion(state, &completion);
                        } else if !state.completions.is_empty() {
                            // Fill common prefix first
                            let applied = Self::apply_common_prefix(state);
                            if !applied {
                                state.comp_index = Some(0);
                            }
                        }
                    } else {
                        // Cycle forward
                        let len = state.completions.len();
                        state.comp_index = Some(match state.comp_index {
                            Some(i) => (i + 1) % len,
                            None => 0,
                        });
                    }
                }
            }
            Action::MoveUp => {
                // Shift+Tab: cycle backward
                if let Some(ref mut state) = self.goto_path[side] {
                    if !state.completions.is_empty() {
                        let len = state.completions.len();
                        state.comp_index = Some(match state.comp_index {
                            Some(i) => (i + len - 1) % len,
                            None => len - 1,
                        });
                    }
                }
            }
            Action::CursorLeft => {
                if let Some(ref mut state) = self.goto_path[side] {
                    if state.cursor > 0 {
                        state.cursor = state.input[..state.cursor]
                            .char_indices()
                            .last()
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                    }
                    state.completions.clear();
                    state.comp_index = None;
                    state.comp_base = None;
                }
            }
            Action::CursorRight => {
                if let Some(ref mut state) = self.goto_path[side] {
                    if state.cursor < state.input.len() {
                        state.cursor += state.input[state.cursor..]
                            .chars()
                            .next()
                            .map(|c| c.len_utf8())
                            .unwrap_or(0);
                    }
                    state.completions.clear();
                    state.comp_index = None;
                    state.comp_base = None;
                }
            }
            Action::CursorLineStart => {
                if let Some(ref mut state) = self.goto_path[side] {
                    state.cursor = 0;
                    state.completions.clear();
                    state.comp_index = None;
                    state.comp_base = None;
                }
            }
            Action::CursorLineEnd => {
                if let Some(ref mut state) = self.goto_path[side] {
                    state.cursor = state.input.len();
                    state.completions.clear();
                    state.comp_index = None;
                    state.comp_base = None;
                }
            }
            _ => {}
        }
    }

    /// Expand ~ and split the input into (parent_dir, prefix_to_match).
    fn expand_goto_input(input: &str) -> (PathBuf, String) {
        let expanded = if let Some(rest) = input.strip_prefix('~') {
            if let Some(home) = std::env::var_os("HOME") {
                format!("{}{}", home.to_string_lossy(), rest)
            } else {
                input.to_string()
            }
        } else {
            input.to_string()
        };

        let path = PathBuf::from(&expanded);
        if expanded.ends_with('/') || expanded.is_empty() {
            // Completing inside a directory
            (path, String::new())
        } else {
            // Completing a partial name
            let parent = path.parent().unwrap_or(&path).to_path_buf();
            let prefix = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            (parent, prefix)
        }
    }

    fn populate_completions(state: &mut GotoPathState) {
        let (dir, prefix) = Self::expand_goto_input(&state.input);
        let prefix_lower = prefix.to_lowercase();

        state.comp_base = Some(state.input.clone());
        state.comp_index = None;
        state.completions.clear();

        if let Ok(entries) = std::fs::read_dir(&dir) {
            let mut matches: Vec<String> = entries
                .flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    let is_dir = e.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
                    if !is_dir {
                        return None;
                    }
                    if name.to_lowercase().starts_with(&prefix_lower) {
                        Some(name)
                    } else {
                        None
                    }
                })
                .collect();
            matches.sort_by_key(|a| a.to_lowercase());
            state.completions = matches;
        }
    }

    /// Apply the longest common prefix of all completions. Returns true if the
    /// input was actually extended (i.e. there was a common prefix beyond what
    /// was already typed).
    fn apply_common_prefix(state: &mut GotoPathState) -> bool {
        if state.completions.is_empty() {
            return false;
        }

        let (_, prefix) = Self::expand_goto_input(&state.input);

        // Find longest common prefix among completions
        let first = &state.completions[0];
        let mut common_len = first.len();
        for candidate in &state.completions[1..] {
            common_len = common_len.min(
                first
                    .chars()
                    .zip(candidate.chars())
                    .take_while(|(a, b)| a.to_lowercase().eq(b.to_lowercase()))
                    .count(),
            );
        }

        let common: String = first.chars().take(common_len).collect();
        let common_chars = common.chars().count();
        let prefix_chars = prefix.chars().count();
        if common_chars > prefix_chars {
            // Append the characters beyond what was already typed
            let suffix: String = common.chars().skip(prefix_chars).collect();
            state.input.insert_str(state.cursor, &suffix);
            state.cursor += suffix.len();
            true
        } else {
            false
        }
    }

    fn apply_completion(state: &mut GotoPathState, name: &str) {
        let (_, prefix) = Self::expand_goto_input(&state.input);

        // Append the characters beyond the typed prefix + trailing /
        let prefix_chars = prefix.chars().count();
        let suffix: String = name
            .chars()
            .skip(prefix_chars)
            .chain(std::iter::once('/'))
            .collect();
        state.input.insert_str(state.cursor, &suffix);
        state.cursor += suffix.len();
        state.completions.clear();
        state.comp_index = None;
        state.comp_base = None;
    }

    fn goto_path_navigate(&mut self, side: usize, input: &str) {
        let expanded = if let Some(rest) = input.strip_prefix('~') {
            if let Some(home) = std::env::var_os("HOME") {
                let home = home.to_string_lossy();
                format!("{}{}", home, rest)
            } else {
                input.to_string()
            }
        } else {
            input.to_string()
        };

        let path = PathBuf::from(&expanded);
        if path.is_dir() {
            self.panels[side].current_dir = path;
            self.panels[side].reload();
            self.panels[side].table_state.select(Some(0));
            self.panels[side].refresh_git(&mut self.git_cache);
            if let Some(ref mut w) = self.dir_watcher {
                w.watch_dirs([&self.panels[0].current_dir, &self.panels[1].current_dir]);
            }
        } else {
            self.status_message = Some(format!("Not a directory: {}", expanded));
        }
    }

    fn handle_fuzzy_search_action(&mut self, action: Action) {
        let side = self.active_panel;
        match action {
            Action::DialogCancel => {
                self.fuzzy_search[side] = None;
            }
            Action::DialogConfirm => {
                if let Some(ref state) = self.fuzzy_search[side] {
                    if let Some(&(path_idx, _)) = state.results.get(state.selected) {
                        let rel_path = &state.all_paths[path_idx];
                        let full_path = self.panels[side].current_dir.join(rel_path);
                        if full_path.is_file() {
                            self.fuzzy_search[side] = None;
                            self.mode = AppMode::Editing(Box::new(
                                crate::editor::EditorState::open(full_path),
                            ));
                            return;
                        }
                    }
                }
                self.fuzzy_search[side] = None;
            }
            Action::DialogInput(c) => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    state.input.insert(state.cursor, c);
                    state.cursor += c.len_utf8();
                    state.update_results();
                }
            }
            Action::DialogBackspace => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    if state.cursor > 0 {
                        let prev = state.input[..state.cursor]
                            .char_indices()
                            .last()
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                        state.input.remove(prev);
                        state.cursor = prev;
                        state.update_results();
                    }
                }
            }
            Action::MoveDown => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    let len = state.results.len().min(8);
                    if len > 0 {
                        state.selected = (state.selected + 1) % len;
                    }
                }
            }
            Action::MoveUp => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    let len = state.results.len().min(8);
                    if len > 0 {
                        state.selected = (state.selected + len - 1) % len;
                    }
                }
            }
            Action::CursorLeft => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    if state.cursor > 0 {
                        state.cursor = state.input[..state.cursor]
                            .char_indices()
                            .last()
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                    }
                }
            }
            Action::CursorRight => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    if state.cursor < state.input.len() {
                        state.cursor += state.input[state.cursor..]
                            .chars()
                            .next()
                            .map(|c| c.len_utf8())
                            .unwrap_or(0);
                    }
                }
            }
            Action::CursorLineStart => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    state.cursor = 0;
                }
            }
            Action::CursorLineEnd => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    state.cursor = state.input.len();
                }
            }
            _ => {}
        }
    }

    /// Parse "line" or "line:col" (1-based) and jump in the current viewer/editor.
    fn goto_line_col(&mut self, input: &str) {
        let parts: Vec<&str> = input.split(':').collect();
        let line = match parts[0].parse::<usize>() {
            Ok(n) if n > 0 => n - 1, // convert to 0-based
            _ => return,
        };
        let col = if parts.len() > 1 {
            parts[1].parse::<usize>().unwrap_or(1).saturating_sub(1)
        } else {
            0
        };

        match &mut self.mode {
            AppMode::Viewing(v) => {
                // Scan ahead if needed
                v.scroll_offset = line;
                v.scroll_down(0); // clamps and loads buffer
            }
            AppMode::HexViewing(h) => {
                // Each row = 16 bytes, interpret line as a row number
                h.scroll_offset = line;
                h.scroll_down(0);
            }
            AppMode::Editing(e) => {
                if !e.scan_complete {
                    e.scan_to_line(line + 100);
                }
                let total = e.total_virtual_lines();
                e.cursor_line = line.min(total.saturating_sub(1));
                e.cursor_col = col;
                e.desired_col = col;
                e.clamp_cursor_col();
                e.scroll_to_cursor();
            }
            _ => {}
        }
    }

    fn handle_edit_builtin(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if !entry.is_dir {
                self.mode = AppMode::Editing(Box::new(EditorState::open(entry.path)));
            }
        }
    }

    fn handle_editor_action(&mut self, action: Action) {
        // Clear selection on non-selection movement and editing actions
        let clears_selection = matches!(
            action,
            Action::MoveUp
                | Action::MoveDown
                | Action::CursorLeft
                | Action::CursorRight
                | Action::CursorLineStart
                | Action::CursorLineEnd
                | Action::WordLeft
                | Action::WordRight
                | Action::MoveToTop
                | Action::MoveToBottom
                | Action::PageUp
                | Action::PageDown
                | Action::DialogInput(_)
                | Action::DialogBackspace
                | Action::EditorDeleteForward
                | Action::EditorNewline
                | Action::EditorDeleteLine
        );
        if clears_selection {
            if let AppMode::Editing(ref mut e) = self.mode {
                e.clear_selection();
            }
        }

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => {
                let modified = matches!(self.mode, AppMode::Editing(ref e) if e.modified);
                if modified {
                    self.unsaved_dialog = Some(UnsavedDialogField::Save);
                } else {
                    self.should_quit = true;
                }
            }
            Action::GotoLinePrompt => {
                self.goto_line_input = Some(String::new());
            }
            Action::DialogCancel => {
                let modified = matches!(self.mode, AppMode::Editing(ref e) if e.modified);
                if modified {
                    self.unsaved_dialog = Some(UnsavedDialogField::Save);
                } else {
                    self.mode = AppMode::Normal;
                    self.needs_clear = true;
                }
            }

            // Selection
            Action::SelectUp => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_up();
                }
            }
            Action::SelectDown => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_down();
                }
            }
            Action::SelectLeft => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_left();
                }
            }
            Action::SelectRight => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_right();
                }
            }
            Action::SelectLineStart => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_line_start();
                }
            }
            Action::SelectLineEnd => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_line_end();
                }
            }
            Action::SelectPageUp => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_page_up();
                }
            }
            Action::SelectPageDown => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_page_down();
                }
            }
            Action::SelectAll => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.select_all();
                }
            }
            Action::CopySelection => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.copy_to_clipboard();
                }
            }

            // Movement
            Action::MoveUp => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_up();
                }
            }
            Action::MoveDown => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_down();
                }
            }
            Action::CursorLeft => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_left();
                }
            }
            Action::CursorRight => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_right();
                }
            }
            Action::CursorLineStart => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_line_start();
                }
            }
            Action::CursorLineEnd => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_line_end();
                }
            }
            Action::WordLeft => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.word_left();
                }
            }
            Action::WordRight => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.word_right();
                }
            }
            Action::MoveToTop => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.goto_top();
                }
            }
            Action::MoveToBottom => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.goto_bottom();
                }
            }
            Action::PageUp => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.page_up();
                }
            }
            Action::PageDown => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.page_down();
                }
            }

            // Editing
            Action::DialogInput(c) => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.status_msg = None;
                    e.insert_char(c);
                }
            }
            Action::DialogBackspace => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.status_msg = None;
                    e.delete_char_backward();
                }
            }
            Action::EditorDeleteForward => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.status_msg = None;
                    e.delete_char_forward();
                }
            }
            Action::EditorNewline => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.status_msg = None;
                    e.insert_newline();
                }
            }
            Action::EditorDeleteLine => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.status_msg = None;
                    e.delete_line();
                }
            }
            Action::EditorUndo => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.undo();
                }
            }
            Action::EditorRedo => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.redo();
                }
            }
            Action::EditorSave => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    match e.save() {
                        Ok(()) => {}
                        Err(err) => e.status_msg = Some(format!("Save failed: {}", err)),
                    }
                }
                // Reload panels after save
                self.reload_panels();
            }

            // Search
            Action::SearchPrompt => {
                let (query, cursor, direction, case_sensitive) =
                    if let AppMode::Editing(ref e) = self.mode {
                        if let Some(ref p) = e.last_search {
                            (
                                p.query.clone(),
                                p.query.chars().count(),
                                p.direction,
                                p.case_sensitive,
                            )
                        } else if !self.persisted.search_query.is_empty() {
                            // Restore from persisted state
                            let dir = if self.persisted.search_direction_forward {
                                SearchDirection::Forward
                            } else {
                                SearchDirection::Backward
                            };
                            (
                                self.persisted.search_query.clone(),
                                self.persisted.search_query.chars().count(),
                                dir,
                                self.persisted.search_case_sensitive,
                            )
                        } else {
                            (String::new(), 0, SearchDirection::Forward, false)
                        }
                    } else {
                        (String::new(), 0, SearchDirection::Forward, false)
                    };
                self.search_dialog = Some(SearchDialogState {
                    query,
                    cursor,
                    direction,
                    case_sensitive,
                    focused: SearchDialogField::Query,
                });
            }
            Action::FindNext => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    if let Some(params) = e.last_search.clone() {
                        if !e.find(&params) {
                            e.status_msg = Some(format!("'{}' not found", params.query));
                        }
                    } else {
                        e.status_msg = Some("No previous search".to_string());
                    }
                }
            }

            // Mouse
            Action::MouseClick(col, row) => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.click_at(col, row);
                }
            }
            Action::MouseScrollUp(_, _) => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_up();
                    e.cursor_up();
                    e.cursor_up();
                }
            }
            Action::MouseScrollDown(_, _) => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    e.cursor_down();
                    e.cursor_down();
                    e.cursor_down();
                }
            }

            _ => {}
        }
    }

    fn handle_toggle_viewer(&mut self) {
        match &self.mode {
            AppMode::Viewing(v) => {
                let path = v.path.clone();
                self.mode = AppMode::HexViewing(Box::new(HexViewerState::open(path)));
            }
            AppMode::HexViewing(h) => {
                let path = h.path.clone();
                self.mode = AppMode::Viewing(Box::new(ViewerState::open(path)));
            }
            _ => {}
        }
    }

    fn open_file(&mut self, path: PathBuf) {
        if HexViewerState::is_binary(&path) {
            self.mode = AppMode::HexViewing(Box::new(HexViewerState::open(path)));
        } else {
            self.mode = AppMode::Viewing(Box::new(ViewerState::open(path)));
        }
    }

    // --- File operation handlers ---

    fn handle_copy(&mut self) {
        let paths = self.active_panel().effective_selection_paths();
        if paths.is_empty() {
            return;
        }
        let mut dest = self
            .inactive_panel()
            .current_dir
            .to_string_lossy()
            .to_string();
        if !dest.ends_with('/') {
            dest.push('/');
        }
        let display_name = if paths.len() == 1 {
            paths[0]
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        } else {
            format!("{} items", paths.len())
        };
        self.mode = AppMode::CopyDialog(CopyDialogState::new(display_name, paths, dest, false));
    }

    fn handle_move(&mut self) {
        let paths = self.active_panel().effective_selection_paths();
        if paths.is_empty() {
            return;
        }
        let mut dest = self
            .inactive_panel()
            .current_dir
            .to_string_lossy()
            .to_string();
        if !dest.ends_with('/') {
            dest.push('/');
        }
        let display_name = if paths.len() == 1 {
            paths[0]
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        } else {
            format!("{} items", paths.len())
        };
        self.mode = AppMode::CopyDialog(CopyDialogState::new(display_name, paths, dest, true));
    }

    fn handle_rename(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry() {
            if entry.name == ".." {
                return;
            }
            let name = entry.name.clone();
            let cursor = name.chars().count();
            self.mode = AppMode::Dialog(DialogState {
                kind: DialogKind::InputRename,
                title: "Rename".to_string(),
                message: format!("Rename '{}':", name),
                input: name,
                cursor,
                has_input: true,
                focused: DialogField::Input,
            });
        }
    }

    fn handle_create_dir(&mut self) {
        self.mode = AppMode::MkdirDialog(MkdirDialogState::new());
    }

    fn handle_delete(&mut self) {
        let paths = self.active_panel().effective_selection_paths();
        if paths.is_empty() {
            return;
        }
        let message = if paths.len() == 1 {
            let name = paths[0]
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let kind = if paths[0].is_dir() {
                "directory"
            } else {
                "file"
            };
            format!("Delete {} '{}'?", kind, name)
        } else {
            format!("Delete {} items?", paths.len())
        };
        self.mode = AppMode::Dialog(DialogState {
            kind: DialogKind::ConfirmDelete,
            title: "Delete".to_string(),
            message,
            input: String::new(),
            cursor: 0,
            has_input: false,
            focused: DialogField::ButtonOk,
        });
    }

    fn handle_view_file(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if !entry.is_dir {
                self.open_file(entry.path);
            }
        }
    }

    fn handle_edit_file(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if !entry.is_dir {
                self.status_message = Some(format!("__EDIT__{}", entry.path.to_string_lossy()));
            }
        }
    }

    // --- Search dialog handler ---

    fn handle_search_dialog_action(&mut self, action: Action) {
        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::DialogCancel => {
                self.search_dialog = None;
            }
            Action::DialogConfirm => {
                let is_cancel = self
                    .search_dialog
                    .as_ref()
                    .map(|s| s.focused == SearchDialogField::ButtonCancel)
                    .unwrap_or(false);
                if is_cancel {
                    self.search_dialog = None;
                } else {
                    self.confirm_search_dialog();
                }
            }
            Action::MoveUp => {
                if let Some(ref mut s) = self.search_dialog {
                    s.focused = s.focused.prev();
                }
            }
            Action::MoveDown => {
                if let Some(ref mut s) = self.search_dialog {
                    s.focused = s.focused.next();
                }
            }
            Action::Toggle => {
                if let Some(ref mut s) = self.search_dialog {
                    match s.focused {
                        SearchDialogField::Direction => {
                            s.direction = match s.direction {
                                SearchDirection::Forward => SearchDirection::Backward,
                                SearchDirection::Backward => SearchDirection::Forward,
                            };
                        }
                        SearchDialogField::CaseSensitive => {
                            s.case_sensitive = !s.case_sensitive;
                        }
                        _ => {}
                    }
                }
            }
            Action::DialogInput(c) => {
                if let Some(ref mut s) = self.search_dialog {
                    s.insert_char(c);
                }
            }
            Action::DialogBackspace => {
                if let Some(ref mut s) = self.search_dialog {
                    s.delete_char_backward();
                }
            }
            Action::CursorLeft => {
                if let Some(ref mut s) = self.search_dialog {
                    s.cursor_left();
                }
            }
            Action::CursorRight => {
                if let Some(ref mut s) = self.search_dialog {
                    s.cursor_right();
                }
            }
            Action::CursorLineStart => {
                if let Some(ref mut s) = self.search_dialog {
                    s.cursor_home();
                }
            }
            Action::CursorLineEnd => {
                if let Some(ref mut s) = self.search_dialog {
                    s.cursor_end();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let Some(ref mut s) = self.search_dialog {
                    s.focused = match s.focused {
                        SearchDialogField::ButtonFind => SearchDialogField::ButtonCancel,
                        SearchDialogField::ButtonCancel => SearchDialogField::ButtonFind,
                        other => other,
                    };
                }
            }
            _ => {}
        }
    }

    fn confirm_search_dialog(&mut self) {
        let state = match self.search_dialog.take() {
            Some(s) => s,
            None => return,
        };

        if state.query.is_empty() {
            return;
        }

        use crate::editor::SearchParams;
        let params = SearchParams {
            query: state.query,
            direction: state.direction,
            case_sensitive: state.case_sensitive,
        };

        // Persist search parameters
        self.persisted.search_query = params.query.clone();
        self.persisted.search_direction_forward =
            matches!(params.direction, SearchDirection::Forward);
        self.persisted.search_case_sensitive = params.case_sensitive;

        if let AppMode::Editing(ref mut e) = self.mode {
            e.last_search = Some(params.clone());
            if !e.find(&params) {
                e.status_msg = Some(format!("'{}' not found", params.query));
            }
        }
    }

    // --- CI panel handler ---

    fn handle_ci_action(&mut self, action: Action) {
        let side = match self.ci_focused {
            Some(s) => s,
            None => return,
        };

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {
                // Poll all CI panels and check downloads
                for ci in self.ci_panels.iter_mut().flatten() {
                    ci.poll();
                    if let Some(result) = ci.poll_download() {
                        match result {
                            Ok(path) => {
                                // Open downloaded log in editor
                                self.ci_focused = None;
                                self.mode = AppMode::Editing(Box::new(
                                    crate::editor::EditorState::open(path),
                                ));
                                return;
                            }
                            Err(e) => {
                                self.status_message = Some(format!("Download failed: {}", e));
                            }
                        }
                    }
                }
                if let Some(ref w) = self.dir_watcher {
                    if w.has_changes() {
                        self.reload_panels();
                    }
                }
                if self.git_cache.poll_pending() {
                    self.panels[0].refresh_git(&mut self.git_cache);
                    self.panels[1].refresh_git(&mut self.git_cache);
                }
            }
            Action::MoveUp => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.move_up();
                }
            }
            Action::MoveDown => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.move_down();
                }
            }
            Action::PageUp => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.page_up();
                }
            }
            Action::PageDown => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.page_down();
                }
            }
            Action::MoveToTop => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.move_to_top();
                }
            }
            Action::MoveToBottom => {
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.move_to_bottom();
                }
            }
            Action::Enter => {
                // enter() returns Some if a step was selected for log viewing
                let log_info = self.ci_panels[side].as_mut().and_then(|ci| ci.enter());
                if let Some((run_id, step)) = log_info {
                    self.start_ci_log_download(side, run_id, &step);
                }
            }
            Action::CursorRight => {
                // Right: expand only (don't download on steps)
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.enter(); // returns Some for steps but we ignore it
                }
            }
            Action::GoUp => {
                // Left: collapse expanded check, or jump to parent check from step
                if let Some(ref mut ci) = self.ci_panels[side] {
                    ci.collapse_or_parent();
                }
            }
            Action::SwitchPanel => {
                self.handle_switch_panel();
            }
            Action::SwitchPanelReverse => {
                self.handle_switch_panel_reverse();
            }
            Action::ToggleCi => {
                if self.ci_panels[side]
                    .as_ref()
                    .map(|ci| ci.download.is_some())
                    .unwrap_or(false)
                {
                    self.status_message =
                        Some("Download in progress — wait for it to complete".to_string());
                } else {
                    self.ci_panels[side] = None;
                    self.ci_focused = None;
                }
            }
            Action::OpenPr => {
                if let Some(ref ci) = self.ci_panels[side] {
                    if let Some(url) = ci.selected_url() {
                        crate::panel::github::open_url(url);
                    }
                }
            }
            // Let mouse events through to the normal handler
            Action::MouseClick(col, row) => self.handle_mouse_click(col, row),
            Action::MouseDoubleClick(col, row) => self.handle_mouse_double_click(col, row),
            Action::MouseScrollUp(col, row) => self.handle_mouse_scroll(col, row, -3),
            Action::MouseScrollDown(col, row) => self.handle_mouse_scroll(col, row, 3),
            _ => {}
        }
    }

    fn handle_terminal_action(&mut self, action: Action) {
        match action {
            Action::None => {}
            Action::Tick | Action::Resize(_, _) => {
                // Poll terminal output
                if let Some(ref mut tp) = self.terminal_panel {
                    tp.poll();
                    if tp.exited {
                        self.terminal_panel = None;
                        self.terminal_focused = false;
                    }
                }
                // Resize terminal to match panel area
                if matches!(action, Action::Resize(_, _)) {
                    self.resize_terminal();
                }
                // Also poll CI, watchers, git
                for ci in self.ci_panels.iter_mut().flatten() {
                    ci.poll();
                }
                if let Some(ref w) = self.dir_watcher {
                    if w.has_changes() {
                        self.reload_panels();
                    }
                }
                if self.git_cache.poll_pending() {
                    self.panels[0].refresh_git(&mut self.git_cache);
                    self.panels[1].refresh_git(&mut self.git_cache);
                }
            }
            Action::TerminalInput(bytes) => {
                if let Some(ref mut tp) = self.terminal_panel {
                    // Auto-scroll to bottom when user types
                    tp.scroll_to_bottom();
                    tp.write_bytes(&bytes);
                }
            }
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),
            Action::ToggleTerminal => {
                self.terminal_panel = None;
                self.terminal_focused = false;
            }
            Action::TerminalOpenFile => self.handle_terminal_open_file(),
            Action::Quit => {
                self.quit_confirm = Some(true);
            }
            Action::MouseClick(col, row) => {
                if self.click_in_terminal(col, row) {
                    // Click inside terminal — stay focused
                } else {
                    self.terminal_focused = false;
                    self.handle_mouse_click(col, row);
                }
            }
            Action::MouseDoubleClick(col, row) => {
                if self.click_in_terminal(col, row) {
                    // Double-click inside terminal — absorb
                } else {
                    self.terminal_focused = false;
                    self.handle_mouse_double_click(col, row);
                }
            }
            Action::MouseScrollUp(col, row) => {
                self.forward_mouse_scroll_to_terminal(col, row, true);
            }
            Action::MouseScrollDown(col, row) => {
                self.forward_mouse_scroll_to_terminal(col, row, false);
            }
            _ => {}
        }
    }

    fn click_in_terminal(&self, col: u16, row: u16) -> bool {
        let area = self.panel_areas[self.terminal_side];
        col >= area.x && col < area.x + area.width && row >= area.y && row < area.y + area.height
    }

    fn forward_mouse_scroll_to_terminal(&mut self, _col: u16, _row: u16, up: bool) {
        if let Some(ref mut tp) = self.terminal_panel {
            if up {
                tp.scroll_up(3);
            } else {
                tp.scroll_down(3);
            }
        }
    }

    fn handle_terminal_open_file(&mut self) {
        let (path, line, col) = match self.terminal_panel {
            Some(ref tp) => match tp.find_file_reference() {
                Some(r) => r,
                None => {
                    self.status_message = Some("No file:line reference found".to_string());
                    return;
                }
            },
            None => return,
        };

        let mut editor = crate::editor::EditorState::open(path);
        let target_line = line.saturating_sub(1); // convert to 0-based
        let target_col = col.saturating_sub(1);
        // Ensure the editor has scanned far enough
        if !editor.scan_complete {
            editor.scan_to_line(target_line + 100);
        }
        editor.cursor_line = target_line;
        editor.cursor_col = target_col;
        editor.desired_col = target_col;
        editor.scroll_to_cursor();
        self.mode = AppMode::Editing(Box::new(editor));
    }

    fn resize_terminal(&mut self) {
        if let Some(ref mut tp) = self.terminal_panel {
            let area = self.panel_areas[self.terminal_side];
            let cols = area.width.saturating_sub(2).max(1);
            let rows = area.height.saturating_sub(2).max(1);
            tp.resize(cols, rows);
        }
    }

    fn start_ci_log_download(&mut self, side: usize, run_id: u64, step: &crate::ci::CiStep) {
        let ci = match &mut self.ci_panels[side] {
            Some(ci) => ci,
            None => return,
        };

        if ci.download.is_some() {
            return; // already downloading
        }

        // Build output filename in the active panel's current directory
        let safe_name: String = step
            .name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let output_path = self.panels[self.active_panel]
            .current_dir
            .join(format!("{}.log", safe_name));

        if let Some(ref url) = step.log_url {
            // Azure: direct URL download
            ci.download = Some(crate::ci::LogDownload::start(
                url,
                output_path,
                step.name.clone(),
            ));
        } else if run_id > 0 {
            // GitHub Actions: per-job log download (plain text, fast)
            let repo = ci.repo.clone();
            // Find the job_id for this check
            let job_id = self.ci_panels[side]
                .as_ref()
                .and_then(|ci| {
                    if let crate::ci::CiView::Tree {
                        items, selected, ..
                    } = &ci.view
                    {
                        // Walk back from selected to find the parent check
                        for i in (0..=*selected).rev() {
                            if let crate::ci::TreeItem::Check { check, .. } = &items[i] {
                                return Some(check.job_id);
                            }
                        }
                    }
                    None
                })
                .unwrap_or(0);

            if job_id > 0 {
                let ci = self.ci_panels[side].as_mut().unwrap();
                ci.download = Some(crate::ci::LogDownload::start_github(
                    &repo,
                    run_id,
                    step.number,
                    &step.name,
                    output_path,
                    job_id,
                ));
            } else {
                self.status_message = Some("Cannot download: no job ID found".to_string());
            }
        } else {
            self.status_message = Some("Cannot download logs: no run ID found".to_string());
        }
    }

    // --- Mkdir dialog handler ---

    fn handle_mkdir_dialog_action(&mut self, action: Action) {
        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::DialogCancel => {
                self.mode = AppMode::Normal;
            }
            Action::DialogConfirm => {
                let is_cancel = matches!(
                    self.mode,
                    AppMode::MkdirDialog(MkdirDialogState {
                        focused: MkdirDialogField::ButtonCancel,
                        ..
                    })
                );
                if is_cancel {
                    self.mode = AppMode::Normal;
                } else {
                    self.confirm_mkdir_dialog();
                }
            }
            Action::MoveUp => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.focused = state.focused.prev();
                }
            }
            Action::MoveDown => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.focused = state.focused.next();
                }
            }
            Action::Toggle => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.process_multiple = !state.process_multiple;
                }
            }
            Action::DialogInput(c) => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    if state.focused == MkdirDialogField::Input {
                        state.insert_char(c);
                    }
                }
            }
            Action::DialogBackspace => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    if state.focused == MkdirDialogField::Input {
                        state.delete_char_backward();
                    }
                }
            }
            Action::CursorLeft => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.cursor_left();
                }
            }
            Action::CursorRight => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.cursor_right();
                }
            }
            Action::CursorLineStart => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.cursor_home();
                }
            }
            Action::CursorLineEnd => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.cursor_end();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                // Swap between OK and Cancel buttons
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.focused = match state.focused {
                        MkdirDialogField::ButtonOk => MkdirDialogField::ButtonCancel,
                        MkdirDialogField::ButtonCancel => MkdirDialogField::ButtonOk,
                        other => other,
                    };
                }
            }
            _ => {}
        }
    }

    fn confirm_mkdir_dialog(&mut self) {
        let (input, process_multiple) = match &self.mode {
            AppMode::MkdirDialog(s) => (s.input.clone(), s.process_multiple),
            _ => return,
        };

        if input.is_empty() {
            self.mode = AppMode::Normal;
            return;
        }

        let dir = self.active_panel().current_dir.clone();
        let names: Vec<&str> = if process_multiple {
            input
                .split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec![input.trim()]
        };

        let mut first_err: Option<anyhow::Error> = None;
        for name in names {
            if let Err(e) = fs_ops::create_directory(&dir, name) {
                first_err = Some(e);
                break;
            }
        }

        match first_err {
            None => self.status_message = None,
            Some(e) => self.status_message = Some(format!("Error: {}", e)),
        }

        self.mode = AppMode::Normal;
        self.reload_panels();
    }

    // --- Copy dialog handler ---

    fn handle_copy_dialog_action(&mut self, action: Action) {
        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::DialogCancel => {
                self.mode = AppMode::Normal;
            }
            Action::DialogConfirm => {
                let is_cancel = matches!(
                    self.mode,
                    AppMode::CopyDialog(CopyDialogState {
                        focused: CopyDialogField::ButtonCancel,
                        ..
                    })
                );
                if is_cancel {
                    self.mode = AppMode::Normal;
                } else {
                    self.confirm_copy_dialog();
                }
            }
            Action::MoveUp => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.focused = state.focused.prev();
                }
            }
            Action::MoveDown => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.focused = state.focused.next();
                }
            }
            Action::Toggle => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.toggle_focused();
                }
            }
            Action::DialogInput(c) => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    if state.focused == CopyDialogField::Destination {
                        state.insert_char(c);
                    }
                }
            }
            Action::DialogBackspace => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    if state.focused == CopyDialogField::Destination {
                        state.delete_char_backward();
                    }
                }
            }
            Action::CursorLeft => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.cursor_left();
                }
            }
            Action::CursorRight => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.cursor_right();
                }
            }
            Action::CursorLineStart => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.cursor_home();
                }
            }
            Action::CursorLineEnd => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.cursor_end();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.focused = match state.focused {
                        CopyDialogField::ButtonCopy => CopyDialogField::ButtonCancel,
                        CopyDialogField::ButtonCancel => CopyDialogField::ButtonCopy,
                        other => other,
                    };
                }
            }
            _ => {}
        }
    }

    fn confirm_copy_dialog(&mut self) {
        let (source_paths, dest, is_move) = {
            let state = match &self.mode {
                AppMode::CopyDialog(s) => s,
                _ => return,
            };
            if state.source_paths.is_empty() {
                self.mode = AppMode::Normal;
                return;
            }
            (
                state.source_paths.clone(),
                PathBuf::from(&state.destination),
                state.is_move,
            )
        };

        let mut first_err: Option<anyhow::Error> = None;
        for source_path in &source_paths {
            let result = if is_move {
                fs_ops::move_entry(source_path, &dest)
            } else {
                fs_ops::copy_entry(source_path, &dest)
            };
            if let Err(e) = result {
                first_err = Some(e);
                break;
            }
        }

        match first_err {
            None => self.status_message = None,
            Some(e) => self.status_message = Some(format!("Error: {}", e)),
        }

        self.mode = AppMode::Normal;
        self.reload_panels();
    }

    // --- Mouse handlers ---

    fn panel_at(&self, col: u16, row: u16) -> Option<usize> {
        for i in 0..2 {
            let a = self.panel_areas[i];
            if col >= a.x && col < a.x + a.width && row >= a.y && row < a.y + a.height {
                return Some(i);
            }
        }
        None
    }

    fn row_to_entry_index(&self, panel_idx: usize, row: u16) -> Option<usize> {
        let a = self.panel_areas[panel_idx];
        let first_data_row = a.y + 2;
        if row < first_data_row || row >= a.y + a.height.saturating_sub(1) {
            return None;
        }
        let offset = (row - first_data_row) as usize;
        let panel = &self.panels[panel_idx];
        let scroll = panel.table_state.offset();
        let idx = scroll + offset;
        if idx < panel.entries.len() {
            Some(idx)
        } else {
            None
        }
    }

    fn handle_mouse_click(&mut self, col: u16, row: u16) {
        if let AppMode::Editing(ref mut e) = self.mode {
            e.click_at(col, row);
            return;
        }
        if matches!(self.mode, AppMode::Viewing(_) | AppMode::HexViewing(_)) {
            return;
        }
        if matches!(
            self.mode,
            AppMode::Dialog(_) | AppMode::MkdirDialog(_) | AppMode::CopyDialog(_)
        ) {
            return;
        }

        // Check if click is in a CI panel
        for side in 0..2 {
            if let Some(ci_area) = self.ci_panel_areas[side] {
                if col >= ci_area.x
                    && col < ci_area.x + ci_area.width
                    && row >= ci_area.y
                    && row < ci_area.y + ci_area.height
                {
                    self.ci_focused = Some(side);
                    // Compute which item was clicked
                    if let Some(ref mut ci) = self.ci_panels[side] {
                        // Account for border (1 row for top border)
                        let inner_y = ci_area.y + 1;
                        if row >= inner_y {
                            let click_offset = (row - inner_y) as usize;
                            let scroll = match &ci.view {
                                crate::ci::CiView::Tree { scroll, .. } => *scroll,
                                _ => 0,
                            };
                            let target = scroll + click_offset;
                            let item_count = match &ci.view {
                                crate::ci::CiView::Tree { items, .. } => items.len(),
                                _ => 0,
                            };
                            if target < item_count {
                                if let crate::ci::CiView::Tree { selected, .. } = &mut ci.view {
                                    *selected = target;
                                }
                            }
                        }
                    }
                    return;
                }
            }
        }

        // Check if click is in the terminal panel
        if self.terminal_panel.is_some() {
            let term_area = self.panel_areas[self.terminal_side];
            if col >= term_area.x
                && col < term_area.x + term_area.width
                && row >= term_area.y
                && row < term_area.y + term_area.height
            {
                self.terminal_focused = true;
                self.ci_focused = None;
                return;
            }
        }

        // Click on a file panel — unfocus CI and terminal
        if let Some(panel_idx) = self.panel_at(col, row) {
            self.ci_focused = None;
            self.terminal_focused = false;
            self.active_panel = panel_idx;
            if let Some(entry_idx) = self.row_to_entry_index(panel_idx, row) {
                self.panels[panel_idx].table_state.select(Some(entry_idx));
            }
        }
    }

    fn handle_mouse_double_click(&mut self, col: u16, row: u16) {
        self.handle_mouse_click(col, row);
        self.handle_enter();
    }

    fn handle_mouse_scroll(&mut self, col: u16, row: u16, delta: i32) {
        match &mut self.mode {
            AppMode::Viewing(v) => {
                if delta < 0 {
                    v.scroll_up((-delta) as usize);
                } else {
                    v.scroll_down(delta as usize);
                }
                return;
            }
            AppMode::HexViewing(h) => {
                if delta < 0 {
                    h.scroll_up((-delta) as usize);
                } else {
                    h.scroll_down(delta as usize);
                }
                return;
            }
            AppMode::Dialog(_) | AppMode::MkdirDialog(_) | AppMode::CopyDialog(_) => return,
            _ => {}
        }

        if let Some(panel_idx) = self.panel_at(col, row) {
            self.panels[panel_idx].move_selection(delta);
        }
    }

    // --- Quick search handlers ---

    fn handle_quick_search(&mut self, c: char) {
        match &mut self.mode {
            AppMode::QuickSearch => {
                let panel = &mut self.panels[self.active_panel];
                let query = panel.quick_search.get_or_insert_with(String::new);
                query.push(c);
                let q = query.clone();
                panel.jump_to_match(&q);
            }
            AppMode::Normal => {
                self.mode = AppMode::QuickSearch;
                let panel = &mut self.panels[self.active_panel];
                panel.quick_search = Some(c.to_string());
                let q = c.to_string();
                panel.jump_to_match(&q);
            }
            _ => {}
        }
    }

    fn handle_quick_search_clear(&mut self) {
        self.panels[self.active_panel].quick_search = None;
        self.mode = AppMode::Normal;
    }

    // --- Dialog handler (delete, rename) ---

    fn handle_dialog_action(&mut self, action: Action) {
        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::DialogCancel => {
                self.mode = AppMode::Normal;
            }
            Action::DialogConfirm => {
                let is_cancel = matches!(
                    self.mode,
                    AppMode::Dialog(DialogState {
                        focused: DialogField::ButtonCancel,
                        ..
                    })
                );
                if is_cancel {
                    self.mode = AppMode::Normal;
                } else {
                    self.confirm_dialog();
                }
            }
            Action::MoveUp => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.focused = state.focused.prev(state.has_input);
                }
            }
            Action::MoveDown => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.focused = state.focused.next(state.has_input);
                }
            }
            Action::DialogInput(c) => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    if state.focused == DialogField::Input {
                        state.insert_char(c);
                    }
                }
            }
            Action::DialogBackspace => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    if state.focused == DialogField::Input {
                        state.delete_char_backward();
                    }
                }
            }
            Action::CursorLeft => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.cursor_left();
                }
            }
            Action::CursorRight => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.cursor_right();
                }
            }
            Action::CursorLineStart => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.cursor_home();
                }
            }
            Action::CursorLineEnd => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.cursor_end();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.focused = match state.focused {
                        DialogField::ButtonOk => DialogField::ButtonCancel,
                        DialogField::ButtonCancel => DialogField::ButtonOk,
                        other => other,
                    };
                }
            }
            _ => {}
        }
    }

    fn confirm_dialog(&mut self) {
        let dialog = match &self.mode {
            AppMode::Dialog(d) => d.clone(),
            _ => return,
        };

        let result = match dialog.kind {
            DialogKind::ConfirmDelete => {
                let paths = self.active_panel().effective_selection_paths();
                let mut first_err: Option<anyhow::Error> = None;
                for path in &paths {
                    if let Err(e) = fs_ops::delete_entry(path) {
                        first_err = Some(e);
                        break;
                    }
                }
                match first_err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
            DialogKind::InputRename => {
                if dialog.input.is_empty() {
                    Ok(())
                } else if let Some(entry) = self.active_panel().selected_entry() {
                    fs_ops::rename_entry(&entry.path, &dialog.input)
                } else {
                    Ok(())
                }
            }
        };

        match result {
            Ok(()) => self.status_message = None,
            Err(e) => self.status_message = Some(format!("Error: {}", e)),
        }

        self.mode = AppMode::Normal;
        self.reload_panels();
    }

    fn handle_dialog_cancel(&mut self) {
        match &self.mode {
            AppMode::Viewing(_) | AppMode::HexViewing(_) => {
                self.mode = AppMode::Normal;
                self.needs_clear = true;
            }
            _ => {}
        }
    }

    fn handle_dialog_backspace(&mut self) {
        if let AppMode::QuickSearch = &self.mode {
            let panel = &mut self.panels[self.active_panel];
            if let Some(ref mut query) = panel.quick_search {
                query.pop();
                if query.is_empty() {
                    panel.quick_search = None;
                    self.mode = AppMode::Normal;
                } else {
                    let q = query.clone();
                    panel.jump_to_match(&q);
                }
            }
        }
    }
}

#[cfg(test)]
mod fuzzy_tests {
    use super::*;

    fn score(query: &str, candidate: &str) -> Option<i64> {
        let query_chars: Vec<char> = query.to_lowercase().chars().collect();
        let chars: Vec<char> = candidate.chars().collect();
        let lower_chars: Vec<char> = candidate.to_lowercase().chars().collect();
        let filename_start = chars
            .iter()
            .rposition(|&c| c == '/')
            .map(|i| i + 1)
            .unwrap_or(0);
        let entry = FileEntry {
            path: candidate.to_string(),
            lower_chars,
            chars,
            filename_start,
        };
        fuzzy_score_precomputed(&query_chars, &entry)
    }

    #[test]
    fn exact_match() {
        assert!(score("main.rs", "main.rs").is_some());
    }

    #[test]
    fn prefix_match() {
        assert!(score("main", "main.rs").is_some());
    }

    #[test]
    fn substring_chars_in_order() {
        // "aprs" matches "app.rs" — a, p, r, s in order
        assert!(score("aprs", "app.rs").is_some());
    }

    #[test]
    fn middle_of_filename() {
        assert!(score("view", "panel_view.rs").is_some());
    }

    #[test]
    fn path_match() {
        assert!(score("src/main", "src/main.rs").is_some());
    }

    #[test]
    fn no_match_wrong_order() {
        // "srm" — s, r, m not in order in "main.rs"
        assert!(score("srm", "main.rs").is_none());
    }

    #[test]
    fn no_match_missing_chars() {
        assert!(score("xyz", "main.rs").is_none());
    }

    #[test]
    fn case_insensitive() {
        assert!(score("MAIN", "main.rs").is_some());
        assert!(score("main", "Main.rs").is_some());
    }

    #[test]
    fn empty_query_matches_all() {
        assert!(score("", "anything.rs").is_some());
    }

    #[test]
    fn query_longer_than_candidate_rejected() {
        assert!(score("toolongquery", "short").is_none());
    }

    #[test]
    fn consecutive_bonus() {
        // "main" consecutively in "main.rs" should score higher than spread across "myappinfo.rs"
        let s1 = score("main", "main.rs").unwrap();
        let s2 = score("main", "myappinfo.rs").unwrap();
        assert!(
            s1 > s2,
            "consecutive match ({}) should beat spread ({})",
            s1,
            s2
        );
    }

    #[test]
    fn filename_match_beats_path_match() {
        // "mod" in filename "mod.rs" should rank higher than in path "models/x.rs"
        let s1 = score("mod", "mod.rs").unwrap();
        let s2 = score("mod", "some/deep/path/models/data.rs").unwrap();
        assert!(
            s1 > s2,
            "filename match ({}) should beat deep path ({})",
            s1,
            s2
        );
    }

    #[test]
    fn shorter_path_preferred() {
        let s1 = score("app", "app.rs").unwrap();
        let s2 = score("app", "some/very/long/path/to/app.rs").unwrap();
        assert!(
            s1 > s2,
            "short path ({}) should beat long path ({})",
            s1,
            s2
        );
    }

    #[test]
    fn word_boundary_bonus() {
        // "pv" matching at word boundaries (panel_view) should beat middle matches
        let s1 = score("pv", "panel_view.rs").unwrap();
        let s2 = score("pv", "approve.rs").unwrap();
        assert!(
            s1 > s2,
            "boundary match ({}) should beat middle ({})",
            s1,
            s2
        );
    }

    #[test]
    fn collect_files_skips_git() {
        let dir = std::env::current_dir().unwrap();
        let files = collect_files_recursive(&dir, 10_000, 20);
        // Should not contain any paths starting with .git/
        assert!(
            !files.iter().any(|p| p.starts_with(".git/")),
            "should skip .git directory"
        );
        // Should contain our own source files
        assert!(
            files.iter().any(|p| p.ends_with("main.rs")),
            "should find main.rs"
        );
    }

    #[test]
    fn fuzzy_search_state_update_results() {
        let paths = vec![
            "src/main.rs".to_string(),
            "src/app.rs".to_string(),
            "src/editor.rs".to_string(),
            "README.md".to_string(),
        ];
        let mut state = FuzzySearchState::new(paths);

        // Empty query shows all
        assert_eq!(state.results.len(), 4);

        // Type "app" — should match app.rs
        state.input = "app".to_string();
        state.cursor = 3;
        state.update_results();
        assert!(!state.results.is_empty());
        let top_path = &state.all_paths[state.results[0].0];
        assert!(
            top_path.contains("app"),
            "top result should contain 'app', got: {}",
            top_path
        );

        // Type "xyz" — should match nothing
        state.input = "xyz".to_string();
        state.cursor = 3;
        state.update_results();
        assert!(state.results.is_empty());
    }

    // --- Edge cases ---

    #[test]
    fn unicode_filenames() {
        // Accented chars match themselves (no normalization, same as fzf)
        assert!(score("café", "café.txt").is_some());
        assert!(score("cafe", "café.txt").is_none()); // e ≠ é
                                                      // CJK characters
        assert!(score("日本", "日本語.md").is_some());
        // Mixed ASCII and Unicode
        assert!(score("txt", "café.txt").is_some());
    }

    #[test]
    fn single_char_query() {
        assert!(score("a", "app.rs").is_some());
        assert!(score("z", "app.rs").is_none());
    }

    #[test]
    fn query_equals_candidate() {
        let s = score("main.rs", "main.rs").unwrap();
        // Should be a high score (all consecutive + boundary matches)
        assert!(s > 0, "exact match should have positive score, got {}", s);
    }

    #[test]
    fn deeply_nested_path() {
        assert!(score("file", "a/b/c/d/e/f/g/file.rs").is_some());
        // Shallow should beat deep
        let s1 = score("file", "file.rs").unwrap();
        let s2 = score("file", "a/b/c/d/e/f/g/file.rs").unwrap();
        assert!(s1 > s2);
    }

    #[test]
    fn dotfiles() {
        assert!(score("git", ".gitignore").is_some());
        assert!(score("env", ".env").is_some());
        assert!(score("gi", ".gitignore").is_some());
    }

    #[test]
    fn duplicate_chars_in_query() {
        // "ss" should match two s's in "settings.rs"
        assert!(score("ss", "settings.rs").is_some());
        // "tt" needs two t's — "settings" has two t's
        assert!(score("tt", "settings.rs").is_some());
        // "zz" needs two z's — none in "settings.rs"
        assert!(score("zz", "settings.rs").is_none());
    }

    #[test]
    fn special_chars_in_paths() {
        assert!(score("my", "my file.rs").is_some());
        assert!(score("my", "my-file.rs").is_some());
        assert!(score("my", "my_file.rs").is_some());
    }

    #[test]
    fn results_truncated_at_100() {
        // Create 200 matching files
        let paths: Vec<String> = (0..200).map(|i| format!("file{}.rs", i)).collect();
        let mut state = FuzzySearchState::new(paths);
        state.input = "file".to_string();
        state.cursor = 4;
        state.update_results();
        assert!(
            state.results.len() <= 100,
            "results should be capped at 100, got {}",
            state.results.len()
        );
    }

    #[test]
    fn empty_file_list() {
        let state = FuzzySearchState::new(vec![]);
        assert!(state.results.is_empty());
    }

    #[test]
    fn all_files_match_no_panic() {
        let paths = vec!["a.rs".to_string(), "b.rs".to_string(), "c.rs".to_string()];
        let mut state = FuzzySearchState::new(paths);
        state.input = "rs".to_string();
        state.cursor = 2;
        state.update_results();
        assert_eq!(state.results.len(), 3);
    }

    #[test]
    fn extension_only_match() {
        // Searching for just an extension
        assert!(score("rs", "main.rs").is_some());
        assert!(score("md", "README.md").is_some());
    }

    #[test]
    fn query_with_slash() {
        // User types path separator in query
        let s = score("src/app", "src/app.rs");
        assert!(s.is_some());
    }

    #[test]
    fn repeated_pattern_picks_best() {
        // "test" appears in both path and filename
        let s1 = score("test", "test.rs").unwrap();
        let s2 = score("test", "test/test_helper.rs").unwrap();
        // Direct filename match should rank higher
        assert!(
            s1 > s2,
            "direct filename ({}) should beat path+filename ({})",
            s1,
            s2
        );
    }

    // --- Go-to-path tests ---

    #[test]
    fn expand_tilde() {
        let (dir, prefix) = App::expand_goto_input("~/Documents/pro");
        let home = std::env::var("HOME").unwrap();
        assert_eq!(dir, PathBuf::from(format!("{}/Documents", home)));
        assert_eq!(prefix, "pro");
    }

    #[test]
    fn expand_tilde_trailing_slash() {
        let (dir, prefix) = App::expand_goto_input("~/Documents/");
        let home = std::env::var("HOME").unwrap();
        assert_eq!(dir, PathBuf::from(format!("{}/Documents/", home)));
        assert_eq!(prefix, "");
    }

    #[test]
    fn expand_absolute_path() {
        let (dir, prefix) = App::expand_goto_input("/usr/loc");
        assert_eq!(dir, PathBuf::from("/usr"));
        assert_eq!(prefix, "loc");
    }

    #[test]
    fn expand_absolute_trailing_slash() {
        let (dir, prefix) = App::expand_goto_input("/usr/local/");
        assert_eq!(dir, PathBuf::from("/usr/local/"));
        assert_eq!(prefix, "");
    }

    #[test]
    fn expand_empty_input() {
        let (dir, prefix) = App::expand_goto_input("");
        assert_eq!(dir, PathBuf::from(""));
        assert_eq!(prefix, "");
    }

    #[test]
    fn expand_just_tilde() {
        let (dir, prefix) = App::expand_goto_input("~");
        let home = std::env::var("HOME").unwrap();
        // "~" expands to home dir, no trailing slash, so it's treated as a partial name
        // parent of /Users/foo is /Users, prefix is "foo"
        assert!(dir.to_string_lossy().len() > 0);
        assert!(!prefix.is_empty() || dir == PathBuf::from(&home));
    }

    #[test]
    fn apply_completion_basic() {
        let mut state = GotoPathState {
            input: "/usr/lo".to_string(),
            cursor: 7,
            completions: vec!["local".to_string()],
            comp_index: None,
            comp_base: None,
        };
        App::apply_completion(&mut state, "local");
        assert_eq!(state.input, "/usr/local/");
        assert_eq!(state.cursor, 11);
    }

    #[test]
    fn apply_completion_from_empty_prefix() {
        let mut state = GotoPathState {
            input: "/usr/".to_string(),
            cursor: 5,
            completions: vec!["local".to_string()],
            comp_index: None,
            comp_base: None,
        };
        App::apply_completion(&mut state, "local");
        assert_eq!(state.input, "/usr/local/");
        assert_eq!(state.cursor, 11);
    }

    #[test]
    fn apply_common_prefix_extends() {
        let mut state = GotoPathState {
            input: "/usr/lo".to_string(),
            cursor: 7,
            completions: vec!["local".to_string(), "locale".to_string()],
            comp_index: None,
            comp_base: None,
        };
        let applied = App::apply_common_prefix(&mut state);
        assert!(applied);
        assert_eq!(state.input, "/usr/local");
        assert_eq!(state.cursor, 10);
    }

    #[test]
    fn apply_common_prefix_no_extension() {
        let mut state = GotoPathState {
            input: "/usr/local".to_string(),
            cursor: 10,
            completions: vec!["local".to_string(), "locale".to_string()],
            comp_index: None,
            comp_base: None,
        };
        // Already typed the full common prefix
        let applied = App::apply_common_prefix(&mut state);
        assert!(!applied);
    }

    #[test]
    fn apply_common_prefix_empty_completions() {
        let mut state = GotoPathState {
            input: "/usr/xyz".to_string(),
            cursor: 8,
            completions: vec![],
            comp_index: None,
            comp_base: None,
        };
        let applied = App::apply_common_prefix(&mut state);
        assert!(!applied);
    }

    #[test]
    fn populate_completions_real_fs() {
        // Test against /usr which should exist and have subdirs
        let mut state = GotoPathState {
            input: "/usr/".to_string(),
            cursor: 5,
            completions: vec![],
            comp_index: None,
            comp_base: None,
        };
        App::populate_completions(&mut state);
        // /usr should have at least some subdirectories (bin, lib, etc.)
        assert!(!state.completions.is_empty(), "should find dirs in /usr");
        // All completions should be directory names
        for name in &state.completions {
            let path = PathBuf::from("/usr").join(name);
            assert!(path.is_dir(), "{} should be a directory", name);
        }
    }

    #[test]
    fn populate_completions_with_prefix() {
        let mut state = GotoPathState {
            input: "/usr/lo".to_string(),
            cursor: 7,
            completions: vec![],
            comp_index: None,
            comp_base: None,
        };
        App::populate_completions(&mut state);
        // Should match "local" if it exists
        if PathBuf::from("/usr/local").is_dir() {
            assert!(
                state.completions.iter().any(|c| c == "local"),
                "should find 'local' in /usr with prefix 'lo'"
            );
        }
    }

    #[test]
    fn populate_completions_case_insensitive() {
        let mut state = GotoPathState {
            input: "/usr/LO".to_string(),
            cursor: 7,
            completions: vec![],
            comp_index: None,
            comp_base: None,
        };
        App::populate_completions(&mut state);
        if PathBuf::from("/usr/local").is_dir() {
            assert!(
                state
                    .completions
                    .iter()
                    .any(|c| c.to_lowercase() == "local"),
                "case-insensitive matching should find 'local'"
            );
        }
    }

    #[test]
    fn populate_completions_invalid_dir() {
        let mut state = GotoPathState {
            input: "/nonexistent_path_12345/".to_string(),
            cursor: 23,
            completions: vec![],
            comp_index: None,
            comp_base: None,
        };
        App::populate_completions(&mut state);
        assert!(
            state.completions.is_empty(),
            "invalid dir should yield no completions"
        );
    }

    #[test]
    fn navigate_results_wraps() {
        let paths = vec!["a.rs".to_string(), "b.rs".to_string(), "c.rs".to_string()];
        let mut state = FuzzySearchState::new(paths);
        assert_eq!(state.selected, 0);

        // Move down wraps
        let len = state.results.len().min(8);
        state.selected = (state.selected + 1) % len;
        assert_eq!(state.selected, 1);
        state.selected = (state.selected + 1) % len;
        assert_eq!(state.selected, 2);
        state.selected = (state.selected + 1) % len;
        assert_eq!(state.selected, 0); // wrapped

        // Move up wraps
        state.selected = (state.selected + len - 1) % len;
        assert_eq!(state.selected, 2); // wrapped backward
    }
}
