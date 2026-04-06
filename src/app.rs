use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

const DOUBLE_CLICK_MS: u128 = 400;

const SPLIT_RESIZE_STEP: u16 = 2;
const SPLIT_MIN_PCT: u16 = 20;
const SPLIT_MAX_PCT: u16 = 80;
const SPLIT_DEFAULT_PCT: u16 = 60;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::action::Action;
use crate::ci::CiPanel;
use crate::editor::EditorState;
use crate::file_search::SearchState;
use crate::fs_ops;
use crate::fs_ops::archive::ArchiveFormat;
use crate::hex_viewer::HexViewerState;
use crate::panel::git::GitCache;
use crate::panel::sort::SortField;
use crate::panel::Panel;
use crate::parquet_viewer::ParquetViewerState;
use crate::pr_diff::PrDiffPanel;
use crate::state::AppState;
use crate::terminal::TerminalPanel;
use crate::text_input::TextInput;
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
    /// Which panel currently has keyboard focus.
    pub focus: PanelFocus,
    pub mode: AppMode,
    pub should_quit: bool,
    pub status_message: Option<String>,
    status_message_prev: Option<String>,
    status_message_at: Option<std::time::Instant>,
    pub panel_areas: [Rect; 2],
    pub ci_panel_areas: [Option<Rect>; 2],
    pub shell_panel_areas: [Option<Rect>; 2],
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
    /// Popup overlay: shown centered, dismissed with any key. (title, message)
    pub popup: Option<(String, String)>,
    /// Deferred action to execute after the popup is dismissed (e.g. open editor).
    popup_after: Option<PathBuf>,
    /// Shared git status cache across panels.
    git_cache: GitCache,
    /// Persistent state (search, paths, sort, etc.)
    pub persisted: AppState,
    /// Filesystem watcher for auto-refresh on external changes.
    dir_watcher: Option<DirWatcher>,
    /// CI panels (one per file panel side, independently togglable).
    pub ci_panels: [Option<CiPanel>; 2],
    /// PR diff panels (one per file panel side, independently togglable).
    pub diff_panels: [Option<PrDiffPanel>; 2],
    /// Rendered areas for diff panels (for click detection).
    pub diff_panel_areas: [Option<Rect>; 2],
    /// Go-to-path input per panel side. When Some, a path editor is shown at the top of the panel.
    pub goto_path: [Option<GotoPathState>; 2],
    /// Fuzzy file search per panel side.
    pub fuzzy_search: [Option<FuzzySearchState>; 2],
    /// Help dialog scroll offset.
    pub help_scroll: Option<usize>,
    /// Settings dialog: selected item index, or None when closed.
    pub settings_open: Option<usize>,
    /// Shell panels (one per file panel side, like CI panels).
    pub shell_panels: [Option<TerminalPanel>; 2],
    /// Claude Code panels (one per file panel side, like shell panels).
    pub claude_panels: [Option<TerminalPanel>; 2],
    /// Rendered areas for Claude panels (for click detection and resize).
    pub claude_panel_areas: [Option<Rect>; 2],
    /// SSH panels (one per file panel side, like shell panels).
    pub ssh_panels: [Option<TerminalPanel>; 2],
    /// Rendered areas for SSH panels (for click detection and resize).
    pub ssh_panel_areas: [Option<Rect>; 2],
    /// SSH host info for each side (for reconnection on disconnect).
    pub ssh_hosts: [Option<crate::ssh::SshHost>; 2],
    /// Whether the SSH dialog is open.
    pub ssh_dialog: Option<SshDialogState>,
    /// Session management dialog (tmux sessions).
    pub session_dialog: Option<SessionDialogState>,
    /// Split ratio per side: percentage for file panel (top). Default 60.
    pub bottom_split_pct: [u16; 2],
    /// Per-side maximize toggle for bottom panels.
    pub bottom_maximized: [bool; 2],
    /// File content search results (shown on opposite panel side).
    pub file_search: Option<SearchState>,
    /// Which side the search results are displayed on.
    pub file_search_side: usize,
    /// File content search dialog.
    pub file_search_dialog: Option<FileSearchDialogState>,
    /// Wakeup sender for the event loop (given to terminal reader threads).
    wakeup_sender: Option<crate::event::WakeupSender>,
    /// Previous frame's cursor position (to detect changes and avoid blink reset).
    pub last_cursor_pos: Option<(u16, u16)>,
    /// Whether the UI needs a redraw (set on any state change, cleared after draw).
    pub dirty: bool,
    /// Content area of the currently rendered dialog (set during render, used for click detection).
    pub dialog_content_area: Option<Rect>,
    /// Background archive progress (shown in status bar).
    pub archive_progress: Option<ArchiveProgress>,
    /// Pending background remote connection (SMB or WebDAV).
    pending_remote: Option<PendingRemoteConnect>,
    /// Stashed diff viewer context for F4 editor↔diff toggle.
    pub stashed_diff: Option<StashedDiff>,
}

/// Result of a background remote connection attempt.
/// Contains either a boxed RemoteFs implementation or an error.
struct RemoteConnectResult {
    result: anyhow::Result<Box<dyn crate::remote_fs::RemoteFs + Send>>,
}

struct PendingRemoteConnect {
    rx: std::sync::mpsc::Receiver<RemoteConnectResult>,
    side: usize,
    started: std::time::Instant,
}

pub struct StashedDiff {
    pub repo_root: PathBuf,
    pub file_path: String,
    pub base_branch: String,
    pub cursor: usize,
}

pub struct GotoPathState {
    pub input: TextInput,
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
    pub input: TextInput,
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
            input: TextInput::new(String::new()),
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
        if self.input.text.is_empty() {
            self.results = (0..self.entries.len().min(100)).map(|i| (i, 0)).collect();
        } else {
            // Pre-compute query chars once
            let query_chars: Vec<char> = self.input.text.to_lowercase().chars().collect();
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
    ArchiveDialog(ArchiveDialogState),
    Viewing(Box<ViewerState>),
    HexViewing(Box<HexViewerState>),
    ParquetViewing(Box<ParquetViewerState>),
    DiffViewing(Box<crate::diff_viewer::DiffViewerState>),
    Editing(Box<EditorState>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PanelFocus {
    FilePanel,
    Ci(usize),
    Diff(usize),
    Shell(usize),
    Claude(usize),
    Ssh(usize),
    Search,
}

// --- Simple dialog (delete, mkdir, rename) ---

#[derive(Clone)]
pub struct DialogState {
    pub kind: DialogKind,
    pub title: String,
    pub message: String,
    pub input: TextInput,
    pub has_input: bool,
    pub focused: DialogField,
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
    InputCreateFile,
}

// --- Make folder dialog ---

#[derive(Clone)]
pub struct MkdirDialogState {
    pub input: TextInput,
    pub process_multiple: bool,
    pub focused: MkdirDialogField,
}

impl MkdirDialogState {
    pub fn new() -> Self {
        Self {
            input: TextInput::new(String::new()),
            process_multiple: false,
            focused: MkdirDialogField::Input,
        }
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
    pub query: TextInput,
    pub direction: SearchDirection,
    pub case_sensitive: bool,
    pub focused: SearchDialogField,
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
    pub destination: TextInput,
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
    /// Panel index that was active when the dialog was opened (source side).
    pub source_panel: usize,
}

impl CopyDialogState {
    pub fn new(
        source_name: String,
        source_paths: Vec<PathBuf>,
        destination: String,
        is_move: bool,
        source_panel: usize,
    ) -> Self {
        Self {
            source_name,
            source_paths,
            destination: TextInput::new(destination),
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
            source_panel,
        }
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

// --- File search dialog ---

#[derive(Clone, Copy, PartialEq)]
pub enum FileSearchField {
    Path,
    Term,
    Filter,
    Regex,
    ButtonSearch,
    ButtonCancel,
}

impl FileSearchField {
    pub fn next(self) -> Self {
        match self {
            Self::Term => Self::Path,
            Self::Path => Self::Filter,
            Self::Filter => Self::Regex,
            Self::Regex => Self::ButtonSearch,
            Self::ButtonSearch => Self::ButtonCancel,
            Self::ButtonCancel => Self::Term,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Term => Self::ButtonCancel,
            Self::Path => Self::Term,
            Self::Filter => Self::Path,
            Self::Regex => Self::Filter,
            Self::ButtonSearch => Self::Regex,
            Self::ButtonCancel => Self::ButtonSearch,
        }
    }
    pub fn is_input(self) -> bool {
        matches!(self, Self::Path | Self::Term | Self::Filter)
    }
}

pub struct FileSearchDialogState {
    pub path: crate::text_input::TextInput,
    pub term: crate::text_input::TextInput,
    pub filter: crate::text_input::TextInput,
    pub is_regex: bool,
    pub focused: FileSearchField,
}

impl FileSearchDialogState {
    pub fn new(path: String, term: String, filter: String, is_regex: bool) -> Self {
        Self {
            path: crate::text_input::TextInput::new(path),
            term: crate::text_input::TextInput::new(term),
            filter: crate::text_input::TextInput::new(filter),
            is_regex,
            focused: FileSearchField::Term,
        }
    }

    pub fn active_input(&mut self) -> Option<&mut crate::text_input::TextInput> {
        match self.focused {
            FileSearchField::Path => Some(&mut self.path),
            FileSearchField::Term => Some(&mut self.term),
            FileSearchField::Filter => Some(&mut self.filter),
            _ => None,
        }
    }

    /// Select all text in the newly focused input field.
    pub fn select_focused(&mut self) {
        if let Some(input) = self.active_input() {
            input.select_all();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RemoteProtocol {
    Ssh,
    Sftp,
    Smb,
    WebDav,
    S3,
    Gcs,
    AzureBlob,
    Nfs,
}

impl RemoteProtocol {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ssh => "SSH",
            Self::Sftp => "SFTP",
            Self::Smb => "SMB",
            Self::WebDav => "WebDAV",
            Self::S3 => "S3",
            Self::Gcs => "GCS",
            Self::AzureBlob => "Azure",
            Self::Nfs => "NFS",
        }
    }
    pub fn next(&self) -> Self {
        match self {
            Self::Ssh => Self::Sftp,
            Self::Sftp => Self::Smb,
            Self::Smb => Self::WebDav,
            Self::WebDav => Self::S3,
            Self::S3 => Self::Gcs,
            Self::Gcs => Self::AzureBlob,
            Self::AzureBlob => Self::Nfs,
            Self::Nfs => Self::Ssh,
        }
    }
    pub fn prev(&self) -> Self {
        match self {
            Self::Ssh => Self::Nfs,
            Self::Sftp => Self::Ssh,
            Self::Smb => Self::Sftp,
            Self::WebDav => Self::Smb,
            Self::S3 => Self::WebDav,
            Self::Gcs => Self::S3,
            Self::AzureBlob => Self::Gcs,
            Self::Nfs => Self::AzureBlob,
        }
    }
}

pub struct SshDialogState {
    pub protocol: RemoteProtocol,
    /// Input field: for SSH/SFTP = user@host, for SMB = host, for WebDAV = URL.
    pub input: crate::text_input::TextInput,
    /// SSH host list (for SSH/SFTP modes).
    pub hosts: Vec<crate::ssh::SshHost>,
    pub filtered: Vec<usize>,
    pub selected: usize,
    /// Saved connections (all protocols). Shown at the top of the dialog.
    pub saved_connections: Vec<crate::saved_connections::SavedConnection>,
    /// Index into saved_connections for saved connection selection. None = not in saved mode.
    pub saved_selected: Option<usize>,
    /// SMB-specific fields.
    pub smb_share: crate::text_input::TextInput,
    pub smb_user: crate::text_input::TextInput,
    pub smb_pass: crate::text_input::TextInput,
    /// WebDAV-specific fields.
    pub webdav_user: crate::text_input::TextInput,
    pub webdav_pass: crate::text_input::TextInput,
    /// Which field is focused in SMB/WebDAV mode (0=host/url, 1=share/user, 2=user/pass, 3=pass).
    pub field_focus: usize,
    // S3 fields
    pub s3_bucket: crate::text_input::TextInput,
    pub s3_profile: crate::text_input::TextInput,
    pub s3_endpoint: crate::text_input::TextInput,
    pub s3_region: crate::text_input::TextInput,
    // GCS fields
    pub gcs_bucket: crate::text_input::TextInput,
    pub gcs_project: crate::text_input::TextInput,
    // Azure Blob fields
    pub azure_account: crate::text_input::TextInput,
    pub azure_container: crate::text_input::TextInput,
    pub azure_sas: crate::text_input::TextInput,
    pub azure_conn_str: crate::text_input::TextInput,
    // NFS fields
    pub nfs_host: crate::text_input::TextInput,
    pub nfs_export: crate::text_input::TextInput,
    pub nfs_options: crate::text_input::TextInput,
}

impl SshDialogState {
    pub fn new() -> Self {
        let hosts = crate::ssh::load_all_hosts();
        let filtered: Vec<usize> = (0..hosts.len()).collect();
        let saved_connections = crate::saved_connections::load_connections();
        let has_saved = !saved_connections.is_empty();
        Self {
            protocol: RemoteProtocol::Ssh,
            input: crate::text_input::TextInput::new(String::new()),
            hosts,
            filtered,
            selected: 0,
            saved_connections,
            saved_selected: if has_saved { Some(0) } else { None },
            smb_share: crate::text_input::TextInput::new(String::new()),
            smb_user: crate::text_input::TextInput::new(String::new()),
            smb_pass: crate::text_input::TextInput::new(String::new()),
            webdav_user: crate::text_input::TextInput::new(String::new()),
            webdav_pass: crate::text_input::TextInput::new(String::new()),
            field_focus: 0,
            s3_bucket: crate::text_input::TextInput::new(String::new()),
            s3_profile: crate::text_input::TextInput::new(String::new()),
            s3_endpoint: crate::text_input::TextInput::new(String::new()),
            s3_region: crate::text_input::TextInput::new(String::new()),
            gcs_bucket: crate::text_input::TextInput::new(String::new()),
            gcs_project: crate::text_input::TextInput::new(String::new()),
            azure_account: crate::text_input::TextInput::new(String::new()),
            azure_container: crate::text_input::TextInput::new(String::new()),
            azure_sas: crate::text_input::TextInput::new(String::new()),
            azure_conn_str: crate::text_input::TextInput::new(String::new()),
            nfs_host: crate::text_input::TextInput::new(String::new()),
            nfs_export: crate::text_input::TextInput::new(String::new()),
            nfs_options: crate::text_input::TextInput::new(String::new()),
        }
    }

    pub fn update_filter(&mut self) {
        let query = self.input.text.to_lowercase();
        if query.is_empty() {
            self.filtered = (0..self.hosts.len()).collect();
        } else {
            self.filtered = self
                .hosts
                .iter()
                .enumerate()
                .filter(|(_, h)| {
                    h.name.to_lowercase().contains(&query)
                        || h.hostname.to_lowercase().contains(&query)
                        || h.user
                            .as_deref()
                            .map(|u| u.to_lowercase().contains(&query))
                            .unwrap_or(false)
                        || h.group
                            .as_deref()
                            .map(|g| g.to_lowercase().contains(&query))
                            .unwrap_or(false)
                })
                .map(|(i, _)| i)
                .collect();
        }
        self.selected = 0;
    }

    pub fn selected_host(&self) -> Option<&crate::ssh::SshHost> {
        self.filtered
            .get(self.selected)
            .and_then(|&i| self.hosts.get(i))
    }

    /// Get the active text input for the current protocol/field.
    pub fn active_input_mut(&mut self) -> &mut crate::text_input::TextInput {
        match self.protocol {
            RemoteProtocol::Ssh | RemoteProtocol::Sftp => &mut self.input,
            RemoteProtocol::Smb => match self.field_focus {
                0 => &mut self.input,
                1 => &mut self.smb_share,
                2 => &mut self.smb_user,
                _ => &mut self.smb_pass,
            },
            RemoteProtocol::WebDav => match self.field_focus {
                0 => &mut self.input,
                1 => &mut self.webdav_user,
                _ => &mut self.webdav_pass,
            },
            RemoteProtocol::S3 => match self.field_focus {
                0 => &mut self.s3_bucket,
                1 => &mut self.s3_profile,
                2 => &mut self.s3_endpoint,
                _ => &mut self.s3_region,
            },
            RemoteProtocol::Gcs => match self.field_focus {
                0 => &mut self.gcs_bucket,
                _ => &mut self.gcs_project,
            },
            RemoteProtocol::AzureBlob => match self.field_focus {
                0 => &mut self.azure_account,
                1 => &mut self.azure_container,
                2 => &mut self.azure_sas,
                _ => &mut self.azure_conn_str,
            },
            RemoteProtocol::Nfs => match self.field_focus {
                0 => &mut self.nfs_host,
                1 => &mut self.nfs_export,
                _ => &mut self.nfs_options,
            },
        }
    }

    pub fn max_fields(&self) -> usize {
        match self.protocol {
            RemoteProtocol::Ssh | RemoteProtocol::Sftp => 1,
            RemoteProtocol::Smb => 4,
            RemoteProtocol::WebDav | RemoteProtocol::Nfs => 3,
            RemoteProtocol::AzureBlob => 4,
            RemoteProtocol::S3 => 4,
            RemoteProtocol::Gcs => 2,
        }
    }
}

pub struct SessionDialogState {
    pub sessions: Vec<crate::session::MmSession>,
    pub selected: usize,
    pub input: crate::text_input::TextInput,
    /// True when the user is in "create new session" input mode.
    pub creating: bool,
}

impl SessionDialogState {
    pub fn new() -> Self {
        let sessions = crate::session::list_sessions();
        Self {
            sessions,
            selected: 0,
            input: crate::text_input::TextInput::new(String::new()),
            creating: false,
        }
    }

    pub fn refresh(&mut self) {
        self.sessions = crate::session::list_sessions();
        if self.selected >= self.sessions.len() && !self.sessions.is_empty() {
            self.selected = self.sessions.len() - 1;
        }
    }

    pub fn selected_session(&self) -> Option<&crate::session::MmSession> {
        self.sessions.get(self.selected)
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

// --- Archive dialog ---

#[derive(Clone, Copy, PartialEq)]
pub enum ArchiveDialogField {
    ArchiveName,
    Destination,
    Format,
    ButtonArchive,
    ButtonCancel,
}

impl ArchiveDialogField {
    pub fn next(self) -> Self {
        match self {
            Self::ArchiveName => Self::Destination,
            Self::Destination => Self::Format,
            Self::Format => Self::ButtonArchive,
            Self::ButtonArchive => Self::ButtonCancel,
            Self::ButtonCancel => Self::ArchiveName,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::ArchiveName => Self::ButtonCancel,
            Self::Destination => Self::ArchiveName,
            Self::Format => Self::Destination,
            Self::ButtonArchive => Self::Format,
            Self::ButtonCancel => Self::ButtonArchive,
        }
    }
    pub fn is_input(self) -> bool {
        matches!(self, Self::ArchiveName | Self::Destination)
    }
}

pub struct ArchiveDialogState {
    pub source_paths: Vec<PathBuf>,
    pub source_name: String,
    pub archive_name: TextInput,
    pub destination: TextInput,
    pub format: ArchiveFormat,
    pub focused: ArchiveDialogField,
}

impl ArchiveDialogState {
    pub fn new(
        source_name: String,
        source_paths: Vec<PathBuf>,
        dest_dir: String,
        format: ArchiveFormat,
    ) -> Self {
        let suggested = fs_ops::archive::suggest_archive_name(&source_paths, format);
        Self {
            source_paths,
            source_name,
            archive_name: TextInput::new(suggested),
            destination: TextInput::new(dest_dir),
            format,
            focused: ArchiveDialogField::ArchiveName,
        }
    }

    pub fn active_input(&mut self) -> Option<&mut TextInput> {
        match self.focused {
            ArchiveDialogField::ArchiveName => Some(&mut self.archive_name),
            ArchiveDialogField::Destination => Some(&mut self.destination),
            _ => None,
        }
    }

    /// Update the archive name extension when format changes.
    pub fn update_name_extension(&mut self) {
        let name = &self.archive_name.text;
        // Strip any existing archive extension
        let base = strip_archive_extension(name);
        let new_name = format!("{}{}", base, self.format.extension());
        self.archive_name = TextInput::new(new_name);
        self.archive_name.select_all();
    }
}

fn strip_archive_extension(name: &str) -> &str {
    for ext in &[".tar.zst", ".tar.gz", ".tar.xz", ".zip"] {
        if let Some(base) = name.strip_suffix(ext) {
            return base;
        }
    }
    // Try stripping just a single extension
    name.rsplit_once('.').map(|(base, _)| base).unwrap_or(name)
}

pub struct ArchiveProgress {
    pub total_bytes: u64,
    pub done_bytes: Arc<AtomicU64>,
    pub finished: Arc<AtomicBool>,
    pub error: Arc<Mutex<Option<String>>>,
    pub output_path: PathBuf,
}

// ============================================================
// App implementation
// ============================================================

impl App {
    pub fn new() -> Self {
        let persisted = AppState::load();

        // Clean up stale temp files from previous remote editing sessions
        let tmp_edit_dir = std::env::temp_dir().join("middle-manager-edit");
        if tmp_edit_dir.exists() {
            let _ = std::fs::remove_dir_all(&tmp_edit_dir);
        }
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
            focus: PanelFocus::FilePanel,
            mode: AppMode::Normal,
            should_quit: false,
            status_message: None,
            status_message_prev: None,
            status_message_at: None,
            panel_areas: [Rect::default(); 2],
            ci_panel_areas: [None, None],
            shell_panel_areas: [None, None],
            last_click: None,
            goto_line_input: None,
            needs_clear: false,
            search_dialog: None,
            unsaved_dialog: None,
            git_cache,
            persisted,
            dir_watcher,
            ci_panels: [None, None],
            diff_panels: [None, None],
            diff_panel_areas: [None, None],
            quit_confirm: None,
            popup: None,
            popup_after: None,
            goto_path: [None, None],
            fuzzy_search: [None, None],
            help_scroll: None,
            settings_open: None,
            shell_panels: [None, None],
            claude_panels: [None, None],
            claude_panel_areas: [None, None],
            ssh_panels: [None, None],
            ssh_panel_areas: [None, None],
            ssh_hosts: [None, None],
            ssh_dialog: None,
            session_dialog: None,
            bottom_split_pct: [SPLIT_DEFAULT_PCT, SPLIT_DEFAULT_PCT],
            bottom_maximized: [false, false],
            file_search: None,
            file_search_side: 1,
            file_search_dialog: None,
            wakeup_sender: None,
            last_cursor_pos: None,
            dirty: true,
            dialog_content_area: None,
            archive_progress: None,
            pending_remote: None,
            stashed_diff: None,
        }
    }

    /// Set the wakeup sender (called from main after creating the event handler).
    pub fn set_wakeup_sender(&mut self, sender: crate::event::WakeupSender) {
        self.wakeup_sender = Some(sender);
    }

    /// Restore bottom panels from persisted state. Call after set_wakeup_sender.
    pub fn restore_bottom_panels(&mut self) {
        let wakeup = match self.wakeup_sender {
            Some(ref w) => w.clone(),
            None => return,
        };

        for side in 0..2 {
            let panels_str = if side == 0 {
                &self.persisted.left_bottom_panels
            } else {
                &self.persisted.right_bottom_panels
            };
            if panels_str.is_empty() {
                continue;
            }

            let dir = self.panels[side].current_dir.clone();
            let area_width = 80u16; // initial estimate; corrected on first render
            let area_height = 24u16;

            for panel_type in panels_str.split(',') {
                match panel_type.trim() {
                    "ci" => {
                        if let Some(ref gi) = self.panels[side].git_info {
                            self.ci_panels[side] = Some(CiPanel::for_branch(&dir, &gi.branch));
                            self.bottom_split_pct[side] = self.persisted.split_pct_ci;
                        }
                    }
                    "diff" => {
                        if self.panels[side].git_info.is_some() {
                            self.diff_panels[side] = Some(PrDiffPanel::for_branch(&dir));
                            self.bottom_split_pct[side] = self.persisted.split_pct_ci;
                        }
                    }
                    "shell" => {
                        if let Ok(tp) = TerminalPanel::spawn_shell(
                            &dir,
                            area_width,
                            area_height,
                            wakeup.clone(),
                        ) {
                            self.shell_panels[side] = Some(tp);
                            self.bottom_split_pct[side] = self.persisted.split_pct_shell;
                        }
                    }
                    "claude" => {
                        let claude_dir = if side == 0 {
                            self.persisted.claude_dir_left.as_deref()
                        } else {
                            self.persisted.claude_dir_right.as_deref()
                        }
                        .map(PathBuf::from)
                        .filter(|p| p.is_dir())
                        .unwrap_or_else(|| dir.clone());
                        if let Ok(tp) = TerminalPanel::spawn_claude_continue(
                            &claude_dir,
                            area_width,
                            area_height,
                            wakeup.clone(),
                        ) {
                            self.claude_panels[side] = Some(tp);
                            self.bottom_maximized[side] = true;
                            self.bottom_split_pct[side] = self.persisted.split_pct_claude;
                        }
                    }
                    "ssh" => {
                        // Restore SSH host info (panel stays None until user reconnects)
                        let host_name = if side == 0 {
                            self.persisted.ssh_host_left.as_deref()
                        } else {
                            self.persisted.ssh_host_right.as_deref()
                        };
                        if let Some(name) = host_name {
                            // Try to find the host in saved/config hosts
                            let hosts = crate::ssh::load_all_hosts();
                            if let Some(host) = hosts.into_iter().find(|h| h.name == name) {
                                // Auto-connect on restore
                                if let Ok(tp) = TerminalPanel::spawn_ssh(
                                    &host,
                                    area_width,
                                    area_height,
                                    wakeup.clone(),
                                ) {
                                    self.ssh_panels[side] = Some(tp);
                                    self.ssh_hosts[side] = Some(host);
                                    self.bottom_split_pct[side] = self.persisted.split_pct_ssh;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Save current state to disk.
    /// Whether the cursor is in a dialog input field (uses block cursor for visibility).
    pub fn save_state(&mut self) {
        self.persisted.left_panel_path =
            Some(self.panels[0].current_dir.to_string_lossy().to_string());
        self.persisted.right_panel_path =
            Some(self.panels[1].current_dir.to_string_lossy().to_string());
        self.persisted.left_sort_field = sort_field_to_u8(self.panels[0].sort_field);
        self.persisted.left_sort_ascending = self.panels[0].sort_ascending;
        self.persisted.right_sort_field = sort_field_to_u8(self.panels[1].sort_field);
        self.persisted.right_sort_ascending = self.panels[1].sort_ascending;
        // Save split ratios and open bottom panels
        for side in 0..2 {
            let pct = self.bottom_split_pct[side];
            let mut panels = Vec::new();
            if self.ci_panels[side].is_some() {
                self.persisted.split_pct_ci = pct;
                panels.push("ci");
            }
            if self.diff_panels[side].is_some() {
                panels.push("diff");
            }
            if self.shell_panels[side].is_some() {
                self.persisted.split_pct_shell = pct;
                panels.push("shell");
            }
            if let Some(ref cp) = self.claude_panels[side] {
                self.persisted.split_pct_claude = pct;
                panels.push("claude");
                let dir = Some(cp.spawn_dir.to_string_lossy().to_string());
                if side == 0 {
                    self.persisted.claude_dir_left = dir;
                } else {
                    self.persisted.claude_dir_right = dir;
                }
            }
            if self.ssh_panels[side].is_some() || self.ssh_hosts[side].is_some() {
                self.persisted.split_pct_ssh = pct;
                panels.push("ssh");
                let host_name = self.ssh_hosts[side].as_ref().map(|h| h.name.clone());
                if side == 0 {
                    self.persisted.ssh_host_left = host_name;
                } else {
                    self.persisted.ssh_host_right = host_name;
                }
            }
            if side == 0 {
                self.persisted.left_bottom_panels = panels.join(",");
            } else {
                self.persisted.right_bottom_panels = panels.join(",");
            }
        }
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
                    if self.focus == PanelFocus::Ci(side) {
                        self.focus = PanelFocus::FilePanel;
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

    /// Delete a path using the appropriate backend for the active panel.
    fn remote_delete(&self, path: &std::path::Path) -> anyhow::Result<()> {
        match &self.panels[self.active_panel].source {
            crate::panel::PanelSource::Local => fs_ops::delete_entry(path),
            crate::panel::PanelSource::Remote { connection } => connection.remove_recursive(path),
        }
    }

    /// Create a directory using the appropriate backend for the active panel.
    /// Create an empty file using the appropriate backend.
    fn remote_create_file(&self, path: &std::path::Path) -> anyhow::Result<()> {
        match &self.panels[self.active_panel].source {
            crate::panel::PanelSource::Local => {
                std::fs::File::create(path)?;
                Ok(())
            }
            crate::panel::PanelSource::Remote { connection } => {
                // For remote: create a temp empty file, upload it, delete the temp
                let tmp = std::env::temp_dir().join(format!(
                    "mm-touch-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ));
                std::fs::File::create(&tmp)?;
                let result = connection.upload(&tmp, path);
                let _ = std::fs::remove_file(&tmp);
                result.map(|_| ())
            }
        }
    }

    fn remote_mkdir(&self, path: &std::path::Path) -> anyhow::Result<()> {
        match &self.panels[self.active_panel].source {
            crate::panel::PanelSource::Local => {
                std::fs::create_dir_all(path)?;
                Ok(())
            }
            crate::panel::PanelSource::Remote { connection } => connection.mkdir(path),
        }
    }

    /// Rename a path using the appropriate backend for the active panel.
    fn remote_rename(&self, src: &std::path::Path, dst: &std::path::Path) -> anyhow::Result<()> {
        match &self.panels[self.active_panel].source {
            crate::panel::PanelSource::Local => {
                std::fs::rename(src, dst)?;
                Ok(())
            }
            crate::panel::PanelSource::Remote { connection } => connection.rename(src, dst),
        }
    }

    /// Download a remote file/dir to local. Returns bytes transferred.
    fn remote_download(
        &self,
        side: usize,
        remote: &std::path::Path,
        local: &std::path::Path,
        is_dir: bool,
    ) -> anyhow::Result<u64> {
        match &self.panels[side].source {
            crate::panel::PanelSource::Remote { connection } => {
                if is_dir { connection.download_dir(remote, local) }
                else { connection.download(remote, local) }
            }
            _ => anyhow::bail!("Not a remote panel"),
        }
    }

    /// Upload a local file/dir to remote. Returns bytes transferred.
    fn remote_upload(
        &self,
        side: usize,
        local: &std::path::Path,
        remote: &std::path::Path,
        is_dir: bool,
    ) -> anyhow::Result<u64> {
        match &self.panels[side].source {
            crate::panel::PanelSource::Remote { connection } => {
                if is_dir { connection.upload_dir(local, remote) }
                else { connection.upload(local, remote) }
            }
            _ => anyhow::bail!("Not a remote panel"),
        }
    }

    /// Which side (0 or 1) the focused bottom panel is on, or active_panel if no bottom panel focused.
    fn focused_side(&self) -> usize {
        match self.focus {
            PanelFocus::Ci(s)
            | PanelFocus::Diff(s)
            | PanelFocus::Shell(s)
            | PanelFocus::Claude(s)
            | PanelFocus::Ssh(s) => s,
            _ => self.active_panel,
        }
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
                KeyCode::Char('z')
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::EditorRedo
                }
                KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::EditorUndo
                }
                KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::EditorDeleteLine
                } // cut
                KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::SelectAll
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::CopySelection
                }
                KeyCode::Delete => Action::EditorDeleteForward,
                KeyCode::Left if key.modifiers.contains(KeyModifiers::SHIFT) => Action::SelectLeft,
                KeyCode::Right if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectRight
                }
                KeyCode::Home if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectLineStart
                }
                KeyCode::End if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectLineEnd
                }
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

        // File content search dialog intercepts keys
        if let Some(ref state) = self.file_search_dialog {
            let focused = state.focused;
            let on_buttons = matches!(
                focused,
                FileSearchField::ButtonSearch | FileSearchField::ButtonCancel
            );
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Tab => Action::MoveDown,
                KeyCode::BackTab => Action::MoveUp,
                KeyCode::Up if !focused.is_input() || on_buttons => Action::MoveUp,
                KeyCode::Down if on_buttons => Action::None,
                KeyCode::Down if !focused.is_input() => Action::MoveDown,
                KeyCode::Left if on_buttons => Action::SwitchPanel,
                KeyCode::Right if on_buttons => Action::SwitchPanel,
                KeyCode::Char(' ') if focused == FileSearchField::Regex => Action::Toggle,
                // Text input with selection, undo/redo, cut support
                KeyCode::Char('z')
                    if focused.is_input()
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                        && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::EditorRedo
                }
                KeyCode::Char('z')
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    Action::EditorUndo
                }
                KeyCode::Char('x')
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    Action::EditorDeleteLine
                }
                KeyCode::Char('a')
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    Action::SelectAll
                }
                KeyCode::Char('c')
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    Action::CopySelection
                }
                KeyCode::Char(c) if focused.is_input() => Action::DialogInput(c),
                KeyCode::Backspace if focused.is_input() => Action::DialogBackspace,
                KeyCode::Delete if focused.is_input() => Action::EditorDeleteForward,
                KeyCode::Left
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::SelectLeft
                }
                KeyCode::Right
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::SelectRight
                }
                KeyCode::Home
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::SelectLineStart
                }
                KeyCode::End
                    if focused.is_input() && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::SelectLineEnd
                }
                KeyCode::Left if focused.is_input() => Action::CursorLeft,
                KeyCode::Right if focused.is_input() => Action::CursorRight,
                KeyCode::Home if focused.is_input() => Action::CursorLineStart,
                KeyCode::End if focused.is_input() => Action::CursorLineEnd,
                _ => Action::None,
            };
        }

        // Fuzzy file search input intercepts keys
        if self.fuzzy_search[self.active_panel].is_some() {
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Backspace => Action::DialogBackspace,
                KeyCode::Char('z')
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && key.modifiers.contains(KeyModifiers::SHIFT) =>
                {
                    Action::EditorRedo
                }
                KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::EditorUndo
                }
                KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::EditorDeleteLine
                } // cut
                KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::SelectAll
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Action::CopySelection
                }
                KeyCode::Delete => Action::EditorDeleteForward,
                KeyCode::Left if key.modifiers.contains(KeyModifiers::SHIFT) => Action::SelectLeft,
                KeyCode::Right if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectRight
                }
                KeyCode::Home if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectLineStart
                }
                KeyCode::End if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    Action::SelectLineEnd
                }
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

        // Bottom panel focus intercepts only in normal/quick-search modes
        // (full-screen modes like DiffViewing/Editing map their own keys via AppMode match below)
        if matches!(self.mode, AppMode::Normal | AppMode::QuickSearch) {
            // Claude panel intercepts keys when focused
            if matches!(self.focus, PanelFocus::Claude(_)) {
                return match key.code {
                    KeyCode::F(5) => Action::TerminalOpenFile,
                    KeyCode::F(12) => Action::ToggleClaude,
                    KeyCode::F(10) => Action::Quit,
                    KeyCode::F(1) => Action::SwitchPanel,
                    _ => Action::TerminalInput(crate::terminal::encode_key_event(key)),
                };
            }

            // File search results intercepts keys when focused
            if self.focus == PanelFocus::Search && self.file_search.is_some() {
                return match key.code {
                    KeyCode::Esc => Action::DialogCancel,
                    KeyCode::Enter => Action::DialogConfirm,
                    KeyCode::Up => Action::MoveUp,
                    KeyCode::Down => Action::MoveDown,
                    KeyCode::PageUp => Action::PageUp,
                    KeyCode::PageDown => Action::PageDown,
                    KeyCode::Home => Action::MoveToTop,
                    KeyCode::End => Action::MoveToBottom,
                    KeyCode::Right => Action::CursorRight,
                    KeyCode::Left => Action::GoUp,
                    KeyCode::Tab => Action::SwitchPanel,
                    KeyCode::BackTab => Action::SwitchPanelReverse,
                    KeyCode::F(10) => Action::Quit,
                    _ => Action::None,
                };
            }

            // Shell panel intercepts keys when focused
            if matches!(self.focus, PanelFocus::Shell(_)) {
                return match key.code {
                    KeyCode::F(1) => Action::SwitchPanel,
                    KeyCode::F(10) => Action::Quit,
                    KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Action::ToggleShell
                    }
                    KeyCode::Up if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeUp
                    }
                    KeyCode::Down if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeDown
                    }
                    KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomMaximize
                    }
                    _ => Action::TerminalInput(crate::terminal::encode_key_event(key)),
                };
            }

            // SSH panel intercepts keys when focused
            if matches!(self.focus, PanelFocus::Ssh(_)) {
                return match key.code {
                    KeyCode::F(1) => Action::SwitchPanel,
                    KeyCode::F(10) => Action::Quit,
                    KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Action::ToggleSsh
                    }
                    KeyCode::F(2) if key.modifiers.contains(KeyModifiers::SHIFT) => {
                        Action::ToggleSsh
                    }
                    KeyCode::Up if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeUp
                    }
                    KeyCode::Down if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeDown
                    }
                    KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomMaximize
                    }
                    _ => Action::TerminalInput(crate::terminal::encode_key_event(key)),
                };
            }

            // CI panel intercepts keys when focused
            if matches!(self.focus, PanelFocus::Ci(_)) {
                return match key.code {
                    KeyCode::Up if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeUp
                    }
                    KeyCode::Down if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeDown
                    }
                    KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomMaximize
                    }
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
                    KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::ExtractCiFailures,
                    KeyCode::Tab => Action::SwitchPanel,
                    KeyCode::BackTab => Action::SwitchPanelReverse,
                    KeyCode::F(2) => {
                        if key.modifiers.contains(KeyModifiers::SHIFT) {
                            Action::ToggleSsh
                        } else {
                            Action::ToggleCi
                        }
                    }
                    KeyCode::F(10) => Action::Quit,
                    _ => Action::None,
                };
            }

            // Diff panel intercepts keys when focused
            if matches!(self.focus, PanelFocus::Diff(_)) {
                return match key.code {
                    KeyCode::Up if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeUp
                    }
                    KeyCode::Down if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomResizeDown
                    }
                    KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                        Action::BottomMaximize
                    }
                    KeyCode::Up => Action::MoveUp,
                    KeyCode::Down => Action::MoveDown,
                    KeyCode::PageUp => Action::PageUp,
                    KeyCode::PageDown => Action::PageDown,
                    KeyCode::Home => Action::MoveToTop,
                    KeyCode::End => Action::MoveToBottom,
                    KeyCode::Enter => Action::Enter,
                    KeyCode::F(4) => Action::EditBuiltin,
                    KeyCode::Right => Action::CursorRight,
                    KeyCode::Left => Action::GoUp,
                    KeyCode::Tab => Action::SwitchPanel,
                    KeyCode::BackTab => Action::SwitchPanelReverse,
                    KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Action::ToggleDiff
                    }
                    KeyCode::Esc => Action::QuickSearchClear,
                    KeyCode::F(10) => Action::Quit,
                    KeyCode::Char(c) if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' => {
                        Action::QuickSearch(c)
                    }
                    _ => Action::None,
                };
            }
        }

        // Settings dialog intercepts keys when open
        if self.settings_open.is_some() {
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Up => Action::MoveUp,
                KeyCode::Down => Action::MoveDown,
                KeyCode::Left => Action::CursorLeft,
                KeyCode::Right => Action::CursorRight,
                KeyCode::Char(' ') => Action::Toggle,
                KeyCode::Enter => Action::Enter,
                _ => Action::None,
            };
        }

        // Session dialog intercepts keys when open
        if let Some(ref dialog) = self.session_dialog {
            if dialog.creating {
                return match key.code {
                    KeyCode::Esc => Action::DialogCancel,
                    KeyCode::Enter => Action::DialogConfirm,
                    KeyCode::Backspace => Action::DialogBackspace,
                    KeyCode::Char(c) => Action::DialogInput(c),
                    _ => Action::None,
                };
            }
            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                KeyCode::Up => Action::MoveUp,
                KeyCode::Down => Action::MoveDown,
                KeyCode::Char('n') => Action::CreateDir, // reuse as "new"
                KeyCode::Char('d') | KeyCode::Delete => Action::Delete,
                _ => Action::None,
            };
        }

        // Remote connect dialog intercepts keys when open
        if let Some(ref dialog) = self.ssh_dialog {
            // Saved connections browsing mode
            if dialog.saved_selected.is_some() {
                return match key.code {
                    KeyCode::Esc => Action::DialogCancel,
                    KeyCode::Enter => Action::DialogConfirm,
                    KeyCode::Up => Action::MoveUp,
                    KeyCode::Down => Action::MoveDown,
                    KeyCode::Tab => Action::Toggle, // Switch to protocol input mode
                    KeyCode::Delete => Action::Delete, // Delete saved connection
                    KeyCode::F(2) => Action::EditorSave, // Save current fields
                    _ => Action::None,
                };
            }

            return match key.code {
                KeyCode::Esc => Action::DialogCancel,
                KeyCode::Enter => Action::DialogConfirm,
                // Alt+Left/Right cycle protocol
                KeyCode::Left if key.modifiers.contains(KeyModifiers::ALT) => Action::SwitchPanelReverse,
                KeyCode::Right if key.modifiers.contains(KeyModifiers::ALT) => Action::SwitchPanel,
                // Tab/BackTab cycle fields (host list for SSH/SFTP, form fields for others)
                KeyCode::Tab => Action::MoveDown,
                KeyCode::BackTab => Action::MoveUp,
                KeyCode::Up => Action::MoveUp,
                KeyCode::Down => Action::MoveDown,
                KeyCode::Backspace => Action::DialogBackspace,
                KeyCode::F(2) => Action::EditorSave, // Save current fields
                KeyCode::Char(c) => Action::DialogInput(c),
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
            AppMode::ArchiveDialog(state) => Self::map_archive_dialog_key(key, state.focused),
            AppMode::Viewing(_) | AppMode::HexViewing(_) => self.map_viewer_key(key),
            AppMode::ParquetViewing(_) => self.map_parquet_key(key),
            AppMode::DiffViewing(ref d) => {
                if d.search_input.is_some() {
                    return match key.code {
                        KeyCode::Esc => Action::DialogCancel,
                        KeyCode::Enter => Action::DialogConfirm,
                        KeyCode::Backspace => Action::DialogBackspace,
                        KeyCode::Char(c) => Action::DialogInput(c),
                        _ => Action::None,
                    };
                }
                Self::map_diff_viewer_key(key)
            }
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
            KeyCode::F(1) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::ToggleSettings
                } else {
                    Action::ShowHelp
                }
            }
            KeyCode::F(3) => Action::ViewFile,
            KeyCode::F(4) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::EditFile // external $EDITOR
                } else {
                    Action::EditBuiltin
                }
            }
            KeyCode::F(5) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::Archive
                } else {
                    Action::Copy
                }
            }
            KeyCode::F(6) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::Rename
                } else {
                    Action::Move
                }
            }
            KeyCode::F(7) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::CreateFile
                } else {
                    Action::CreateDir
                }
            }
            KeyCode::F(8) => Action::Delete,
            KeyCode::F(9) => Action::CycleSort,
            KeyCode::F(10) => Action::Quit,
            KeyCode::F(2) => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Action::ToggleSsh
                } else {
                    Action::ToggleCi
                }
            }
            KeyCode::F(11) => Action::OpenPr,
            KeyCode::F(12) => Action::ToggleClaude,
            KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::CopyName,
            KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::CopyPath,
            KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::GotoPathPrompt
            }
            KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::ToggleShell
            }
            KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::FuzzySearchPrompt
            }
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::FileSearchPrompt
            }
            KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::ToggleSsh
            }
            KeyCode::Char('y') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::ToggleSessions
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::ToggleDiff
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
            KeyCode::Char('z')
                if focused == DialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::EditorRedo
            }
            KeyCode::Char('z')
                if focused == DialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorUndo
            }
            KeyCode::Char('x')
                if focused == DialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorDeleteLine
            }
            KeyCode::Char('a')
                if focused == DialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::SelectAll
            }
            KeyCode::Char('c')
                if focused == DialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::CopySelection
            }
            KeyCode::Char(c) if focused == DialogField::Input => Action::DialogInput(c),
            KeyCode::Backspace if focused == DialogField::Input => Action::DialogBackspace,
            KeyCode::Delete if focused == DialogField::Input => Action::EditorDeleteForward,
            KeyCode::Left
                if focused == DialogField::Input && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLeft
            }
            KeyCode::Right
                if focused == DialogField::Input && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectRight
            }
            KeyCode::Home
                if focused == DialogField::Input && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineStart
            }
            KeyCode::End
                if focused == DialogField::Input && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineEnd
            }
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
            KeyCode::Char('z')
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::EditorRedo
            }
            KeyCode::Char('z')
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorUndo
            }
            KeyCode::Char('x')
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorDeleteLine
            }
            KeyCode::Char('a')
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::SelectAll
            }
            KeyCode::Char('c')
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::CopySelection
            }
            KeyCode::Char(c) if focused == MkdirDialogField::Input => Action::DialogInput(c),
            KeyCode::Backspace if focused == MkdirDialogField::Input => Action::DialogBackspace,
            KeyCode::Delete if focused == MkdirDialogField::Input => Action::EditorDeleteForward,
            KeyCode::Left
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLeft
            }
            KeyCode::Right
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectRight
            }
            KeyCode::Home
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineStart
            }
            KeyCode::End
                if focused == MkdirDialogField::Input
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineEnd
            }
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
            KeyCode::Char('z')
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::EditorRedo
            }
            KeyCode::Char('z')
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorUndo
            }
            KeyCode::Char('x')
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorDeleteLine
            }
            KeyCode::Char('a')
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::SelectAll
            }
            KeyCode::Char('c')
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::CopySelection
            }
            KeyCode::Char(c) if focused == CopyDialogField::Destination => Action::DialogInput(c),
            KeyCode::Backspace if focused == CopyDialogField::Destination => {
                Action::DialogBackspace
            }
            KeyCode::Delete if focused == CopyDialogField::Destination => {
                Action::EditorDeleteForward
            }
            KeyCode::Left
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLeft
            }
            KeyCode::Right
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectRight
            }
            KeyCode::Home
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineStart
            }
            KeyCode::End
                if focused == CopyDialogField::Destination
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineEnd
            }
            KeyCode::Left if focused == CopyDialogField::Destination => Action::CursorLeft,
            KeyCode::Right if focused == CopyDialogField::Destination => Action::CursorRight,
            KeyCode::Home if focused == CopyDialogField::Destination => Action::CursorLineStart,
            KeyCode::End if focused == CopyDialogField::Destination => Action::CursorLineEnd,
            _ => Action::None,
        }
    }

    fn map_archive_dialog_key(key: KeyEvent, focused: ArchiveDialogField) -> Action {
        let on_buttons = matches!(
            focused,
            ArchiveDialogField::ButtonArchive | ArchiveDialogField::ButtonCancel
        );
        let on_input = focused.is_input();
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
            KeyCode::Char(' ') if focused == ArchiveDialogField::Format => Action::Toggle,
            KeyCode::Char('z')
                if on_input
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::EditorRedo
            }
            KeyCode::Char('z') if on_input && key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::EditorUndo
            }
            KeyCode::Char('x') if on_input && key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::EditorDeleteLine
            }
            KeyCode::Char('a') if on_input && key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::SelectAll
            }
            KeyCode::Char('c') if on_input && key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::CopySelection
            }
            KeyCode::Char(c) if on_input => Action::DialogInput(c),
            KeyCode::Backspace if on_input => Action::DialogBackspace,
            KeyCode::Delete if on_input => Action::EditorDeleteForward,
            KeyCode::Left if on_input && key.modifiers.contains(KeyModifiers::SHIFT) => {
                Action::SelectLeft
            }
            KeyCode::Right if on_input && key.modifiers.contains(KeyModifiers::SHIFT) => {
                Action::SelectRight
            }
            KeyCode::Home if on_input && key.modifiers.contains(KeyModifiers::SHIFT) => {
                Action::SelectLineStart
            }
            KeyCode::End if on_input && key.modifiers.contains(KeyModifiers::SHIFT) => {
                Action::SelectLineEnd
            }
            KeyCode::Left if on_input => Action::CursorLeft,
            KeyCode::Right if on_input => Action::CursorRight,
            KeyCode::Home if on_input => Action::CursorLineStart,
            KeyCode::End if on_input => Action::CursorLineEnd,
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
            KeyCode::Char('z')
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::EditorRedo
            }
            KeyCode::Char('z')
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorUndo
            }
            KeyCode::Char('x')
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::EditorDeleteLine
            }
            KeyCode::Char('a')
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::SelectAll
            }
            KeyCode::Char('c')
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                Action::CopySelection
            }
            KeyCode::Char(c) if focused == SearchDialogField::Query => Action::DialogInput(c),
            KeyCode::Backspace if focused == SearchDialogField::Query => Action::DialogBackspace,
            KeyCode::Delete if focused == SearchDialogField::Query => Action::EditorDeleteForward,
            KeyCode::Left
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLeft
            }
            KeyCode::Right
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectRight
            }
            KeyCode::Home
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineStart
            }
            KeyCode::End
                if focused == SearchDialogField::Query
                    && key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                Action::SelectLineEnd
            }
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

    fn map_diff_viewer_key(key: KeyEvent) -> Action {
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        if ctrl {
            return match key.code {
                KeyCode::Char('c') => Action::CopySelection,
                KeyCode::Char('a') => Action::SelectAll,
                KeyCode::Char('f') => Action::SearchPrompt,
                _ => Action::None,
            };
        }
        match key.code {
            // Alt/Opt+Up/Down: prev/next diff hunk
            KeyCode::Up if alt => Action::WordLeft, // prev hunk
            KeyCode::Down if alt => Action::FindNext, // next hunk
            KeyCode::Up if shift => Action::SelectUp,
            KeyCode::Down if shift => Action::SelectDown,
            KeyCode::Left if shift => Action::SelectLeft,
            KeyCode::Right if shift => Action::SelectRight,
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left => Action::CursorLeft,
            KeyCode::Right => Action::CursorRight,
            KeyCode::Home => Action::CursorLineStart,
            KeyCode::End => Action::CursorLineEnd,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Char('n') => Action::FindNext,
            KeyCode::Char('N') => Action::WordLeft,
            KeyCode::Char('g') => Action::GotoLinePrompt,
            KeyCode::Tab => Action::SwitchPanel,
            KeyCode::F(4) => Action::EditBuiltin,
            KeyCode::Char('q') | KeyCode::Esc => Action::DialogCancel,
            _ => Action::None,
        }
    }

    fn map_parquet_key(&self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Up => Action::MoveUp,
            KeyCode::Down => Action::MoveDown,
            KeyCode::Left => Action::CursorLeft,
            KeyCode::Right => Action::CursorRight,
            KeyCode::Enter => Action::Enter,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Home => Action::MoveToTop,
            KeyCode::End => Action::MoveToBottom,
            KeyCode::Tab | KeyCode::F(4) => Action::Toggle,
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

    /// Poll all async data sources. Called once per tick/resize, before input dispatch.
    fn poll_async(&mut self) {
        // Auto-clear status messages after 5 seconds of no update.
        // Reset timer when message content changes (for progress updates).
        if self.status_message != self.status_message_prev {
            self.status_message_at = self.status_message.as_ref().map(|_| std::time::Instant::now());
            self.status_message_prev = self.status_message.clone();
        }
        if let Some(at) = self.status_message_at {
            if at.elapsed() > std::time::Duration::from_secs(5) {
                self.status_message = None;
                self.status_message_prev = None;
                self.status_message_at = None;
            }
        }

        // Mark dirty if any async data sources are active
        let has_active_async = self.ci_panels.iter().any(|c| c.is_some())
            || self.diff_panels.iter().any(|d| d.is_some())
            || self
                .file_search
                .as_ref()
                .map(|s| s.searching)
                .unwrap_or(false)
            || self.git_cache.has_pending()
            || self.archive_progress.is_some()
            || self.pending_remote.is_some();
        if has_active_async {
            self.dirty = true;
        }
        // Poll CI panels for async results, downloads, and failure extraction
        for ci in self.ci_panels.iter_mut().flatten() {
            ci.poll();
            if let Some(result) = ci.poll_download() {
                match result {
                    Ok(path) => {
                        if matches!(self.focus, PanelFocus::Ci(_)) {
                            self.focus = PanelFocus::FilePanel;
                        }
                        self.mode =
                            AppMode::Editing(Box::new(crate::editor::EditorState::open(path)));
                        return;
                    }
                    Err(e) => {
                        self.status_message = Some(format!("Download failed: {}", e));
                    }
                }
            }
            // Poll failure extraction
            if let Some(ref mut extraction) = ci.failure_extraction {
                if let Some(result) = extraction.poll() {
                    let repo = ci.repo.clone();
                    let pr_number = ci.pr_number;
                    ci.failure_extraction = None;
                    match result {
                        Ok(failures) => {
                            let output_path = std::env::temp_dir().join(format!(
                                "mm-ci-failures-{}.md",
                                std::process::id()
                            ));
                            match crate::ci::write_failures_file(&output_path, &failures, &repo, pr_number) {
                                Ok(()) => {
                                    // Build a summary for the popup
                                    let unique: std::collections::HashSet<&str> = failures.iter().map(|f| f.test_name.as_str()).collect();
                                    let mut by_check: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
                                    for f in &failures {
                                        *by_check.entry(&f.check_name).or_default() += 1;
                                    }
                                    let mut summary = String::new();
                                    if failures.is_empty() {
                                        summary.push_str("No test failures found in the logs.\n\nThe failed checks may not contain\nrecognizable test output.");
                                    } else {
                                        summary.push_str(&format!("{} unique failure(s) across {} check(s):\n", unique.len(), by_check.len()));
                                        for (check, count) in &by_check {
                                            summary.push_str(&format!("\n  {} ({} failure{})", check, count, if *count == 1 { "" } else { "s" }));
                                        }
                                        summary.push_str("\n\nPress any key to view full report.");
                                    }
                                    self.popup = Some(("CI Failure Extraction".to_string(), summary));
                                    self.popup_after = Some(output_path);
                                    return;
                                }
                                Err(e) => {
                                    self.status_message = Some(format!("Failed to write: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            self.status_message = Some(format!("Extraction failed: {}", e));
                        }
                    }
                } else {
                    // Still in progress -- show status
                    self.status_message = Some(extraction.progress.clone());
                }
            }
        }
        // Poll diff panels
        for diff in self.diff_panels.iter_mut().flatten() {
            diff.poll();
        }
        // Poll Claude panels
        for side in 0..2 {
            if let Some(ref mut tp) = self.claude_panels[side] {
                tp.poll();
                if tp.exited {
                    self.claude_panels[side] = None;
                    if self.focus == PanelFocus::Claude(side) {
                        self.focus = PanelFocus::FilePanel;
                    }
                }
            }
        }
        // Poll shell panels
        for side in 0..2 {
            if let Some(ref mut sp) = self.shell_panels[side] {
                sp.poll();
                if sp.exited {
                    self.shell_panels[side] = None;
                    if self.focus == PanelFocus::Shell(side) {
                        self.focus = PanelFocus::FilePanel;
                    }
                }
            }
        }
        // Poll SSH panels (keep alive on exit for reconnect)
        for side in 0..2 {
            if let Some(ref mut sp) = self.ssh_panels[side] {
                sp.poll();
                // Don't remove on exit — keep panel alive with scrollback for reconnect.
                // The title is updated to show "[Disconnected]".
                if sp.exited && !sp.title.contains("[Disconnected]") {
                    sp.title = format!(" {} [Disconnected - Enter to reconnect] ", sp.title.trim());
                }
            }
        }
        // Poll file search results
        if let Some(ref mut state) = self.file_search {
            state.poll();
        }
        // Poll for async PR query results
        if self.git_cache.poll_pending() {
            self.panels[0].refresh_git(&mut self.git_cache);
            self.panels[1].refresh_git(&mut self.git_cache);
            self.dirty = true;
        }
        // Check for filesystem changes (kqueue/inotify — zero cost if idle)
        if let Some(ref w) = self.dir_watcher {
            if w.has_changes() {
                self.reload_panels();
            }
        }
        // Poll pending remote connections (background threads)
        if let Some(ref pending) = self.pending_remote {
            let elapsed = pending.started.elapsed();
            // Timeout after 30 seconds
            if elapsed > std::time::Duration::from_secs(30) {
                crate::debug_log::log("Connection timed out after 30s");
                self.popup = Some(("Error".to_string(), "Connection timed out after 30 seconds.\n\nPress any key, then Ctrl+T to retry.".to_string()));
                self.pending_remote = None;
                self.dirty = true;
            } else if let Ok(result) = pending.rx.try_recv() {
                let side = pending.side;
                match result.result {
                    Ok(conn) => {
                        let label = conn.display_label();
                        crate::debug_log::log(&format!("Connection result received: {}", label));
                        let boxed: Box<dyn crate::remote_fs::RemoteFs> = conn;
                        let connection: std::rc::Rc<dyn crate::remote_fs::RemoteFs> =
                            std::rc::Rc::from(boxed);
                        self.panels[side].switch_to_remote(connection);
                        self.status_message = Some(format!("Connected to {}", label));
                    }
                    Err(e) => {
                        crate::debug_log::log_error("connect_result", &format!("{}", e));
                        self.popup = Some(("Error".to_string(), format!("Connection failed:\n\n{}", e)));
                    }
                }
                self.pending_remote = None;
                self.dirty = true;
            } else {
                // Still waiting -- show progress with elapsed time
                let secs = elapsed.as_secs();
                self.status_message = Some(format!("Connecting... ({}s)", secs));
            }
        }
        // Poll archive progress
        if let Some(ref progress) = self.archive_progress {
            if progress.finished.load(Ordering::Acquire) {
                let err = progress.error.lock().unwrap().take();
                if let Some(e) = err {
                    self.status_message = Some(format!("Archive error: {}", e));
                } else {
                    let name = progress
                        .output_path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    self.status_message = Some(format!("Created {}", name));
                }
                self.archive_progress = None;
                self.reload_panels();
            } else {
                let done = progress.done_bytes.load(Ordering::Relaxed);
                let total = progress.total_bytes.max(1);
                let pct = (done as f64 / total as f64 * 100.0) as u8;
                self.status_message = Some(format!("Archiving... {}%", pct));
            }
        }
    }

    pub fn handle_action(&mut self, action: Action) {
        // Mark dirty for any action that changes state (not idle ticks)
        if !matches!(action, Action::Tick | Action::None) {
            self.dirty = true;
        }

        // Global tick: poll all async sources regardless of focus
        if matches!(action, Action::Tick | Action::Resize(_, _)) {
            self.poll_async();
            if matches!(action, Action::Resize(_, _)) {
                self.resize_all_bottom_panels();
            }
            return;
        }

        // Error popup: any key dismisses it
        if self.popup.is_some() {
            self.popup = None;
            // Execute deferred action (e.g. open editor after extraction summary)
            if let Some(path) = self.popup_after.take() {
                self.focus = PanelFocus::FilePanel;
                self.mode = AppMode::Editing(Box::new(
                    crate::editor::EditorState::open(path),
                ));
            }
            return;
        }

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

        // Settings dialog intercepts when active
        if self.settings_open.is_some() {
            match action {
                Action::DialogCancel => self.settings_open = None,
                Action::MoveUp => {
                    if let Some(ref mut sel) = self.settings_open {
                        *sel = sel.saturating_sub(1);
                    }
                }
                Action::MoveDown => {
                    if let Some(ref mut sel) = self.settings_open {
                        let max_setting = 0; // Number of settings - 1
                        *sel = (*sel + 1).min(max_setting);
                    }
                }
                Action::Enter | Action::DialogConfirm | Action::CursorRight | Action::Toggle => {
                    // Cycle the selected setting
                    if let Some(0) = self.settings_open {
                        // Theme: cycle to next
                        let next = crate::theme::current_theme_name().next();
                        crate::theme::set_theme(next);
                        self.persisted.theme = next.to_str().to_string();
                        self.persisted.save();
                        self.needs_clear = true;
                        self.status_message = Some(format!("Theme: {}", next.label()));
                    }
                }
                Action::CursorLeft => {
                    // Cycle backward
                    if let Some(0) = self.settings_open {
                        let prev = crate::theme::current_theme_name().prev();
                        crate::theme::set_theme(prev);
                        self.persisted.theme = prev.to_str().to_string();
                        self.persisted.save();
                        self.needs_clear = true;
                        self.status_message = Some(format!("Theme: {}", prev.label()));
                    }
                }
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
                            self.restore_or_close_editor();
                            self.reload_panels();
                        }
                        UnsavedDialogField::Discard => {
                            self.unsaved_dialog = None;
                            self.restore_or_close_editor();
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

        // Bottom panel focus intercepts only apply in normal/quick-search modes
        // (full-screen modes like DiffViewing/Editing handle their own keys)
        if matches!(self.mode, AppMode::Normal | AppMode::QuickSearch) {
            // CI panel intercepts when focused
            if matches!(self.focus, PanelFocus::Ci(_)) {
                self.handle_ci_action(action);
                return;
            }

            // Diff panel intercepts when focused
            if matches!(self.focus, PanelFocus::Diff(_)) {
                self.handle_diff_action(action);
                return;
            }

            // Claude panel intercepts when focused
            if matches!(self.focus, PanelFocus::Claude(_)) {
                self.handle_claude_action(action);
                return;
            }

            // Shell panel intercepts when focused
            if matches!(self.focus, PanelFocus::Shell(_)) {
                self.handle_shell_action(action);
                return;
            }
        }

        // SSH panel intercepts when focused
        if matches!(self.focus, PanelFocus::Ssh(_)) {
            self.handle_ssh_action(action);
            return;
        }

        // SSH dialog intercepts when open
        if self.ssh_dialog.is_some() {
            self.handle_ssh_dialog_action(action);
            return;
        }

        // Session dialog intercepts when open
        if self.session_dialog.is_some() {
            self.handle_session_dialog_action(action);
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
        if matches!(self.mode, AppMode::ArchiveDialog(_)) {
            self.handle_archive_dialog_action(action);
            return;
        }
        if matches!(self.mode, AppMode::Editing(_)) {
            self.handle_editor_action(action);
            return;
        }

        // File content search dialog intercepts
        if self.file_search_dialog.is_some() {
            self.handle_file_search_dialog(action);
            return;
        }

        // File search results intercepts when focused
        if self.focus == PanelFocus::Search && self.file_search.is_some() {
            self.handle_file_search_results(action);
            return;
        }

        // Fuzzy file search intercepts all input when active
        if self.fuzzy_search[self.active_panel].is_some() {
            self.handle_fuzzy_search_action(action);
            return;
        }

        // Diff viewer: intercept cursor movement, selection, search, next/prev change, edit-switch
        if matches!(self.mode, AppMode::DiffViewing(_)) {
            // Handle search input mode first
            if let AppMode::DiffViewing(ref mut d) = self.mode {
                if d.search_input.is_some() {
                    match action {
                        Action::DialogCancel => {
                            d.search_input = None;
                        }
                        Action::DialogConfirm | Action::EditorNewline | Action::Enter => {
                            if let Some(query) = d.search_input.take() {
                                if query.is_empty() {
                                    d.clear_search();
                                } else {
                                    d.search(&query);
                                    d.search_next();
                                }
                            }
                        }
                        Action::DialogInput(c) => {
                            if let Some(ref mut input) = d.search_input {
                                input.push(c);
                            }
                        }
                        Action::DialogBackspace => {
                            if let Some(ref mut input) = d.search_input {
                                input.pop();
                            }
                        }
                        _ => {}
                    }
                    return;
                }
            }

            match action {
                Action::CursorLeft => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        d.move_cursor_left();
                    }
                    return;
                }
                Action::CursorRight => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        d.move_cursor_right();
                    }
                    return;
                }
                Action::CursorLineStart => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        d.cursor_home();
                    }
                    return;
                }
                Action::CursorLineEnd => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        d.cursor_end();
                    }
                    return;
                }
                Action::SelectUp => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.select_up();
                    }
                    return;
                }
                Action::SelectDown => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.select_down();
                    }
                    return;
                }
                Action::SelectLeft => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.select_left();
                    }
                    return;
                }
                Action::SelectRight => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.select_right();
                    }
                    return;
                }
                Action::SelectAll => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.select_all();
                    }
                    return;
                }
                Action::CopySelection => {
                    if let AppMode::DiffViewing(ref d) = self.mode {
                        d.copy_to_clipboard();
                    }
                    return;
                }
                Action::SwitchPanel => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        d.switch_side();
                    }
                    return;
                }
                Action::FindNext => {
                    // n: search next if search active, otherwise next change
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        if d.search_query.is_some() {
                            d.search_next();
                        } else {
                            d.next_change();
                        }
                    }
                    return;
                }
                Action::WordLeft => {
                    // N: search prev if search active, otherwise prev change
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                        if d.search_query.is_some() {
                            d.search_prev();
                        } else {
                            d.prev_change();
                        }
                    }
                    return;
                }
                Action::SearchPrompt => {
                    // Ctrl+F: open search input
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.search_input = Some(d.search_query.clone().unwrap_or_default());
                    }
                    return;
                }
                Action::GotoLinePrompt => {
                    if matches!(self.mode, AppMode::DiffViewing(_)) {
                        self.goto_line_input = Some(String::new());
                    }
                    return;
                }
                Action::EditBuiltin => {
                    // Switch from diff viewer to editor at current line
                    if let AppMode::DiffViewing(ref d) = self.mode {
                        let line = d.current_line();
                        let cursor_offset = d.cursor.saturating_sub(d.scroll);
                        let cursor = d.cursor;
                        let file_path = d.path.clone();
                        let (repo_root, base_branch) = if let PanelFocus::Diff(side) = self.focus {
                            self.diff_panels[side]
                                .as_ref()
                                .map(|dp| (dp.repo_root.clone(), dp.base_branch.clone()))
                        } else {
                            None
                        }
                        .unwrap_or_else(|| {
                            (
                                self.panels[self.active_panel].current_dir.clone(),
                                String::new(),
                            )
                        });
                        let full_path = repo_root.join(&file_path);
                        self.stashed_diff = Some(StashedDiff {
                            repo_root,
                            file_path,
                            base_branch,
                            cursor,
                        });
                        let mut editor = EditorState::open(full_path);
                        let target = line.saturating_sub(1);
                        if !editor.scan_complete {
                            editor.scan_to_line(target + 100);
                        }
                        let total = editor.total_virtual_lines();
                        editor.cursor_line = target.min(total.saturating_sub(1));
                        // Maintain viewport offset: cursor stays at same visual row
                        editor.scroll_y = editor.cursor_line.saturating_sub(cursor_offset);
                        self.needs_clear = true;
                        self.mode = AppMode::Editing(Box::new(editor));
                    }
                    return;
                }
                // Clear selection on non-selection movement
                Action::MoveUp
                | Action::MoveDown
                | Action::PageUp
                | Action::PageDown
                | Action::MoveToTop
                | Action::MoveToBottom => {
                    if let AppMode::DiffViewing(ref mut d) = self.mode {
                        d.clear_selection();
                    }
                    // fall through to normal dispatch
                }
                _ => {} // fall through to normal dispatch
            }
        }

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::Toggle => self.handle_toggle_viewer(),
            Action::GotoLinePrompt => {
                // Only works in viewer/hex/editor/parquet modes
                if matches!(
                    self.mode,
                    AppMode::Viewing(_)
                        | AppMode::HexViewing(_)
                        | AppMode::ParquetViewing(_)
                        | AppMode::Editing(_)
                ) {
                    self.goto_line_input = Some(String::new());
                }
            }
            Action::EditBuiltin => self.handle_edit_builtin(),
            Action::CursorLeft => {
                if let AppMode::ParquetViewing(ref mut p) = self.mode {
                    p.scroll_left();
                }
            }
            Action::CursorRight => {
                if let AppMode::ParquetViewing(ref mut p) = self.mode {
                    p.scroll_right();
                }
            }
            Action::CursorLineStart
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
            Action::Archive => self.handle_archive(),
            Action::Move => self.handle_move(),
            Action::Rename => self.handle_rename(),
            Action::CreateDir => self.handle_create_dir(),
            Action::CreateFile => self.handle_create_file(),
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
                let mut input = TextInput::new(path);
                input.select_all();
                self.goto_path[self.active_panel] = Some(GotoPathState {
                    input,
                    completions: Vec::new(),
                    comp_index: None,
                    comp_base: None,
                });
            }

            // Help
            Action::ShowHelp => {
                self.help_scroll = Some(0);
            }

            Action::ToggleSettings => {
                if self.settings_open.is_some() {
                    self.settings_open = None;
                } else {
                    self.settings_open = Some(0);
                }
            }

            // CI failure extraction (only works when CI panel is focused, handled above)
            Action::ExtractCiFailures => {}

            // File content search dialog
            Action::FileSearchPrompt => {
                let path = self
                    .active_panel()
                    .current_dir
                    .to_string_lossy()
                    .to_string();
                let mut dlg = FileSearchDialogState::new(
                    path,
                    self.persisted.file_search_term.clone(),
                    self.persisted.file_search_filter.clone(),
                    self.persisted.file_search_regex,
                );
                dlg.select_focused();
                self.file_search_dialog = Some(dlg);
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
                        if self.focus == PanelFocus::Ci(side) {
                            self.focus = PanelFocus::FilePanel;
                        }
                    }
                } else if let Some(ref gi) = self.active_panel().git_info {
                    let dir = self.active_panel().current_dir.clone();
                    self.ci_panels[side] = Some(CiPanel::for_branch(&dir, &gi.branch));
                    self.focus = PanelFocus::Ci(side);
                    self.bottom_split_pct[side] = self.persisted.split_pct_ci;
                } else {
                    self.status_message = Some("Not in a git repository".to_string());
                }
            }
            Action::ToggleDiff => {
                let side = self.active_panel;
                if self.diff_panels[side].is_some() {
                    self.diff_panels[side] = None;
                    if self.focus == PanelFocus::Diff(side) {
                        self.focus = PanelFocus::FilePanel;
                    }
                } else if self.active_panel().git_info.is_some() {
                    let dir = self.active_panel().current_dir.clone();
                    self.diff_panels[side] = Some(PrDiffPanel::for_branch(&dir));
                    self.focus = PanelFocus::Diff(side);
                    self.bottom_split_pct[side] = self.persisted.split_pct_ci;
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

            // Terminal — opens maximized on the opposite panel's side
            Action::ToggleClaude => {
                let side = 1 - self.active_panel;
                if self.claude_panels[side].is_some() {
                    self.claude_panels[side] = None;
                    self.bottom_maximized[side] = false;
                    if self.focus == PanelFocus::Claude(side) {
                        self.focus = PanelFocus::FilePanel;
                    }
                } else if let Some(ref wakeup) = self.wakeup_sender {
                    let dir = self.panels[self.active_panel].current_dir.clone();
                    let area = self.panel_areas[side];
                    let cols = area.width.saturating_sub(2).max(1);
                    let rows = area.height.saturating_sub(2).max(1);
                    match TerminalPanel::spawn_claude(&dir, cols, rows, wakeup.clone()) {
                        Ok(tp) => {
                            self.claude_panels[side] = Some(tp);
                            self.focus = PanelFocus::Claude(side);
                            self.bottom_maximized[side] = true;
                        }
                        Err(e) => {
                            self.status_message = Some(format!("Failed to start terminal: {}", e));
                        }
                    }
                } else {
                    self.status_message = Some("Event loop not ready".to_string());
                }
            }

            // Shell
            Action::ToggleShell => self.toggle_shell(),

            // SSH
            Action::ToggleSsh => self.toggle_ssh(),

            // Sessions
            Action::ToggleSessions => {
                if self.session_dialog.is_some() {
                    self.session_dialog = None;
                } else {
                    self.session_dialog = Some(SessionDialogState::new());
                }
            }

            // Bottom panel resize/maximize
            Action::BottomResizeUp => {
                let side = self.focused_side();
                self.bottom_split_pct[side] = self.bottom_split_pct[side]
                    .saturating_sub(SPLIT_RESIZE_STEP)
                    .max(SPLIT_MIN_PCT);
            }
            Action::BottomResizeDown => {
                let side = self.focused_side();
                self.bottom_split_pct[side] =
                    (self.bottom_split_pct[side] + SPLIT_RESIZE_STEP).min(SPLIT_MAX_PCT);
            }
            Action::BottomMaximize => {
                let side = self.focused_side();
                self.bottom_maximized[side] = !self.bottom_maximized[side];
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
            AppMode::ParquetViewing(p) => p.move_up(1),
            AppMode::DiffViewing(d) => d.scroll_up(1),
            _ => self.active_panel_mut().move_selection(-1),
        }
    }

    fn handle_move_down(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_down(1),
            AppMode::HexViewing(h) => h.scroll_down(1),
            AppMode::ParquetViewing(p) => p.move_down(1),
            AppMode::DiffViewing(d) => d.scroll_down(1),
            _ => self.active_panel_mut().move_selection(1),
        }
    }

    fn handle_move_to_top(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_to_top(),
            AppMode::HexViewing(h) => h.scroll_to_top(),
            AppMode::ParquetViewing(p) => p.move_to_top(),
            AppMode::DiffViewing(d) => d.scroll_to_top(),
            _ => self.active_panel_mut().move_to_top(),
        }
    }

    fn handle_move_to_bottom(&mut self) {
        match &mut self.mode {
            AppMode::Viewing(v) => v.scroll_to_bottom(),
            AppMode::HexViewing(h) => h.scroll_to_bottom(),
            AppMode::ParquetViewing(p) => p.move_to_bottom(),
            AppMode::DiffViewing(d) => d.scroll_to_bottom(),
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
            AppMode::ParquetViewing(p) => p.page_up(),
            AppMode::DiffViewing(d) => d.page_up(),
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
            AppMode::ParquetViewing(p) => p.page_down(),
            AppMode::DiffViewing(d) => d.page_down(),
            _ => self.active_panel_mut().move_selection(20),
        }
    }

    fn handle_enter(&mut self) {
        // Parquet: Enter toggles expand/collapse
        if let AppMode::ParquetViewing(ref mut p) = self.mode {
            p.toggle_expand();
            return;
        }

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
        #[derive(PartialEq)]
        enum Target {
            Panel(usize),
            Ci(usize),
            Diff(usize),
            Shell(usize),
            Claude(usize),
            Ssh(usize),
            Search(usize),
        }

        let mut order: Vec<Target> = Vec::with_capacity(12);
        let has_search = self.file_search.is_some();

        // Left side: skip file panel if search replaces it
        if !(has_search && self.file_search_side == 0) {
            order.push(Target::Panel(0));
        }
        if self.ci_panels[0].is_some() {
            order.push(Target::Ci(0));
        }
        if self.diff_panels[0].is_some() {
            order.push(Target::Diff(0));
        }
        if self.shell_panels[0].is_some() {
            order.push(Target::Shell(0));
        }
        if self.claude_panels[0].is_some() {
            order.push(Target::Claude(0));
        }
        if self.ssh_panels[0].is_some() {
            order.push(Target::Ssh(0));
        }
        if has_search && self.file_search_side == 0 {
            order.push(Target::Search(0));
        }

        // Right side: skip file panel if search replaces it
        if !(has_search && self.file_search_side == 1) {
            order.push(Target::Panel(1));
        }
        if self.ci_panels[1].is_some() {
            order.push(Target::Ci(1));
        }
        if self.diff_panels[1].is_some() {
            order.push(Target::Diff(1));
        }
        if self.shell_panels[1].is_some() {
            order.push(Target::Shell(1));
        }
        if self.claude_panels[1].is_some() {
            order.push(Target::Claude(1));
        }
        if self.ssh_panels[1].is_some() {
            order.push(Target::Ssh(1));
        }
        if has_search && self.file_search_side == 1 {
            order.push(Target::Search(1));
        }

        if order.is_empty() {
            return;
        }

        if order.len() == 2
            && matches!(order[0], Target::Panel(_))
            && matches!(order[1], Target::Panel(_))
        {
            self.active_panel = 1 - self.active_panel;
            self.focus = PanelFocus::FilePanel;
            return;
        }

        let current = match self.focus {
            PanelFocus::Search => order
                .iter()
                .position(|t| *t == Target::Search(self.file_search_side)),
            PanelFocus::Ssh(side) => order.iter().position(|t| *t == Target::Ssh(side)),
            PanelFocus::Claude(side) => order.iter().position(|t| *t == Target::Claude(side)),
            PanelFocus::Shell(side) => order.iter().position(|t| *t == Target::Shell(side)),
            PanelFocus::Diff(side) => order.iter().position(|t| *t == Target::Diff(side)),
            PanelFocus::Ci(side) => order.iter().position(|t| *t == Target::Ci(side)),
            PanelFocus::FilePanel => order
                .iter()
                .position(|t| *t == Target::Panel(self.active_panel)),
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

        self.unfocus_all();
        match &order[next] {
            Target::Panel(side) => {
                self.active_panel = *side;
                self.focus = PanelFocus::FilePanel;
            }
            Target::Ci(side) => {
                self.focus = PanelFocus::Ci(*side);
            }
            Target::Diff(side) => {
                self.focus = PanelFocus::Diff(*side);
            }
            Target::Shell(side) => {
                self.focus = PanelFocus::Shell(*side);
            }
            Target::Claude(side) => {
                self.focus = PanelFocus::Claude(*side);
            }
            Target::Ssh(side) => {
                self.focus = PanelFocus::Ssh(*side);
            }
            Target::Search(_) => {
                self.focus = PanelFocus::Search;
            }
        }
    }

    /// Clear all bottom-panel focus flags.
    fn unfocus_all(&mut self) {
        self.focus = PanelFocus::FilePanel;
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
                    self.goto_path_navigate(side, &state.input.text);
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
            _ => {
                if let Some(ref mut state) = self.goto_path[side] {
                    if state.input.handle_action(&action) {
                        state.completions.clear();
                        state.comp_index = None;
                        state.comp_base = None;
                    }
                }
            }
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
        let (dir, prefix) = Self::expand_goto_input(&state.input.text);
        let prefix_lower = prefix.to_lowercase();

        state.comp_base = Some(state.input.text.clone());
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

        let (_, prefix) = Self::expand_goto_input(&state.input.text);

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
            state.input.text.insert_str(state.input.cursor, &suffix);
            state.input.cursor += suffix.len();
            true
        } else {
            false
        }
    }

    fn apply_completion(state: &mut GotoPathState, name: &str) {
        let (_, prefix) = Self::expand_goto_input(&state.input.text);

        // Append the characters beyond the typed prefix + trailing /
        let prefix_chars = prefix.chars().count();
        let suffix: String = name
            .chars()
            .skip(prefix_chars)
            .chain(std::iter::once('/'))
            .collect();
        state.input.text.insert_str(state.input.cursor, &suffix);
        state.input.cursor += suffix.len();
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
            _ => {
                if let Some(ref mut state) = self.fuzzy_search[side] {
                    let old_text = state.input.text.clone();
                    state.input.handle_action(&action);
                    if state.input.text != old_text {
                        state.update_results();
                    }
                }
            }
        }
    }

    fn handle_file_search_dialog(&mut self, action: Action) {
        match action {
            Action::DialogCancel => {
                self.file_search_dialog = None;
            }
            Action::DialogConfirm => {
                if let Some(state) = self.file_search_dialog.take() {
                    if state.focused == FileSearchField::ButtonCancel {
                        return;
                    }
                    if !state.term.text.is_empty() {
                        // Persist search params
                        self.persisted.file_search_term = state.term.text.clone();
                        self.persisted.file_search_filter = state.filter.text.clone();
                        self.persisted.file_search_regex = state.is_regex;

                        let dir = PathBuf::from(&state.path.text);
                        let search_side = 1 - self.active_panel;
                        let mut search = SearchState::new(
                            dir,
                            state.term.text.clone(),
                            state.filter.text.clone(),
                            state.is_regex,
                        );
                        search.poll(); // get initial results
                        self.file_search = Some(search);
                        self.file_search_side = search_side;
                        self.focus = PanelFocus::Search;
                    }
                }
            }
            Action::MoveDown => {
                if let Some(ref mut state) = self.file_search_dialog {
                    state.term.clear_selection();
                    state.path.clear_selection();
                    state.filter.clear_selection();
                    state.focused = state.focused.next();
                    state.select_focused();
                }
            }
            Action::MoveUp => {
                if let Some(ref mut state) = self.file_search_dialog {
                    state.term.clear_selection();
                    state.path.clear_selection();
                    state.filter.clear_selection();
                    state.focused = state.focused.prev();
                    state.select_focused();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let Some(ref mut state) = self.file_search_dialog {
                    state.term.clear_selection();
                    state.path.clear_selection();
                    state.filter.clear_selection();
                    state.focused = match state.focused {
                        FileSearchField::ButtonSearch => FileSearchField::ButtonCancel,
                        FileSearchField::ButtonCancel => FileSearchField::ButtonSearch,
                        other => other,
                    };
                    state.select_focused();
                }
            }
            Action::Toggle => {
                if let Some(ref mut state) = self.file_search_dialog {
                    if state.focused == FileSearchField::Regex {
                        state.is_regex = !state.is_regex;
                    }
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let Some(ref mut state) = self.file_search_dialog {
                    if let Some(input) = state.active_input() {
                        input.handle_action(&action);
                    }
                }
            }
        }
    }

    fn handle_file_search_results(&mut self, action: Action) {
        match action {
            Action::DialogCancel => {
                self.file_search = None;
                self.focus = PanelFocus::FilePanel;
            }
            Action::DialogConfirm => {
                // Open selected match in editor and highlight search term
                if let Some(ref state) = self.file_search {
                    let query = state.query.clone();
                    if let Some((path, line)) = state.selected_location() {
                        let mut editor = EditorState::open(path);
                        let target_line = (line as usize).saturating_sub(1);
                        if !editor.scan_complete {
                            editor.scan_to_line(target_line + 100);
                        }
                        editor.cursor_line = target_line;
                        editor.cursor_col = 0;
                        editor.desired_col = 0;
                        // Set scroll_y directly since visible_lines is 0 pre-render
                        editor.scroll_y = target_line;
                        // Find and highlight the search term on this line
                        let params = crate::editor::SearchParams {
                            query,
                            direction: SearchDirection::Forward,
                            case_sensitive: false,
                        };
                        editor.find(&params);
                        // Restore scroll position (find may have changed it)
                        editor.scroll_y = target_line;
                        editor.last_search = Some(params);
                        self.focus = PanelFocus::FilePanel;
                        self.mode = AppMode::Editing(Box::new(editor));
                    }
                }
            }
            Action::MoveUp => {
                if let Some(ref mut state) = self.file_search {
                    state.move_up();
                }
            }
            Action::MoveDown => {
                if let Some(ref mut state) = self.file_search {
                    state.move_down();
                }
            }
            Action::PageUp => {
                if let Some(ref mut state) = self.file_search {
                    state.page_up(20);
                }
            }
            Action::PageDown => {
                if let Some(ref mut state) = self.file_search {
                    state.page_down(20);
                }
            }
            Action::MoveToTop => {
                if let Some(ref mut state) = self.file_search {
                    state.selected = 0;
                }
            }
            Action::MoveToBottom => {
                if let Some(ref mut state) = self.file_search {
                    let count = state.visible_count();
                    state.selected = count.saturating_sub(1);
                }
            }
            Action::CursorRight => {
                // Expand file
                if let Some(ref mut state) = self.file_search {
                    let items = state.visible_items();
                    if let Some(crate::file_search::SearchItem::File(fi)) =
                        items.get(state.selected)
                    {
                        state.files[*fi].expanded = true;
                    }
                }
            }
            Action::GoUp => {
                // Left on child: jump to parent. Left on parent: collapse.
                if let Some(ref mut state) = self.file_search {
                    let items = state.visible_items();
                    match items.get(state.selected) {
                        Some(crate::file_search::SearchItem::Match(fi, _)) => {
                            // Jump to the parent file entry
                            if let Some(pos) = items.iter().position(|item| {
                                matches!(item, crate::file_search::SearchItem::File(f) if *f == *fi)
                            }) {
                                state.selected = pos;
                            }
                        }
                        Some(crate::file_search::SearchItem::File(fi)) => {
                            // Collapse this file
                            state.files[*fi].expanded = false;
                        }
                        None => {}
                    }
                }
            }
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),
            Action::Tick | Action::Resize(_, _) => {}
            Action::MouseClick(col, row) => {
                // Check if click is inside the search results panel
                let search_area = self.panel_areas[self.file_search_side];
                if col >= search_area.x
                    && col < search_area.x + search_area.width
                    && row >= search_area.y
                    && row < search_area.y + search_area.height
                {
                    // Click inside search panel — stay focused
                    // Map click to a result row
                    if let Some(ref mut state) = self.file_search {
                        let inner_y = search_area.y + 1; // account for border
                        if row >= inner_y {
                            let click_offset = (row - inner_y) as usize;
                            let target = state.scroll + click_offset;
                            let count = state.visible_count();
                            if target < count {
                                state.selected = target;
                            }
                        }
                    }
                } else {
                    // Click outside — unfocus search, pass to normal handler
                    self.focus = PanelFocus::FilePanel;
                    self.handle_mouse_click(col, row);
                }
            }
            Action::MouseScrollUp(_, _) => {
                if let Some(ref mut state) = self.file_search {
                    state.page_up(3);
                }
            }
            Action::MouseScrollDown(_, _) => {
                if let Some(ref mut state) = self.file_search {
                    state.page_down(3);
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
            AppMode::ParquetViewing(p) => {
                p.goto_row(line);
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
            AppMode::DiffViewing(d) => {
                let target = line.min(d.lines.len().saturating_sub(1));
                d.cursor = target;
                d.cursor_col = col;
                d.scroll_down(0); // clamps and ensures visibility
            }
            _ => {}
        }
    }

    fn handle_edit_builtin(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if entry.is_dir {
                return;
            }
            if self.active_panel().source.is_remote() {
                // Download to temp file, edit, upload on save
                match self.download_for_edit(&entry.path) {
                    Ok(tmp_path) => {
                        let mut editor = EditorState::open(tmp_path.clone());
                        // Store the remote path for upload-on-save
                        editor.remote_source = Some((
                            entry.path.clone(),
                            self.active_panel,
                        ));
                        self.mode = AppMode::Editing(Box::new(editor));
                    }
                    Err(e) => {
                        self.popup = Some(("Error".to_string(), format!("Failed to download file:\n\n{}", e)));
                    }
                }
            } else {
                self.mode = AppMode::Editing(Box::new(EditorState::open(entry.path)));
            }
        }
    }

    fn download_for_edit(&self, remote_path: &std::path::Path) -> anyhow::Result<PathBuf> {
        let filename = remote_path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "untitled".to_string());
        let tmp_dir = std::env::temp_dir().join("middle-manager-edit");
        std::fs::create_dir_all(&tmp_dir)?;
        let tmp_path = tmp_dir.join(&filename);

        match &self.panels[self.active_panel].source {
            crate::panel::PanelSource::Remote { connection } => {
                connection.download(remote_path, &tmp_path)?;
            }
            _ => anyhow::bail!("Not a remote panel"),
        }
        Ok(tmp_path)
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
                    self.restore_or_close_editor();
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
                        Ok(()) => {
                            // If editing a remote file, upload it back
                            if let Some((ref remote_path, panel_side)) = e.remote_source {
                                let local_path = e.path.clone();
                                let remote_path = remote_path.clone();
                                match &self.panels[panel_side].source {
                                    crate::panel::PanelSource::Remote { connection } => {
                                        match connection.upload(&local_path, &remote_path) {
                                            Ok(_) => {
                                                e.status_msg = Some("Saved and uploaded".to_string());
                                            }
                                            Err(err) => {
                                                e.status_msg = Some(format!("Saved locally, upload failed: {}", err));
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(err) => e.status_msg = Some(format!("Save failed: {}", err)),
                    }
                }
                // Reload panels after save
                self.reload_panels();
            }

            // Search
            Action::SearchPrompt => {
                let (query, direction, case_sensitive) = if let AppMode::Editing(ref e) = self.mode
                {
                    if let Some(ref p) = e.last_search {
                        (p.query.clone(), p.direction, p.case_sensitive)
                    } else if !self.persisted.search_query.is_empty() {
                        // Restore from persisted state
                        let dir = if self.persisted.search_direction_forward {
                            SearchDirection::Forward
                        } else {
                            SearchDirection::Backward
                        };
                        (
                            self.persisted.search_query.clone(),
                            dir,
                            self.persisted.search_case_sensitive,
                        )
                    } else {
                        (String::new(), SearchDirection::Forward, false)
                    }
                } else {
                    (String::new(), SearchDirection::Forward, false)
                };
                let mut q = TextInput::new(query);
                q.select_all();
                self.search_dialog = Some(SearchDialogState {
                    query: q,
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
        match &mut self.mode {
            AppMode::Viewing(v) => {
                let path = v.path.clone();
                self.mode = AppMode::HexViewing(Box::new(HexViewerState::open(path)));
            }
            AppMode::HexViewing(h) => {
                let path = h.path.clone();
                self.mode = AppMode::Viewing(Box::new(ViewerState::open(path)));
            }
            AppMode::ParquetViewing(p) => {
                p.switch_view();
            }
            _ => {}
        }
    }

    fn open_file(&mut self, path: PathBuf) {
        // Try parquet viewer for .parquet files
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            if let Ok(pq) = ParquetViewerState::open(path.clone()) {
                self.mode = AppMode::ParquetViewing(Box::new(pq));
                return;
            }
            // Fall through to binary/text viewer on parse failure
        }

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
        let mut dlg = CopyDialogState::new(display_name, paths, dest, false, self.active_panel);
        dlg.destination.select_all();
        self.mode = AppMode::CopyDialog(dlg);
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
        let mut dlg = CopyDialogState::new(display_name, paths, dest, true, self.active_panel);
        dlg.destination.select_all();
        self.mode = AppMode::CopyDialog(dlg);
    }

    fn handle_rename(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry() {
            if entry.name == ".." {
                return;
            }
            let name = entry.name.clone();
            let message = format!("Rename '{}':", name);
            let mut input = TextInput::new(name);
            input.select_all();
            self.mode = AppMode::Dialog(DialogState {
                kind: DialogKind::InputRename,
                title: "Rename".to_string(),
                message,
                input,
                has_input: true,
                focused: DialogField::Input,
            });
        }
    }

    fn handle_create_dir(&mut self) {
        self.mode = AppMode::MkdirDialog(MkdirDialogState::new());
    }

    fn handle_create_file(&mut self) {
        self.mode = AppMode::Dialog(DialogState {
            kind: DialogKind::InputCreateFile,
            title: "New File".to_string(),
            message: "Create new file:".to_string(),
            has_input: true,
            input: TextInput::new(String::new()),
            focused: DialogField::Input,
        });
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
            input: TextInput::new(String::new()),
            has_input: false,
            focused: DialogField::ButtonOk,
        });
    }

    fn handle_view_file(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if !entry.is_dir {
                if self.active_panel().source.is_remote() {
                    match self.download_for_edit(&entry.path) {
                        Ok(tmp_path) => self.open_file(tmp_path),
                        Err(e) => {
                            self.popup = Some(("Error".to_string(), format!("Failed to download:\n\n{}", e)));
                        }
                    }
                } else {
                    self.open_file(entry.path);
                }
            }
        }
    }

    fn handle_edit_file(&mut self) {
        if let Some(entry) = self.active_panel().selected_entry().cloned() {
            if !entry.is_dir {
                if self.active_panel().source.is_remote() {
                    self.popup = Some(("Info".to_string(), "Use F4 (built-in editor) for remote files.\nExternal editor not supported for remote.".to_string()));
                } else {
                    self.status_message = Some(format!("__EDIT__{}", entry.path.to_string_lossy()));
                }
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
                    s.query.clear_selection();
                    s.focused = s.focused.prev();
                    if s.focused == SearchDialogField::Query {
                        s.query.select_all();
                    }
                }
            }
            Action::MoveDown => {
                if let Some(ref mut s) = self.search_dialog {
                    s.query.clear_selection();
                    s.focused = s.focused.next();
                    if s.focused == SearchDialogField::Query {
                        s.query.select_all();
                    }
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
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let Some(ref mut s) = self.search_dialog {
                    s.query.clear_selection();
                    s.focused = match s.focused {
                        SearchDialogField::ButtonFind => SearchDialogField::ButtonCancel,
                        SearchDialogField::ButtonCancel => SearchDialogField::ButtonFind,
                        other => other,
                    };
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let Some(ref mut s) = self.search_dialog {
                    s.query.handle_action(&action);
                }
            }
        }
    }

    fn confirm_search_dialog(&mut self) {
        let state = match self.search_dialog.take() {
            Some(s) => s,
            None => return,
        };

        if state.query.text.is_empty() {
            return;
        }

        use crate::editor::SearchParams;
        let params = SearchParams {
            query: state.query.text,
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
        let side = match self.focus {
            PanelFocus::Ci(s) => s,
            _ => return,
        };

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
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
                    self.focus = PanelFocus::FilePanel;
                }
            }
            Action::OpenPr => {
                if let Some(ref ci) = self.ci_panels[side] {
                    if let Some(url) = ci.selected_url() {
                        crate::panel::github::open_url(url);
                    }
                }
            }
            Action::ExtractCiFailures => {
                self.start_failure_extraction(side);
            }
            Action::BottomResizeUp => {
                self.bottom_split_pct[side] = self.bottom_split_pct[side]
                    .saturating_sub(SPLIT_RESIZE_STEP)
                    .max(SPLIT_MIN_PCT);
            }
            Action::BottomResizeDown => {
                self.bottom_split_pct[side] =
                    (self.bottom_split_pct[side] + SPLIT_RESIZE_STEP).min(SPLIT_MAX_PCT);
            }
            Action::BottomMaximize => {
                self.bottom_maximized[side] = !self.bottom_maximized[side];
            }
            // Let mouse events through to the normal handler
            Action::MouseClick(col, row) => self.handle_mouse_click(col, row),
            Action::MouseDoubleClick(col, row) => self.handle_mouse_double_click(col, row),
            Action::MouseScrollUp(col, row) => self.handle_mouse_scroll(col, row, -3),
            Action::MouseScrollDown(col, row) => self.handle_mouse_scroll(col, row, 3),
            _ => {}
        }
    }

    fn handle_diff_action(&mut self, action: Action) {
        let side = match self.focus {
            PanelFocus::Diff(s) => s,
            _ => return,
        };

        // Check if quick search is active (before clearing)
        let has_search = self.diff_panels[side]
            .as_ref()
            .is_some_and(|d| d.quick_search.is_some());

        // Clear quick search on navigation (but not on Up/Down which cycle matches)
        if !matches!(
            action,
            Action::None
                | Action::Tick
                | Action::Resize(_, _)
                | Action::QuickSearch(_)
                | Action::QuickSearchClear
                | Action::MoveUp
                | Action::MoveDown
        ) {
            if let Some(ref mut diff) = self.diff_panels[side] {
                diff.quick_search = None;
            }
        }

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::MoveUp => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    if has_search {
                        diff.jump_to_prev_match();
                    } else {
                        diff.move_up();
                    }
                }
            }
            Action::MoveDown => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    if has_search {
                        diff.jump_to_next_match();
                    } else {
                        diff.move_down();
                    }
                }
            }
            Action::PageUp => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.page_up();
                }
            }
            Action::PageDown => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.page_down();
                }
            }
            Action::MoveToTop => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.move_to_top();
                }
            }
            Action::MoveToBottom => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.move_to_bottom();
                }
            }
            Action::Enter => {
                // Enter on file: open diff viewer; on dir: expand/collapse
                // Check if selected item is a file (need immutable borrow first)
                let file_info = self.diff_panels[side].as_ref().and_then(|diff| {
                    if let crate::pr_diff::DiffView::Tree {
                        items, selected, ..
                    } = &diff.view
                    {
                        if let Some(crate::pr_diff::DiffTreeItem::File { path, .. }) =
                            items.get(*selected)
                        {
                            return Some((
                                diff.repo_root.clone(),
                                path.clone(),
                                diff.base_branch.clone(),
                            ));
                        }
                    }
                    None
                });
                if let Some((repo_root, path, base_branch)) = file_info {
                    let dv =
                        crate::diff_viewer::DiffViewerState::open(&repo_root, &path, &base_branch);
                    self.mode = AppMode::DiffViewing(Box::new(dv));
                } else if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.enter();
                }
            }
            Action::EditBuiltin => {
                // F4: open file in editor
                let file_path = self.diff_panels[side].as_mut().and_then(|d| d.enter());
                if let Some(path) = file_path {
                    self.mode = AppMode::Editing(Box::new(crate::editor::EditorState::open(path)));
                }
            }
            Action::CursorRight => {
                // Right: expand only (like Enter on dirs, no-op on files)
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.enter();
                }
            }
            Action::GoUp => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.collapse_or_parent();
                }
            }
            Action::SwitchPanel => {
                self.handle_switch_panel();
            }
            Action::SwitchPanelReverse => {
                self.handle_switch_panel_reverse();
            }
            Action::QuickSearch(c) => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    let query = diff.quick_search.get_or_insert_with(String::new);
                    query.push(c);
                    let q = query.clone();
                    diff.jump_to_match(&q);
                }
            }
            Action::QuickSearchClear => {
                if let Some(ref mut diff) = self.diff_panels[side] {
                    diff.quick_search = None;
                }
            }
            Action::ToggleDiff => {
                self.diff_panels[side] = None;
                self.focus = PanelFocus::FilePanel;
            }
            Action::BottomResizeUp => {
                self.bottom_split_pct[side] = self.bottom_split_pct[side]
                    .saturating_sub(SPLIT_RESIZE_STEP)
                    .max(SPLIT_MIN_PCT);
            }
            Action::BottomResizeDown => {
                self.bottom_split_pct[side] =
                    (self.bottom_split_pct[side] + SPLIT_RESIZE_STEP).min(SPLIT_MAX_PCT);
            }
            Action::BottomMaximize => {
                self.bottom_maximized[side] = !self.bottom_maximized[side];
            }
            Action::MouseClick(col, row) => self.handle_mouse_click(col, row),
            Action::MouseDoubleClick(col, row) => self.handle_mouse_double_click(col, row),
            Action::MouseScrollUp(col, row) => self.handle_mouse_scroll(col, row, -3),
            Action::MouseScrollDown(col, row) => self.handle_mouse_scroll(col, row, 3),
            _ => {}
        }
    }

    fn handle_claude_action(&mut self, action: Action) {
        let side = match self.focus {
            PanelFocus::Claude(s) => s,
            _ => return,
        };

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::TerminalInput(bytes) => {
                if let Some(ref mut tp) = self.claude_panels[side] {
                    // Auto-scroll to bottom when user types
                    tp.scroll_to_bottom();
                    tp.write_bytes(&bytes);
                }
            }
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),
            Action::ToggleClaude => {
                self.claude_panels[side] = None;
                self.focus = PanelFocus::FilePanel;
                self.bottom_maximized[side] = false;
            }
            Action::TerminalOpenFile => self.handle_terminal_open_file(),
            Action::Quit => {
                self.quit_confirm = Some(true);
            }
            Action::MouseClick(col, row) => {
                if self.click_in_claude(col, row) {
                    // Click inside Claude panel — stay focused
                } else {
                    self.focus = PanelFocus::FilePanel;
                    self.handle_mouse_click(col, row);
                }
            }
            Action::MouseDoubleClick(col, row) => {
                if self.click_in_claude(col, row) {
                    // Double-click inside Claude panel — absorb
                } else {
                    self.focus = PanelFocus::FilePanel;
                    self.handle_mouse_double_click(col, row);
                }
            }
            Action::MouseScrollUp(_, _) | Action::PageUp => {
                if let Some(ref mut tp) = self.claude_panels[side] {
                    let lines = if matches!(action, Action::PageUp) { 20 } else { 3 };
                    tp.scroll_up(lines);
                }
            }
            Action::MouseScrollDown(_, _) | Action::PageDown => {
                if let Some(ref mut tp) = self.claude_panels[side] {
                    let lines = if matches!(action, Action::PageDown) { 20 } else { 3 };
                    tp.scroll_down(lines);
                }
            }
            _ => {}
        }
    }

    fn click_in_claude(&self, col: u16, row: u16) -> bool {
        for side in 0..2 {
            if let Some(area) = self.claude_panel_areas[side] {
                if col >= area.x
                    && col < area.x + area.width
                    && row >= area.y
                    && row < area.y + area.height
                {
                    return true;
                }
            }
        }
        false
    }

    fn handle_terminal_open_file(&mut self) {
        let side = match self.focus {
            PanelFocus::Claude(s) => s,
            _ => return,
        };
        let (path, line, col) = match self.claude_panels[side] {
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

    fn resize_claude_panels(&mut self) {
        for side in 0..2 {
            if let Some(ref mut tp) = self.claude_panels[side] {
                if let Some(area) = self.claude_panel_areas[side] {
                    let cols = area.width.saturating_sub(2).max(1);
                    let rows = area.height.saturating_sub(2).max(1);
                    tp.resize(cols, rows);
                }
            }
        }
    }

    /// Resize all bottom terminal panels to match their rendered areas.
    /// Called after the first render to sync PTY dimensions.
    pub fn resize_all_bottom_panels(&mut self) {
        self.resize_claude_panels();
        self.resize_shells();
        self.resize_ssh_panels();
    }

    fn toggle_shell(&mut self) {
        let side = self.active_panel;
        if self.shell_panels[side].is_some() {
            self.shell_panels[side] = None;
            if self.focus == PanelFocus::Shell(side) {
                self.focus = PanelFocus::FilePanel;
            }
        } else if let Some(ref wakeup) = self.wakeup_sender {
            let dir = self.panels[side].current_dir.clone();
            // Use the CI area dimensions if available, otherwise estimate 40% height
            let area = self.panel_areas[side];
            let cols = area.width.saturating_sub(2).max(1);
            let rows = (area.height * 40 / 100).saturating_sub(2).max(1);
            match TerminalPanel::spawn_shell(&dir, cols, rows, wakeup.clone()) {
                Ok(tp) => {
                    self.shell_panels[side] = Some(tp);
                    self.focus = PanelFocus::Shell(side);
                    self.bottom_split_pct[side] = self.persisted.split_pct_shell;
                }
                Err(e) => {
                    self.status_message = Some(format!("Failed to start shell: {}", e));
                }
            }
        }
    }

    fn handle_shell_action(&mut self, action: Action) {
        let side = match self.focus {
            PanelFocus::Shell(s) => s,
            _ => return,
        };

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::TerminalInput(bytes) => {
                if let Some(ref mut sp) = self.shell_panels[side] {
                    sp.scroll_to_bottom();
                    sp.write_bytes(&bytes);
                }
            }
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),
            Action::ToggleShell => self.toggle_shell(),
            Action::Quit => {
                self.quit_confirm = Some(true);
            }
            Action::BottomResizeUp => {
                self.bottom_split_pct[side] = self.bottom_split_pct[side]
                    .saturating_sub(SPLIT_RESIZE_STEP)
                    .max(SPLIT_MIN_PCT);
            }
            Action::BottomResizeDown => {
                self.bottom_split_pct[side] =
                    (self.bottom_split_pct[side] + SPLIT_RESIZE_STEP).min(SPLIT_MAX_PCT);
            }
            Action::BottomMaximize => {
                self.bottom_maximized[side] = !self.bottom_maximized[side];
            }
            Action::MouseClick(col, row) => self.handle_mouse_click(col, row),
            Action::MouseDoubleClick(col, row) => self.handle_mouse_double_click(col, row),
            Action::MouseScrollUp(_, _) => {
                if let Some(ref mut sp) = self.shell_panels[side] {
                    sp.scroll_up(3);
                }
            }
            Action::MouseScrollDown(_, _) => {
                if let Some(ref mut sp) = self.shell_panels[side] {
                    sp.scroll_down(3);
                }
            }
            _ => {}
        }
    }

    fn resize_shells(&mut self) {
        for side in 0..2 {
            if let Some(ref mut sp) = self.shell_panels[side] {
                if let Some(area) = self.shell_panel_areas[side] {
                    let cols = area.width.saturating_sub(2).max(1);
                    let rows = area.height.saturating_sub(2).max(1);
                    sp.resize(cols, rows);
                }
            }
        }
    }

    fn resize_ssh_panels(&mut self) {
        for side in 0..2 {
            if let Some(ref mut sp) = self.ssh_panels[side] {
                if let Some(area) = self.ssh_panel_areas[side] {
                    let cols = area.width.saturating_sub(2).max(1);
                    let rows = area.height.saturating_sub(2).max(1);
                    sp.resize(cols, rows);
                }
            }
        }
    }

    fn toggle_ssh(&mut self) {
        let side = self.active_panel;

        // If the file panel is remote, disconnect and return to local
        if self.panels[side].source.is_remote() {
            let label = self.panels[side].source.label().unwrap_or_default();
            let local_path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
            self.panels[side].switch_to_local(local_path);
            self.status_message = Some(format!("{} disconnected", label));
            return;
        }

        if self.ssh_panels[side].is_some() {
            // Close SSH panel
            self.ssh_panels[side] = None;
            self.ssh_hosts[side] = None;
            if self.focus == PanelFocus::Ssh(side) {
                self.focus = PanelFocus::FilePanel;
            }
        } else {
            // Open SSH dialog to pick a host
            self.ssh_dialog = Some(SshDialogState::new());
        }
    }

    fn handle_ssh_action(&mut self, action: Action) {
        let side = match self.focus {
            PanelFocus::Ssh(s) => s,
            _ => return,
        };

        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::TerminalInput(ref bytes) => {
                let is_exited = self
                    .ssh_panels[side]
                    .as_ref()
                    .map(|sp| sp.exited)
                    .unwrap_or(false);
                if is_exited {
                    // On Enter, reconnect to the same host
                    if bytes == b"\r" {
                        if let Some(host) = self.ssh_hosts[side].clone() {
                            self.ssh_panels[side] = None;
                            self.connect_ssh(host);
                        }
                    }
                    // Ignore other input when disconnected
                } else if let Some(ref mut sp) = self.ssh_panels[side] {
                    sp.scroll_to_bottom();
                    sp.write_bytes(bytes);
                }
            }
            Action::SwitchPanel => self.handle_switch_panel(),
            Action::SwitchPanelReverse => self.handle_switch_panel_reverse(),
            Action::ToggleSsh => {
                self.ssh_panels[side] = None;
                self.ssh_hosts[side] = None;
                self.focus = PanelFocus::FilePanel;
            }
            Action::Quit => {
                self.quit_confirm = Some(true);
            }
            Action::BottomResizeUp => {
                self.bottom_split_pct[side] = self.bottom_split_pct[side]
                    .saturating_sub(SPLIT_RESIZE_STEP)
                    .max(SPLIT_MIN_PCT);
            }
            Action::BottomResizeDown => {
                self.bottom_split_pct[side] =
                    (self.bottom_split_pct[side] + SPLIT_RESIZE_STEP).min(SPLIT_MAX_PCT);
            }
            Action::BottomMaximize => {
                self.bottom_maximized[side] = !self.bottom_maximized[side];
            }
            Action::MouseClick(col, row) => {
                if self.click_in_ssh(col, row) {
                    // Click inside SSH panel — stay focused
                } else {
                    self.focus = PanelFocus::FilePanel;
                    self.handle_mouse_click(col, row);
                }
            }
            Action::MouseDoubleClick(col, row) => {
                if self.click_in_ssh(col, row) {
                    // absorb
                } else {
                    self.focus = PanelFocus::FilePanel;
                    self.handle_mouse_double_click(col, row);
                }
            }
            Action::MouseScrollUp(_, _) => {
                if let Some(ref mut sp) = self.ssh_panels[side] {
                    sp.scroll_up(3);
                }
            }
            Action::MouseScrollDown(_, _) => {
                if let Some(ref mut sp) = self.ssh_panels[side] {
                    sp.scroll_down(3);
                }
            }
            _ => {}
        }
    }

    fn click_in_ssh(&self, col: u16, row: u16) -> bool {
        for side in 0..2 {
            if let Some(area) = self.ssh_panel_areas[side] {
                if col >= area.x
                    && col < area.x + area.width
                    && row >= area.y
                    && row < area.y + area.height
                {
                    return true;
                }
            }
        }
        false
    }

    fn handle_ssh_dialog_action(&mut self, action: Action) {
        // Handle saved connections browsing mode
        if let Some(ref d) = self.ssh_dialog {
            if d.saved_selected.is_some() {
                match action {
                    Action::DialogCancel => { self.ssh_dialog = None; return; }
                    Action::MoveUp => {
                        if let Some(ref mut d) = self.ssh_dialog {
                            if let Some(ref mut sel) = d.saved_selected {
                                *sel = sel.saturating_sub(1);
                            }
                        }
                        return;
                    }
                    Action::MoveDown => {
                        if let Some(ref mut d) = self.ssh_dialog {
                            let max = d.saved_connections.len().saturating_sub(1);
                            if let Some(ref mut sel) = d.saved_selected {
                                *sel = (*sel + 1).min(max);
                            }
                        }
                        return;
                    }
                    Action::Toggle => {
                        // Switch from saved mode to protocol input mode
                        if let Some(ref mut d) = self.ssh_dialog {
                            d.saved_selected = None;
                        }
                        return;
                    }
                    Action::Delete => {
                        // Delete the selected saved connection
                        if let Some(ref mut d) = self.ssh_dialog {
                            if let Some(sel) = d.saved_selected {
                                if sel < d.saved_connections.len() {
                                    d.saved_connections.remove(sel);
                                    crate::saved_connections::save_connections(&d.saved_connections);
                                    if d.saved_connections.is_empty() {
                                        d.saved_selected = None;
                                    } else if sel >= d.saved_connections.len() {
                                        d.saved_selected = Some(d.saved_connections.len() - 1);
                                    }
                                    self.status_message = Some("Connection deleted".to_string());
                                }
                            }
                        }
                        return;
                    }
                    Action::DialogConfirm => {
                        // Connect using saved connection
                        let conn = self.ssh_dialog.as_ref()
                            .and_then(|d| d.saved_selected)
                            .and_then(|sel| self.ssh_dialog.as_ref()?.saved_connections.get(sel).cloned());
                        self.ssh_dialog = None;
                        if let Some(c) = conn {
                            self.connect_saved(&c);
                        }
                        return;
                    }
                    _ => return,
                }
            }
        }

        // Handle F2 (save current connection)
        if matches!(action, Action::EditorSave) {
            self.save_dialog_as_connection();
            return;
        }

        match action {
            Action::DialogCancel => {
                self.ssh_dialog = None;
            }
            // Alt+Left/Right cycle protocol
            Action::SwitchPanelReverse => {
                if let Some(ref mut d) = self.ssh_dialog {
                    d.protocol = d.protocol.prev();
                    d.field_focus = 0;
                }
            }
            Action::SwitchPanel => {
                if let Some(ref mut d) = self.ssh_dialog {
                    d.protocol = d.protocol.next();
                    d.field_focus = 0;
                }
            }
            Action::Toggle => {
                // Only used from saved connections mode (Tab to switch to input)
            }
            Action::DialogConfirm | Action::Enter => {
                let protocol = self.ssh_dialog.as_ref().map(|d| d.protocol);
                crate::debug_log::log(&format!("DialogConfirm: protocol={:?}", protocol));
                match protocol {
                    Some(RemoteProtocol::Ssh) | Some(RemoteProtocol::Sftp) => {
                        let (host, is_sftp) = if let Some(ref dialog) = self.ssh_dialog {
                            let h = dialog.selected_host().cloned()
                                .or_else(|| crate::ssh::SshHost::from_quick_connect(&dialog.input.text));
                            (h, dialog.protocol == RemoteProtocol::Sftp)
                        } else {
                            (None, false)
                        };
                        let had_input = self.ssh_dialog.as_ref()
                            .map(|d| !d.input.text.trim().is_empty())
                            .unwrap_or(false);
                        self.ssh_dialog = None;
                        if let Some(host) = host {
                            if is_sftp {
                                self.connect_sftp(host);
                            } else {
                                self.connect_ssh(host);
                            }
                        } else if had_input {
                            self.status_message = Some("Invalid host format. Use: user@host[:port]".to_string());
                        }
                    }
                    Some(RemoteProtocol::Smb) => {
                        let (host, share, user, pass) = if let Some(ref d) = self.ssh_dialog {
                            (d.input.text.clone(), d.smb_share.text.clone(),
                             d.smb_user.text.clone(), d.smb_pass.text.clone())
                        } else {
                            (String::new(), String::new(), String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        if host.is_empty() || share.is_empty() {
                            self.status_message = Some("Host and share name are required".to_string());
                        } else {
                            self.connect_smb(&host, &share, &user, &pass);
                        }
                    }
                    Some(RemoteProtocol::WebDav) => {
                        let (url, user, pass) = if let Some(ref d) = self.ssh_dialog {
                            (d.input.text.clone(), d.webdav_user.text.clone(),
                             d.webdav_pass.text.clone())
                        } else {
                            (String::new(), String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        if url.is_empty() {
                            self.status_message = Some("URL is required".to_string());
                        } else {
                            self.connect_webdav(&url, &user, &pass);
                        }
                    }
                    Some(RemoteProtocol::S3) => {
                        let (bucket, profile, endpoint, region) = if let Some(ref d) = self.ssh_dialog {
                            (d.s3_bucket.text.clone(), d.s3_profile.text.clone(),
                             d.s3_endpoint.text.clone(), d.s3_region.text.clone())
                        } else {
                            (String::new(), String::new(), String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        if bucket.is_empty() {
                            self.status_message = Some("Bucket name is required".to_string());
                        } else {
                            self.connect_s3(&bucket, &profile, &endpoint, &region);
                        }
                    }
                    Some(RemoteProtocol::Gcs) => {
                        let (bucket, project) = if let Some(ref d) = self.ssh_dialog {
                            (d.gcs_bucket.text.clone(), d.gcs_project.text.clone())
                        } else {
                            (String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        if bucket.is_empty() {
                            self.status_message = Some("Bucket name is required".to_string());
                        } else {
                            self.connect_gcs(&bucket, &project);
                        }
                    }
                    Some(RemoteProtocol::AzureBlob) => {
                        let (account, container, sas, conn_str) = if let Some(ref d) = self.ssh_dialog {
                            crate::debug_log::log(&format!(
                                "Azure fields: account={:?} container={:?} sas={} conn_str={}",
                                d.azure_account.text, d.azure_container.text,
                                if d.azure_sas.text.is_empty() { "(empty)" } else { "(set)" },
                                if d.azure_conn_str.text.is_empty() { "(empty)" } else { "(set)" },
                            ));
                            (d.azure_account.text.clone(), d.azure_container.text.clone(),
                             d.azure_sas.text.clone(), d.azure_conn_str.text.clone())
                        } else {
                            (String::new(), String::new(), String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        // Extract account name from connection string if account field is empty
                        let account = if account.is_empty() && !conn_str.is_empty() {
                            conn_str
                                .split(';')
                                .find_map(|part| part.strip_prefix("AccountName="))
                                .unwrap_or("azure")
                                .to_string()
                        } else {
                            account
                        };
                        // Container is now optional -- when empty, lists containers at account level
                        if !conn_str.is_empty() || !account.is_empty() {
                            self.connect_azure_blob(&account, &container, &sas, &conn_str);
                        } else {
                            self.status_message = Some("Account name or connection string is required".to_string());
                            self.dirty = true;
                        }
                    }
                    Some(RemoteProtocol::Nfs) => {
                        let (host, export, options) = if let Some(ref d) = self.ssh_dialog {
                            (d.nfs_host.text.clone(), d.nfs_export.text.clone(),
                             d.nfs_options.text.clone())
                        } else {
                            (String::new(), String::new(), String::new())
                        };
                        self.ssh_dialog = None;
                        if host.is_empty() || export.is_empty() {
                            self.status_message = Some("Host and export path are required".to_string());
                        } else {
                            self.connect_nfs(&host, &export, &options);
                        }
                    }
                    None => { self.ssh_dialog = None; }
                }
            }
            Action::MoveUp => {
                if let Some(ref mut d) = self.ssh_dialog {
                    let is_ssh = matches!(d.protocol, RemoteProtocol::Ssh | RemoteProtocol::Sftp);
                    if is_ssh {
                        d.selected = d.selected.saturating_sub(1);
                    } else {
                        // Tab/BackTab cycles fields in SMB/WebDAV
                        d.field_focus = d.field_focus.saturating_sub(1);
                    }
                }
            }
            Action::MoveDown => {
                if let Some(ref mut d) = self.ssh_dialog {
                    let is_ssh = matches!(d.protocol, RemoteProtocol::Ssh | RemoteProtocol::Sftp);
                    if is_ssh {
                        if !d.filtered.is_empty() {
                            d.selected = (d.selected + 1).min(d.filtered.len() - 1);
                        }
                    } else {
                        let max = d.max_fields().saturating_sub(1);
                        d.field_focus = (d.field_focus + 1).min(max);
                    }
                }
            }
            Action::DialogInput(c) => {
                if let Some(ref mut d) = self.ssh_dialog {
                    let is_ssh = matches!(d.protocol, RemoteProtocol::Ssh | RemoteProtocol::Sftp);
                    d.active_input_mut().insert_char(c);
                    if is_ssh {
                        d.update_filter();
                    }
                }
            }
            Action::DialogBackspace => {
                if let Some(ref mut d) = self.ssh_dialog {
                    let is_ssh = matches!(d.protocol, RemoteProtocol::Ssh | RemoteProtocol::Sftp);
                    d.active_input_mut().backspace();
                    if is_ssh {
                        d.update_filter();
                    }
                }
            }
            _ => {}
        }
    }

    fn connect_ssh(&mut self, host: crate::ssh::SshHost) {
        let side = self.active_panel;
        if let Some(ref wakeup) = self.wakeup_sender {
            let area = self.panel_areas[side];
            let cols = area.width.saturating_sub(2).max(1);
            let rows = (area.height * 40 / 100).saturating_sub(2).max(1);
            match TerminalPanel::spawn_ssh(&host, cols, rows, wakeup.clone()) {
                Ok(tp) => {
                    self.ssh_panels[side] = Some(tp);
                    self.ssh_hosts[side] = Some(host);
                    self.focus = PanelFocus::Ssh(side);
                    self.bottom_split_pct[side] = self.persisted.split_pct_ssh;
                }
                Err(e) => {
                    self.status_message = Some(format!("SSH failed: {}", e));
                }
            }
        }
    }

    fn connect_sftp(&mut self, host: crate::ssh::SshHost) {
        self.status_message = Some(format!("Connecting SFTP to {}...", host.display_label()));
        self.spawn_remote_connect(move || {
            crate::sftp::SftpConnection::connect(&host)
                .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_smb(&mut self, host: &str, share: &str, user: &str, pass: &str) {
        self.status_message = Some(format!("Connecting SMB to {}\\{}...", host, share));
        let host = host.to_string();
        let share = share.to_string();
        let user = user.to_string();
        let pass = pass.to_string();
        self.spawn_remote_connect(move || {
            crate::smb_client::SmbConnection::connect(&host, &share, &user, &pass)
                .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_webdav(&mut self, url: &str, user: &str, pass: &str) {
        self.status_message = Some(format!("Connecting WebDAV to {}...", url));
        let url = url.to_string();
        let user = user.to_string();
        let pass = pass.to_string();
        self.spawn_remote_connect(move || {
            crate::webdav::WebDavConnection::connect(&url, &user, &pass)
                .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_s3(&mut self, bucket: &str, profile: &str, endpoint: &str, region: &str) {
        self.status_message = Some(format!("Connecting S3 to {}...", bucket));
        let bucket = bucket.to_string();
        let profile = if profile.is_empty() { None } else { Some(profile.to_string()) };
        let endpoint = if endpoint.is_empty() { None } else { Some(endpoint.to_string()) };
        let region = if region.is_empty() { None } else { Some(region.to_string()) };
        self.spawn_remote_connect(move || {
            crate::s3::S3Connection::connect(
                &bucket,
                profile.as_deref(),
                endpoint.as_deref(),
                region.as_deref(),
            )
            .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_gcs(&mut self, bucket: &str, project: &str) {
        self.status_message = Some(format!("Connecting GCS to {}...", bucket));
        let bucket = bucket.to_string();
        let project = if project.is_empty() { None } else { Some(project.to_string()) };
        self.spawn_remote_connect(move || {
            crate::gcs::GcsConnection::connect(&bucket, project.as_deref())
                .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_azure_blob(&mut self, account: &str, container: &str, sas: &str, conn_str: &str) {
        self.status_message = Some(format!("Connecting Azure to {}/{}...", account, container));
        let account = account.to_string();
        let container = container.to_string();
        let sas = if sas.is_empty() { None } else { Some(sas.to_string()) };
        let conn_str = if conn_str.is_empty() { None } else { Some(conn_str.to_string()) };
        self.spawn_remote_connect(move || {
            crate::azure_blob::AzureBlobConnection::connect(
                &account, &container, sas.as_deref(), conn_str.as_deref(),
            )
            .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    fn connect_nfs(&mut self, host: &str, export: &str, options: &str) {
        self.status_message = Some(format!("Mounting NFS {}:{}...", host, export));
        let host = host.to_string();
        let export = export.to_string();
        let options = options.to_string();
        self.spawn_remote_connect(move || {
            crate::nfs_client::NfsConnection::connect(&host, &export, &options)
                .map(|c| Box::new(c) as Box<dyn crate::remote_fs::RemoteFs + Send>)
        });
    }

    /// Spawn a background thread to establish a remote connection.
    fn connect_saved(&mut self, conn: &crate::saved_connections::SavedConnection) {
        match conn.protocol.as_str() {
            "ssh" => {
                if let Some(host) = crate::ssh::SshHost::from_quick_connect(
                    &conn.display_label().strip_prefix("SSH: ").unwrap_or(&conn.name),
                ) {
                    self.connect_ssh(host);
                }
            }
            "sftp" => {
                let host = crate::ssh::SshHost {
                    name: conn.name.clone(),
                    hostname: conn.host.clone().unwrap_or_default(),
                    port: conn.port,
                    user: conn.user.clone(),
                    identity_file: conn.identity_file.clone(),
                    group: None,
                    jump_host: conn.jump_host.clone(),
                    extra_args: vec![],
                    source: crate::ssh::HostSource::Saved,
                };
                self.connect_sftp(host);
            }
            "smb" => {
                self.connect_smb(
                    conn.host.as_deref().unwrap_or(""),
                    conn.share.as_deref().unwrap_or(""),
                    conn.user.as_deref().unwrap_or(""),
                    conn.password.as_deref().unwrap_or(""),
                );
            }
            "webdav" => {
                self.connect_webdav(
                    conn.url.as_deref().unwrap_or(""),
                    conn.user.as_deref().unwrap_or(""),
                    conn.password.as_deref().unwrap_or(""),
                );
            }
            "s3" => {
                self.connect_s3(
                    conn.bucket.as_deref().unwrap_or(""),
                    conn.profile.as_deref().unwrap_or(""),
                    conn.endpoint_url.as_deref().unwrap_or(""),
                    conn.region.as_deref().unwrap_or(""),
                );
            }
            "gcs" => {
                self.connect_gcs(
                    conn.bucket.as_deref().unwrap_or(""),
                    conn.project.as_deref().unwrap_or(""),
                );
            }
            "azure" => {
                self.connect_azure_blob(
                    conn.account.as_deref().unwrap_or(""),
                    conn.container.as_deref().unwrap_or(""),
                    conn.sas_token.as_deref().unwrap_or(""),
                    conn.connection_string.as_deref().unwrap_or(""),
                );
            }
            "nfs" => {
                self.connect_nfs(
                    conn.host.as_deref().unwrap_or(""),
                    conn.export.as_deref().unwrap_or(""),
                    conn.mount_options.as_deref().unwrap_or(""),
                );
            }
            _ => {
                self.status_message = Some(format!("Unknown protocol: {}", conn.protocol));
            }
        }
    }

    fn save_dialog_as_connection(&mut self) {
        crate::debug_log::log("save_dialog_as_connection called");
        let conn = if let Some(ref d) = self.ssh_dialog {
            let protocol = match d.protocol {
                RemoteProtocol::Ssh => "ssh",
                RemoteProtocol::Sftp => "sftp",
                RemoteProtocol::Smb => "smb",
                RemoteProtocol::WebDav => "webdav",
                RemoteProtocol::S3 => "s3",
                RemoteProtocol::Gcs => "gcs",
                RemoteProtocol::AzureBlob => "azure",
                RemoteProtocol::Nfs => "nfs",
            };
            let mut c = crate::saved_connections::SavedConnection {
                name: String::new(),
                protocol: protocol.to_string(),
                host: None, port: None, user: None, password: None,
                share: None, url: None, bucket: None, profile: None,
                endpoint_url: None, region: None, project: None,
                account: None, container: None, sas_token: None,
                connection_string: None, export: None, mount_options: None,
                identity_file: None, jump_host: None,
            };
            match d.protocol {
                RemoteProtocol::Ssh | RemoteProtocol::Sftp => {
                    c.host = Some(d.input.text.clone());
                }
                RemoteProtocol::Smb => {
                    c.host = Some(d.input.text.clone());
                    c.share = Some(d.smb_share.text.clone());
                    c.user = Some(d.smb_user.text.clone());
                    c.password = Some(d.smb_pass.text.clone());
                }
                RemoteProtocol::WebDav => {
                    c.url = Some(d.input.text.clone());
                    c.user = Some(d.webdav_user.text.clone());
                    c.password = Some(d.webdav_pass.text.clone());
                }
                RemoteProtocol::S3 => {
                    c.bucket = Some(d.s3_bucket.text.clone());
                    c.profile = Some(d.s3_profile.text.clone());
                    c.endpoint_url = Some(d.s3_endpoint.text.clone());
                    c.region = Some(d.s3_region.text.clone());
                }
                RemoteProtocol::Gcs => {
                    c.bucket = Some(d.gcs_bucket.text.clone());
                    c.project = Some(d.gcs_project.text.clone());
                }
                RemoteProtocol::AzureBlob => {
                    c.account = Some(d.azure_account.text.clone());
                    c.container = Some(d.azure_container.text.clone());
                    c.sas_token = Some(d.azure_sas.text.clone());
                    c.connection_string = Some(d.azure_conn_str.text.clone());
                }
                RemoteProtocol::Nfs => {
                    c.host = Some(d.nfs_host.text.clone());
                    c.export = Some(d.nfs_export.text.clone());
                    c.mount_options = Some(d.nfs_options.text.clone());
                }
            }
            c.name = c.display_label();
            Some(c)
        } else {
            None
        };

        if let Some(mut c) = conn {
            // Auto-extract account from connection string if name is unhelpful
            if c.name.ends_with(": /") || c.name.ends_with(": ?/?") {
                if let Some(ref cs) = c.connection_string {
                    if let Some(acct) = cs.split(';').find_map(|p| p.strip_prefix("AccountName=")) {
                        c.name = format!("Azure: {}", acct);
                    }
                }
            }
            if let Some(ref mut d) = self.ssh_dialog {
                let name = c.name.clone();
                d.saved_connections.push(c);
                crate::saved_connections::save_connections(&d.saved_connections);
                // Show popup over the dialog so user can see it
                self.popup = Some(("Saved".to_string(), format!("{}\n\nConnection saved for quick access.", name)));
            }
        }
    }

    fn spawn_remote_connect<F>(&mut self, f: F)
    where
        F: FnOnce() -> anyhow::Result<Box<dyn crate::remote_fs::RemoteFs + Send>> + Send + 'static,
    {
        crate::debug_log::log("spawn_remote_connect: starting background thread");
        self.dirty = true;
        let (tx, rx) = std::sync::mpsc::channel();
        let side = self.active_panel;
        std::thread::spawn(move || {
            let result = f();
            match &result {
                Ok(conn) => crate::debug_log::log(&format!("Connection succeeded: {}", conn.display_label())),
                Err(e) => crate::debug_log::log_error("connect", &format!("{}", e)),
            }
            let _ = tx.send(RemoteConnectResult { result });
        });
        self.pending_remote = Some(PendingRemoteConnect {
            rx,
            side,
            started: std::time::Instant::now(),
        });
    }

    fn handle_session_dialog_action(&mut self, action: Action) {
        // Check if we're in "create new" input mode
        let creating = self
            .session_dialog
            .as_ref()
            .map(|d| d.creating)
            .unwrap_or(false);

        if creating {
            match action {
                Action::DialogCancel => {
                    // Cancel creation, go back to list
                    if let Some(ref mut d) = self.session_dialog {
                        d.creating = false;
                        d.input = crate::text_input::TextInput::new(String::new());
                    }
                }
                Action::DialogConfirm => {
                    let name = self
                        .session_dialog
                        .as_ref()
                        .map(|d| d.input.text.trim().to_string())
                        .unwrap_or_default();
                    if !name.is_empty() {
                        match crate::session::create_session(&name) {
                            Ok(()) => {
                                self.status_message =
                                    Some(format!("Created session '{}'. Use --session {} to attach.", name, name));
                            }
                            Err(e) => {
                                self.status_message = Some(format!("Failed to create session: {}", e));
                            }
                        }
                    }
                    if let Some(ref mut d) = self.session_dialog {
                        d.creating = false;
                        d.input = crate::text_input::TextInput::new(String::new());
                        d.refresh();
                    }
                }
                Action::DialogInput(c) => {
                    if let Some(ref mut d) = self.session_dialog {
                        d.input.insert_char(c);
                    }
                }
                Action::DialogBackspace => {
                    if let Some(ref mut d) = self.session_dialog {
                        d.input.backspace();
                    }
                }
                _ => {}
            }
            return;
        }

        match action {
            Action::DialogCancel => {
                self.session_dialog = None;
            }
            Action::DialogConfirm => {
                // Attach to selected session (spawn tmux attach in a terminal panel)
                let session_name = self
                    .session_dialog
                    .as_ref()
                    .and_then(|d| d.selected_session())
                    .map(|s| s.name.clone());
                self.session_dialog = None;

                if let Some(name) = session_name {
                    self.attach_tmux_session(&name);
                }
            }
            Action::MoveUp => {
                if let Some(ref mut d) = self.session_dialog {
                    d.selected = d.selected.saturating_sub(1);
                }
            }
            Action::MoveDown => {
                if let Some(ref mut d) = self.session_dialog {
                    if !d.sessions.is_empty() {
                        d.selected = (d.selected + 1).min(d.sessions.len() - 1);
                    }
                }
            }
            Action::CreateDir => {
                // 'n' key = new session
                if let Some(ref mut d) = self.session_dialog {
                    d.creating = true;
                }
            }
            Action::Delete => {
                // 'd' or Delete = kill session
                let session_name = self
                    .session_dialog
                    .as_ref()
                    .and_then(|d| d.selected_session())
                    .map(|s| s.name.clone());
                if let Some(name) = session_name {
                    match crate::session::kill_session(&name) {
                        Ok(()) => {
                            self.status_message = Some(format!("Killed session '{}'", name));
                        }
                        Err(e) => {
                            self.status_message = Some(format!("Failed to kill session: {}", e));
                        }
                    }
                    if let Some(ref mut d) = self.session_dialog {
                        d.refresh();
                    }
                }
            }
            _ => {}
        }
    }

    fn attach_tmux_session(&mut self, session_name: &str) {
        let side = self.active_panel;
        if let Some(ref wakeup) = self.wakeup_sender {
            let area = self.panel_areas[side];
            let cols = area.width.saturating_sub(2).max(1);
            let rows = (area.height * 40 / 100).saturating_sub(2).max(1);
            let (cmd, args) = crate::session::attach_command(session_name);
            let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let dir = std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("/"));
            match TerminalPanel::spawn_cmd(&cmd, &args_refs, &dir, cols, rows,
                format!(" tmux: {} ", session_name), true, wakeup.clone())
            {
                Ok(tp) => {
                    self.shell_panels[side] = Some(tp);
                    self.focus = PanelFocus::Shell(side);
                    self.bottom_split_pct[side] = self.persisted.split_pct_shell;
                }
                Err(e) => {
                    self.status_message = Some(format!("tmux attach failed: {}", e));
                }
            }
        }
    }

    fn start_failure_extraction(&mut self, side: usize) {
        let ci = match &mut self.ci_panels[side] {
            Some(ci) => ci,
            None => return,
        };

        if ci.failure_extraction.is_some() {
            self.status_message = Some("Extraction already in progress...".to_string());
            return;
        }

        let failed_checks: Vec<crate::ci::CiCheck> = match &ci.view {
            crate::ci::CiView::Tree { checks, .. } => checks
                .iter()
                .filter(|c| c.status == crate::ci::CiStatus::Failure)
                .cloned()
                .collect(),
            _ => return,
        };

        if failed_checks.is_empty() {
            self.status_message = Some("No failed checks to extract".to_string());
            return;
        }

        // Check auth requirements before starting
        let has_github = failed_checks.iter().any(|c| c.details_url.contains("github.com"));
        let has_azure = failed_checks.iter().any(|c| c.azure_info.is_some());
        let mut warnings = Vec::new();

        if has_github && !crate::ci::check_gh_auth() {
            warnings.push("GitHub: `gh auth login` required for log access.");
        }
        if has_azure && !crate::ci::has_azure_pat() {
            warnings.push("Azure DevOps: PAT required. Set AZURE_DEVOPS_PAT\nor store via: secret-tool store --label 'mm azure'\n  service middle-manager account azure-pat");
        }

        if !warnings.is_empty() {
            self.popup = Some((
                "Authentication Required".to_string(),
                warnings.join("\n\n"),
            ));
            return;
        }

        let repo = ci.repo.clone();
        ci.failure_extraction = Some(crate::ci::FailureExtraction::start(repo, failed_checks));
        self.status_message = Some("Extracting test failures from CI logs...".to_string());
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
                let ci = match self.ci_panels[side].as_mut() {
                    Some(ci) => ci,
                    None => return,
                };
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
                    state.input.clear_selection();
                    state.focused = state.focused.prev();
                    if state.focused == MkdirDialogField::Input {
                        state.input.select_all();
                    }
                }
            }
            Action::MoveDown => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.input.clear_selection();
                    state.focused = state.focused.next();
                    if state.focused == MkdirDialogField::Input {
                        state.input.select_all();
                    }
                }
            }
            Action::Toggle => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.process_multiple = !state.process_multiple;
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                // Swap between OK and Cancel buttons
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.input.clear_selection();
                    state.focused = match state.focused {
                        MkdirDialogField::ButtonOk => MkdirDialogField::ButtonCancel,
                        MkdirDialogField::ButtonCancel => MkdirDialogField::ButtonOk,
                        other => other,
                    };
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let AppMode::MkdirDialog(ref mut state) = self.mode {
                    state.input.handle_action(&action);
                }
            }
        }
    }

    fn confirm_mkdir_dialog(&mut self) {
        let (input, process_multiple) = match &self.mode {
            AppMode::MkdirDialog(s) => (s.input.text.clone(), s.process_multiple),
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
            let result = self.remote_mkdir(&dir.join(name));
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
                    state.destination.clear_selection();
                    state.focused = state.focused.prev();
                    if state.focused == CopyDialogField::Destination {
                        state.destination.select_all();
                    }
                }
            }
            Action::MoveDown => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.destination.clear_selection();
                    state.focused = state.focused.next();
                    if state.focused == CopyDialogField::Destination {
                        state.destination.select_all();
                    }
                }
            }
            Action::Toggle => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.toggle_focused();
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.destination.clear_selection();
                    state.focused = match state.focused {
                        CopyDialogField::ButtonCopy => CopyDialogField::ButtonCancel,
                        CopyDialogField::ButtonCancel => CopyDialogField::ButtonCopy,
                        other => other,
                    };
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let AppMode::CopyDialog(ref mut state) = self.mode {
                    state.destination.handle_action(&action);
                }
            }
        }
    }

    fn confirm_copy_dialog(&mut self) {
        let (source_paths, dest, is_move, source_panel) = {
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
                PathBuf::from(&state.destination.text),
                state.is_move,
                state.source_panel,
            )
        };

        // Determine source and dest filesystem types using the panel that was
        // active when the dialog was opened, not the currently active panel.
        let src_remote = self.panels[source_panel].source.is_remote();
        let dst_remote = self.panels[1 - source_panel].source.is_remote();
        let src_side = source_panel;
        let dst_side = 1 - source_panel;

        let mut first_err: Option<anyhow::Error> = None;
        let total_files = source_paths.len();
        for (i, source_path) in source_paths.iter().enumerate() {
            let file_name = source_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let dest_path = dest.join(&file_name);

            // Show progress for remote operations
            if src_remote || dst_remote {
                let op = if is_move { "Moving" } else { "Copying" };
                self.status_message = Some(format!(
                    "{} {}/{}: {}",
                    op, i + 1, total_files, file_name
                ));
                self.dirty = true;
            }

            let result = match (src_remote, dst_remote) {
                (false, false) => {
                    // Local -> Local
                    if is_move {
                        fs_ops::move_entry(source_path, &dest)
                    } else {
                        fs_ops::copy_entry(source_path, &dest)
                    }
                }
                (true, false) => {
                    // Remote -> Local (download)
                    // Determine if source is a dir from entries
                    let is_dir = self.panels[src_side]
                        .entries
                        .iter()
                        .find(|e| e.path == *source_path)
                        .map(|e| e.is_dir)
                        .unwrap_or(false);
                    self.remote_download(src_side, source_path, &dest_path, is_dir)
                        .map(|_| ())
                }
                (false, true) => {
                    // Local -> Remote (upload)
                    let is_dir = source_path.is_dir();
                    self.remote_upload(dst_side, source_path, &dest_path, is_dir)
                        .map(|_| ())
                }
                (true, true) => {
                    // Remote -> Remote: not supported yet
                    Err(anyhow::anyhow!("Remote-to-remote copy not yet supported"))
                }
            };
            if let Err(e) = result {
                first_err = Some(e);
                break;
            }
        }

        // For move operations from remote, delete source after copy.
        // Use src_side (snapshotted at dialog-open time) instead of
        // self.active_panel, which may have changed while the dialog was open.
        if is_move && first_err.is_none() && src_remote {
            for source_path in &source_paths {
                let del_result = match &self.panels[src_side].source {
                    crate::panel::PanelSource::Local => fs_ops::delete_entry(source_path),
                    crate::panel::PanelSource::Remote { connection } => {
                        connection.remove_recursive(source_path)
                    }
                };
                if let Err(e) = del_result {
                    first_err = Some(e);
                    break;
                }
            }
        }

        match first_err {
            None => {
                if src_remote || dst_remote {
                    let op = if is_move { "Moved" } else { "Copied" };
                    self.status_message = Some(format!("{} {} file(s)", op, total_files));
                } else {
                    self.status_message = None;
                }
            }
            Some(e) => self.status_message = Some(format!("Error: {}", e)),
        }

        self.mode = AppMode::Normal;
        self.reload_panels();
    }

    // --- Archive handlers ---

    fn handle_archive(&mut self) {
        let paths = self.active_panel().effective_selection_paths();
        if paths.is_empty() {
            return;
        }
        let display_name = if paths.len() == 1 {
            paths[0]
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        } else {
            format!("{} items", paths.len())
        };
        let mut dest = self
            .active_panel()
            .current_dir
            .to_string_lossy()
            .to_string();
        if !dest.ends_with('/') {
            dest.push('/');
        }
        let mut dlg = ArchiveDialogState::new(display_name, paths, dest, ArchiveFormat::TarZst);
        dlg.archive_name.select_all();
        self.mode = AppMode::ArchiveDialog(dlg);
    }

    fn handle_archive_dialog_action(&mut self, action: Action) {
        match action {
            Action::None | Action::Tick | Action::Resize(_, _) => {}
            Action::Quit => self.should_quit = true,
            Action::DialogCancel => {
                self.mode = AppMode::Normal;
            }
            Action::DialogConfirm => {
                let is_cancel = matches!(
                    self.mode,
                    AppMode::ArchiveDialog(ArchiveDialogState {
                        focused: ArchiveDialogField::ButtonCancel,
                        ..
                    })
                );
                if is_cancel {
                    self.mode = AppMode::Normal;
                } else {
                    self.confirm_archive_dialog();
                }
            }
            Action::MoveUp => {
                if let AppMode::ArchiveDialog(ref mut state) = self.mode {
                    state.archive_name.clear_selection();
                    state.destination.clear_selection();
                    state.focused = state.focused.prev();
                    match state.focused {
                        ArchiveDialogField::ArchiveName => state.archive_name.select_all(),
                        ArchiveDialogField::Destination => state.destination.select_all(),
                        _ => {}
                    }
                }
            }
            Action::MoveDown => {
                if let AppMode::ArchiveDialog(ref mut state) = self.mode {
                    state.archive_name.clear_selection();
                    state.destination.clear_selection();
                    state.focused = state.focused.next();
                    match state.focused {
                        ArchiveDialogField::ArchiveName => state.archive_name.select_all(),
                        ArchiveDialogField::Destination => state.destination.select_all(),
                        _ => {}
                    }
                }
            }
            Action::Toggle => {
                if let AppMode::ArchiveDialog(ref mut state) = self.mode {
                    if state.focused == ArchiveDialogField::Format {
                        state.format = state.format.next();
                        state.update_name_extension();
                    }
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let AppMode::ArchiveDialog(ref mut state) = self.mode {
                    state.focused = match state.focused {
                        ArchiveDialogField::ButtonArchive => ArchiveDialogField::ButtonCancel,
                        ArchiveDialogField::ButtonCancel => ArchiveDialogField::ButtonArchive,
                        other => other,
                    };
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let AppMode::ArchiveDialog(ref mut state) = self.mode {
                    if let Some(input) = state.active_input() {
                        input.handle_action(&action);
                    }
                }
            }
        }
    }

    fn confirm_archive_dialog(&mut self) {
        let (paths, archive_name, dest_dir, format) = {
            let state = match &self.mode {
                AppMode::ArchiveDialog(s) => s,
                _ => return,
            };
            if state.source_paths.is_empty() {
                self.mode = AppMode::Normal;
                return;
            }
            (
                state.source_paths.clone(),
                state.archive_name.text.clone(),
                state.destination.text.clone(),
                state.format,
            )
        };

        let dest_path = PathBuf::from(&dest_dir);
        let output_path = dest_path.join(&archive_name);

        // Check if file already exists — auto-resolve collision
        let final_path = if output_path.exists() {
            let ext = format.extension();
            let base = strip_archive_extension(&archive_name);
            let resolved = fs_ops::archive::resolve_collision(&dest_path, base, ext);
            dest_path.join(resolved)
        } else {
            output_path
        };

        let total_bytes = fs_ops::archive::compute_total_size(&paths);
        let done_bytes = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cancel = Arc::new(AtomicBool::new(false));

        let progress = ArchiveProgress {
            total_bytes,
            done_bytes: Arc::clone(&done_bytes),
            finished: Arc::clone(&finished),
            error: Arc::clone(&error),
            output_path: final_path.clone(),
        };

        // Spawn background thread
        let done_bytes_t = Arc::clone(&done_bytes);
        let finished_t = Arc::clone(&finished);
        let error_t = Arc::clone(&error);
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fs_ops::archive::create_archive(&paths, &final_path, format, done_bytes_t, cancel)
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    *error_t.lock().unwrap() = Some(format!("{}", e));
                }
                Err(_) => {
                    *error_t.lock().unwrap() = Some("Archive thread panicked".into());
                }
            }
            finished_t.store(true, Ordering::Release);
        });

        self.archive_progress = Some(progress);
        self.status_message = Some("Archiving...".into());
        self.mode = AppMode::Normal;
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

    fn handle_dialog_click_at(&mut self, col: u16, row: u16) {
        if let Some(content) = self.dialog_content_area {
            if col >= content.x
                && col < content.x + content.width
                && row >= content.y
                && row < content.y + content.height
            {
                let y_off = (row - content.y) as usize;
                self.handle_dialog_click(y_off);
                self.dirty = true;
            }
        }
    }

    fn handle_dialog_click(&mut self, y_off: usize) {
        // File search dialog: term=2, path=5, filter=8, regex=10
        if let Some(ref mut state) = self.file_search_dialog {
            state.term.clear_selection();
            state.path.clear_selection();
            state.filter.clear_selection();
            state.focused = match y_off {
                0..=2 => FileSearchField::Term,
                3..=5 => FileSearchField::Path,
                6..=8 => FileSearchField::Filter,
                9..=10 => FileSearchField::Regex,
                _ => FileSearchField::ButtonSearch,
            };
            state.select_focused();
            return;
        }
        // Search dialog: query at y=2
        if let Some(ref mut s) = self.search_dialog {
            s.query.clear_selection();
            s.focused = if y_off <= 2 {
                SearchDialogField::Query
            } else {
                s.focused // keep current
            };
            if s.focused == SearchDialogField::Query {
                s.query.select_all();
            }
            return;
        }
        match &mut self.mode {
            AppMode::Dialog(ref mut d) => {
                d.input.clear_selection();
                if d.has_input && y_off <= 2 {
                    d.focused = DialogField::Input;
                    d.input.select_all();
                }
            }
            AppMode::MkdirDialog(ref mut state) => {
                state.input.clear_selection();
                if y_off <= 2 {
                    state.focused = MkdirDialogField::Input;
                    state.input.select_all();
                }
            }
            AppMode::CopyDialog(ref mut state) => {
                state.destination.clear_selection();
                if y_off <= 2 {
                    state.focused = CopyDialogField::Destination;
                    state.destination.select_all();
                }
            }
            AppMode::ArchiveDialog(ref mut state) => {
                state.archive_name.clear_selection();
                state.destination.clear_selection();
                state.focused = match y_off {
                    0..=4 => ArchiveDialogField::ArchiveName, // y1=label, y2=label, y3=input
                    5..=7 => ArchiveDialogField::Destination, // y5=label, y6=input
                    8..=9 => ArchiveDialogField::Format,      // y8=sep, y9=format
                    _ => ArchiveDialogField::ButtonArchive,   // y10=sep, y11=buttons
                };
                match state.focused {
                    ArchiveDialogField::ArchiveName => state.archive_name.select_all(),
                    ArchiveDialogField::Destination => state.destination.select_all(),
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn handle_mouse_click(&mut self, col: u16, row: u16) {
        if let AppMode::Editing(ref mut e) = self.mode {
            e.click_at(col, row);
            return;
        }
        if matches!(
            self.mode,
            AppMode::Viewing(_) | AppMode::HexViewing(_) | AppMode::ParquetViewing(_)
        ) {
            return;
        }
        // Diff viewer: click positions cursor on left or right side
        if let AppMode::DiffViewing(ref mut d) = self.mode {
            // Use terminal size to approximate layout (border=1 on each side)
            let (term_w, _) = crossterm::terminal::size().unwrap_or((80, 24));
            let inner_x: u16 = 1; // left border
            let inner_width = term_w.saturating_sub(2);
            let total_width = inner_width as usize;
            let half_width = total_width.saturating_sub(1) / 2;
            let num_width = crate::ui::diff_viewer_view::digit_count(d.max_line_num).max(3);
            let inner_y: u16 = 1; // top border

            let right_panel_x = inner_x as usize + half_width + 1;
            let click_row = row as usize;
            let click_col = col as usize;

            if click_row >= inner_y as usize {
                let line_idx = d.scroll + (click_row - inner_y as usize);
                if line_idx < d.lines.len() {
                    d.cursor = line_idx;
                    d.ensure_cursor_visible();
                    d.clear_selection();

                    // Determine which side was clicked
                    if click_col >= right_panel_x {
                        d.cursor_side = crate::diff_viewer::DiffSide::Right;
                        d.cursor_col = click_col.saturating_sub(right_panel_x + num_width + 1);
                    } else {
                        d.cursor_side = crate::diff_viewer::DiffSide::Left;
                        d.cursor_col = click_col.saturating_sub(inner_x as usize + num_width + 1);
                    }
                    // Clamp col
                    let len = d.current_side_line_len();
                    if d.cursor_col > len {
                        d.cursor_col = len;
                    }
                }
            }
            return;
        }
        // Click inside dialogs: focus the clicked input field
        if self.dialog_content_area.is_some() {
            self.handle_dialog_click_at(col, row);
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
                    self.focus = PanelFocus::Ci(side);
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

        // Check if click is in a diff panel
        for side in 0..2 {
            if let Some(diff_area) = self.diff_panel_areas[side] {
                if self.diff_panels[side].is_some()
                    && col >= diff_area.x
                    && col < diff_area.x + diff_area.width
                    && row >= diff_area.y
                    && row < diff_area.y + diff_area.height
                {
                    self.focus = PanelFocus::Diff(side);
                    // Select the clicked row
                    if let Some(ref mut diff) = self.diff_panels[side] {
                        let inner_y = diff_area.y + 1;
                        if row >= inner_y {
                            let click_offset = (row - inner_y) as usize;
                            if let crate::pr_diff::DiffView::Tree {
                                items,
                                selected,
                                scroll,
                                ..
                            } = &mut diff.view
                            {
                                let target = *scroll + click_offset;
                                if target < items.len() {
                                    *selected = target;
                                }
                            }
                        }
                    }
                    return;
                }
            }
        }

        // Check if click is in a Claude panel
        for side in 0..2 {
            if let Some(claude_area) = self.claude_panel_areas[side] {
                if self.claude_panels[side].is_some()
                    && col >= claude_area.x
                    && col < claude_area.x + claude_area.width
                    && row >= claude_area.y
                    && row < claude_area.y + claude_area.height
                {
                    self.focus = PanelFocus::Claude(side);
                    return;
                }
            }
        }

        // Check if click is in a shell panel
        for side in 0..2 {
            if let Some(shell_area) = self.shell_panel_areas[side] {
                if self.shell_panels[side].is_some()
                    && col >= shell_area.x
                    && col < shell_area.x + shell_area.width
                    && row >= shell_area.y
                    && row < shell_area.y + shell_area.height
                {
                    self.focus = PanelFocus::Shell(side);
                    return;
                }
            }
        }

        // Check if click is in an SSH panel
        for side in 0..2 {
            if let Some(ssh_area) = self.ssh_panel_areas[side] {
                if self.ssh_panels[side].is_some()
                    && col >= ssh_area.x
                    && col < ssh_area.x + ssh_area.width
                    && row >= ssh_area.y
                    && row < ssh_area.y + ssh_area.height
                {
                    self.focus = PanelFocus::Ssh(side);
                    return;
                }
            }
        }

        // Check if click is in the search results panel
        if self.file_search.is_some() {
            let search_area = self.panel_areas[self.file_search_side];
            if col >= search_area.x
                && col < search_area.x + search_area.width
                && row >= search_area.y
                && row < search_area.y + search_area.height
            {
                self.focus = PanelFocus::Search;
                // Select the clicked row
                if let Some(ref mut state) = self.file_search {
                    let inner_y = search_area.y + 1;
                    if row >= inner_y {
                        let target = state.scroll + (row - inner_y) as usize;
                        if target < state.visible_count() {
                            state.selected = target;
                        }
                    }
                }
                return;
            }
        }

        // Click on a file panel — unfocus everything
        if let Some(panel_idx) = self.panel_at(col, row) {
            self.focus = PanelFocus::FilePanel;
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
            AppMode::ParquetViewing(p) => {
                if delta < 0 {
                    p.move_up((-delta) as usize);
                } else {
                    p.move_down(delta as usize);
                }
                return;
            }
            AppMode::DiffViewing(d) => {
                if delta < 0 {
                    d.scroll_up((-delta) as usize);
                } else {
                    d.scroll_down(delta as usize);
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
                    state.input.clear_selection();
                    state.focused = state.focused.prev(state.has_input);
                    if state.focused == DialogField::Input {
                        state.input.select_all();
                    }
                }
            }
            Action::MoveDown => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.input.clear_selection();
                    state.focused = state.focused.next(state.has_input);
                    if state.focused == DialogField::Input {
                        state.input.select_all();
                    }
                }
            }
            Action::SwitchPanel | Action::SwitchPanelReverse => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.input.clear_selection();
                    state.focused = match state.focused {
                        DialogField::ButtonOk => DialogField::ButtonCancel,
                        DialogField::ButtonCancel => DialogField::ButtonOk,
                        other => other,
                    };
                }
            }
            Action::MouseClick(col, row) => self.handle_dialog_click_at(col, row),
            _ => {
                if let AppMode::Dialog(ref mut state) = self.mode {
                    state.input.handle_action(&action);
                }
            }
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
                    if let Err(e) = self.remote_delete(path) {
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
                if dialog.input.text.is_empty() {
                    Ok(())
                } else if let Some(entry) = self.active_panel().selected_entry() {
                    let entry_path = entry.path.clone();
                    let new_name = dialog.input.text.clone();
                    let parent = entry_path.parent().unwrap_or(std::path::Path::new("/"));
                    self.remote_rename(&entry_path, &parent.join(&new_name))
                } else {
                    Ok(())
                }
            }
            DialogKind::InputCreateFile => {
                if dialog.input.text.is_empty() {
                    Ok(())
                } else {
                    let dir = self.active_panel().current_dir.clone();
                    let name = dialog.input.text.clone();
                    let path = dir.join(&name);
                    self.remote_create_file(&path)
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

    /// Close editor: if we came from a diff viewer, re-open it; otherwise go to Normal.
    fn restore_or_close_editor(&mut self) {
        if let Some(stash) = self.stashed_diff.take() {
            // Capture editor viewport offset before replacing mode
            let cursor_offset = if let AppMode::Editing(ref e) = self.mode {
                e.cursor_line.saturating_sub(e.scroll_y)
            } else {
                5 // fallback: show some context above
            };

            let mut dv = crate::diff_viewer::DiffViewerState::open(
                &stash.repo_root,
                &stash.file_path,
                &stash.base_branch,
            );
            dv.cursor = stash.cursor.min(dv.lines.len().saturating_sub(1));
            dv.scroll = dv.cursor.saturating_sub(cursor_offset);
            self.mode = AppMode::DiffViewing(Box::new(dv));
        } else {
            self.mode = AppMode::Normal;
            self.focus = PanelFocus::FilePanel;
        }
        self.needs_clear = true;
    }

    fn handle_dialog_cancel(&mut self) {
        match &self.mode {
            AppMode::Viewing(_)
            | AppMode::HexViewing(_)
            | AppMode::ParquetViewing(_)
            | AppMode::DiffViewing(_) => {
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
        state.input = TextInput::new("app".to_string());
        state.update_results();
        assert!(!state.results.is_empty());
        let top_path = &state.all_paths[state.results[0].0];
        assert!(
            top_path.contains("app"),
            "top result should contain 'app', got: {}",
            top_path
        );

        // Type "xyz" — should match nothing
        state.input = TextInput::new("xyz".to_string());
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
        state.input = TextInput::new("file".to_string());
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
        state.input = TextInput::new("rs".to_string());
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
            input: TextInput::new("/usr/lo".to_string()),
            completions: vec!["local".to_string()],
            comp_index: None,
            comp_base: None,
        };
        App::apply_completion(&mut state, "local");
        assert_eq!(state.input.text, "/usr/local/");
        assert_eq!(state.input.cursor, 11);
    }

    #[test]
    fn apply_completion_from_empty_prefix() {
        let mut state = GotoPathState {
            input: TextInput::new("/usr/".to_string()),
            completions: vec!["local".to_string()],
            comp_index: None,
            comp_base: None,
        };
        App::apply_completion(&mut state, "local");
        assert_eq!(state.input.text, "/usr/local/");
        assert_eq!(state.input.cursor, 11);
    }

    #[test]
    fn apply_common_prefix_extends() {
        let mut state = GotoPathState {
            input: TextInput::new("/usr/lo".to_string()),
            completions: vec!["local".to_string(), "locale".to_string()],
            comp_index: None,
            comp_base: None,
        };
        let applied = App::apply_common_prefix(&mut state);
        assert!(applied);
        assert_eq!(state.input.text, "/usr/local");
        assert_eq!(state.input.cursor, 10);
    }

    #[test]
    fn apply_common_prefix_no_extension() {
        let mut state = GotoPathState {
            input: TextInput::new("/usr/local".to_string()),
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
            input: TextInput::new("/usr/xyz".to_string()),
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
            input: TextInput::new("/usr/".to_string()),
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
            input: TextInput::new("/usr/lo".to_string()),
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
            input: TextInput::new("/usr/LO".to_string()),
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
            input: TextInput::new("/nonexistent_path_12345/".to_string()),
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
