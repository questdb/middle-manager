use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

const DOUBLE_CLICK_MS: u128 = 400;

use crate::action::Action;
use crate::editor::EditorState;
use crate::fs_ops;
use crate::hex_viewer::HexViewerState;
use crate::panel::Panel;
use crate::viewer::ViewerState;

pub struct App {
    pub panels: [Panel; 2],
    pub active_panel: usize,
    pub mode: AppMode,
    pub should_quit: bool,
    pub status_message: Option<String>,
    pub panel_areas: [Rect; 2],
    last_click: Option<(Instant, u16, u16)>,
    /// Go-to-line prompt state. When Some, an input overlay is shown.
    pub goto_line_input: Option<String>,
    /// Set to true when the UI needs a full terminal clear (e.g. leaving full-screen mode).
    pub needs_clear: bool,
    /// Search dialog overlay (shown on top of editor).
    pub search_dialog: Option<SearchDialogState>,
}

#[derive(Clone)]
pub enum AppMode {
    Normal,
    QuickSearch,
    Dialog(DialogState),
    MkdirDialog(MkdirDialogState),
    CopyDialog(CopyDialogState),
    Viewing(ViewerState),
    HexViewing(HexViewerState),
    Editing(EditorState),
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
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        Self {
            panels: [Panel::new(cwd.clone()), Panel::new(cwd)],
            active_panel: 0,
            mode: AppMode::Normal,
            should_quit: false,
            status_message: None,
            panel_areas: [Rect::default(); 2],
            last_click: None,
            goto_line_input: None,
            needs_clear: false,
            search_dialog: None,
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
            KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,
            KeyCode::Char(c) if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' => {
                Action::QuickSearch(c)
            }
            KeyCode::Esc => Action::Quit,
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
                KeyCode::Char('k') => Action::EditorDeleteLine,
                KeyCode::Char('c') => Action::CopySelection,
                KeyCode::Char('a') => Action::SelectAll,
                KeyCode::Char('f') => Action::SearchPrompt,
                KeyCode::Char('g') => Action::GotoLinePrompt,
                KeyCode::Char('q') => Action::DialogCancel,
                KeyCode::Home => Action::MoveToTop,
                KeyCode::End => Action::MoveToBottom,
                _ => Action::None,
            };
        }

        match key.code {
            KeyCode::F(7) if shift => Action::FindNext,
            KeyCode::F(7) => Action::SearchPrompt,
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
        // Go-to-line prompt intercepts all input when active
        if self.goto_line_input.is_some() {
            self.handle_goto_line_action(action);
            return;
        }

        // Search dialog overlay intercepts when active
        if self.search_dialog.is_some() {
            self.handle_search_dialog_action(action);
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
            Action::None | Action::Tick => {}
            Action::Quit => self.should_quit = true,
            Action::Resize(_, _) => {}
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

            // File operations
            Action::Copy => self.handle_copy(),
            Action::Move => self.handle_move(),
            Action::Rename => self.handle_rename(),
            Action::CreateDir => self.handle_create_dir(),
            Action::Delete => self.handle_delete(),
            Action::ViewFile => self.handle_view_file(),
            Action::EditFile => self.handle_edit_file(),

            // Sorting
            Action::CycleSort => {
                self.active_panel_mut().cycle_sort();
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
            } else {
                self.open_file(entry.path);
            }
        }
    }

    fn handle_go_up(&mut self) {
        self.active_panel_mut().navigate_up();
    }

    fn handle_switch_panel(&mut self) {
        self.active_panel = 1 - self.active_panel;
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
                self.mode = AppMode::Editing(EditorState::open(entry.path));
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
            Action::Quit => self.should_quit = true,
            Action::GotoLinePrompt => {
                self.goto_line_input = Some(String::new());
            }
            Action::DialogCancel => {
                self.mode = AppMode::Normal;
                self.needs_clear = true;
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
            Action::EditorSave => {
                if let AppMode::Editing(ref mut e) = self.mode {
                    match e.save() {
                        Ok(()) => {}
                        Err(err) => e.status_msg = Some(format!("Save failed: {}", err)),
                    }
                }
                // Reload panels after save
                self.panels[0].reload();
                self.panels[1].reload();
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

            _ => {}
        }
    }

    fn handle_toggle_viewer(&mut self) {
        match &self.mode {
            AppMode::Viewing(v) => {
                let path = v.path.clone();
                self.mode = AppMode::HexViewing(HexViewerState::open(path));
            }
            AppMode::HexViewing(h) => {
                let path = h.path.clone();
                self.mode = AppMode::Viewing(ViewerState::open(path));
            }
            _ => {}
        }
    }

    fn open_file(&mut self, path: PathBuf) {
        if HexViewerState::is_binary(&path) {
            self.mode = AppMode::HexViewing(HexViewerState::open(path));
        } else {
            self.mode = AppMode::Viewing(ViewerState::open(path));
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
            Action::SwitchPanel => {
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

        if let AppMode::Editing(ref mut e) = self.mode {
            e.last_search = Some(params.clone());
            if !e.find(&params) {
                e.status_msg = Some(format!("'{}' not found", params.query));
            }
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
            Action::SwitchPanel => {
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
            input.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect()
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
        self.panels[0].reload();
        self.panels[1].reload();
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
            Action::SwitchPanel => {
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
        self.panels[0].reload();
        self.panels[1].reload();
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
        if matches!(
            self.mode,
            AppMode::Viewing(_) | AppMode::HexViewing(_) | AppMode::Editing(_)
        ) {
            return;
        }
        if matches!(
            self.mode,
            AppMode::Dialog(_) | AppMode::MkdirDialog(_) | AppMode::CopyDialog(_)
        ) {
            return;
        }

        if let Some(panel_idx) = self.panel_at(col, row) {
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
            Action::SwitchPanel => {
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
        self.panels[0].reload();
        self.panels[1].reload();
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
