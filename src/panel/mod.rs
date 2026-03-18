pub mod entry;
pub mod sort;

use std::path::PathBuf;

use ratatui::widgets::TableState;

use entry::FileEntry;
use sort::{sort_entries, SortField};

pub struct Panel {
    pub current_dir: PathBuf,
    pub entries: Vec<FileEntry>,
    pub table_state: TableState,
    pub sort_field: SortField,
    pub sort_ascending: bool,
    pub quick_search: Option<String>,
    pub error: Option<String>,
}

impl Panel {
    pub fn new(path: PathBuf) -> Self {
        let mut panel = Self {
            current_dir: path,
            entries: Vec::new(),
            table_state: TableState::default(),
            sort_field: SortField::Name,
            sort_ascending: true,
            quick_search: None,
            error: None,
        };
        panel.reload();
        if !panel.entries.is_empty() {
            panel.table_state.select(Some(0));
        }
        panel
    }

    pub fn reload(&mut self) {
        self.entries.clear();
        self.error = None;

        // Add parent directory entry
        if let Some(parent) = self.current_dir.parent() {
            self.entries
                .push(FileEntry::parent_entry(parent.to_path_buf()));
        }

        match std::fs::read_dir(&self.current_dir) {
            Ok(read_dir) => {
                for entry in read_dir.flatten() {
                    match FileEntry::from_dir_entry(&entry) {
                        Ok(fe) => self.entries.push(fe),
                        Err(_) => continue,
                    }
                }
            }
            Err(e) => {
                self.error = Some(format!("Cannot read directory: {}", e));
                return;
            }
        }

        self.apply_sort();
    }

    pub fn apply_sort(&mut self) {
        // Keep ".." at the top, sort the rest
        let has_parent = self
            .entries
            .first()
            .map(|e| e.name == "..")
            .unwrap_or(false);

        if has_parent && self.entries.len() > 1 {
            sort_entries(&mut self.entries[1..], self.sort_field, self.sort_ascending);
        } else {
            sort_entries(&mut self.entries, self.sort_field, self.sort_ascending);
        }
    }

    pub fn selected_entry(&self) -> Option<&FileEntry> {
        self.table_state
            .selected()
            .and_then(|i| self.entries.get(i))
    }

    pub fn selected_index(&self) -> usize {
        self.table_state.selected().unwrap_or(0)
    }

    pub fn move_selection(&mut self, delta: i32) {
        if self.entries.is_empty() {
            return;
        }
        let current = self.selected_index() as i32;
        let max = (self.entries.len() as i32) - 1;
        let new = (current + delta).clamp(0, max) as usize;
        self.table_state.select(Some(new));
    }

    pub fn move_to_top(&mut self) {
        if !self.entries.is_empty() {
            self.table_state.select(Some(0));
        }
    }

    pub fn move_to_bottom(&mut self) {
        if !self.entries.is_empty() {
            self.table_state.select(Some(self.entries.len() - 1));
        }
    }

    pub fn navigate_into(&mut self) -> bool {
        if let Some(entry) = self.selected_entry().cloned() {
            if entry.is_dir {
                if entry.name == ".." {
                    self.navigate_up();
                } else {
                    self.current_dir = entry.path;
                    self.reload();
                    self.table_state.select(Some(0));
                }
                return true;
            }
        }
        false
    }

    pub fn navigate_up(&mut self) {
        if let Some(parent) = self.current_dir.parent().map(|p| p.to_path_buf()) {
            let old_name = self
                .current_dir
                .file_name()
                .map(|n| n.to_string_lossy().into_owned());
            self.current_dir = parent;
            self.reload();

            // Try to select the directory we came from
            if let Some(name) = old_name {
                if let Some(idx) = self.entries.iter().position(|e| e.name == name) {
                    self.table_state.select(Some(idx));
                    return;
                }
            }
            self.table_state.select(Some(0));
        }
    }

    pub fn cycle_sort(&mut self) {
        self.sort_field = self.sort_field.next();
        self.apply_sort();
    }

    pub fn jump_to_match(&mut self, query: &str) {
        let query_lower = query.to_lowercase();
        if let Some(idx) = self
            .entries
            .iter()
            .position(|e| e.name.to_lowercase().starts_with(&query_lower))
        {
            self.table_state.select(Some(idx));
        }
    }
}
