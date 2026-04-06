pub mod entry;
pub mod git;
pub mod github;
pub mod sort;

use std::collections::BTreeSet;
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
    pub selected_indices: BTreeSet<usize>,
    pub git_info: Option<git::GitInfo>,
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
            selected_indices: BTreeSet::new(),
            git_info: None,
        };
        panel.reload();
        if !panel.entries.is_empty() {
            panel.table_state.select(Some(0));
        }
        panel
    }

    pub fn reload(&mut self) {
        // Preserve selection across reload by remembering selected paths
        let selected_paths: std::collections::HashSet<std::path::PathBuf> = self
            .selected_indices
            .iter()
            .filter_map(|&i| self.entries.get(i).map(|e| e.path.clone()))
            .collect();

        // Preserve the cursor position by name
        let cursor_name = self.selected_entry().map(|e| e.name.clone());

        self.entries.clear();
        self.selected_indices.clear();
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

        // Restore selection
        if !selected_paths.is_empty() {
            for (i, entry) in self.entries.iter().enumerate() {
                if selected_paths.contains(&entry.path) {
                    self.selected_indices.insert(i);
                }
            }
        }

        // Restore cursor position
        if let Some(name) = cursor_name {
            if let Some(idx) = self.entries.iter().position(|e| e.name == name) {
                self.table_state.select(Some(idx));
            }
        }
    }

    /// Refresh git info using the shared cache.
    pub fn refresh_git(&mut self, cache: &mut git::GitCache) {
        self.git_info = cache.get_info(&self.current_dir);
    }

    pub fn apply_sort(&mut self) {
        // Preserve selection across sort by converting to paths
        let selected_paths: std::collections::HashSet<PathBuf> = self
            .selected_indices
            .iter()
            .filter_map(|&i| self.entries.get(i).map(|e| e.path.clone()))
            .collect();

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

        // Rebuild selected indices from paths
        if !selected_paths.is_empty() {
            self.selected_indices.clear();
            for (i, entry) in self.entries.iter().enumerate() {
                if selected_paths.contains(&entry.path) {
                    self.selected_indices.insert(i);
                }
            }
        }
    }

    pub fn selected_entry(&self) -> Option<&FileEntry> {
        self.table_state
            .selected()
            .and_then(|i| self.entries.get(i))
    }

    /// Move cursor to the entry with the given name, if found.
    pub fn select_by_name(&mut self, name: &str) {
        if let Some(idx) = self.entries.iter().position(|e| e.name == name) {
            self.table_state.select(Some(idx));
        }
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

    /// Toggle selection on the current entry and move cursor down.
    pub fn toggle_select_current(&mut self) {
        let idx = self.selected_index();
        if idx < self.entries.len()
            && self.entries[idx].name != ".."
            && !self.selected_indices.remove(&idx)
        {
            self.selected_indices.insert(idx);
        }
        self.move_selection(1);
    }

    /// Toggle selection on current item, then move up.
    pub fn select_move_up(&mut self) {
        let idx = self.selected_index();
        if idx < self.entries.len()
            && self.entries[idx].name != ".."
            && !self.selected_indices.remove(&idx)
        {
            self.selected_indices.insert(idx);
        }
        self.move_selection(-1);
    }

    /// Toggle selection on current item, then move down.
    pub fn select_move_down(&mut self) {
        let idx = self.selected_index();
        if idx < self.entries.len()
            && self.entries[idx].name != ".."
            && !self.selected_indices.remove(&idx)
        {
            self.selected_indices.insert(idx);
        }
        self.move_selection(1);
    }

    /// Returns paths of all selected entries, or the single cursor entry if none selected.
    pub fn effective_selection_paths(&self) -> Vec<PathBuf> {
        if !self.selected_indices.is_empty() {
            self.selected_indices
                .iter()
                .filter_map(|&i| self.entries.get(i))
                .filter(|e| e.name != "..")
                .map(|e| e.path.clone())
                .collect()
        } else if let Some(entry) = self.selected_entry() {
            if entry.name == ".." {
                Vec::new()
            } else {
                vec![entry.path.clone()]
            }
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Helper: create a Panel rooted at `dir` with a few files.
    fn panel_with_files(names: &[&str]) -> (tempfile::TempDir, Panel) {
        let dir = tempdir().expect("failed to create temp dir");
        for name in names {
            fs::write(dir.path().join(name), "").expect("failed to create file");
        }
        let panel = Panel::new(dir.path().to_path_buf());
        (dir, panel)
    }

    #[test]
    fn select_by_name_finds_existing_entry() {
        let (_dir, mut panel) = panel_with_files(&["alpha.txt", "beta.txt", "gamma.txt"]);

        // Sanity: panel starts with selection at 0
        assert_eq!(panel.table_state.selected(), Some(0));

        // Select a file that definitely exists
        panel.select_by_name("beta.txt");

        let selected = panel
            .selected_entry()
            .expect("should have a selected entry");
        assert_eq!(selected.name, "beta.txt");

        // The underlying index must match the position in entries
        let expected_idx = panel
            .entries
            .iter()
            .position(|e| e.name == "beta.txt")
            .unwrap();
        assert_eq!(panel.table_state.selected(), Some(expected_idx));
    }

    #[test]
    fn select_by_name_nonexistent_leaves_selection_unchanged() {
        let (_dir, mut panel) = panel_with_files(&["alpha.txt", "beta.txt"]);

        // Move to a known position first
        panel.select_by_name("beta.txt");
        let before = panel.table_state.selected();

        // Try selecting something that does not exist
        panel.select_by_name("does_not_exist.txt");

        assert_eq!(
            panel.table_state.selected(),
            before,
            "selection should not change when name is not found"
        );
    }

    #[test]
    fn select_by_name_selects_parent_entry() {
        let (_dir, mut panel) = panel_with_files(&["file.txt"]);

        // The temp dir has a parent, so ".." should be present
        assert!(
            panel.entries.iter().any(|e| e.name == ".."),
            "panel should contain a '..' parent entry"
        );

        // Move away from ".." first
        panel.select_by_name("file.txt");
        assert_eq!(panel.selected_entry().unwrap().name, "file.txt");

        // Now select the parent entry
        panel.select_by_name("..");

        let selected = panel
            .selected_entry()
            .expect("should have a selected entry");
        assert_eq!(selected.name, "..");
        assert!(selected.is_dir);
    }
}
