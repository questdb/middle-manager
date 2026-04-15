pub mod entry;
pub mod git;
pub mod github;
pub mod sort;

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ratatui::widgets::TableState;

use entry::FileEntry;
use sort::{sort_entries, SortField};

/// The data source for a panel — local filesystem or remote.
pub enum PanelSource {
    Local,
    Remote {
        connection: Rc<dyn crate::remote_fs::RemoteFs>,
    },
}

impl PanelSource {
    pub fn is_remote(&self) -> bool {
        matches!(self, PanelSource::Remote { .. })
    }

    /// Display label for the panel header.
    pub fn label(&self) -> Option<String> {
        match self {
            PanelSource::Local => None,
            PanelSource::Remote { connection } => {
                Some(format!("{} [experimental]", connection.display_label()))
            }
        }
    }
}

/// State of a directory size calculation.
#[derive(Clone)]
pub enum DirSizeState {
    /// Background scan in progress. Accumulator holds bytes counted so far.
    Calculating {
        accumulator: Arc<AtomicU64>,
        finished: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
    },
    /// Scan complete.
    Done(u64),
}

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
    pub source: PanelSource,
    /// Calculated directory sizes (persists until panel reload).
    pub dir_sizes: HashMap<PathBuf, DirSizeState>,
    /// Pending F3 size total: (dir paths to wait for, file bytes already summed, file count, dir count).
    pub pending_size_total: Option<(Vec<PathBuf>, u64, usize, usize)>,
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
            source: PanelSource::Local,
            dir_sizes: HashMap::new(),
            pending_size_total: None,
        };
        panel.reload();
        if !panel.entries.is_empty() {
            panel.table_state.select(Some(0));
        }
        panel
    }

    /// Switch this panel to a remote source.
    pub fn switch_to_remote(&mut self, connection: Rc<dyn crate::remote_fs::RemoteFs>) {
        let home = connection.home_dir();
        self.source = PanelSource::Remote { connection };
        self.current_dir = home;
        self.git_info = None;
        self.reload();
        self.table_state.select(Some(0));
    }

    /// Switch this panel back to local filesystem.
    pub fn switch_to_local(&mut self, path: PathBuf) {
        self.source = PanelSource::Local;
        self.current_dir = path;
        self.reload();
        self.table_state.select(Some(0));
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
        self.cancel_size_calcs();
        self.dir_sizes.clear();
        self.pending_size_total = None;
        self.error = None;

        // Add parent directory entry
        if let Some(parent) = self.current_dir.parent() {
            self.entries
                .push(FileEntry::parent_entry(parent.to_path_buf()));
        }

        match &self.source {
            PanelSource::Local => match std::fs::read_dir(&self.current_dir) {
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
            },
            PanelSource::Remote { connection } => match connection.read_dir(&self.current_dir) {
                Ok(entries) => self.entries.extend(entries),
                Err(e) => {
                    self.error = Some(format!("Remote error: {}", e));
                    return;
                }
            },
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
            sort_entries(
                &mut self.entries[1..],
                self.sort_field,
                self.sort_ascending,
                &self.dir_sizes,
            );
        } else {
            sort_entries(
                &mut self.entries,
                self.sort_field,
                self.sort_ascending,
                &self.dir_sizes,
            );
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

    // --- Directory size calculation ---

    /// Start an async directory size calculation for the given path.
    /// Returns false if already calculating or done.
    pub fn start_size_calc(&mut self, path: PathBuf) -> bool {
        if self.dir_sizes.contains_key(&path) {
            return false;
        }
        let accumulator = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let acc = accumulator.clone();
        let fin = finished.clone();
        let can = cancelled.clone();
        let p = path.clone();
        std::thread::spawn(move || {
            calc_dir_size_recursive(&p, &acc, &can);
            fin.store(true, Ordering::Release);
        });
        self.dir_sizes.insert(
            path,
            DirSizeState::Calculating {
                accumulator,
                finished,
                cancelled,
            },
        );
        true
    }

    /// Signal all in-progress size calculations to stop.
    fn cancel_size_calcs(&self) {
        for state in self.dir_sizes.values() {
            if let DirSizeState::Calculating { cancelled, .. } = state {
                cancelled.store(true, Ordering::Release);
            }
        }
    }

    /// Poll in-progress calculations and promote finished ones to Done.
    /// Returns true if any calculation finished this tick.
    pub fn poll_dir_sizes(&mut self) -> bool {
        let mut newly_done = Vec::new();
        for (path, state) in &self.dir_sizes {
            if let DirSizeState::Calculating {
                accumulator,
                finished,
                ..
            } = state
            {
                if finished.load(Ordering::Acquire) {
                    newly_done.push((path.clone(), accumulator.load(Ordering::Relaxed)));
                }
            }
        }
        let changed = !newly_done.is_empty();
        for (path, size) in newly_done {
            self.dir_sizes.insert(path, DirSizeState::Done(size));
        }
        // Re-sort if sorting by size so newly calculated dirs move to correct position
        if changed && self.sort_field == SortField::Size {
            self.apply_sort();
        }
        changed
    }

    /// Get the display size for an entry, checking dir_sizes for overrides.
    pub fn display_size(&self, entry: &FileEntry) -> String {
        if entry.is_dir {
            if let Some(state) = self.dir_sizes.get(&entry.path) {
                return match state {
                    DirSizeState::Calculating { accumulator, .. } => {
                        let bytes = accumulator.load(Ordering::Relaxed);
                        if bytes == 0 {
                            "\u{25F7}".to_string() // spinner ◷
                        } else {
                            format!("\u{25F7}{}", format_size_short(bytes))
                        }
                    }
                    DirSizeState::Done(size) => format_size_short(*size),
                };
            }
            "<DIR>".to_string()
        } else {
            entry.formatted_size()
        }
    }

    /// Check if any size calculations are in progress.
    pub fn has_pending_size_calcs(&self) -> bool {
        self.dir_sizes
            .values()
            .any(|s| matches!(s, DirSizeState::Calculating { .. }))
    }

    /// Check if all dirs in the pending total are done. Returns the formatted total string.
    pub fn check_size_total(&mut self) -> Option<String> {
        let (dir_paths, file_bytes, file_count, dir_count) = self.pending_size_total.as_ref()?;
        // Check if all dir scans are done
        let mut dir_total: u64 = 0;
        for path in dir_paths {
            match self.dir_sizes.get(path) {
                Some(DirSizeState::Done(size)) => dir_total += size,
                _ => return None, // still calculating
            }
        }
        let total = file_bytes + dir_total;
        let item_count = file_count + dir_count;
        let msg = format!(
            "{} item{}: {} ({} file{}, {} dir{})",
            item_count,
            if item_count == 1 { "" } else { "s" },
            format_size_short(total),
            file_count,
            if *file_count == 1 { "" } else { "s" },
            dir_count,
            if *dir_count == 1 { "" } else { "s" },
        );
        self.pending_size_total = None;
        Some(msg)
    }
}

/// Recursively calculate directory size. Runs in a background thread.
/// Uses symlink_metadata to avoid following symlink cycles.
/// Checks `cancelled` flag to stop early on panel reload.
fn calc_dir_size_recursive(path: &std::path::Path, acc: &AtomicU64, cancelled: &AtomicBool) {
    if cancelled.load(Ordering::Relaxed) {
        return;
    }
    let entries = match std::fs::read_dir(path) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        if cancelled.load(Ordering::Relaxed) {
            return;
        }
        // Use symlink_metadata to avoid following symlinks (prevents cycles)
        let meta = match std::fs::symlink_metadata(entry.path()) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_symlink() {
            // Count symlink's target size but don't recurse into symlinked dirs
            if let Ok(target_meta) = entry.metadata() {
                if !target_meta.is_dir() {
                    acc.fetch_add(target_meta.len(), Ordering::Relaxed);
                }
            }
        } else if meta.is_dir() {
            calc_dir_size_recursive(&entry.path(), acc, cancelled);
        } else {
            acc.fetch_add(meta.len(), Ordering::Relaxed);
        }
    }
}

/// Format a byte size compactly for the 8-char size column.
pub fn format_size_short(size: u64) -> String {
    if size < 1024 {
        format!("{}", size)
    } else if size < 1024 * 1024 {
        format!("{}K", size / 1024)
    } else if size < 1024 * 1024 * 1024 {
        format!("{:.1}M", size as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}G", size as f64 / (1024.0 * 1024.0 * 1024.0))
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
