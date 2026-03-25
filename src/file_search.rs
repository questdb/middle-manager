//! Fast file content search using ripgrep's engine (ignore + grep-searcher + grep-regex).
//! Runs in a background thread, streams results via mpsc channel.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use grep_regex::RegexMatcher;
use grep_searcher::sinks::UTF8;
use grep_searcher::Searcher;
use ignore::WalkBuilder;

/// A single matching line in a file.
#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub path: PathBuf,
    pub line_number: u64,
    pub text: String,
}

/// Result state for the search panel.
pub struct SearchState {
    /// The query string.
    pub query: String,
    /// Receiver for streaming results from the background search.
    rx: mpsc::Receiver<SearchMatch>,
    /// All results received so far, grouped by file.
    pub files: Vec<FileMatches>,
    /// Fast lookup: path -> index in files vec.
    file_index: HashMap<PathBuf, usize>,
    /// Index into the flat list of visible items (files + matches).
    pub selected: usize,
    /// Scroll offset.
    pub scroll: usize,
    /// Whether the search is still running.
    pub searching: bool,
    /// Cancel flag shared with the background thread.
    cancel: Arc<AtomicBool>,
    /// Total match count.
    pub total_matches: usize,
    /// The directory we searched in.
    pub root: PathBuf,
}

/// Matches grouped under one file.
#[derive(Debug, Clone)]
pub struct FileMatches {
    pub path: PathBuf,
    /// Relative path for display.
    pub rel_path: String,
    pub matches: Vec<LineMatch>,
    pub expanded: bool,
}

#[derive(Debug, Clone)]
pub struct LineMatch {
    pub line_number: u64,
    pub text: String,
}

/// An item in the flat visible list (for navigation).
#[derive(Debug, Clone)]
pub enum SearchItem {
    File(usize),         // index into files
    Match(usize, usize), // (file_index, match_index)
}

impl SearchState {
    /// Start a new search. Spawns a background thread.
    /// `filter` is a glob pattern like "*.rs" (empty = all files).
    /// `is_regex` controls whether query is treated as regex or literal.
    pub fn new(root: PathBuf, query: String, filter: String, is_regex: bool) -> Self {
        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel.clone();
        let root_clone = root.clone();
        let query_clone = query.clone();
        let filter_clone = filter.clone();

        std::thread::spawn(move || {
            run_search(
                &root_clone,
                &query_clone,
                &filter_clone,
                is_regex,
                tx,
                cancel_clone,
            );
        });

        Self {
            query,
            rx,
            files: Vec::new(),
            file_index: HashMap::new(),
            selected: 0,
            scroll: 0,
            searching: true,
            cancel,
            total_matches: 0,
            root,
        }
    }

    /// Poll for new results from the background thread. Call on each tick.
    pub fn poll(&mut self) {
        let mut count = 0;
        while let Ok(m) = self.rx.try_recv() {
            self.add_match(m);
            count += 1;
            // Process in batches to avoid blocking the UI
            if count > 100 {
                break;
            }
        }
        if count == 0 && self.searching {
            // Check if channel is closed (search finished)
            match self.rx.try_recv() {
                Ok(m) => self.add_match(m), // don't drop a late arrival
                Err(mpsc::TryRecvError::Disconnected) => self.searching = false,
                Err(mpsc::TryRecvError::Empty) => {} // still running, no data yet
            }
        }
    }

    fn add_match(&mut self, m: SearchMatch) {
        self.total_matches += 1;

        // O(1) file group lookup via HashMap
        let file_idx = if let Some(&idx) = self.file_index.get(&m.path) {
            idx
        } else {
            let rel_path = m
                .path
                .strip_prefix(&self.root)
                .unwrap_or(&m.path)
                .to_string_lossy()
                .to_string();
            let idx = self.files.len();
            self.file_index.insert(m.path.clone(), idx);
            self.files.push(FileMatches {
                path: m.path,
                rel_path,
                matches: Vec::new(),
                expanded: true,
            });
            idx
        };

        self.files[file_idx].matches.push(LineMatch {
            line_number: m.line_number,
            text: m.text.trim_end().to_string(),
        });
    }

    /// Build the flat visible item list for navigation.
    pub fn visible_items(&self) -> Vec<SearchItem> {
        let mut items = Vec::new();
        for (fi, file) in self.files.iter().enumerate() {
            items.push(SearchItem::File(fi));
            if file.expanded {
                for (mi, _) in file.matches.iter().enumerate() {
                    items.push(SearchItem::Match(fi, mi));
                }
            }
        }
        items
    }

    /// Total number of visible items.
    pub fn visible_count(&self) -> usize {
        self.files
            .iter()
            .map(|f| 1 + if f.expanded { f.matches.len() } else { 0 })
            .sum()
    }

    pub fn move_up(&mut self) {
        let count = self.visible_count();
        if count > 0 && self.selected > 0 {
            self.selected -= 1;
        }
    }

    pub fn move_down(&mut self) {
        let count = self.visible_count();
        if count > 0 && self.selected + 1 < count {
            self.selected += 1;
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        self.selected = self.selected.saturating_sub(page_size);
    }

    pub fn page_down(&mut self, page_size: usize) {
        let count = self.visible_count();
        self.selected = (self.selected + page_size).min(count.saturating_sub(1));
    }

    /// Toggle expand/collapse on a file entry.
    /// Get the file path and line number for the selected match.
    pub fn selected_location(&self) -> Option<(PathBuf, u64)> {
        let items = self.visible_items();
        match items.get(self.selected)? {
            SearchItem::Match(fi, mi) => {
                let file = &self.files[*fi];
                let line = &file.matches[*mi];
                Some((file.path.clone(), line.line_number))
            }
            SearchItem::File(fi) => {
                // Enter on a file header: open first match
                let file = &self.files[*fi];
                file.matches
                    .first()
                    .map(|m| (file.path.clone(), m.line_number))
            }
        }
    }

    /// Ensure the selected item is visible by adjusting scroll.
    pub fn scroll_to_selected(&mut self, visible_height: usize) {
        if visible_height == 0 {
            return;
        }
        if self.selected < self.scroll {
            self.scroll = self.selected;
        } else if self.selected >= self.scroll + visible_height {
            self.scroll = self.selected - visible_height + 1;
        }
    }
}

impl Drop for SearchState {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

/// Run the search in a background thread.
fn run_search(
    root: &Path,
    query: &str,
    filter: &str,
    is_regex: bool,
    tx: mpsc::Sender<SearchMatch>,
    cancel: Arc<AtomicBool>,
) {
    let pattern = if is_regex {
        query.to_string()
    } else {
        // Escape regex metacharacters for literal search
        query
            .chars()
            .flat_map(|c| {
                if "\\[](){}*+?|^$.".contains(c) {
                    vec!['\\', c]
                } else {
                    vec![c]
                }
            })
            .collect()
    };

    let matcher = match RegexMatcher::new_line_matcher(&pattern) {
        Ok(m) => m,
        Err(_) => return,
    };

    let mut searcher = Searcher::new();

    let mut walker_builder = WalkBuilder::new(root);
    walker_builder
        .hidden(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true);

    // Apply file filter glob if provided
    if !filter.is_empty() {
        let mut ob = ignore::overrides::OverrideBuilder::new(root);
        if ob.add(filter).is_ok() {
            if let Ok(glob) = ob.build() {
                walker_builder.overrides(glob);
            }
        }
    }

    let walker = walker_builder.build();

    for entry in walker {
        if cancel.load(Ordering::Relaxed) {
            return;
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        // Skip directories and non-files
        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }

        let path = entry.path().to_path_buf();

        let tx_clone = tx.clone();
        let _ = searcher.search_path(
            &matcher,
            &path,
            UTF8(|line_number, line| {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(false); // stop searching this file
                }
                let _ = tx_clone.send(SearchMatch {
                    path: path.clone(),
                    line_number,
                    text: line.to_string(),
                });
                Ok(true)
            }),
        );
    }
}
