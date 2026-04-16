use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiffFileStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
}

impl DiffFileStatus {
    pub fn marker(&self) -> &'static str {
        match self {
            Self::Added => "+",
            Self::Modified => "\u{25CF}", // ●
            Self::Deleted => "\u{2212}",  // −
            Self::Renamed => "\u{2192}",  // →
            Self::Copied => "C",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiffFile {
    pub path: String,
    pub status: DiffFileStatus,
}

#[derive(Debug, Clone)]
pub enum DiffTreeItem {
    Dir {
        name: String,
        path: String,
        depth: usize,
        expanded: bool,
    },
    File {
        name: String,
        path: String,
        status: DiffFileStatus,
        depth: usize,
    },
}

pub enum DiffView {
    Tree {
        files: Vec<DiffFile>,
        collapsed: HashSet<String>,
        items: Vec<DiffTreeItem>,
        selected: usize,
        scroll: usize,
    },
    Loading(String),
    Error(String),
}

type DiffReceiver = std::sync::mpsc::Receiver<Result<PrDiffData, String>>;

pub struct PrDiffPanel {
    pub view: DiffView,
    pub pr_number: Option<u64>,
    pub base_branch: String,
    pending: Option<DiffReceiver>,
    pub spinner_tick: usize,
    pub visible_height: usize,
    pub repo_root: PathBuf,
    /// Quick search filter — typed chars jump to matching file.
    pub quick_search: Option<String>,
}

struct PrDiffData {
    pr_number: Option<u64>,
    base_branch: String,
    files: Vec<DiffFile>,
}

impl PrDiffPanel {
    pub fn for_branch(dir: &Path) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = dir.to_path_buf();
        let repo_root = dir.clone();
        std::thread::spawn(move || {
            let result = query_pr_diff(&dir);
            let _ = tx.send(result);
        });

        Self {
            view: DiffView::Loading("Fetching PR diff...".to_string()),
            pr_number: None,
            base_branch: String::new(),
            pending: Some(rx),
            spinner_tick: 0,
            visible_height: 0,
            repo_root,
            quick_search: None,
        }
    }

    pub fn poll(&mut self) {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);

        if let Some(ref rx) = self.pending {
            if let Ok(result) = rx.try_recv() {
                self.pending = None;
                match result {
                    Ok(data) => {
                        self.pr_number = data.pr_number;
                        self.base_branch = data.base_branch;
                        let items = build_tree_items(&data.files, &HashSet::new());
                        self.view = DiffView::Tree {
                            files: data.files,
                            collapsed: HashSet::new(),
                            items,
                            selected: 0,
                            scroll: 0,
                        };
                    }
                    Err(e) => {
                        self.view = DiffView::Error(e);
                    }
                }
            } else {
                let c = SPINNER[self.spinner_tick % SPINNER.len()];
                self.view = DiffView::Loading(format!("{} Fetching PR diff...", c));
            }
        }
    }

    pub fn status_counts(&self) -> (usize, usize, usize) {
        match &self.view {
            DiffView::Tree { files, .. } => {
                let (mut added, mut modified, mut deleted) = (0, 0, 0);
                for f in files {
                    match f.status {
                        DiffFileStatus::Added => added += 1,
                        DiffFileStatus::Modified => modified += 1,
                        DiffFileStatus::Deleted => deleted += 1,
                        _ => {}
                    }
                }
                (added, modified, deleted)
            }
            _ => (0, 0, 0),
        }
    }

    pub fn enter(&mut self) -> Option<PathBuf> {
        if let DiffView::Tree {
            files,
            collapsed,
            items,
            selected,
            ..
        } = &mut self.view
        {
            let sel = *selected;
            match items.get(sel) {
                Some(DiffTreeItem::Dir { path, expanded, .. }) => {
                    let path = path.clone();
                    if *expanded {
                        collapsed.insert(path.clone());
                    } else {
                        collapsed.remove(&path);
                    }
                    *items = build_tree_items(files, collapsed);
                    // Try to keep selection on the same dir
                    *selected = items
                        .iter()
                        .position(|item| match item {
                            DiffTreeItem::Dir { path: p, .. } => *p == path,
                            _ => false,
                        })
                        .unwrap_or(sel.min(items.len().saturating_sub(1)));
                    None
                }
                Some(DiffTreeItem::File { path, status, .. }) => {
                    if *status == DiffFileStatus::Deleted {
                        None // Can't open deleted files
                    } else {
                        Some(self.repo_root.join(path))
                    }
                }
                None => None,
            }
        } else {
            None
        }
    }

    pub fn collapse_or_parent(&mut self) {
        if let DiffView::Tree {
            files,
            collapsed,
            items,
            selected,
            scroll,
            ..
        } = &mut self.view
        {
            let sel = *selected;
            // Extract info before mutating
            let (is_dir, is_expanded, dir_path, depth) = match &items[sel] {
                DiffTreeItem::Dir {
                    path,
                    expanded,
                    depth,
                    ..
                } => (true, *expanded, Some(path.clone()), *depth),
                DiffTreeItem::File { depth, .. } => (false, false, None, *depth),
            };

            if is_dir && is_expanded {
                // Collapse expanded dir
                let path = dir_path.unwrap();
                collapsed.insert(path.clone());
                *items = build_tree_items(files, collapsed);
                *selected = items
                    .iter()
                    .position(|item| match item {
                        DiffTreeItem::Dir { path: p, .. } => *p == path,
                        _ => false,
                    })
                    .unwrap_or(sel.min(items.len().saturating_sub(1)));
            } else if depth > 0 {
                // Collapsed dir or file: jump to parent dir
                for i in (0..sel).rev() {
                    if let DiffTreeItem::Dir { depth: d, .. } = &items[i] {
                        if *d < depth {
                            *selected = i;
                            if *selected < *scroll {
                                *scroll = *selected;
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    pub fn move_up(&mut self) {
        if let DiffView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            if *selected > 0 {
                *selected -= 1;
                if *selected < *scroll {
                    *scroll = *selected;
                }
            }
        }
    }

    pub fn move_down(&mut self) {
        let vh = self.visible_height;
        if let DiffView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            if *selected + 1 < items.len() {
                *selected += 1;
                if vh > 0 && *selected >= *scroll + vh {
                    *scroll = *selected - vh + 1;
                }
            }
        }
    }

    pub fn page_up(&mut self) {
        let vh = self.visible_height.max(1);
        if let DiffView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            *selected = selected.saturating_sub(vh);
            if *selected < *scroll {
                *scroll = *selected;
            }
        }
    }

    pub fn page_down(&mut self) {
        let vh = self.visible_height.max(1);
        if let DiffView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            let max = items.len().saturating_sub(1);
            *selected = (*selected + vh).min(max);
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }

    pub fn move_to_top(&mut self) {
        if let DiffView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            *selected = 0;
            *scroll = 0;
        }
    }

    pub fn move_to_bottom(&mut self) {
        let vh = self.visible_height;
        if let DiffView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            let max = items.len().saturating_sub(1);
            *selected = max;
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }

    /// Jump to the first file/dir whose name matches the query (case-insensitive prefix).
    pub fn jump_to_match(&mut self, query: &str) {
        self.jump_to_match_from(query, 0);
    }

    /// Jump to next item matching the current quick search query.
    pub fn jump_to_next_match(&mut self) {
        if let Some(query) = self.quick_search.clone() {
            self.jump_to_match_from(&query, 1);
        }
    }

    /// Jump to previous item matching the current quick search query.
    pub fn jump_to_prev_match(&mut self) {
        let query = match self.quick_search {
            Some(ref q) => q.to_lowercase(),
            None => return,
        };
        {
            let vh = self.visible_height;
            if let DiffView::Tree {
                items,
                selected,
                scroll,
                ..
            } = &mut self.view
            {
                let len = items.len();
                for offset in 1..=len {
                    let idx = (*selected + len - offset) % len;
                    let name = match &items[idx] {
                        DiffTreeItem::Dir { name, .. } | DiffTreeItem::File { name, .. } => name,
                    };
                    if name.to_lowercase().starts_with(&query) {
                        *selected = idx;
                        if *selected < *scroll {
                            *scroll = *selected;
                        }
                        if vh > 0 && *selected >= *scroll + vh {
                            *scroll = *selected - vh + 1;
                        }
                        return;
                    }
                }
            }
        }
    }

    fn jump_to_match_from(&mut self, query: &str, start_offset: usize) {
        let vh = self.visible_height;
        if let DiffView::Tree {
            items,
            selected,
            scroll,
            ..
        } = &mut self.view
        {
            let query_lower = query.to_lowercase();
            let len = items.len();
            for offset in start_offset..start_offset + len {
                let idx = (*selected + offset) % len;
                let name = match &items[idx] {
                    DiffTreeItem::Dir { name, .. } | DiffTreeItem::File { name, .. } => name,
                };
                if name.to_lowercase().starts_with(&query_lower) {
                    *selected = idx;
                    if *selected < *scroll {
                        *scroll = *selected;
                    }
                    if vh > 0 && *selected >= *scroll + vh {
                        *scroll = *selected - vh + 1;
                    }
                    return;
                }
            }
        }
    }
}

/// Build a flat list of tree items from sorted files, respecting collapsed dirs.
fn build_tree_items(files: &[DiffFile], collapsed: &HashSet<String>) -> Vec<DiffTreeItem> {
    let mut items = Vec::new();
    let mut emitted_dirs: HashSet<String> = HashSet::new();

    for file in files {
        let parts: Vec<&str> = file.path.split('/').collect();
        let dir_parts = &parts[..parts.len() - 1];
        let file_name = parts[parts.len() - 1];

        // Check if any ancestor dir is collapsed — skip this file if so
        let mut skip = false;
        let mut ancestor = String::new();
        for (i, part) in dir_parts.iter().enumerate() {
            if i > 0 {
                ancestor.push('/');
            }
            ancestor.push_str(part);
            if collapsed.contains(&ancestor) {
                skip = true;
                // Still need to ensure the collapsed dir itself is emitted
                emit_dirs_up_to(dir_parts, i + 1, collapsed, &mut emitted_dirs, &mut items);
                break;
            }
        }
        if skip {
            continue;
        }

        // Emit any missing directory entries
        emit_dirs_up_to(
            dir_parts,
            dir_parts.len(),
            collapsed,
            &mut emitted_dirs,
            &mut items,
        );

        // Emit the file
        items.push(DiffTreeItem::File {
            name: file_name.to_string(),
            path: file.path.clone(),
            status: file.status,
            depth: dir_parts.len(),
        });
    }

    items
}

fn emit_dirs_up_to(
    dir_parts: &[&str],
    up_to: usize,
    collapsed: &HashSet<String>,
    emitted_dirs: &mut HashSet<String>,
    items: &mut Vec<DiffTreeItem>,
) {
    let mut path = String::new();
    for (i, part) in dir_parts.iter().take(up_to).enumerate() {
        if i > 0 {
            path.push('/');
        }
        path.push_str(part);

        if emitted_dirs.contains(&path) {
            // Check if this dir is collapsed — if so, don't emit deeper levels
            if collapsed.contains(&path) {
                return;
            }
            continue;
        }

        let expanded = !collapsed.contains(&path);
        emitted_dirs.insert(path.clone());
        items.push(DiffTreeItem::Dir {
            name: part.to_string(),
            path: path.clone(),
            depth: i,
            expanded,
        });

        if !expanded {
            return;
        }
    }
}

/// Query PR changed files via gh + git.
fn query_pr_diff(dir: &Path) -> Result<PrDiffData, String> {
    // Step 1: Get PR info (number + base branch)
    let output = Command::new("gh")
        .args(["pr", "view", "--json", "number,baseRefName"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to run gh: {}", e))?;

    if !output.status.success() {
        return Err("No PR found for this branch".to_string());
    }

    #[derive(Deserialize)]
    struct PrInfo {
        number: Option<u64>,
        #[serde(rename = "baseRefName")]
        base_ref_name: String,
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let pr: PrInfo =
        serde_json::from_str(&text).map_err(|e| format!("Failed to parse PR info: {}", e))?;

    // Step 2: Fetch the base branch so the merge-base is up to date
    let _ = Command::new("git")
        .args(["fetch", "origin", &pr.base_ref_name])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output();

    // Step 3: Get file changes via git diff --name-status
    let diff_ref = format!("origin/{}...HEAD", pr.base_ref_name);
    let output = Command::new("git")
        .args(["diff", "--name-status", &diff_ref])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to run git diff: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "git diff failed — is origin/{} available?",
            pr.base_ref_name
        ));
    }

    let diff_text = String::from_utf8_lossy(&output.stdout);
    let mut files: Vec<DiffFile> = diff_text
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                return None;
            }
            let status_char = parts[0].chars().next()?;
            let status = match status_char {
                'A' => DiffFileStatus::Added,
                'M' => DiffFileStatus::Modified,
                'D' => DiffFileStatus::Deleted,
                'R' => DiffFileStatus::Renamed,
                'C' => DiffFileStatus::Copied,
                _ => DiffFileStatus::Modified,
            };
            // For renames/copies, the new path is the last column
            let path = parts.last()?.to_string();
            Some(DiffFile { path, status })
        })
        .collect();

    files.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(PrDiffData {
        pr_number: pr.number,
        base_branch: pr.base_ref_name,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_files(paths: &[(&str, DiffFileStatus)]) -> Vec<DiffFile> {
        paths
            .iter()
            .map(|(p, s)| DiffFile {
                path: p.to_string(),
                status: *s,
            })
            .collect()
    }

    fn item_names(items: &[DiffTreeItem]) -> Vec<String> {
        items
            .iter()
            .map(|item| match item {
                DiffTreeItem::Dir { name, .. } => format!("{}/", name),
                DiffTreeItem::File { name, .. } => name.clone(),
            })
            .collect()
    }

    fn item_depths(items: &[DiffTreeItem]) -> Vec<usize> {
        items
            .iter()
            .map(|item| match item {
                DiffTreeItem::Dir { depth, .. } | DiffTreeItem::File { depth, .. } => *depth,
            })
            .collect()
    }

    // --- build_tree_items tests ---

    #[test]
    fn tree_flat_files() {
        let files = make_files(&[
            ("README.md", DiffFileStatus::Added),
            ("main.rs", DiffFileStatus::Modified),
        ]);
        let items = build_tree_items(&files, &HashSet::new());
        assert_eq!(item_names(&items), vec!["README.md", "main.rs"]);
        assert_eq!(item_depths(&items), vec![0, 0]);
    }

    #[test]
    fn tree_single_dir() {
        let files = make_files(&[
            ("src/app.rs", DiffFileStatus::Modified),
            ("src/main.rs", DiffFileStatus::Added),
        ]);
        let items = build_tree_items(&files, &HashSet::new());
        assert_eq!(item_names(&items), vec!["src/", "app.rs", "main.rs"]);
        assert_eq!(item_depths(&items), vec![0, 1, 1]);
    }

    #[test]
    fn tree_nested_dirs() {
        let files = make_files(&[
            ("src/ui/diff_view.rs", DiffFileStatus::Added),
            ("src/ui/mod.rs", DiffFileStatus::Modified),
        ]);
        let items = build_tree_items(&files, &HashSet::new());
        assert_eq!(
            item_names(&items),
            vec!["src/", "ui/", "diff_view.rs", "mod.rs"]
        );
        assert_eq!(item_depths(&items), vec![0, 1, 2, 2]);
    }

    #[test]
    fn tree_mixed_root_and_nested() {
        let files = make_files(&[
            ("Cargo.toml", DiffFileStatus::Modified),
            ("src/app.rs", DiffFileStatus::Modified),
            ("src/ui/mod.rs", DiffFileStatus::Added),
        ]);
        let items = build_tree_items(&files, &HashSet::new());
        assert_eq!(
            item_names(&items),
            vec!["Cargo.toml", "src/", "app.rs", "ui/", "mod.rs"]
        );
        assert_eq!(item_depths(&items), vec![0, 0, 1, 1, 2]);
    }

    #[test]
    fn tree_sibling_dirs() {
        let files = make_files(&[
            ("src/a/file.rs", DiffFileStatus::Added),
            ("src/b/file.rs", DiffFileStatus::Modified),
        ]);
        let items = build_tree_items(&files, &HashSet::new());
        assert_eq!(
            item_names(&items),
            vec!["src/", "a/", "file.rs", "b/", "file.rs"]
        );
        assert_eq!(item_depths(&items), vec![0, 1, 2, 1, 2]);
    }

    #[test]
    fn tree_collapse_hides_children() {
        let files = make_files(&[
            ("src/app.rs", DiffFileStatus::Modified),
            ("src/ui/mod.rs", DiffFileStatus::Added),
            ("test.rs", DiffFileStatus::Added),
        ]);
        let mut collapsed = HashSet::new();
        collapsed.insert("src".to_string());
        let items = build_tree_items(&files, &collapsed);
        assert_eq!(item_names(&items), vec!["src/", "test.rs"]);
        // The collapsed dir should be marked as not expanded
        if let DiffTreeItem::Dir { expanded, .. } = &items[0] {
            assert!(!expanded);
        } else {
            panic!("expected Dir");
        }
    }

    #[test]
    fn tree_collapse_nested_dir() {
        let files = make_files(&[
            ("src/app.rs", DiffFileStatus::Modified),
            ("src/ui/a.rs", DiffFileStatus::Added),
            ("src/ui/b.rs", DiffFileStatus::Added),
        ]);
        let mut collapsed = HashSet::new();
        collapsed.insert("src/ui".to_string());
        let items = build_tree_items(&files, &collapsed);
        assert_eq!(item_names(&items), vec!["src/", "app.rs", "ui/"]);
        assert_eq!(item_depths(&items), vec![0, 1, 1]);
    }

    #[test]
    fn tree_empty_files() {
        let items = build_tree_items(&[], &HashSet::new());
        assert!(items.is_empty());
    }

    // --- DiffFileStatus tests ---

    #[test]
    fn status_markers() {
        assert_eq!(DiffFileStatus::Added.marker(), "+");
        assert_eq!(DiffFileStatus::Modified.marker(), "\u{25CF}");
        assert_eq!(DiffFileStatus::Deleted.marker(), "\u{2212}");
        assert_eq!(DiffFileStatus::Renamed.marker(), "\u{2192}");
    }

    // --- Navigation tests ---

    fn make_panel_with_files(paths: &[(&str, DiffFileStatus)]) -> PrDiffPanel {
        let files = make_files(paths);
        let items = build_tree_items(&files, &HashSet::new());
        PrDiffPanel {
            view: DiffView::Tree {
                files: files.clone(),
                collapsed: HashSet::new(),
                items,
                selected: 0,
                scroll: 0,
            },
            pr_number: None,
            base_branch: String::new(),
            pending: None,
            spinner_tick: 0,
            visible_height: 10,
            repo_root: PathBuf::from("/tmp"),
            quick_search: None,
        }
    }

    fn selected(panel: &PrDiffPanel) -> usize {
        match &panel.view {
            DiffView::Tree { selected, .. } => *selected,
            _ => panic!("not a tree"),
        }
    }

    fn scroll(panel: &PrDiffPanel) -> usize {
        match &panel.view {
            DiffView::Tree { scroll, .. } => *scroll,
            _ => panic!("not a tree"),
        }
    }

    fn item_count(panel: &PrDiffPanel) -> usize {
        match &panel.view {
            DiffView::Tree { items, .. } => items.len(),
            _ => 0,
        }
    }

    #[test]
    fn nav_move_down_and_up() {
        let mut p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Modified),
        ]);
        assert_eq!(selected(&p), 0);
        p.move_down();
        assert_eq!(selected(&p), 1);
        p.move_down(); // at end, shouldn't go further
        assert_eq!(selected(&p), 1);
        p.move_up();
        assert_eq!(selected(&p), 0);
        p.move_up(); // at start, shouldn't go further
        assert_eq!(selected(&p), 0);
    }

    #[test]
    fn nav_move_to_top_bottom() {
        let mut p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Modified),
            ("c.rs", DiffFileStatus::Deleted),
        ]);
        p.move_to_bottom();
        assert_eq!(selected(&p), 2);
        p.move_to_top();
        assert_eq!(selected(&p), 0);
    }

    #[test]
    fn nav_page_down_clamps() {
        let mut p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Modified),
        ]);
        p.visible_height = 10;
        p.page_down();
        assert_eq!(selected(&p), 1); // clamped to last item
    }

    #[test]
    fn nav_scroll_follows_selection() {
        let mut p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Modified),
            ("c.rs", DiffFileStatus::Deleted),
            ("d.rs", DiffFileStatus::Added),
            ("e.rs", DiffFileStatus::Modified),
        ]);
        p.visible_height = 2;
        p.move_down();
        p.move_down();
        assert_eq!(selected(&p), 2);
        assert_eq!(scroll(&p), 1); // scrolled to keep selection visible
    }

    // --- Expand/collapse tests ---

    #[test]
    fn enter_on_dir_collapses() {
        let mut p = make_panel_with_files(&[
            ("src/a.rs", DiffFileStatus::Added),
            ("src/b.rs", DiffFileStatus::Modified),
        ]);
        // items: [src/, a.rs, b.rs] — selected=0 (src/ expanded)
        assert_eq!(item_count(&p), 3);
        p.enter(); // collapse src/
        assert_eq!(item_count(&p), 1); // just src/ collapsed
    }

    #[test]
    fn enter_on_dir_re_expands() {
        let mut p = make_panel_with_files(&[
            ("src/a.rs", DiffFileStatus::Added),
            ("src/b.rs", DiffFileStatus::Modified),
        ]);
        p.enter(); // collapse
        assert_eq!(item_count(&p), 1);
        p.enter(); // expand
        assert_eq!(item_count(&p), 3);
    }

    #[test]
    fn collapse_or_parent_collapses_expanded_dir() {
        let mut p = make_panel_with_files(&[
            ("src/a.rs", DiffFileStatus::Added),
            ("src/b.rs", DiffFileStatus::Modified),
        ]);
        assert_eq!(item_count(&p), 3);
        p.collapse_or_parent(); // selected=0 (src/ expanded) → collapse
        assert_eq!(item_count(&p), 1);
    }

    #[test]
    fn collapse_or_parent_jumps_to_parent_from_file() {
        let mut p = make_panel_with_files(&[
            ("src/a.rs", DiffFileStatus::Added),
            ("src/b.rs", DiffFileStatus::Modified),
        ]);
        p.move_down(); // select a.rs (index 1)
        assert_eq!(selected(&p), 1);
        p.collapse_or_parent(); // should jump to parent dir (index 0)
        assert_eq!(selected(&p), 0);
    }

    // --- status_counts tests ---

    #[test]
    fn status_counts_correct() {
        let p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Added),
            ("c.rs", DiffFileStatus::Modified),
            ("d.rs", DiffFileStatus::Deleted),
        ]);
        assert_eq!(p.status_counts(), (2, 1, 1));
    }

    // --- Quick search tests ---

    #[test]
    fn quick_search_finds_file() {
        let mut p = make_panel_with_files(&[
            ("src/app.rs", DiffFileStatus::Modified),
            ("src/main.rs", DiffFileStatus::Modified),
        ]);
        // items: [src/, app.rs, main.rs]
        p.jump_to_match("main");
        assert_eq!(selected(&p), 2); // main.rs
    }

    #[test]
    fn quick_search_case_insensitive() {
        let mut p = make_panel_with_files(&[
            ("Cargo.toml", DiffFileStatus::Modified),
            ("src/app.rs", DiffFileStatus::Modified),
        ]);
        p.jump_to_match("cargo");
        assert_eq!(selected(&p), 0); // Cargo.toml
    }

    #[test]
    fn quick_search_finds_dir() {
        let mut p = make_panel_with_files(&[
            ("src/app.rs", DiffFileStatus::Modified),
            ("test/foo.rs", DiffFileStatus::Added),
        ]);
        p.jump_to_match("test");
        assert_eq!(selected(&p), 2); // test/ dir
    }

    #[test]
    fn quick_search_no_match_stays() {
        let mut p = make_panel_with_files(&[
            ("a.rs", DiffFileStatus::Added),
            ("b.rs", DiffFileStatus::Modified),
        ]);
        p.move_down();
        assert_eq!(selected(&p), 1);
        p.jump_to_match("zzz");
        assert_eq!(selected(&p), 1); // unchanged
    }

    // --- Jump next/prev match tests ---

    #[test]
    fn jump_next_match_cycles() {
        let mut p = make_panel_with_files(&[
            ("src/alpha.rs", DiffFileStatus::Added),
            ("src/beta.rs", DiffFileStatus::Modified),
            ("src/app.rs", DiffFileStatus::Modified),
        ]);
        // items: [src/, alpha.rs, beta.rs, app.rs]
        p.quick_search = Some("a".to_string());
        p.jump_to_match("a"); // finds alpha.rs at index 1
        assert_eq!(selected(&p), 1);
        p.jump_to_next_match(); // finds app.rs at index 3
        assert_eq!(selected(&p), 3);
        p.jump_to_next_match(); // wraps to alpha.rs at index 1
        assert_eq!(selected(&p), 1);
    }

    #[test]
    fn jump_prev_match_cycles() {
        let mut p = make_panel_with_files(&[
            ("src/alpha.rs", DiffFileStatus::Added),
            ("src/beta.rs", DiffFileStatus::Modified),
            ("src/app.rs", DiffFileStatus::Modified),
        ]);
        // items: [src/, alpha.rs, beta.rs, app.rs]
        p.quick_search = Some("a".to_string());
        p.jump_to_match("a"); // finds alpha.rs at index 1
        assert_eq!(selected(&p), 1);
        p.jump_to_prev_match(); // wraps to app.rs at index 3
        assert_eq!(selected(&p), 3);
    }

    // --- Enter tests ---

    #[test]
    fn enter_on_file_returns_path() {
        let mut p = make_panel_with_files(&[("src/app.rs", DiffFileStatus::Modified)]);
        p.move_down(); // select app.rs (index 1)
        let result = p.enter();
        assert!(result.is_some());
    }

    #[test]
    fn enter_on_deleted_file_returns_none() {
        let mut p = make_panel_with_files(&[("old.rs", DiffFileStatus::Deleted)]);
        // select old.rs (index 0, root level file)
        let result = p.enter();
        assert!(result.is_none()); // deleted files can't be opened
    }

    #[test]
    fn enter_on_dir_toggles_collapse() {
        let mut p = make_panel_with_files(&[
            ("src/a.rs", DiffFileStatus::Added),
            ("src/b.rs", DiffFileStatus::Modified),
        ]);
        // selected=0 is src/ dir (expanded)
        assert_eq!(item_count(&p), 3); // src/, a.rs, b.rs
        let result = p.enter(); // collapse
        assert!(result.is_none()); // dir enter returns None
        assert_eq!(item_count(&p), 1); // just src/ collapsed
    }

    // --- Collapse or parent on collapsed dir ---

    #[test]
    fn collapse_or_parent_on_collapsed_dir_jumps_to_parent() {
        let mut p = make_panel_with_files(&[
            ("src/ui/a.rs", DiffFileStatus::Added),
            ("src/ui/b.rs", DiffFileStatus::Modified),
        ]);
        // items: [src/, ui/, a.rs, b.rs]
        p.move_down(); // select ui/ (index 1)
        p.enter(); // collapse ui/
                   // items: [src/, ui/] — selected=1 (ui/ collapsed)
        p.collapse_or_parent(); // should jump to parent src/ (index 0)
        assert_eq!(selected(&p), 0);
    }
}
