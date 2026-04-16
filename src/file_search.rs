//! Fast file content search using ripgrep's engine (ignore + grep-searcher + grep-regex).
//! Runs in a background thread, streams results via mpsc channel.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkMatch};
use ignore::WalkBuilder;

/// An available rg file type (e.g. "rust" → "*.rs").
#[derive(Debug, Clone)]
pub struct RgFileType {
    pub name: String,
    pub globs: String,
}

/// Return the cached list of all default rg file types.
pub fn rg_file_types() -> &'static [RgFileType] {
    use std::sync::OnceLock;
    static TYPES: OnceLock<Vec<RgFileType>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut builder = ignore::types::TypesBuilder::new();
        builder.add_defaults();
        builder
            .definitions()
            .into_iter()
            .map(|def| RgFileType {
                name: def.name().to_string(),
                globs: def.globs().join(", "),
            })
            .collect()
    })
}

/// Full search configuration matching rg's CLI options.
pub struct SearchConfig {
    pub root: PathBuf,
    pub query: String,
    // Glob / type filtering
    pub filter: String,
    pub file_type: String,
    pub type_exclude: String,
    // Pattern matching
    pub is_regex: bool,
    pub case_insensitive: bool,
    pub smart_case: bool,
    pub whole_word: bool,
    pub whole_line: bool,
    pub invert_match: bool,
    pub multiline: bool,
    pub multiline_dotall: bool,
    pub crlf: bool,
    // File traversal
    pub hidden: bool,
    pub follow_symlinks: bool,
    pub no_gitignore: bool,
    pub binary: bool,
    pub glob_case_insensitive: bool,
    pub one_file_system: bool,
    // Output
    pub trim_whitespace: bool,
    pub before_context: usize,
    pub after_context: usize,
    // Limits
    pub max_depth: Option<usize>,
    pub max_count: Option<u64>,
    pub max_filesize: Option<u64>,
    pub encoding: String,
}

/// Parse a human-readable file size like "100K", "1M", "2G" into bytes.
pub fn parse_filesize(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix(['K', 'k']) {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix(['M', 'm']) {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix(['G', 'g']) {
        (n, 1024 * 1024 * 1024)
    } else {
        (s, 1u64)
    };
    num_str
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
}

/// A single matching line in a file.
#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub path: PathBuf,
    pub line_number: u64,
    pub text: String,
    /// True if this is a context line (before/after), not a match line.
    pub is_context: bool,
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
    /// Local filter within results (type to narrow).
    pub filter: String,
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
    /// True if this is a context line (before/after), not a match line.
    pub is_context: bool,
}

/// An item in the flat visible list (for navigation).
#[derive(Debug, Clone)]
pub enum SearchItem {
    File(usize),         // index into files
    Match(usize, usize), // (file_index, match_index)
}

impl SearchState {
    /// Start a new search. Spawns a background thread.
    pub fn new(config: SearchConfig) -> Self {
        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel.clone();
        let query = config.query.clone();
        let root = config.root.clone();

        std::thread::spawn(move || {
            run_search(config, tx, cancel_clone);
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
            filter: String::new(),
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
            is_context: m.is_context,
        });
    }

    /// Check if a file passes the given lowercased filter.
    fn file_matches_filter(file: &FileMatches, filter_lower: &str) -> bool {
        if file.rel_path.to_lowercase().contains(filter_lower) {
            return true;
        }
        file.matches
            .iter()
            .any(|m| m.text.to_lowercase().contains(filter_lower))
    }

    /// Build the flat visible item list for navigation, respecting filter.
    pub fn visible_items(&self) -> Vec<SearchItem> {
        let mut items = Vec::new();
        let filter_lower = self.filter.to_lowercase();
        let has_filter = !filter_lower.is_empty();

        for (fi, file) in self.files.iter().enumerate() {
            if has_filter && !Self::file_matches_filter(file, &filter_lower) {
                continue;
            }
            // If the file path matches the filter, show all its matches
            let path_matches = has_filter && file.rel_path.to_lowercase().contains(&filter_lower);
            items.push(SearchItem::File(fi));
            if file.expanded {
                for (mi, m) in file.matches.iter().enumerate() {
                    if has_filter && !path_matches && !m.text.to_lowercase().contains(&filter_lower)
                    {
                        continue;
                    }
                    items.push(SearchItem::Match(fi, mi));
                }
            }
        }
        items
    }

    /// Total number of visible items (counts without allocating).
    pub fn visible_count(&self) -> usize {
        let filter_lower = self.filter.to_lowercase();
        let has_filter = !filter_lower.is_empty();
        let mut count = 0;
        for file in &self.files {
            if has_filter && !Self::file_matches_filter(file, &filter_lower) {
                continue;
            }
            let path_matches = has_filter && file.rel_path.to_lowercase().contains(&filter_lower);
            count += 1; // file header
            if file.expanded {
                if has_filter && !path_matches {
                    count += file
                        .matches
                        .iter()
                        .filter(|m| m.text.to_lowercase().contains(&filter_lower))
                        .count();
                } else {
                    count += file.matches.len();
                }
            }
        }
        count
    }

    /// Clamp selected index after filter changes.
    pub fn clamp_selected(&mut self) {
        let count = self.visible_count();
        if count == 0 {
            self.selected = 0;
        } else if self.selected >= count {
            self.selected = count - 1;
        }
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
fn run_search(config: SearchConfig, tx: mpsc::Sender<SearchMatch>, cancel: Arc<AtomicBool>) {
    // --- Build the regex matcher ---
    let mut rmb = RegexMatcherBuilder::new();
    rmb.case_insensitive(config.case_insensitive)
        .case_smart(config.smart_case)
        .word(config.whole_word)
        .multi_line(config.multiline)
        .crlf(config.crlf);

    if config.multiline_dotall {
        rmb.dot_matches_new_line(true);
    }
    if config.whole_line {
        rmb.whole_line(true);
    }
    if !config.is_regex {
        rmb.fixed_strings(true);
    }

    let matcher = match rmb.build(&config.query) {
        Ok(m) => m,
        Err(_) => return,
    };

    // --- Build the searcher ---
    let mut sb = SearcherBuilder::new();
    sb.invert_match(config.invert_match)
        .multi_line(config.multiline)
        .before_context(config.before_context)
        .after_context(config.after_context);

    if config.crlf {
        sb.line_terminator(grep_matcher::LineTerminator::crlf());
    }

    if config.binary {
        sb.binary_detection(BinaryDetection::none());
    }

    if !config.encoding.is_empty() {
        if let Ok(enc) = grep_searcher::Encoding::new(&config.encoding) {
            sb.encoding(Some(enc));
        }
    }

    let mut searcher = sb.build();

    // --- Build the directory walker ---
    let root = &config.root;
    let mut wb = WalkBuilder::new(root);
    wb.hidden(!config.hidden)
        .git_ignore(!config.no_gitignore)
        .git_global(!config.no_gitignore)
        .git_exclude(!config.no_gitignore)
        .follow_links(config.follow_symlinks)
        .same_file_system(config.one_file_system);

    if config.glob_case_insensitive {
        wb.ignore_case_insensitive(true);
    }

    if let Some(depth) = config.max_depth {
        wb.max_depth(Some(depth));
    }

    if let Some(size) = config.max_filesize {
        wb.max_filesize(Some(size));
    }

    // File type filtering
    if !config.file_type.is_empty() || !config.type_exclude.is_empty() {
        let mut tb = ignore::types::TypesBuilder::new();
        tb.add_defaults();
        for t in config
            .file_type
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tb.select(t);
        }
        for t in config
            .type_exclude
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tb.negate(t);
        }
        if let Ok(types) = tb.build() {
            wb.types(types);
        }
    }

    // Glob filter (supports comma-separated patterns)
    if !config.filter.is_empty() {
        let mut ob = ignore::overrides::OverrideBuilder::new(root);
        let mut any_ok = false;
        for pat in config
            .filter
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if ob.add(pat).is_ok() {
                any_ok = true;
            }
        }
        if any_ok {
            if let Ok(overrides) = ob.build() {
                wb.overrides(overrides);
            }
        }
    }

    let walker = wb.build();
    let trim = config.trim_whitespace;
    let max_count = config.max_count;

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
        let mut sink = MatchSink {
            path: path.clone(),
            tx: tx.clone(),
            cancel: &cancel,
            trim,
            max_count,
            file_match_count: 0,
        };

        let _ = searcher.search_path(&matcher, &path, &mut sink);
    }
}

/// Custom sink that captures both match and context lines.
struct MatchSink<'a> {
    path: PathBuf,
    tx: mpsc::Sender<SearchMatch>,
    cancel: &'a AtomicBool,
    trim: bool,
    max_count: Option<u64>,
    file_match_count: u64,
}

impl MatchSink<'_> {
    fn send_line(&self, line_number: Option<u64>, bytes: &[u8], is_context: bool) {
        let line_number = line_number.unwrap_or(0);
        let text = String::from_utf8_lossy(bytes);
        let text = if self.trim {
            text.trim_start().to_string()
        } else {
            text.to_string()
        };
        let _ = self.tx.send(SearchMatch {
            path: self.path.clone(),
            line_number,
            text,
            is_context,
        });
    }
}

impl Sink for &mut MatchSink<'_> {
    type Error = std::io::Error;

    fn matched(&mut self, _searcher: &Searcher, mat: &SinkMatch<'_>) -> Result<bool, Self::Error> {
        if self.cancel.load(Ordering::Relaxed) {
            return Ok(false);
        }
        if let Some(limit) = self.max_count {
            if self.file_match_count >= limit {
                return Ok(false);
            }
        }
        self.file_match_count += 1;
        self.send_line(mat.line_number(), mat.bytes(), false);
        Ok(true)
    }

    fn context(
        &mut self,
        _searcher: &Searcher,
        ctx: &SinkContext<'_>,
    ) -> Result<bool, Self::Error> {
        if self.cancel.load(Ordering::Relaxed) {
            return Ok(false);
        }
        self.send_line(ctx.line_number(), ctx.bytes(), true);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Create a default SearchConfig pointing at the given root with query.
    fn cfg(root: PathBuf, query: &str) -> SearchConfig {
        SearchConfig {
            root,
            query: query.to_string(),
            filter: String::new(),
            file_type: String::new(),
            type_exclude: String::new(),
            is_regex: true,
            case_insensitive: false,
            smart_case: false,
            whole_word: false,
            whole_line: false,
            invert_match: false,
            multiline: false,
            multiline_dotall: false,
            crlf: false,
            hidden: false,
            follow_symlinks: false,
            no_gitignore: false,
            binary: false,
            glob_case_insensitive: false,
            one_file_system: false,
            trim_whitespace: false,
            before_context: 0,
            after_context: 0,
            max_depth: None,
            max_count: None,
            max_filesize: None,
            encoding: String::new(),
        }
    }

    /// Run a search to completion and return all matches.
    fn run(config: SearchConfig) -> Vec<SearchMatch> {
        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        run_search(config, tx, cancel);
        rx.into_iter().collect()
    }

    /// Create a temp dir with the given files and contents.
    fn setup(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, content) in files {
            let path = dir.path().join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&path, content).unwrap();
        }
        dir
    }

    // -------------------------------------------------------
    // parse_filesize
    // -------------------------------------------------------

    #[test]
    fn parse_filesize_bytes() {
        assert_eq!(parse_filesize("1024"), Some(1024));
    }

    #[test]
    fn parse_filesize_kilo() {
        assert_eq!(parse_filesize("1K"), Some(1024));
        assert_eq!(parse_filesize("2k"), Some(2048));
    }

    #[test]
    fn parse_filesize_mega() {
        assert_eq!(parse_filesize("1M"), Some(1024 * 1024));
    }

    #[test]
    fn parse_filesize_giga() {
        assert_eq!(parse_filesize("1G"), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn parse_filesize_empty() {
        assert_eq!(parse_filesize(""), None);
        assert_eq!(parse_filesize("  "), None);
    }

    #[test]
    fn parse_filesize_invalid() {
        assert_eq!(parse_filesize("abc"), None);
    }

    // -------------------------------------------------------
    // Basic search
    // -------------------------------------------------------

    #[test]
    fn basic_regex_search() {
        let dir = setup(&[("a.txt", "hello world\nfoo bar\nhello again")]);
        let matches = run(cfg(dir.path().to_path_buf(), "hello"));
        assert_eq!(matches.len(), 2);
        assert!(matches[0].text.contains("hello"));
    }

    #[test]
    fn basic_literal_search() {
        let dir = setup(&[("a.txt", "a.b\na+b\na*b")]);
        let mut c = cfg(dir.path().to_path_buf(), "a.b");
        c.is_regex = false;
        let matches = run(c);
        // Only the literal "a.b" should match, not "a+b" or "a*b"
        assert_eq!(matches.len(), 1);
        assert!(matches[0].text.contains("a.b"));
    }

    // -------------------------------------------------------
    // Case insensitive (-i)
    // -------------------------------------------------------

    #[test]
    fn case_insensitive() {
        let dir = setup(&[("a.txt", "Hello\nhello\nHELLO\nworld")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.case_insensitive = true;
        let matches = run(c);
        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn case_sensitive_default() {
        let dir = setup(&[("a.txt", "Hello\nhello\nHELLO")]);
        let matches = run(cfg(dir.path().to_path_buf(), "hello"));
        assert_eq!(matches.len(), 1);
    }

    // -------------------------------------------------------
    // Smart case (-S)
    // -------------------------------------------------------

    #[test]
    fn smart_case_lowercase_query() {
        let dir = setup(&[("a.txt", "Hello\nhello\nHELLO")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.smart_case = true;
        let matches = run(c);
        // Lowercase query → case insensitive
        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn smart_case_mixed_query() {
        let dir = setup(&[("a.txt", "Hello\nhello\nHELLO")]);
        let mut c = cfg(dir.path().to_path_buf(), "Hello");
        c.smart_case = true;
        let matches = run(c);
        // Has uppercase → case sensitive
        assert_eq!(matches.len(), 1);
    }

    // -------------------------------------------------------
    // Whole word (-w)
    // -------------------------------------------------------

    #[test]
    fn whole_word() {
        let dir = setup(&[("a.txt", "error\nerror_code\nmy_error\nerr")]);
        let mut c = cfg(dir.path().to_path_buf(), "error");
        c.whole_word = true;
        let matches = run(c);
        // "error" is a whole word only in the first line
        assert_eq!(matches.len(), 1);
        assert!(matches[0].text.contains("error"));
    }

    // -------------------------------------------------------
    // Whole line (-x)
    // -------------------------------------------------------

    #[test]
    fn whole_line() {
        let dir = setup(&[("a.txt", "hello\nhello world\nworld hello")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.whole_line = true;
        let matches = run(c);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].text.trim(), "hello");
    }

    // -------------------------------------------------------
    // Invert match (-v)
    // -------------------------------------------------------

    #[test]
    fn invert_match() {
        let dir = setup(&[("a.txt", "keep\nskip\nkeep\nskip")]);
        let mut c = cfg(dir.path().to_path_buf(), "skip");
        c.invert_match = true;
        let matches = run(c);
        assert_eq!(matches.len(), 2);
        for m in &matches {
            assert!(m.text.contains("keep"));
        }
    }

    // -------------------------------------------------------
    // Multiline (-U)
    // -------------------------------------------------------

    #[test]
    fn multiline_search() {
        let dir = setup(&[("a.txt", "start\nend\nother")]);
        let mut c = cfg(dir.path().to_path_buf(), "start\\nend");
        c.multiline = true;
        let matches = run(c);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn multiline_dotall() {
        let dir = setup(&[("a.txt", "start\nend")]);
        let mut c = cfg(dir.path().to_path_buf(), "start.end");
        c.multiline = true;
        c.multiline_dotall = true;
        let matches = run(c);
        assert_eq!(matches.len(), 1);
    }

    // -------------------------------------------------------
    // Glob filter (-g)
    // -------------------------------------------------------

    #[test]
    fn glob_filter_include() {
        let dir = setup(&[("a.rs", "hello"), ("b.txt", "hello"), ("c.py", "hello")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.filter = "*.rs".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.to_string_lossy().ends_with(".rs"));
    }

    #[test]
    fn glob_filter_exclude() {
        let dir = setup(&[("a.rs", "hello"), ("b.txt", "hello"), ("c.py", "hello")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.filter = "!*.txt".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 2);
        for m in &matches {
            assert!(!m.path.to_string_lossy().ends_with(".txt"));
        }
    }

    #[test]
    fn glob_filter_comma_separated() {
        let dir = setup(&[("a.rs", "hello"), ("b.txt", "hello"), ("c.py", "hello")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.filter = "*.rs, *.py".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 2);
    }

    // -------------------------------------------------------
    // File type (-t / -T)
    // -------------------------------------------------------

    #[test]
    fn file_type_select() {
        let dir = setup(&[
            ("main.rs", "hello"),
            ("lib.py", "hello"),
            ("doc.md", "hello"),
        ]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.file_type = "rust".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.to_string_lossy().ends_with(".rs"));
    }

    #[test]
    fn file_type_multiple() {
        let dir = setup(&[
            ("main.rs", "hello"),
            ("lib.py", "hello"),
            ("doc.md", "hello"),
        ]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.file_type = "rust, py".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn file_type_negate() {
        let dir = setup(&[
            ("main.rs", "hello"),
            ("lib.py", "hello"),
            ("doc.md", "hello"),
        ]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.type_exclude = "markdown".to_string();
        let matches = run(c);
        assert_eq!(matches.len(), 2);
        for m in &matches {
            assert!(!m.path.to_string_lossy().ends_with(".md"));
        }
    }

    // -------------------------------------------------------
    // Hidden files (-.)
    // -------------------------------------------------------

    #[test]
    fn hidden_files_excluded_by_default() {
        let dir = setup(&[("visible.txt", "hello"), (".hidden.txt", "hello")]);
        let matches = run(cfg(dir.path().to_path_buf(), "hello"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.to_string_lossy().contains("visible"));
    }

    #[test]
    fn hidden_files_included() {
        let dir = setup(&[("visible.txt", "hello"), (".hidden.txt", "hello")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.hidden = true;
        let matches = run(c);
        assert_eq!(matches.len(), 2);
    }

    // -------------------------------------------------------
    // No gitignore (--no-ignore)
    // -------------------------------------------------------

    #[test]
    fn gitignore_respected() {
        let dir = setup(&[
            ("included.txt", "hello"),
            ("ignored.log", "hello"),
            (".gitignore", "*.log"),
        ]);
        // Init a git repo so gitignore is respected
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .output()
            .ok();
        let matches = run(cfg(dir.path().to_path_buf(), "hello"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.to_string_lossy().contains("included"));
    }

    #[test]
    fn no_gitignore() {
        let dir = setup(&[
            ("included.txt", "hello"),
            ("ignored.log", "hello"),
            (".gitignore", "*.log"),
        ]);
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .output()
            .ok();
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.no_gitignore = true;
        let matches = run(c);
        // Should find both files (gitignore disabled)
        assert!(matches.len() >= 2);
    }

    // -------------------------------------------------------
    // Max depth (-d)
    // -------------------------------------------------------

    #[test]
    fn max_depth() {
        let dir = setup(&[
            ("top.txt", "hello"),
            ("a/mid.txt", "hello"),
            ("a/b/deep.txt", "hello"),
        ]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.max_depth = Some(2);
        let matches = run(c);
        // Depth 0=root dir, 1=top.txt & a/, 2=mid.txt & b/
        // So depth 2 includes top.txt and a/mid.txt but not a/b/deep.txt
        assert_eq!(matches.len(), 2);
    }

    // -------------------------------------------------------
    // Max count (-m)
    // -------------------------------------------------------

    #[test]
    fn max_count_per_file() {
        let dir = setup(&[("a.txt", "match\nmatch\nmatch\nmatch\nmatch")]);
        let mut c = cfg(dir.path().to_path_buf(), "match");
        c.max_count = Some(2);
        let matches = run(c);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn max_count_across_files() {
        let dir = setup(&[
            ("a.txt", "match\nmatch\nmatch"),
            ("b.txt", "match\nmatch\nmatch"),
        ]);
        let mut c = cfg(dir.path().to_path_buf(), "match");
        c.max_count = Some(1);
        let matches = run(c);
        // 1 per file × 2 files = 2
        assert_eq!(matches.len(), 2);
    }

    // -------------------------------------------------------
    // Max filesize (--max-filesize)
    // -------------------------------------------------------

    #[test]
    fn max_filesize_filter() {
        let dir = setup(&[("small.txt", "hello"), ("big.txt", &"hello\n".repeat(1000))]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.max_filesize = Some(100); // 100 bytes
        let matches = run(c);
        // Only the small file should be searched
        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.to_string_lossy().contains("small"));
    }

    // -------------------------------------------------------
    // Trim whitespace (--trim)
    // -------------------------------------------------------

    #[test]
    fn trim_whitespace() {
        let dir = setup(&[("a.txt", "    indented\n  also indented")]);
        let mut c = cfg(dir.path().to_path_buf(), "indented");
        c.trim_whitespace = true;
        let matches = run(c);
        assert_eq!(matches.len(), 2);
        for m in &matches {
            assert!(!m.text.starts_with(' '), "should be trimmed: {:?}", m.text);
        }
    }

    #[test]
    fn no_trim_by_default() {
        let dir = setup(&[("a.txt", "    indented")]);
        let matches = run(cfg(dir.path().to_path_buf(), "indented"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].text.starts_with("    "));
    }

    // -------------------------------------------------------
    // CRLF (--crlf)
    // -------------------------------------------------------

    #[test]
    fn crlf_line_terminators() {
        // With CRLF mode, the regex treats \r\n as line endings
        // so \r is not included as part of the match text
        let dir = setup(&[("a.txt", "hello\r\nworld\r\n")]);
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.crlf = true;
        let matches = run(c);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].text.contains("hello"));
    }

    // -------------------------------------------------------
    // Before/After context (-B / -A)
    // -------------------------------------------------------

    #[test]
    fn before_context() {
        let dir = setup(&[("a.txt", "line1\nline2\nMATCH\nline4\nline5")]);
        let mut c = cfg(dir.path().to_path_buf(), "MATCH");
        c.before_context = 2;
        let matches = run(c);
        // Should have 2 context lines before + 1 match = 3 total
        assert_eq!(matches.len(), 3);
        assert!(matches[0].is_context);
        assert!(matches[1].is_context);
        assert!(!matches[2].is_context);
        assert!(matches[2].text.contains("MATCH"));
    }

    #[test]
    fn after_context() {
        let dir = setup(&[("a.txt", "line1\nMATCH\nline3\nline4\nline5")]);
        let mut c = cfg(dir.path().to_path_buf(), "MATCH");
        c.after_context = 2;
        let matches = run(c);
        // 1 match + 2 context lines after = 3 total
        assert_eq!(matches.len(), 3);
        assert!(!matches[0].is_context);
        assert!(matches[0].text.contains("MATCH"));
        assert!(matches[1].is_context);
        assert!(matches[2].is_context);
    }

    #[test]
    fn before_and_after_context() {
        let dir = setup(&[("a.txt", "a\nb\nc\nMATCH\ne\nf\ng")]);
        let mut c = cfg(dir.path().to_path_buf(), "MATCH");
        c.before_context = 1;
        c.after_context = 1;
        let matches = run(c);
        // 1 before + 1 match + 1 after = 3
        assert_eq!(matches.len(), 3);
        assert!(matches[0].is_context); // "c"
        assert!(!matches[1].is_context); // "MATCH"
        assert!(matches[2].is_context); // "e"
    }

    // -------------------------------------------------------
    // Follow symlinks (-L)
    // -------------------------------------------------------

    #[test]
    fn follow_symlinks() {
        let dir = setup(&[("real.txt", "hello")]);
        #[cfg(unix)]
        {
            let link_path = dir.path().join("link.txt");
            std::os::unix::fs::symlink(dir.path().join("real.txt"), &link_path).unwrap();
        }
        let mut c = cfg(dir.path().to_path_buf(), "hello");
        c.follow_symlinks = true;
        let matches = run(c);
        // Should find the match (at least in the real file)
        assert!(!matches.is_empty());
    }

    // -------------------------------------------------------
    // SearchState integration
    // -------------------------------------------------------

    #[test]
    fn search_state_lifecycle() {
        let dir = setup(&[("a.txt", "foo\nbar\nfoo"), ("b.txt", "baz\nfoo")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "foo"));

        // Poll until done
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert!(!state.searching);
        assert_eq!(state.total_matches, 3);
        assert_eq!(state.files.len(), 2);
    }

    #[test]
    fn search_state_navigation() {
        let dir = setup(&[("a.txt", "line1\nline2\nline3")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "line"));

        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(state.visible_count(), 4); // 1 file + 3 matches
        assert_eq!(state.selected, 0);

        state.move_down();
        assert_eq!(state.selected, 1);
        state.move_down();
        assert_eq!(state.selected, 2);
        state.move_up();
        assert_eq!(state.selected, 1);

        // Selected location on a match entry
        state.move_down(); // now on match index 2
        let loc = state.selected_location();
        assert!(loc.is_some());
    }

    #[test]
    fn search_state_cancel_on_drop() {
        let dir = setup(&[("a.txt", &"hello\n".repeat(10000))]);
        let state = SearchState::new(cfg(dir.path().to_path_buf(), "hello"));
        let cancel = state.cancel.clone();
        assert!(!cancel.load(Ordering::Relaxed));
        drop(state);
        assert!(cancel.load(Ordering::Relaxed));
    }

    // -------------------------------------------------------
    // rg_file_types
    // -------------------------------------------------------

    #[test]
    fn rg_file_types_populated() {
        let types = rg_file_types();
        assert!(!types.is_empty());
        // Should have common types
        assert!(types.iter().any(|t| t.name == "rust"));
        assert!(types.iter().any(|t| t.name == "py"));
        assert!(types.iter().any(|t| t.name == "js"));
    }

    // -------------------------------------------------------
    // parse_filesize overflow
    // -------------------------------------------------------

    #[test]
    fn parse_filesize_overflow() {
        // Large value that would overflow u64
        assert_eq!(parse_filesize("999999999999G"), None);
    }

    // -------------------------------------------------------
    // Result filter
    // -------------------------------------------------------

    #[test]
    fn filter_matches_by_text() {
        let dir = setup(&[("a.txt", "hello world\nfoo bar\nhello again")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "hello|foo"));
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(state.total_matches, 3); // hello world, foo bar, hello again

        // Filter to only "foo"
        state.filter = "foo".to_string();
        let items = state.visible_items();
        // Should have 1 file header + 1 matching line
        assert_eq!(items.len(), 2);

        // visible_count should match
        assert_eq!(state.visible_count(), 2);
    }

    #[test]
    fn filter_matches_by_path() {
        let dir = setup(&[("alpha.txt", "hello"), ("beta.txt", "hello")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "hello"));
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(state.files.len(), 2);

        // Filter by filename
        state.filter = "alpha".to_string();
        let items = state.visible_items();
        // 1 file + 1 match
        assert_eq!(items.len(), 2);
        assert_eq!(state.visible_count(), 2);
    }

    #[test]
    fn filter_empty_shows_all() {
        let dir = setup(&[("a.txt", "line1\nline2")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "line"));
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        state.filter = String::new();
        assert_eq!(state.visible_count(), 3); // 1 file + 2 matches
    }

    #[test]
    fn filter_clamp_selected() {
        let dir = setup(&[("a.txt", "aaa\nbbb\nccc")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "."));
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Select last item
        state.selected = state.visible_count() - 1;
        // Apply filter that reduces items
        state.filter = "aaa".to_string();
        state.clamp_selected();
        assert!(state.selected < state.visible_count());
    }

    #[test]
    fn filter_case_insensitive() {
        let dir = setup(&[("a.txt", "Hello World\nbye")]);
        let mut state = SearchState::new(cfg(dir.path().to_path_buf(), "."));
        for _ in 0..100 {
            state.poll();
            if !state.searching {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        state.filter = "hello".to_string();
        // Should match "Hello World" case-insensitively
        assert_eq!(state.visible_count(), 2); // 1 file + 1 match
    }
}
