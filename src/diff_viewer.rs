use std::path::Path;
use std::process::{Command, Stdio};

use ratatui::style::Color;

use crate::syntax::SyntaxHighlighter;
use crate::theme::theme;

/// A line pair for side-by-side display.
pub struct DiffPair {
    pub left: Option<NumberedLine>,
    pub right: Option<NumberedLine>,
    pub kind: DiffKind,
}

pub struct NumberedLine {
    pub num: usize,
    pub text: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DiffKind {
    Equal,
    Added,   // right only (new line)
    Deleted, // left only (removed line)
    Changed, // both sides differ
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DiffSide {
    Left,
    Right,
}

/// A search match in the diff viewer.
pub struct SearchMatch {
    pub pair_idx: usize,
    pub col: usize,
    pub len: usize,
    pub side: DiffSide,
}

pub struct DiffViewerState {
    pub path: String,
    pub lines: Vec<DiffPair>,
    pub cursor: usize,
    pub cursor_col: usize,
    pub cursor_side: DiffSide,
    pub scroll: usize,
    pub visible_lines: usize,
    pub added_count: usize,
    pub deleted_count: usize,
    pub changed_count: usize,
    /// Cached max line number (for line number column width).
    pub max_line_num: usize,
    /// Per-line, per-char syntax color arrays for the left side (indexed by 1-based line number).
    pub syntax_colors_left: Vec<Vec<Color>>,
    /// Per-line, per-char syntax color arrays for the right side (indexed by 1-based line number).
    pub syntax_colors_right: Vec<Vec<Color>>,
    /// Selection anchor: (pair_index, col, side).
    pub selection_anchor: Option<(usize, usize, DiffSide)>,
    /// Search query (inline search input).
    pub search_input: Option<String>,
    /// Active search query (confirmed).
    pub search_query: Option<String>,
    /// All search matches.
    pub search_matches: Vec<SearchMatch>,
}

impl DiffViewerState {
    /// Open a diff viewer for a file, comparing merge-base of the given base branch with working tree.
    pub fn open(repo_root: &Path, file_path: &str, base_branch: &str) -> Self {
        let lines = compute_diff(repo_root, file_path, base_branch);
        let (mut added_count, mut deleted_count, mut changed_count, mut max_line_num) =
            (0, 0, 0, 0usize);
        for pair in &lines {
            match pair.kind {
                DiffKind::Added => added_count += 1,
                DiffKind::Deleted => deleted_count += 1,
                DiffKind::Changed => changed_count += 1,
                DiffKind::Equal => {}
            }
            if let Some(ref n) = pair.left {
                max_line_num = max_line_num.max(n.num);
            }
            if let Some(ref n) = pair.right {
                max_line_num = max_line_num.max(n.num);
            }
        }

        // Build syntax colors
        let (syntax_colors_left, syntax_colors_right) =
            compute_syntax_colors(repo_root, file_path, &lines);

        // Position cursor at first change
        let first_change = lines
            .iter()
            .position(|p| p.kind != DiffKind::Equal)
            .unwrap_or(0);

        // Show ~5 lines of context above the first change
        let initial_scroll = first_change.saturating_sub(5);

        Self {
            path: file_path.to_string(),
            lines,
            cursor: first_change,
            cursor_col: 0,
            cursor_side: DiffSide::Right,
            scroll: initial_scroll,
            visible_lines: 0,
            added_count,
            deleted_count,
            changed_count,
            max_line_num,
            syntax_colors_left,
            syntax_colors_right,
            selection_anchor: None,
            search_input: None,
            search_query: None,
            search_matches: Vec::new(),
        }
    }

    pub fn ensure_cursor_visible(&mut self) {
        if self.cursor < self.scroll {
            self.scroll = self.cursor;
        }
        let vh = self.visible_lines;
        if vh > 0 && self.cursor >= self.scroll + vh {
            self.scroll = self.cursor - vh + 1;
        }
    }

    pub fn scroll_up(&mut self, amount: usize) {
        self.cursor = self.cursor.saturating_sub(amount);
        self.ensure_cursor_visible();
    }

    pub fn scroll_down(&mut self, amount: usize) {
        let max = self.lines.len().saturating_sub(1);
        self.cursor = (self.cursor + amount).min(max);
        self.ensure_cursor_visible();
    }

    pub fn scroll_to_top(&mut self) {
        self.cursor = 0;
        self.scroll = 0;
    }

    pub fn scroll_to_bottom(&mut self) {
        self.cursor = self.lines.len().saturating_sub(1);
        self.ensure_cursor_visible();
    }

    pub fn move_cursor_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor > 0 {
            // Wrap to end of previous line
            self.cursor -= 1;
            self.cursor_col = self.current_side_line_len();
            self.ensure_cursor_visible();
        }
    }

    pub fn move_cursor_right(&mut self) {
        let line_len = self.current_side_line_len();
        if self.cursor_col < line_len {
            self.cursor_col += 1;
        } else if self.cursor + 1 < self.lines.len() {
            // Wrap to start of next line
            self.cursor += 1;
            self.cursor_col = 0;
            self.ensure_cursor_visible();
        }
    }

    pub fn switch_side(&mut self) {
        self.cursor_side = match self.cursor_side {
            DiffSide::Left => DiffSide::Right,
            DiffSide::Right => DiffSide::Left,
        };
        // Clamp col to new side's line length
        let len = self.current_side_line_len();
        if self.cursor_col > len {
            self.cursor_col = len;
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor_col = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor_col = self.current_side_line_len();
    }

    pub fn current_side_line_len(&self) -> usize {
        self.lines.get(self.cursor).map_or(0, |pair| {
            let line = match self.cursor_side {
                DiffSide::Left => pair.left.as_ref(),
                DiffSide::Right => pair.right.as_ref(),
            };
            line.map_or(0, |n| n.text.len())
        })
    }

    pub fn page_up(&mut self) {
        self.scroll_up(self.visible_lines.max(1));
    }

    pub fn page_down(&mut self) {
        self.scroll_down(self.visible_lines.max(1));
    }

    /// Center the scroll so cursor is in the middle of the viewport.
    fn center_cursor(&mut self) {
        let vh = self.visible_lines;
        if vh > 0 {
            self.scroll = self.cursor.saturating_sub(vh / 3);
        }
    }

    /// Jump to the start of the next diff hunk from cursor.
    /// Skips past the rest of the current hunk first.
    pub fn next_change(&mut self) {
        let mut i = self.cursor;
        // Skip past current hunk (non-Equal lines)
        while i < self.lines.len() && self.lines[i].kind != DiffKind::Equal {
            i += 1;
        }
        // Skip Equal lines to find start of next hunk
        while i < self.lines.len() && self.lines[i].kind == DiffKind::Equal {
            i += 1;
        }
        if i < self.lines.len() {
            self.cursor = i;
            self.center_cursor();
        }
    }

    /// Jump to the start of the previous diff hunk from cursor.
    pub fn prev_change(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut i = self.cursor.saturating_sub(1);
        // If we're inside a hunk, go to its start first
        // Skip backwards past current hunk
        while i > 0 && self.lines[i].kind != DiffKind::Equal {
            i -= 1;
        }
        // Skip Equal lines backwards
        while i > 0 && self.lines[i].kind == DiffKind::Equal {
            i -= 1;
        }
        // Now we're at the last line of the previous hunk — find its start
        while i > 0 && self.lines[i - 1].kind != DiffKind::Equal {
            i -= 1;
        }
        if self.lines[i].kind != DiffKind::Equal {
            self.cursor = i;
            self.center_cursor();
        }
    }

    /// Get the right-side line number at the cursor (1-based).
    /// Falls back to left-side if right is absent, or 1.
    pub fn current_line(&self) -> usize {
        self.lines
            .get(self.cursor)
            .and_then(|p| p.right.as_ref().or(p.left.as_ref()).map(|n| n.num))
            .unwrap_or(1)
    }

    // --- Selection ---

    fn ensure_anchor(&mut self) {
        if self.selection_anchor.is_none() {
            self.selection_anchor = Some((self.cursor, self.cursor_col, self.cursor_side));
        }
    }

    pub fn clear_selection(&mut self) {
        self.selection_anchor = None;
    }

    /// Returns ordered (start_pair, start_col, end_pair, end_col, side).
    pub fn selection_range(&self) -> Option<(usize, usize, usize, usize, DiffSide)> {
        let (anchor_line, anchor_col, anchor_side) = self.selection_anchor?;
        if anchor_side != self.cursor_side {
            return None;
        }
        let side = anchor_side;
        let (sl, sc, el, ec) = if anchor_line < self.cursor
            || (anchor_line == self.cursor && anchor_col <= self.cursor_col)
        {
            (anchor_line, anchor_col, self.cursor, self.cursor_col)
        } else {
            (self.cursor, self.cursor_col, anchor_line, anchor_col)
        };
        Some((sl, sc, el, ec, side))
    }

    pub fn selected_text(&self) -> Option<String> {
        let (sl, sc, el, ec, side) = self.selection_range()?;
        let mut result = String::new();
        for i in sl..=el {
            let text = self.lines.get(i).and_then(|p| match side {
                DiffSide::Left => p.left.as_ref().map(|n| n.text.as_str()),
                DiffSide::Right => p.right.as_ref().map(|n| n.text.as_str()),
            });
            let text = text.unwrap_or("");
            let start = if i == sl { sc } else { 0 };
            let end = if i == el { ec } else { text.len() };
            let start = start.min(text.len());
            let end = end.min(text.len());
            if start <= end {
                result.push_str(&text[start..end]);
            }
            if i < el {
                result.push('\n');
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    pub fn copy_to_clipboard(&self) {
        if let Some(text) = self.selected_text() {
            crate::clipboard::copy(&text);
        }
    }

    pub fn select_up(&mut self) {
        self.ensure_anchor();
        self.scroll_up(1);
    }

    pub fn select_down(&mut self) {
        self.ensure_anchor();
        self.scroll_down(1);
    }

    pub fn select_left(&mut self) {
        self.ensure_anchor();
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        }
    }

    pub fn select_right(&mut self) {
        self.ensure_anchor();
        let line_len = self.current_side_line_len();
        if self.cursor_col < line_len {
            self.cursor_col += 1;
        }
    }

    pub fn select_all(&mut self) {
        if self.lines.is_empty() {
            return;
        }
        self.selection_anchor = Some((0, 0, self.cursor_side));
        let last = self.lines.len().saturating_sub(1);
        self.cursor = last;
        let text_len = self
            .lines
            .get(last)
            .and_then(|p| match self.cursor_side {
                DiffSide::Left => p.left.as_ref().map(|n| n.text.len()),
                DiffSide::Right => p.right.as_ref().map(|n| n.text.len()),
            })
            .unwrap_or(0);
        self.cursor_col = text_len;
        self.ensure_cursor_visible();
    }

    // --- Search ---

    pub fn search(&mut self, query: &str) {
        self.search_query = Some(query.to_string());
        self.search_matches.clear();
        if query.is_empty() {
            return;
        }
        let query_lower = query.to_lowercase();
        for (idx, pair) in self.lines.iter().enumerate() {
            if let Some(ref n) = pair.left {
                let text_lower = n.text.to_lowercase();
                let mut start = 0;
                while let Some(pos) = text_lower[start..].find(&query_lower) {
                    self.search_matches.push(SearchMatch {
                        pair_idx: idx,
                        col: start + pos,
                        len: query.len(),
                        side: DiffSide::Left,
                    });
                    start += pos + 1;
                }
            }
            if let Some(ref n) = pair.right {
                let text_lower = n.text.to_lowercase();
                let mut start = 0;
                while let Some(pos) = text_lower[start..].find(&query_lower) {
                    self.search_matches.push(SearchMatch {
                        pair_idx: idx,
                        col: start + pos,
                        len: query.len(),
                        side: DiffSide::Right,
                    });
                    start += pos + 1;
                }
            }
        }
    }

    pub fn search_next(&mut self) -> bool {
        if self.search_matches.is_empty() {
            return false;
        }
        // Find first match after current position
        let pos = self.search_matches.iter().position(|m| {
            m.pair_idx > self.cursor
                || (m.pair_idx == self.cursor
                    && m.side == self.cursor_side
                    && m.col > self.cursor_col)
                || (m.pair_idx == self.cursor && m.side != self.cursor_side)
        });
        let idx = pos.unwrap_or(0); // wrap around
        let m = &self.search_matches[idx];
        self.cursor = m.pair_idx;
        self.cursor_col = m.col;
        self.cursor_side = m.side;
        self.center_cursor();
        true
    }

    pub fn search_prev(&mut self) -> bool {
        if self.search_matches.is_empty() {
            return false;
        }
        // Find last match before current position
        let pos = self.search_matches.iter().rposition(|m| {
            m.pair_idx < self.cursor
                || (m.pair_idx == self.cursor
                    && m.side == self.cursor_side
                    && m.col < self.cursor_col)
                || (m.pair_idx == self.cursor && m.side != self.cursor_side)
        });
        let idx = pos.unwrap_or(self.search_matches.len() - 1); // wrap around
        let m = &self.search_matches[idx];
        self.cursor = m.pair_idx;
        self.cursor_col = m.col;
        self.cursor_side = m.side;
        self.center_cursor();
        true
    }

    pub fn clear_search(&mut self) {
        self.search_query = None;
        self.search_matches.clear();
    }
}

/// Compute per-line, per-char syntax color arrays for both sides.
/// Returns (left_colors, right_colors) indexed by 1-based line number
/// (index 0 is unused/empty for 1-based indexing).
fn compute_syntax_colors(
    repo_root: &Path,
    file_path: &str,
    pairs: &[DiffPair],
) -> (Vec<Vec<Color>>, Vec<Vec<Color>>) {
    let t = theme();
    let full_path = repo_root.join(file_path);
    let mut syntax = SyntaxHighlighter::new(&full_path, t.viewer_text_fg);
    if !syntax.is_active() {
        return (Vec::new(), Vec::new());
    }

    // Reconstruct left and right text from diff pairs
    let mut left_text = String::new();
    let mut left_max_line = 0usize;
    let mut right_text = String::new();
    let mut right_max_line = 0usize;

    // Collect lines in order of their line numbers
    let mut left_lines: Vec<(usize, &str)> = Vec::new();
    let mut right_lines: Vec<(usize, &str)> = Vec::new();

    for pair in pairs {
        if let Some(ref n) = pair.left {
            left_lines.push((n.num, &n.text));
            left_max_line = left_max_line.max(n.num);
        }
        if let Some(ref n) = pair.right {
            right_lines.push((n.num, &n.text));
            right_max_line = right_max_line.max(n.num);
        }
    }

    // Build full text (lines are already in order from diff pair iteration)
    for (i, (_, text)) in left_lines.iter().enumerate() {
        left_text.push_str(text);
        if i + 1 < left_lines.len() {
            left_text.push('\n');
        }
    }
    for (i, (_, text)) in right_lines.iter().enumerate() {
        right_text.push_str(text);
        if i + 1 < right_lines.len() {
            right_text.push('\n');
        }
    }

    // Highlight both texts
    let left_spans = syntax.highlight_text(&left_text);
    let right_spans = syntax.highlight_text(&right_text);

    // Convert to per-line color arrays
    let left_colors = highlight_to_line_colors(
        &left_text,
        &left_spans,
        t.viewer_text_fg,
        left_max_line,
        &left_lines,
    );
    let right_colors = highlight_to_line_colors(
        &right_text,
        &right_spans,
        t.viewer_text_fg,
        right_max_line,
        &right_lines,
    );

    (left_colors, right_colors)
}

/// Convert highlight spans to per-line color arrays indexed by 1-based line number.
fn highlight_to_line_colors(
    text: &str,
    spans: &[(usize, usize, Color)],
    default_color: Color,
    max_line: usize,
    line_entries: &[(usize, &str)],
) -> Vec<Vec<Color>> {
    if text.is_empty() || max_line == 0 {
        return Vec::new();
    }

    // Build byte offsets for each line in the joined text
    let mut line_byte_starts: Vec<usize> = Vec::with_capacity(line_entries.len());
    let mut offset = 0;
    for (i, (_, line_text)) in line_entries.iter().enumerate() {
        line_byte_starts.push(offset);
        offset += line_text.len();
        if i + 1 < line_entries.len() {
            offset += 1; // newline
        }
    }

    // Create result indexed by 1-based line number
    let mut result: Vec<Vec<Color>> = vec![Vec::new(); max_line + 1];

    for (entry_idx, &(line_num, line_text)) in line_entries.iter().enumerate() {
        let char_count = line_text.chars().count();
        if char_count == 0 {
            continue;
        }
        let line_start_byte = line_byte_starts[entry_idx];
        let line_end_byte = line_start_byte + line_text.len();

        // Build per-char color array by mapping char indices to byte offsets
        let mut colors = vec![default_color; char_count];
        let char_byte_offsets: Vec<usize> = line_text.char_indices().map(|(i, _)| i).collect();

        for &(span_start, span_end, color) in spans {
            if span_end <= line_start_byte || span_start >= line_end_byte {
                continue;
            }
            let rel_start = span_start.saturating_sub(line_start_byte);
            let rel_end = span_end
                .saturating_sub(line_start_byte)
                .min(line_text.len());

            for (char_idx, &byte_off) in char_byte_offsets.iter().enumerate() {
                if byte_off >= rel_start && byte_off < rel_end {
                    colors[char_idx] = color;
                }
            }
        }

        if line_num <= max_line {
            result[line_num] = colors;
        }
    }

    result
}

/// Compute side-by-side diff lines using git diff.
fn compute_diff(repo_root: &Path, file_path: &str, base_branch: &str) -> Vec<DiffPair> {
    // Get merge base
    let base_ref = format!("origin/{}", base_branch);
    let merge_base = Command::new("git")
        .args(["merge-base", &base_ref, "HEAD"])
        .current_dir(repo_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or(base_ref);

    // Get unified diff with full context (comparing merge-base with working tree)
    let output = Command::new("git")
        .args([
            "diff",
            "--no-color",
            "-U999999",
            &merge_base,
            "--",
            file_path,
        ])
        .current_dir(repo_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();

    let diff_text = match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => {
            // Fallback: try to read the current file and show it all as "added"
            return fallback_new_file(repo_root, file_path);
        }
    };

    if diff_text.is_empty() {
        // No diff — file is identical or doesn't exist
        return fallback_new_file(repo_root, file_path);
    }

    parse_unified_diff(&diff_text)
}

/// Parse unified diff output into side-by-side line pairs.
fn parse_unified_diff(diff_text: &str) -> Vec<DiffPair> {
    let mut raw_lines: Vec<RawDiffLine> = Vec::new();
    let mut in_hunk = false;

    for line in diff_text.lines() {
        if line.starts_with("@@") {
            in_hunk = true;
            continue;
        }
        if !in_hunk {
            continue; // skip diff header (---, +++, etc.)
        }

        if let Some(rest) = line.strip_prefix('+') {
            raw_lines.push(RawDiffLine::Added(rest.to_string()));
        } else if let Some(rest) = line.strip_prefix('-') {
            raw_lines.push(RawDiffLine::Deleted(rest.to_string()));
        } else if let Some(rest) = line.strip_prefix(' ') {
            raw_lines.push(RawDiffLine::Context(rest.to_string()));
        } else if line == "\\ No newline at end of file" {
            continue;
        } else {
            // Lines without prefix in unified diff (shouldn't happen with -U999999)
            raw_lines.push(RawDiffLine::Context(line.to_string()));
        }
    }

    // Convert raw lines to side-by-side pairs, pairing consecutive deletes+adds as "changed"
    let mut pairs = Vec::new();
    let mut left_num: usize = 0;
    let mut right_num: usize = 0;
    let mut i = 0;

    while i < raw_lines.len() {
        match &raw_lines[i] {
            RawDiffLine::Context(text) => {
                left_num += 1;
                right_num += 1;
                pairs.push(DiffPair {
                    left: Some(NumberedLine {
                        num: left_num,
                        text: text.clone(),
                    }),
                    right: Some(NumberedLine {
                        num: right_num,
                        text: text.clone(),
                    }),
                    kind: DiffKind::Equal,
                });
                i += 1;
            }
            RawDiffLine::Deleted(_) => {
                // Collect consecutive deletes and adds
                let del_start = i;
                while i < raw_lines.len() && matches!(raw_lines[i], RawDiffLine::Deleted(_)) {
                    i += 1;
                }
                let add_start = i;
                while i < raw_lines.len() && matches!(raw_lines[i], RawDiffLine::Added(_)) {
                    i += 1;
                }

                let dels = &raw_lines[del_start..add_start];
                let adds = &raw_lines[add_start..i];

                let max_len = dels.len().max(adds.len());
                for j in 0..max_len {
                    let left = if j < dels.len() {
                        left_num += 1;
                        Some(NumberedLine {
                            num: left_num,
                            text: dels[j].text().to_string(),
                        })
                    } else {
                        None
                    };
                    let right = if j < adds.len() {
                        right_num += 1;
                        Some(NumberedLine {
                            num: right_num,
                            text: adds[j].text().to_string(),
                        })
                    } else {
                        None
                    };

                    let kind = match (&left, &right) {
                        (Some(_), Some(_)) => DiffKind::Changed,
                        (Some(_), None) => DiffKind::Deleted,
                        (None, Some(_)) => DiffKind::Added,
                        (None, None) => unreachable!(),
                    };

                    pairs.push(DiffPair { left, right, kind });
                }
            }
            RawDiffLine::Added(text) => {
                // Pure addition (no preceding delete)
                right_num += 1;
                pairs.push(DiffPair {
                    left: None,
                    right: Some(NumberedLine {
                        num: right_num,
                        text: text.clone(),
                    }),
                    kind: DiffKind::Added,
                });
                i += 1;
            }
        }
    }

    pairs
}

enum RawDiffLine {
    Context(String),
    Added(String),
    Deleted(String),
}

impl RawDiffLine {
    fn text(&self) -> &str {
        match self {
            Self::Context(t) | Self::Added(t) | Self::Deleted(t) => t,
        }
    }
}

/// Create a DiffViewerState from pre-computed pairs (for testing and direct construction).
impl DiffViewerState {
    #[cfg(test)]
    fn from_pairs(path: &str, lines: Vec<DiffPair>) -> Self {
        let added_count = lines.iter().filter(|l| l.kind == DiffKind::Added).count();
        let deleted_count = lines.iter().filter(|l| l.kind == DiffKind::Deleted).count();
        let changed_count = lines.iter().filter(|l| l.kind == DiffKind::Changed).count();
        let max_line_num = lines.len();
        Self {
            path: path.to_string(),
            lines,
            cursor: 0,
            cursor_col: 0,
            cursor_side: DiffSide::Right,
            scroll: 0,
            visible_lines: 20,
            added_count,
            deleted_count,
            changed_count,
            max_line_num,
            syntax_colors_left: Vec::new(),
            syntax_colors_right: Vec::new(),
            selection_anchor: None,
            search_input: None,
            search_query: None,
            search_matches: Vec::new(),
        }
    }
}

/// Fallback when git diff fails: read current file and show all lines as "added".
fn fallback_new_file(repo_root: &Path, file_path: &str) -> Vec<DiffPair> {
    let full_path = repo_root.join(file_path);
    let content = std::fs::read_to_string(&full_path).unwrap_or_default();
    content
        .lines()
        .enumerate()
        .map(|(i, line)| DiffPair {
            left: None,
            right: Some(NumberedLine {
                num: i + 1,
                text: line.to_string(),
            }),
            kind: DiffKind::Added,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(pairs: &[DiffPair]) -> Vec<DiffKind> {
        pairs.iter().map(|p| p.kind).collect()
    }

    fn left_nums(pairs: &[DiffPair]) -> Vec<Option<usize>> {
        pairs
            .iter()
            .map(|p| p.left.as_ref().map(|n| n.num))
            .collect()
    }

    fn right_nums(pairs: &[DiffPair]) -> Vec<Option<usize>> {
        pairs
            .iter()
            .map(|p| p.right.as_ref().map(|n| n.num))
            .collect()
    }

    fn left_texts(pairs: &[DiffPair]) -> Vec<Option<&str>> {
        pairs
            .iter()
            .map(|p| p.left.as_ref().map(|n| n.text.as_str()))
            .collect()
    }

    fn right_texts(pairs: &[DiffPair]) -> Vec<Option<&str>> {
        pairs
            .iter()
            .map(|p| p.right.as_ref().map(|n| n.text.as_str()))
            .collect()
    }

    // --- parse_unified_diff tests ---

    #[test]
    fn parse_empty_diff() {
        let pairs = parse_unified_diff("");
        assert!(pairs.is_empty());
    }

    #[test]
    fn parse_context_only() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,3 +1,3 @@
 line one
 line two
 line three
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 3);
        assert_eq!(kinds(&pairs), vec![DiffKind::Equal; 3]);
        assert_eq!(left_nums(&pairs), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(right_nums(&pairs), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(left_texts(&pairs)[0], Some("line one"));
    }

    #[test]
    fn parse_pure_additions() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,2 +1,4 @@
 line one
+added one
+added two
 line two
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 4);
        assert_eq!(
            kinds(&pairs),
            vec![
                DiffKind::Equal,
                DiffKind::Added,
                DiffKind::Added,
                DiffKind::Equal,
            ]
        );
        // Added lines have no left side
        assert_eq!(left_nums(&pairs)[1], None);
        assert_eq!(left_nums(&pairs)[2], None);
        assert_eq!(right_texts(&pairs)[1], Some("added one"));
        assert_eq!(right_texts(&pairs)[2], Some("added two"));
        // Line numbers: left 1, (skip, skip), 2; right 1, 2, 3, 4
        assert_eq!(left_nums(&pairs), vec![Some(1), None, None, Some(2)]);
        assert_eq!(right_nums(&pairs), vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn parse_pure_deletions() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,4 +1,2 @@
 line one
-deleted one
-deleted two
 line two
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 4);
        assert_eq!(
            kinds(&pairs),
            vec![
                DiffKind::Equal,
                DiffKind::Deleted,
                DiffKind::Deleted,
                DiffKind::Equal,
            ]
        );
        assert_eq!(right_nums(&pairs)[1], None);
        assert_eq!(right_nums(&pairs)[2], None);
        assert_eq!(left_texts(&pairs)[1], Some("deleted one"));
    }

    #[test]
    fn parse_change_pairs_delete_then_add() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,3 +1,3 @@
 before
-old line
+new line
 after
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 3);
        assert_eq!(
            kinds(&pairs),
            vec![DiffKind::Equal, DiffKind::Changed, DiffKind::Equal]
        );
        assert_eq!(left_texts(&pairs)[1], Some("old line"));
        assert_eq!(right_texts(&pairs)[1], Some("new line"));
    }

    #[test]
    fn parse_multi_line_change() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,4 +1,5 @@
 ctx
-old1
-old2
+new1
+new2
+new3
 ctx
";
        let pairs = parse_unified_diff(diff);
        // ctx, Changed(old1/new1), Changed(old2/new2), Added(new3), ctx
        assert_eq!(pairs.len(), 5);
        assert_eq!(kinds(&pairs)[0], DiffKind::Equal);
        assert_eq!(kinds(&pairs)[1], DiffKind::Changed);
        assert_eq!(kinds(&pairs)[2], DiffKind::Changed);
        assert_eq!(kinds(&pairs)[3], DiffKind::Added); // extra add, no matching delete
        assert_eq!(kinds(&pairs)[4], DiffKind::Equal);
        assert_eq!(left_texts(&pairs)[1], Some("old1"));
        assert_eq!(right_texts(&pairs)[1], Some("new1"));
        assert_eq!(left_texts(&pairs)[3], None); // no left for the unmatched add
        assert_eq!(right_texts(&pairs)[3], Some("new3"));
    }

    #[test]
    fn parse_more_deletes_than_adds() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,5 +1,3 @@
 ctx
-old1
-old2
-old3
+new1
 ctx
";
        let pairs = parse_unified_diff(diff);
        // ctx, Changed(old1/new1), Deleted(old2), Deleted(old3), ctx
        assert_eq!(pairs.len(), 5);
        assert_eq!(kinds(&pairs)[1], DiffKind::Changed);
        assert_eq!(kinds(&pairs)[2], DiffKind::Deleted);
        assert_eq!(kinds(&pairs)[3], DiffKind::Deleted);
    }

    #[test]
    fn parse_no_newline_marker_ignored() {
        let diff = "\
--- a/file.txt
+++ b/file.txt
@@ -1,2 +1,2 @@
-old
+new
\\ No newline at end of file
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 1);
        assert_eq!(kinds(&pairs)[0], DiffKind::Changed);
    }

    #[test]
    fn parse_new_file_all_additions() {
        let diff = "\
--- /dev/null
+++ b/new_file.rs
@@ -0,0 +1,3 @@
+line one
+line two
+line three
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 3);
        assert!(pairs.iter().all(|p| p.kind == DiffKind::Added));
        assert_eq!(right_nums(&pairs), vec![Some(1), Some(2), Some(3)]);
        assert!(pairs.iter().all(|p| p.left.is_none()));
    }

    #[test]
    fn parse_deleted_file_all_deletions() {
        let diff = "\
--- a/old_file.rs
+++ /dev/null
@@ -1,2 +0,0 @@
-line one
-line two
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|p| p.kind == DiffKind::Deleted));
        assert_eq!(left_nums(&pairs), vec![Some(1), Some(2)]);
        assert!(pairs.iter().all(|p| p.right.is_none()));
    }

    // --- DiffViewerState navigation tests ---

    fn make_viewer(kinds_list: &[DiffKind]) -> DiffViewerState {
        let lines: Vec<DiffPair> = kinds_list
            .iter()
            .enumerate()
            .map(|(i, &kind)| DiffPair {
                left: Some(NumberedLine {
                    num: i + 1,
                    text: format!("line {}", i + 1),
                }),
                right: Some(NumberedLine {
                    num: i + 1,
                    text: format!("line {}", i + 1),
                }),
                kind,
            })
            .collect();
        DiffViewerState::from_pairs("test.rs", lines)
    }

    #[test]
    fn cursor_down_and_up() {
        let mut v = make_viewer(&[DiffKind::Equal; 50]);
        v.visible_lines = 10;
        v.scroll_down(5);
        assert_eq!(v.cursor, 5);
        v.scroll_up(3);
        assert_eq!(v.cursor, 2);
        v.scroll_up(100); // clamps to 0
        assert_eq!(v.cursor, 0);
    }

    #[test]
    fn cursor_down_clamps_to_last() {
        let mut v = make_viewer(&[DiffKind::Equal; 20]);
        v.visible_lines = 10;
        v.scroll_down(100);
        assert_eq!(v.cursor, 19); // last line (0-indexed)
    }

    #[test]
    fn scroll_follows_cursor() {
        let mut v = make_viewer(&[DiffKind::Equal; 50]);
        v.visible_lines = 10;
        v.scroll_down(15);
        assert_eq!(v.cursor, 15);
        assert_eq!(v.scroll, 6); // cursor at 15, visible 10 → scroll = 15 - 10 + 1 = 6
    }

    #[test]
    fn cursor_to_top_and_bottom() {
        let mut v = make_viewer(&[DiffKind::Equal; 30]);
        v.visible_lines = 10;
        v.scroll_to_bottom();
        assert_eq!(v.cursor, 29);
        v.scroll_to_top();
        assert_eq!(v.cursor, 0);
        assert_eq!(v.scroll, 0);
    }

    #[test]
    fn page_up_and_down() {
        let mut v = make_viewer(&[DiffKind::Equal; 50]);
        v.visible_lines = 10;
        v.page_down();
        assert_eq!(v.cursor, 10);
        v.page_down();
        assert_eq!(v.cursor, 20);
        v.page_up();
        assert_eq!(v.cursor, 10);
    }

    #[test]
    fn next_change_skips_equal() {
        let mut v = make_viewer(&[
            DiffKind::Equal,
            DiffKind::Equal,
            DiffKind::Added,
            DiffKind::Equal,
            DiffKind::Deleted,
        ]);
        v.cursor = 0;
        v.next_change();
        assert_eq!(v.cursor, 2); // jumps to first Added
        v.next_change();
        assert_eq!(v.cursor, 4); // jumps to Deleted
    }

    #[test]
    fn next_change_at_end_stays() {
        let mut v = make_viewer(&[DiffKind::Equal, DiffKind::Added]);
        v.cursor = 1;
        v.next_change(); // no more changes after index 1
        assert_eq!(v.cursor, 1); // unchanged
    }

    #[test]
    fn prev_change_jumps_backward() {
        let mut v = make_viewer(&[
            DiffKind::Added,
            DiffKind::Equal,
            DiffKind::Equal,
            DiffKind::Deleted,
        ]);
        v.cursor = 3;
        v.prev_change();
        assert_eq!(v.cursor, 0); // jumps back to Added
    }

    #[test]
    fn prev_change_at_start_stays() {
        let mut v = make_viewer(&[DiffKind::Added, DiffKind::Equal]);
        v.cursor = 0;
        v.prev_change();
        assert_eq!(v.cursor, 0);
    }

    #[test]
    fn counts_correct() {
        let v = make_viewer(&[
            DiffKind::Equal,
            DiffKind::Added,
            DiffKind::Added,
            DiffKind::Deleted,
            DiffKind::Changed,
            DiffKind::Changed,
            DiffKind::Changed,
        ]);
        assert_eq!(v.added_count, 2);
        assert_eq!(v.deleted_count, 1);
        assert_eq!(v.changed_count, 3);
    }

    // --- Line number continuity tests ---

    #[test]
    fn line_numbers_continuous_after_additions() {
        let diff = "\
--- a/f
+++ b/f
@@ -1,2 +1,4 @@
 a
+x
+y
 b
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(left_nums(&pairs), vec![Some(1), None, None, Some(2)]);
        assert_eq!(right_nums(&pairs), vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn line_numbers_continuous_after_deletions() {
        let diff = "\
--- a/f
+++ b/f
@@ -1,4 +1,2 @@
 a
-x
-y
 b
";
        let pairs = parse_unified_diff(diff);
        assert_eq!(left_nums(&pairs), vec![Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(right_nums(&pairs), vec![Some(1), None, None, Some(2)]);
    }

    // --- Cursor movement tests ---

    #[test]
    fn cursor_left_wraps_to_prev_line() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.cursor = 1;
        v.cursor_col = 0;
        v.move_cursor_left();
        assert_eq!(v.cursor, 0);
        // cursor_col should be at end of previous line
    }

    #[test]
    fn cursor_left_at_start_stays() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.cursor = 0;
        v.cursor_col = 0;
        v.move_cursor_left();
        assert_eq!(v.cursor, 0);
        assert_eq!(v.cursor_col, 0);
    }

    #[test]
    fn cursor_right_wraps_to_next_line() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.cursor = 0;
        v.cursor_col = v.current_side_line_len(); // at end of line
        v.move_cursor_right();
        assert_eq!(v.cursor, 1);
        assert_eq!(v.cursor_col, 0);
    }

    #[test]
    fn cursor_right_at_end_stays() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.cursor = 2; // last line
        v.cursor_col = v.current_side_line_len();
        v.move_cursor_right();
        assert_eq!(v.cursor, 2);
    }

    #[test]
    fn switch_side_toggles() {
        let mut v = make_viewer(&[DiffKind::Equal; 2]);
        assert_eq!(v.cursor_side, DiffSide::Right);
        v.switch_side();
        assert_eq!(v.cursor_side, DiffSide::Left);
        v.switch_side();
        assert_eq!(v.cursor_side, DiffSide::Right);
    }

    #[test]
    fn switch_side_clamps_col() {
        let pairs = vec![DiffPair {
            left: Some(NumberedLine {
                num: 1,
                text: "ab".to_string(),
            }),
            right: Some(NumberedLine {
                num: 1,
                text: "abcdef".to_string(),
            }),
            kind: DiffKind::Changed,
        }];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.cursor_side = DiffSide::Right;
        v.cursor_col = 5; // past left side's length
        v.switch_side();
        assert_eq!(v.cursor_side, DiffSide::Left);
        assert_eq!(v.cursor_col, 2); // clamped to "ab".len()
    }

    #[test]
    fn cursor_home_and_end() {
        let mut v = make_viewer(&[DiffKind::Equal; 2]);
        v.cursor_col = 3;
        v.cursor_home();
        assert_eq!(v.cursor_col, 0);
        v.cursor_end();
        assert!(v.cursor_col > 0); // at end of "line 1"
    }

    #[test]
    fn current_line_returns_right_side() {
        let pairs = vec![DiffPair {
            left: Some(NumberedLine {
                num: 10,
                text: "a".to_string(),
            }),
            right: Some(NumberedLine {
                num: 20,
                text: "b".to_string(),
            }),
            kind: DiffKind::Changed,
        }];
        let v = DiffViewerState::from_pairs("t", pairs);
        assert_eq!(v.current_line(), 20); // right side preferred
    }

    // --- Hunk-level navigation tests ---

    #[test]
    fn next_change_skips_whole_hunk() {
        let mut v = make_viewer(&[
            DiffKind::Added,
            DiffKind::Added,
            DiffKind::Equal,
            DiffKind::Deleted,
        ]);
        v.cursor = 0; // in first hunk
        v.next_change();
        assert_eq!(v.cursor, 3); // skips to second hunk, not line 1
    }

    #[test]
    fn prev_change_skips_to_hunk_start() {
        let mut v = make_viewer(&[
            DiffKind::Added,
            DiffKind::Added,
            DiffKind::Equal,
            DiffKind::Deleted,
            DiffKind::Deleted,
        ]);
        v.cursor = 4; // second line of second hunk
        v.prev_change();
        assert_eq!(v.cursor, 0); // start of first hunk
    }

    // --- Selection tests ---

    #[test]
    fn selection_empty_by_default() {
        let v = make_viewer(&[DiffKind::Equal; 3]);
        assert!(v.selection_range().is_none());
    }

    #[test]
    fn select_right_creates_selection() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.cursor_col = 0;
        v.select_right();
        assert!(v.selection_range().is_some());
        let (sl, sc, el, ec, _) = v.selection_range().unwrap();
        assert_eq!((sl, sc), (0, 0));
        assert_eq!((el, ec), (0, 1));
    }

    #[test]
    fn select_down_extends_to_next_line() {
        let mut v = make_viewer(&[DiffKind::Equal; 5]);
        v.cursor = 1;
        v.cursor_col = 2;
        v.select_down();
        let (sl, sc, el, _ec, _) = v.selection_range().unwrap();
        assert_eq!((sl, sc), (1, 2));
        assert_eq!(el, 2);
    }

    #[test]
    fn clear_selection_works() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.select_right();
        assert!(v.selection_range().is_some());
        v.clear_selection();
        assert!(v.selection_range().is_none());
    }

    #[test]
    fn selection_cleared_on_side_switch() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.select_right();
        assert!(v.selection_range().is_some());
        // side switch via anchor side mismatch
        v.cursor_side = DiffSide::Left;
        assert!(v.selection_range().is_none()); // different side = no range
    }

    #[test]
    fn selected_text_single_line() {
        let pairs = vec![DiffPair {
            left: None,
            right: Some(NumberedLine {
                num: 1,
                text: "hello world".to_string(),
            }),
            kind: DiffKind::Added,
        }];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.cursor_side = DiffSide::Right;
        v.cursor_col = 0;
        v.selection_anchor = Some((0, 0, DiffSide::Right));
        v.cursor_col = 5;
        assert_eq!(v.selected_text(), Some("hello".to_string()));
    }

    // --- Search tests ---

    #[test]
    fn search_finds_matches() {
        let pairs = vec![
            DiffPair {
                left: Some(NumberedLine {
                    num: 1,
                    text: "foo bar".to_string(),
                }),
                right: Some(NumberedLine {
                    num: 1,
                    text: "foo baz".to_string(),
                }),
                kind: DiffKind::Changed,
            },
            DiffPair {
                left: Some(NumberedLine {
                    num: 2,
                    text: "no match".to_string(),
                }),
                right: Some(NumberedLine {
                    num: 2,
                    text: "foo again".to_string(),
                }),
                kind: DiffKind::Changed,
            },
        ];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.search("foo");
        assert_eq!(v.search_matches.len(), 3); // left:0, right:0, right:1
    }

    #[test]
    fn search_case_insensitive() {
        let pairs = vec![DiffPair {
            left: None,
            right: Some(NumberedLine {
                num: 1,
                text: "Hello World".to_string(),
            }),
            kind: DiffKind::Added,
        }];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.search("hello");
        assert_eq!(v.search_matches.len(), 1);
    }

    #[test]
    fn search_empty_query_no_matches() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.search("");
        assert!(v.search_matches.is_empty());
    }

    #[test]
    fn search_next_wraps_around() {
        let pairs = vec![
            DiffPair {
                left: None,
                right: Some(NumberedLine {
                    num: 1,
                    text: "match".to_string(),
                }),
                kind: DiffKind::Added,
            },
            DiffPair {
                left: None,
                right: Some(NumberedLine {
                    num: 2,
                    text: "no".to_string(),
                }),
                kind: DiffKind::Added,
            },
        ];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.search("match");
        assert_eq!(v.search_matches.len(), 1);
        v.cursor = 1; // past the match
        v.cursor_col = 0;
        let found = v.search_next();
        assert!(found);
        assert_eq!(v.cursor, 0); // wrapped back
    }

    #[test]
    fn search_prev_wraps_around() {
        let pairs = vec![
            DiffPair {
                left: None,
                right: Some(NumberedLine {
                    num: 1,
                    text: "no".to_string(),
                }),
                kind: DiffKind::Added,
            },
            DiffPair {
                left: None,
                right: Some(NumberedLine {
                    num: 2,
                    text: "match".to_string(),
                }),
                kind: DiffKind::Added,
            },
        ];
        let mut v = DiffViewerState::from_pairs("t", pairs);
        v.search("match");
        v.cursor = 0;
        v.cursor_col = 0;
        let found = v.search_prev();
        assert!(found);
        assert_eq!(v.cursor, 1); // wrapped to end
    }

    #[test]
    fn clear_search_removes_matches() {
        let mut v = make_viewer(&[DiffKind::Equal; 3]);
        v.search("line");
        assert!(!v.search_matches.is_empty());
        v.clear_search();
        assert!(v.search_matches.is_empty());
        assert!(v.search_query.is_none());
    }
}
