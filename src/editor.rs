use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write as IoWrite};
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::app::SearchDirection;
use crate::syntax::SyntaxHighlighter;

#[derive(Clone)]
pub struct SearchParams {
    pub query: String,
    pub direction: SearchDirection,
    pub case_sensitive: bool,
}

const INDEX_INTERVAL: usize = 1000;

pub struct EditorState {
    pub path: PathBuf,
    pub file_size: u64,

    // Line-level piece table
    segments: Vec<Segment>,

    // Sparse line index for original file
    line_index: Vec<u64>,
    lines_scanned: usize,
    scan_byte_offset: u64,
    pub scan_complete: bool,

    // Cursor (character positions)
    pub cursor_line: usize,
    pub cursor_col: usize,
    pub desired_col: usize,

    // Viewport
    pub scroll_y: usize,
    pub scroll_x: usize,
    pub visible_lines: usize,
    pub visible_cols: usize,
    /// Set by the renderer for mouse click coordinate conversion.
    pub viewport_x: u16,
    pub viewport_y: u16,
    pub line_num_width: usize,

    pub modified: bool,
    pub status_msg: Option<String>,

    /// Selection anchor (line, col). Selection spans from anchor to cursor.
    pub selection_anchor: Option<(usize, usize)>,
    /// True when the selection was set by a search (match highlight).
    /// The next user-initiated selection (Shift+arrow) will clear it
    /// and start a fresh selection from the cursor position.
    search_selection: bool,

    /// Last search parameters for find-next/find-previous.
    pub last_search: Option<SearchParams>,

    /// Syntax highlighter (None if language not detected).
    pub syntax: SyntaxHighlighter,

    /// Undo/redo stacks.
    undo_stack: Vec<UndoEntry>,
    redo_stack: Vec<UndoEntry>,

    /// If editing a remote file: (remote_path, panel_side) for upload-on-save.
    pub remote_source: Option<(PathBuf, usize)>,
}

/// Minimal operation-based undo entry. Stores what changed, not full line copies.
#[derive(Clone, Debug)]
enum UndoOp {
    /// Insert text at (line, col). Inverse: delete the same range.
    Insert {
        line: usize,
        col: usize,
        text: String,
    },
    /// Delete text at (line, col). Inverse: re-insert the text.
    Delete {
        line: usize,
        col: usize,
        text: String,
    },
    /// Split line at (line, col) into two lines. Inverse: join.
    SplitLine { line: usize, col: usize },
    /// Join line with the next line at (line). Inverse: split.
    JoinLine {
        line: usize,
        col: usize, // column where the join happened (= length of first line before join)
    },
    /// Delete an entire line. Inverse: re-insert.
    DeleteLine { line: usize, text: String },
    /// Clear a line (single-line file). Inverse: restore text.
    ClearLine { line: usize, text: String },
}

#[derive(Clone, Debug)]
struct UndoEntry {
    op: UndoOp,
    cursor_before: (usize, usize),
    cursor_after: (usize, usize),
    /// For grouping consecutive same-type ops.
    groupable: bool,
}

#[derive(Clone)]
enum Segment {
    Original { start_line: usize, count: usize },
    Buffer { lines: Vec<String> },
}

impl EditorState {
    pub fn open(path: PathBuf) -> Self {
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let syntax = SyntaxHighlighter::new(&path, ratatui::style::Color::LightCyan);
        let mut state = Self {
            path,
            file_size,
            segments: vec![Segment::Original {
                start_line: 0,
                count: usize::MAX / 2,
            }],
            line_index: vec![0],
            lines_scanned: 0,
            scan_byte_offset: 0,
            scan_complete: false,
            cursor_line: 0,
            cursor_col: 0,
            desired_col: 0,
            scroll_y: 0,
            scroll_x: 0,
            visible_lines: 0,
            visible_cols: 0,
            viewport_x: 0,
            viewport_y: 0,
            line_num_width: 7,
            modified: false,
            status_msg: None,
            selection_anchor: None,
            search_selection: false,
            last_search: None,
            syntax,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            remote_source: None,
        };
        // Pre-scan first batch of lines for immediate display
        state.scan_to_line(10_000);
        // Empty files need at least one editable line
        if state.file_size == 0 {
            state.segments = vec![Segment::Buffer {
                lines: vec![String::new()],
            }];
            state.scan_complete = true;
            state.lines_scanned = 0;
        }
        state
    }

    // --- Virtual line count ---

    pub fn total_virtual_lines(&self) -> usize {
        let mut total = 0;
        for seg in &self.segments {
            total += match seg {
                Segment::Original { start_line, count } => {
                    let available = self.lines_scanned.saturating_sub(*start_line);
                    (*count).min(available)
                }
                Segment::Buffer { lines } => lines.len(),
            };
        }
        total
    }

    // --- Segment resolution ---

    fn find_segment(&self, virtual_line: usize) -> Option<(usize, usize)> {
        let mut line = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            let count = match seg {
                Segment::Original { start_line, count } => {
                    let available = self.lines_scanned.saturating_sub(*start_line);
                    (*count).min(available)
                }
                Segment::Buffer { lines } => lines.len(),
            };
            if virtual_line < line + count {
                return Some((i, virtual_line - line));
            }
            line += count;
        }
        None
    }

    pub fn get_line_text(&mut self, virtual_line: usize) -> Option<String> {
        // Try to scan ahead if needed
        if !self.scan_complete && virtual_line >= self.total_virtual_lines() {
            self.scan_to_line(virtual_line + 100);
        }

        let (seg_idx, offset) = self.find_segment(virtual_line)?;
        match &self.segments[seg_idx] {
            Segment::Buffer { lines } => Some(lines[offset].clone()),
            Segment::Original { start_line, .. } => {
                let orig_line = start_line + offset;
                self.read_original_line(orig_line)
            }
        }
    }

    fn current_line_text(&mut self) -> String {
        self.get_line_text(self.cursor_line).unwrap_or_default()
    }

    fn current_line_len(&mut self) -> usize {
        self.current_line_text().chars().count()
    }

    /// Ensure the given virtual line is in a Buffer segment so it can be edited.
    fn materialize_line(&mut self, virtual_line: usize) {
        let (seg_idx, offset) = match self.find_segment(virtual_line) {
            Some(x) => x,
            None => return,
        };

        if matches!(self.segments[seg_idx], Segment::Buffer { .. }) {
            return;
        }

        let (start_line, count) = match self.segments[seg_idx] {
            Segment::Original { start_line, count } => (start_line, count),
            _ => unreachable!(),
        };

        let text = self
            .read_original_line(start_line + offset)
            .unwrap_or_default();

        let mut new_segs = Vec::with_capacity(3);
        if offset > 0 {
            new_segs.push(Segment::Original {
                start_line,
                count: offset,
            });
        }
        new_segs.push(Segment::Buffer { lines: vec![text] });
        let remaining = count - offset - 1;
        if remaining > 0 {
            new_segs.push(Segment::Original {
                start_line: start_line + offset + 1,
                count: remaining,
            });
        }

        self.segments.splice(seg_idx..seg_idx + 1, new_segs);
    }

    // --- Undo/Redo ---

    fn push_undo(&mut self, entry: UndoEntry) {
        self.redo_stack.clear();

        // Try to merge with top of undo stack (grouping consecutive inserts/deletes)
        if entry.groupable {
            if let Some(top) = self.undo_stack.last_mut() {
                if top.groupable && top.cursor_after == entry.cursor_before {
                    match (&mut top.op, &entry.op) {
                        // Merge consecutive inserts on same line
                        (
                            UndoOp::Insert {
                                line: l1, text: t1, ..
                            },
                            UndoOp::Insert {
                                line: l2, text: t2, ..
                            },
                        ) if l1 == l2 => {
                            // Break on whitespace
                            let last_was_ws = t1
                                .chars()
                                .last()
                                .map(|c| c.is_whitespace())
                                .unwrap_or(false);
                            let new_is_ws = t2.chars().any(|c| c.is_whitespace());
                            if !last_was_ws && !new_is_ws {
                                t1.push_str(t2);
                                top.cursor_after = entry.cursor_after;
                                return;
                            }
                        }
                        // Merge consecutive backward deletes on same line
                        (
                            UndoOp::Delete {
                                line: l1,
                                col: c1,
                                text: t1,
                            },
                            UndoOp::Delete {
                                line: l2,
                                col: c2,
                                text: t2,
                            },
                        ) if l1 == l2 => {
                            // Backward delete: new col is one less, prepend
                            if *c2 + t2.len() == *c1 {
                                t1.insert_str(0, t2);
                                *c1 = *c2;
                                top.cursor_after = entry.cursor_after;
                                return;
                            }
                            // Forward delete: same col, append
                            if *c1 == *c2 {
                                t1.push_str(t2);
                                top.cursor_after = entry.cursor_after;
                                return;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Cap undo stack at 10,000 entries
        if self.undo_stack.len() >= 10_000 {
            self.undo_stack.remove(0);
        }
        self.undo_stack.push(entry);
    }

    /// Insert a new virtual line at the given position.
    fn insert_virtual_line_at(&mut self, virtual_line: usize, text: String) {
        let total = self.total_virtual_lines();
        if total == 0 {
            self.segments.push(Segment::Buffer { lines: vec![text] });
            return;
        }
        if virtual_line >= total {
            let last = total - 1;
            self.materialize_line(last);
            let (seg_idx, offset) = self.find_segment(last).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines.insert(offset + 1, text);
            }
            return;
        }
        self.materialize_line(virtual_line);
        let (seg_idx, offset) = self.find_segment(virtual_line).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            lines.insert(offset, text);
        }
    }

    /// Apply an operation (used by redo).
    fn apply_op(&mut self, op: &UndoOp) {
        match op {
            UndoOp::Insert { line, col, text } => {
                self.materialize_line(*line);
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    let byte_pos = char_to_byte(&lines[offset], *col);
                    lines[offset].insert_str(byte_pos, text);
                }
            }
            UndoOp::Delete { line, col, text } => {
                self.materialize_line(*line);
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    let byte_start = char_to_byte(&lines[offset], *col);
                    let byte_end = char_to_byte(&lines[offset], *col + text.chars().count());
                    lines[offset].drain(byte_start..byte_end);
                }
            }
            UndoOp::SplitLine { line, col } => {
                self.materialize_line(*line);
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    let byte_pos = char_to_byte(&lines[offset], *col);
                    let rest = lines[offset][byte_pos..].to_string();
                    lines[offset].truncate(byte_pos);
                    lines.insert(offset + 1, rest);
                }
            }
            UndoOp::JoinLine { line, .. } => {
                self.materialize_line(*line);
                self.materialize_line(*line + 1);
                let next_text = self.get_line_text(*line + 1).unwrap_or_default();
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    lines[offset].push_str(&next_text);
                }
                self.remove_virtual_line(*line + 1);
            }
            UndoOp::DeleteLine { line, .. } => {
                self.remove_virtual_line(*line);
            }
            UndoOp::ClearLine { line, .. } => {
                self.materialize_line(*line);
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    lines[offset] = String::new();
                }
            }
        }
    }

    /// Apply the inverse of an operation (used by undo).
    fn apply_inverse(&mut self, op: &UndoOp) {
        match op {
            // Inverse of Insert = Delete
            UndoOp::Insert { line, col, text } => {
                self.apply_op(&UndoOp::Delete {
                    line: *line,
                    col: *col,
                    text: text.clone(),
                });
            }
            // Inverse of Delete = Insert
            UndoOp::Delete { line, col, text } => {
                self.apply_op(&UndoOp::Insert {
                    line: *line,
                    col: *col,
                    text: text.clone(),
                });
            }
            // Inverse of SplitLine = JoinLine
            UndoOp::SplitLine { line, col } => {
                self.apply_op(&UndoOp::JoinLine {
                    line: *line,
                    col: *col,
                });
            }
            // Inverse of JoinLine = SplitLine
            UndoOp::JoinLine { line, col } => {
                self.apply_op(&UndoOp::SplitLine {
                    line: *line,
                    col: *col,
                });
            }
            // Inverse of DeleteLine = re-insert
            UndoOp::DeleteLine { line, text } => {
                self.insert_virtual_line_at(*line, text.clone());
            }
            // Inverse of ClearLine = restore text
            UndoOp::ClearLine { line, text } => {
                self.materialize_line(*line);
                let (seg_idx, offset) = self.find_segment(*line).unwrap();
                if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                    lines[offset] = text.clone();
                }
            }
        }
    }

    pub fn undo(&mut self) {
        let entry = match self.undo_stack.pop() {
            Some(e) => e,
            None => return,
        };

        self.apply_inverse(&entry.op);
        self.cursor_line = entry.cursor_before.0;
        self.cursor_col = entry.cursor_before.1;
        self.desired_col = self.cursor_col;
        self.mark_modified();
        self.scroll_to_cursor();

        self.redo_stack.push(entry);
    }

    pub fn redo(&mut self) {
        let entry = match self.redo_stack.pop() {
            Some(e) => e,
            None => return,
        };

        self.apply_op(&entry.op);
        self.cursor_line = entry.cursor_after.0;
        self.cursor_col = entry.cursor_after.1;
        self.desired_col = self.cursor_col;
        self.mark_modified();
        self.scroll_to_cursor();

        self.undo_stack.push(entry);
    }

    fn mark_modified(&mut self) {
        if !self.modified {
            self.syntax.invalidate_cache();
        }
        self.modified = true;
    }

    // --- Editing operations ---

    pub fn insert_char(&mut self, c: char) {
        let vline = self.cursor_line;
        let cursor_before = (self.cursor_line, self.cursor_col);

        self.materialize_line(vline);
        let (seg_idx, offset) = self.find_segment(vline).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            let line = &mut lines[offset];
            let byte_pos = char_to_byte(line, self.cursor_col);
            line.insert(byte_pos, c);
            self.cursor_col += 1;
            self.desired_col = self.cursor_col;
        }

        let mut s = String::new();
        s.push(c);
        self.push_undo(UndoEntry {
            op: UndoOp::Insert {
                line: vline,
                col: cursor_before.1,
                text: s,
            },
            cursor_before,
            cursor_after: (self.cursor_line, self.cursor_col),
            groupable: true,
        });
        self.mark_modified();
        self.scroll_to_cursor();
    }

    pub fn delete_char_backward(&mut self) {
        let cursor_before = (self.cursor_line, self.cursor_col);

        if self.cursor_col == 0 {
            if self.cursor_line == 0 {
                return;
            }
            let prev = self.cursor_line - 1;
            let prev_text = self.get_line_text(prev).unwrap_or_default();
            let new_col = prev_text.chars().count();

            self.materialize_line(prev);
            self.materialize_line(self.cursor_line);
            let next_text = self.get_line_text(self.cursor_line).unwrap_or_default();
            let (seg_idx, offset) = self.find_segment(prev).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset].push_str(&next_text);
            }
            self.remove_virtual_line(self.cursor_line);

            self.cursor_line = prev;
            self.cursor_col = new_col;
            self.desired_col = self.cursor_col;

            self.push_undo(UndoEntry {
                op: UndoOp::JoinLine {
                    line: prev,
                    col: new_col,
                },
                cursor_before,
                cursor_after: (self.cursor_line, self.cursor_col),
                groupable: false,
            });
            self.mark_modified();
            self.scroll_to_cursor();
            return;
        }

        let vline = self.cursor_line;
        self.materialize_line(vline);
        let deleted_char = {
            let (seg_idx, offset) = self.find_segment(vline).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                let line = &mut lines[offset];
                let byte_pos = char_to_byte(line, self.cursor_col);
                let prev_byte = char_to_byte(line, self.cursor_col - 1);
                let deleted: String = line[prev_byte..byte_pos].to_string();
                line.drain(prev_byte..byte_pos);
                self.cursor_col -= 1;
                self.desired_col = self.cursor_col;
                deleted
            } else {
                return;
            }
        };

        self.push_undo(UndoEntry {
            op: UndoOp::Delete {
                line: vline,
                col: self.cursor_col,
                text: deleted_char,
            },
            cursor_before,
            cursor_after: (self.cursor_line, self.cursor_col),
            groupable: true,
        });
        self.mark_modified();
    }

    pub fn delete_char_forward(&mut self) {
        let cursor_before = (self.cursor_line, self.cursor_col);
        let line_len = self.current_line_len();

        if self.cursor_col >= line_len {
            let next = self.cursor_line + 1;
            if next >= self.total_virtual_lines() {
                return;
            }
            let col = self.current_line_len();

            self.materialize_line(self.cursor_line);
            self.materialize_line(next);
            let next_text = self.get_line_text(next).unwrap_or_default();
            let (seg_idx, offset) = self.find_segment(self.cursor_line).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset].push_str(&next_text);
            }
            self.remove_virtual_line(next);

            self.push_undo(UndoEntry {
                op: UndoOp::JoinLine {
                    line: self.cursor_line,
                    col,
                },
                cursor_before,
                cursor_after: cursor_before,
                groupable: false,
            });
            self.mark_modified();
            return;
        }

        let vline = self.cursor_line;
        self.materialize_line(vline);
        let deleted_char = {
            let (seg_idx, offset) = self.find_segment(vline).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                let line = &mut lines[offset];
                let byte_start = char_to_byte(line, self.cursor_col);
                let byte_end = char_to_byte(line, self.cursor_col + 1);
                let deleted: String = line[byte_start..byte_end].to_string();
                line.drain(byte_start..byte_end);
                deleted
            } else {
                return;
            }
        };

        self.push_undo(UndoEntry {
            op: UndoOp::Delete {
                line: vline,
                col: self.cursor_col,
                text: deleted_char,
            },
            cursor_before,
            cursor_after: cursor_before,
            groupable: true,
        });
        self.mark_modified();
    }

    pub fn insert_newline(&mut self) {
        let vline = self.cursor_line;
        let cursor_before = (self.cursor_line, self.cursor_col);

        self.materialize_line(vline);
        let (seg_idx, offset) = self.find_segment(vline).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            let line = &mut lines[offset];
            let byte_pos = char_to_byte(line, self.cursor_col);
            let rest = line[byte_pos..].to_string();
            line.truncate(byte_pos);
            lines.insert(offset + 1, rest);
        }

        self.cursor_line += 1;
        self.cursor_col = 0;
        self.desired_col = 0;

        self.push_undo(UndoEntry {
            op: UndoOp::SplitLine {
                line: vline,
                col: cursor_before.1,
            },
            cursor_before,
            cursor_after: (self.cursor_line, self.cursor_col),
            groupable: false,
        });
        self.mark_modified();
        self.scroll_to_cursor();
    }

    pub fn delete_line(&mut self) {
        let total = self.total_virtual_lines();
        if total == 0 {
            return;
        }

        let cursor_before = (self.cursor_line, self.cursor_col);
        let line_text = self.get_line_text(self.cursor_line).unwrap_or_default();
        let vline = self.cursor_line;

        if total == 1 {
            self.materialize_line(0);
            let (seg_idx, offset) = self.find_segment(0).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset] = String::new();
            }
            self.cursor_col = 0;
            self.desired_col = 0;
            self.push_undo(UndoEntry {
                op: UndoOp::ClearLine {
                    line: 0,
                    text: line_text,
                },
                cursor_before,
                cursor_after: (0, 0),
                groupable: false,
            });
            self.mark_modified();
            return;
        }

        self.remove_virtual_line(self.cursor_line);

        if self.cursor_line >= self.total_virtual_lines() {
            self.cursor_line = self.total_virtual_lines().saturating_sub(1);
        }
        self.clamp_cursor_col();

        self.push_undo(UndoEntry {
            op: UndoOp::DeleteLine {
                line: vline,
                text: line_text,
            },
            cursor_before,
            cursor_after: (self.cursor_line, self.cursor_col),
            groupable: false,
        });
        self.mark_modified();
        self.scroll_to_cursor();
    }

    fn remove_virtual_line(&mut self, virtual_line: usize) {
        self.materialize_line(virtual_line);
        let (seg_idx, offset) = match self.find_segment(virtual_line) {
            Some(x) => x,
            None => return,
        };
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            lines.remove(offset);
            if lines.is_empty() {
                self.segments.remove(seg_idx);
            }
        }
    }

    // --- Cursor movement ---

    pub fn cursor_up(&mut self) {
        if self.cursor_line > 0 {
            self.cursor_line -= 1;
            self.restore_desired_col();
            self.scroll_to_cursor();
        }
    }

    pub fn cursor_down(&mut self) {
        let target = self.cursor_line + 1;
        if !self.scan_complete && target >= self.total_virtual_lines() {
            self.scan_to_line(target + 100);
        }
        if target < self.total_virtual_lines() {
            self.cursor_line = target;
            self.restore_desired_col();
            self.scroll_to_cursor();
        }
    }

    pub fn cursor_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor_line > 0 {
            self.cursor_line -= 1;
            self.cursor_col = self.current_line_len();
        }
        self.desired_col = self.cursor_col;
        self.scroll_to_cursor();
    }

    pub fn cursor_right(&mut self) {
        let len = self.current_line_len();
        if self.cursor_col < len {
            self.cursor_col += 1;
        } else if self.cursor_line + 1 < self.total_virtual_lines() {
            self.cursor_line += 1;
            self.cursor_col = 0;
        }
        self.desired_col = self.cursor_col;
        self.scroll_to_cursor();
    }

    pub fn word_left(&mut self) {
        if self.cursor_col == 0 {
            // Jump to end of previous line
            if self.cursor_line > 0 {
                self.cursor_line -= 1;
                self.cursor_col = self.current_line_len();
            }
            self.desired_col = self.cursor_col;
            self.scroll_to_cursor();
            return;
        }
        let text = self.current_line_text();
        let chars: Vec<char> = text.chars().collect();
        let mut col = self.cursor_col;
        // Skip whitespace/punctuation backwards
        while col > 0 && !chars[col - 1].is_alphanumeric() {
            col -= 1;
        }
        // Skip word characters backwards
        while col > 0 && chars[col - 1].is_alphanumeric() {
            col -= 1;
        }
        self.cursor_col = col;
        self.desired_col = col;
        self.scroll_to_cursor();
    }

    pub fn word_right(&mut self) {
        let text = self.current_line_text();
        let chars: Vec<char> = text.chars().collect();
        let len = chars.len();
        if self.cursor_col >= len {
            // Jump to start of next line
            if self.cursor_line + 1 < self.total_virtual_lines() {
                self.cursor_line += 1;
                self.cursor_col = 0;
            }
            self.desired_col = self.cursor_col;
            self.scroll_to_cursor();
            return;
        }
        let mut col = self.cursor_col;
        // Skip word characters forward
        while col < len && chars[col].is_alphanumeric() {
            col += 1;
        }
        // Skip whitespace/punctuation forward
        while col < len && !chars[col].is_alphanumeric() {
            col += 1;
        }
        self.cursor_col = col;
        self.desired_col = col;
        self.scroll_to_cursor();
    }

    pub fn cursor_line_start(&mut self) {
        self.cursor_col = 0;
        self.desired_col = 0;
        self.scroll_to_cursor();
    }

    pub fn cursor_line_end(&mut self) {
        self.cursor_col = self.current_line_len();
        self.desired_col = self.cursor_col;
        self.scroll_to_cursor();
    }

    pub fn page_up(&mut self) {
        let page = self.visible_lines.max(1);
        self.cursor_line = self.cursor_line.saturating_sub(page);
        self.restore_desired_col();
        self.scroll_to_cursor();
    }

    pub fn page_down(&mut self) {
        let page = self.visible_lines.max(1);
        let target = self.cursor_line + page;
        if !self.scan_complete && target >= self.total_virtual_lines() {
            self.scan_to_line(target + 100);
        }
        self.cursor_line = target.min(self.total_virtual_lines().saturating_sub(1));
        self.restore_desired_col();
        self.scroll_to_cursor();
    }

    pub fn goto_top(&mut self) {
        self.cursor_line = 0;
        self.restore_desired_col();
        self.scroll_to_cursor();
    }

    pub fn goto_bottom(&mut self) {
        self.scan_to_end();
        self.cursor_line = self.total_virtual_lines().saturating_sub(1);
        self.restore_desired_col();
        self.scroll_to_cursor();
    }

    pub fn clamp_cursor_col(&mut self) {
        let len = self.current_line_len();
        if self.cursor_col > len {
            self.cursor_col = len;
        }
    }

    fn restore_desired_col(&mut self) {
        let len = self.current_line_len();
        self.cursor_col = self.desired_col.min(len);
    }

    /// Convert a screen click (col, row) to an editor (line, col) and move cursor there.
    pub fn click_at(&mut self, screen_col: u16, screen_row: u16) {
        let text_x = self.viewport_x as usize + self.line_num_width;
        if (screen_col as usize) < text_x {
            return; // clicked on line number gutter
        }
        if screen_row < self.viewport_y {
            return;
        }

        let row_offset = (screen_row - self.viewport_y) as usize;
        let target_line = self.scroll_y + row_offset;
        if target_line >= self.total_virtual_lines() {
            return;
        }

        let display_col = (screen_col as usize) - text_x;

        // Convert display column to original char column (reverse of orig_col_to_display_col)
        let orig_col = if let Some(text) = self.get_line_text(target_line) {
            display_col_to_orig_col(&text, display_col, self.scroll_x)
        } else {
            0
        };

        self.clear_selection();
        self.cursor_line = target_line;
        self.cursor_col = orig_col;
        self.desired_col = orig_col;
    }

    pub fn scroll_to_cursor(&mut self) {
        // Vertical
        if self.cursor_line < self.scroll_y {
            self.scroll_y = self.cursor_line;
        }
        if self.visible_lines > 0 && self.cursor_line >= self.scroll_y + self.visible_lines {
            self.scroll_y = self.cursor_line - self.visible_lines + 1;
        }
        // Horizontal
        if self.cursor_col < self.scroll_x {
            self.scroll_x = self.cursor_col;
        }
        if self.visible_cols > 0 && self.cursor_col >= self.scroll_x + self.visible_cols {
            self.scroll_x = self.cursor_col - self.visible_cols + 1;
        }
    }

    // --- Search ---
    //
    // Streams raw file bytes for Original segments (single open, large-buffer reads).
    // Buffer segments are searched in-memory. No per-line file I/O.

    pub fn find(&mut self, params: &SearchParams) -> bool {
        if params.query.is_empty() {
            return false;
        }
        // Only scan enough to know where the cursor is (not the whole file).
        // Segments beyond what we've scanned will trigger lazy scanning.
        let query_bytes: Vec<u8> = if params.case_sensitive {
            params.query.as_bytes().to_vec()
        } else {
            params.query.to_lowercase().into_bytes()
        };
        match params.direction {
            SearchDirection::Forward => {
                self.find_streaming(&query_bytes, params.case_sensitive, false)
            }
            SearchDirection::Backward => {
                self.find_streaming(&query_bytes, params.case_sensitive, true)
            }
        }
    }

    /// Build a plan of (segment_index, virtual_line_start, line_count) for searching.
    /// Does NOT scan to end — uses lines_scanned as the count for Original segments.
    /// The byte-level search uses file_size as the end byte for segments that extend
    /// past lines_scanned, so the full file is still searched.
    fn build_search_plan(&mut self) -> Vec<(usize, usize, usize)> {
        // Only scan enough to cover the cursor (which should already be scanned)
        if !self.scan_complete && self.cursor_line >= self.lines_scanned {
            self.scan_to_line(self.cursor_line + 100);
        }
        let mut plan = Vec::with_capacity(self.segments.len());
        let mut vline = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            let count = match seg {
                Segment::Original { start_line, count } => {
                    let available = self.lines_scanned.saturating_sub(*start_line);
                    (*count).min(available)
                }
                Segment::Buffer { lines } => lines.len(),
            };
            if count > 0 {
                plan.push((i, vline, count));
            }
            vline += count;
        }
        plan
    }

    /// Streaming search across all segments. Opens the file once for all Original segments.
    /// Uses two passes for correct wrap-around:
    ///   Forward:  pass 1 = cursor→end, pass 2 = beginning→cursor
    ///   Backward: pass 1 = cursor→beginning, pass 2 = end→cursor
    fn find_streaming(&mut self, query: &[u8], case_sensitive: bool, reverse: bool) -> bool {
        let plan = self.build_search_plan();
        if plan.is_empty() {
            return false;
        }

        let cursor_line = self.cursor_line;
        let cursor_col = self.cursor_col;
        // After a search: cursor is at match START, anchor at match END.
        // For forward: skip past the match end (anchor_col) to avoid re-finding.
        // For reverse: limit to before the match start (cursor_col) to avoid re-finding.
        let anchor_col = self
            .selection_anchor
            .filter(|(l, _)| *l == cursor_line)
            .map(|(_, c)| c)
            .unwrap_or(cursor_col);

        let cursor_plan_idx = plan
            .iter()
            .position(|(_, vl, count)| cursor_line >= *vl && cursor_line < vl + count)
            .unwrap_or(0);

        // Two passes: from cursor in search direction, then wrap around
        for pass in 0..2 {
            // Forward: skip past anchor_col (match end) in pass 0, limit at cursor_col in pass 1
            // Reverse: limit at cursor_col (match start) in pass 0, skip past anchor_col in pass 1
            let col_for_pass = if reverse {
                if pass == 0 {
                    cursor_col
                } else {
                    anchor_col
                }
            } else if pass == 0 {
                anchor_col
            } else {
                cursor_col
            };
            let result = self.find_streaming_pass(
                &plan,
                cursor_plan_idx,
                cursor_line,
                col_for_pass,
                query,
                case_sensitive,
                reverse,
                pass,
            );
            if let Some((line, col, match_len)) = result {
                // Cursor at match START, anchor at match END.
                // This highlights the match and places the cursor on its
                // first character, so Shift+Right selects from there.
                self.selection_anchor = Some((line, col + match_len));
                self.search_selection = true;
                self.cursor_line = line;
                self.cursor_col = col;
                self.desired_col = self.cursor_col;
                self.scroll_to_cursor();
                return true;
            }
        }

        false
    }

    /// Execute one pass of the search.
    /// Pass 0: from cursor to end (forward) or cursor to beginning (backward).
    /// Pass 1: wrap — from beginning to cursor (forward) or end to cursor (backward).
    #[allow(clippy::too_many_arguments)]
    fn find_streaming_pass(
        &mut self,
        plan: &[(usize, usize, usize)],
        cursor_plan_idx: usize,
        cursor_line: usize,
        skip_col: usize,
        query: &[u8],
        case_sensitive: bool,
        reverse: bool,
        pass: usize,
    ) -> Option<(usize, usize, usize)> {
        // Determine which segments to visit and in what order
        let seg_indices: Vec<usize> = if !reverse {
            if pass == 0 {
                // Forward pass 0: cursor_seg through last
                (cursor_plan_idx..plan.len()).collect()
            } else {
                // Forward pass 1 (wrap): first through cursor_seg
                (0..=cursor_plan_idx).collect()
            }
        } else if pass == 0 {
            // Backward pass 0: cursor_seg down to first
            (0..=cursor_plan_idx).rev().collect()
        } else {
            // Backward pass 1 (wrap): last down to cursor_seg
            (cursor_plan_idx..plan.len()).rev().collect()
        };

        for &plan_idx in &seg_indices {
            let (seg_idx, seg_vline_start, seg_line_count) = plan[plan_idx];
            let at_cursor_seg = plan_idx == cursor_plan_idx;

            // Determine search bounds within this segment
            let skip_lines_in_seg = cursor_line - seg_vline_start;

            // skip_to: start searching after this position (forward pass 0, backward wrap)
            // limit_to: stop searching before this position (forward wrap, backward pass 0)
            let (use_skip, use_limit) = if at_cursor_seg {
                match (reverse, pass) {
                    (false, 0) => (true, false), // forward from cursor
                    (false, 1) => (false, true), // forward wrap: start to cursor
                    (true, 0) => (false, true),  // backward: start to cursor (find last)
                    (true, 1) => (true, false),  // backward wrap: cursor to end (find last)
                    _ => (false, false),
                }
            } else {
                (false, false)
            };

            let (seg_start_line, seg_is_original) = match &self.segments[seg_idx] {
                Segment::Original { start_line, .. } => (*start_line, true),
                _ => (0, false),
            };
            let result = if seg_is_original {
                self.search_original_segment(
                    seg_start_line,
                    seg_line_count,
                    seg_vline_start,
                    query,
                    case_sensitive,
                    reverse,
                    if use_skip {
                        Some((skip_lines_in_seg, skip_col))
                    } else {
                        None
                    },
                    if use_limit {
                        Some((skip_lines_in_seg, skip_col))
                    } else {
                        None
                    },
                )
            } else if let Segment::Buffer { lines } = &self.segments[seg_idx] {
                search_buffer_segment(
                    lines,
                    seg_vline_start,
                    query,
                    case_sensitive,
                    reverse,
                    if use_skip {
                        Some((skip_lines_in_seg, skip_col))
                    } else {
                        None
                    },
                    if use_limit {
                        Some((skip_lines_in_seg, skip_col))
                    } else {
                        None
                    },
                )
            } else {
                None
            };

            if result.is_some() {
                return result;
            }
        }

        None
    }

    /// Search an Original segment by streaming raw bytes from disk.
    /// Opens its own file handle — no shared state across calls.
    /// `skip_to`: skip ahead to (line_offset, col) — search starts after this position.
    /// `limit_to`: stop at (line_offset, col) — search only covers content before this position.
    #[allow(clippy::too_many_arguments)]
    fn search_original_segment(
        &mut self,
        orig_start_line: usize,
        line_count: usize,
        vline_start: usize,
        query: &[u8],
        case_sensitive: bool,
        reverse: bool,
        skip_to: Option<(usize, usize)>,
        limit_to: Option<(usize, usize)>,
    ) -> Option<(usize, usize, usize)> {
        let seg_start_byte = self.get_byte_offset(orig_start_line);
        let end_line = orig_start_line + line_count;
        let end_byte = if end_line >= self.lines_scanned {
            self.file_size
        } else {
            self.get_byte_offset(end_line)
        };

        if seg_start_byte >= end_byte {
            return None;
        }

        // Seek directly to the relevant byte range using the sparse line index.
        // skip_to: start reading from this line (skip everything before).
        // limit_to: stop reading after this line (skip everything after).
        let (read_start_byte, start_line_offset) = if let Some((skip_lines, _)) = skip_to {
            if skip_lines > 0 {
                let target_orig_line = orig_start_line + skip_lines;
                let skip_byte = self.get_byte_offset(target_orig_line);
                (skip_byte.max(seg_start_byte), skip_lines)
            } else {
                (seg_start_byte, 0)
            }
        } else {
            (seg_start_byte, 0)
        };

        // Only read up to the limit line (+1 to include the limit line itself).
        let effective_end_byte = if let Some((limit_lines, _)) = limit_to {
            let target_orig_line = orig_start_line + limit_lines + 1;
            let limit_byte = if target_orig_line >= self.lines_scanned {
                end_byte
            } else {
                self.get_byte_offset(target_orig_line)
            };
            limit_byte.min(end_byte)
        } else {
            end_byte
        };

        if read_start_byte >= effective_end_byte {
            return None;
        }

        let match_char_len = char_count_in_bytes(query);
        let search_fn = if case_sensitive {
            find_bytes_sensitive
        } else {
            find_bytes_insensitive
        };

        if reverse {
            self.search_original_reverse(
                read_start_byte,
                effective_end_byte,
                start_line_offset,
                vline_start,
                orig_start_line,
                query,
                search_fn,
                match_char_len,
                skip_to,
                limit_to,
            )
        } else {
            self.search_original_forward(
                read_start_byte,
                effective_end_byte,
                start_line_offset,
                vline_start,
                query,
                search_fn,
                match_char_len,
                skip_to,
                limit_to,
            )
        }
    }

    /// Forward search: read sequentially from start, return first match.
    #[allow(clippy::too_many_arguments)]
    fn search_original_forward(
        &mut self,
        read_start_byte: u64,
        effective_end_byte: u64,
        start_line_offset: usize,
        vline_start: usize,
        query: &[u8],
        search_fn: fn(&[u8], &[u8]) -> Option<usize>,
        match_char_len: usize,
        skip_to: Option<(usize, usize)>,
        limit_to: Option<(usize, usize)>,
    ) -> Option<(usize, usize, usize)> {
        let total_bytes = (effective_end_byte - read_start_byte) as usize;
        const CHUNK_SIZE: usize = 4 * 1024 * 1024;
        let file = File::open(&self.path).ok()?;
        let mut reader = BufReader::with_capacity(CHUNK_SIZE.min(total_bytes), &file);
        reader.seek(SeekFrom::Start(read_start_byte)).ok()?;

        let overlap = query.len().saturating_sub(1);
        let mut current_line = start_line_offset;
        let mut carry = Vec::new();
        let mut bytes_left = total_bytes;
        let mut buf = vec![0u8; CHUNK_SIZE.min(total_bytes)];

        while bytes_left > 0 {
            let to_read = buf.len().min(bytes_left);
            let n = reader.read(&mut buf[..to_read]).ok()?;
            if n == 0 {
                break;
            }
            bytes_left -= n;

            let chunk = if carry.is_empty() {
                &buf[..n]
            } else {
                carry.extend_from_slice(&buf[..n]);
                carry.as_slice()
            };

            let search_start = if let Some((sl, sc)) = skip_to {
                if current_line <= sl {
                    line_col_to_byte_pos(chunk, sl - current_line, sc)
                } else {
                    0
                }
            } else {
                0
            };

            let search_end = if let Some((ll, lc)) = limit_to {
                if current_line <= ll {
                    line_col_to_byte_pos(chunk, ll - current_line, lc).min(chunk.len())
                } else {
                    0
                }
            } else {
                chunk.len()
            };

            if search_start < search_end {
                if let Some(found) = search_fn(&chunk[search_start..search_end], query) {
                    let abs_pos = search_start + found;
                    let (line_at, col_at) = byte_pos_to_line_col(chunk, abs_pos, current_line);
                    return Some((vline_start + line_at, col_at, match_char_len));
                }
            }

            let newlines_in_chunk = bytecount_newlines(chunk);
            current_line += newlines_in_chunk;
            carry.clear();
            if bytes_left > 0 && overlap > 0 && n >= overlap {
                carry.extend_from_slice(&buf[n - overlap..n]);
                current_line -= bytecount_newlines(&buf[n - overlap..n]);
            }
        }
        None
    }

    /// Reverse search: read backwards in chunks from end, rfind in each, return first hit.
    /// O(distance_to_match) instead of O(bytes_before_cursor).
    #[allow(clippy::too_many_arguments)]
    /// Reverse search: read backwards in chunks from end, rfind in each, return first hit.
    /// O(distance_to_match) instead of O(bytes_before_cursor).
    /// Line numbers computed via sparse line index, then converted to segment-relative
    /// and combined with vline_start for correct virtual line numbers.
    #[allow(clippy::too_many_arguments)]
    fn search_original_reverse(
        &mut self,
        read_start_byte: u64,
        effective_end_byte: u64,
        _start_line_offset: usize,
        vline_start: usize,
        orig_start_line: usize,
        query: &[u8],
        search_fn: fn(&[u8], &[u8]) -> Option<usize>,
        match_char_len: usize,
        skip_to: Option<(usize, usize)>,
        limit_to: Option<(usize, usize)>,
    ) -> Option<(usize, usize, usize)> {
        let total_bytes = (effective_end_byte - read_start_byte) as usize;
        if total_bytes == 0 {
            return None;
        }

        const CHUNK_SIZE: usize = 4 * 1024 * 1024;
        let overlap = query.len().saturating_sub(1);
        let file = File::open(&self.path).ok()?;

        let mut chunk_end_byte = effective_end_byte;
        let mut buf = vec![0u8; CHUNK_SIZE];

        while chunk_end_byte > read_start_byte {
            let chunk_size = ((chunk_end_byte - read_start_byte) as usize).min(CHUNK_SIZE);
            let chunk_start_byte = chunk_end_byte - chunk_size as u64;

            let mut reader = BufReader::new(&file);
            reader.seek(SeekFrom::Start(chunk_start_byte)).ok()?;
            let n = reader.read(&mut buf[..chunk_size]).ok()?;
            if n == 0 {
                break;
            }
            let chunk = &buf[..n];

            // Compute segment-relative line at chunk start via the sparse index.
            // line_at_byte_offset returns absolute file line; subtract orig_start_line
            // to get the offset within this segment, matching the forward path.
            let abs_line = self.line_at_byte_offset(chunk_start_byte);
            let seg_relative_line = abs_line.saturating_sub(orig_start_line);

            let search_start = if let Some((sl, sc)) = skip_to {
                if seg_relative_line <= sl {
                    line_col_to_byte_pos(chunk, sl - seg_relative_line, sc)
                } else {
                    0
                }
            } else {
                0
            };

            let search_end = if let Some((ll, lc)) = limit_to {
                if seg_relative_line <= ll {
                    line_col_to_byte_pos(chunk, ll - seg_relative_line, lc).min(n)
                } else {
                    0
                }
            } else {
                n
            };

            if search_start < search_end {
                let slice = &chunk[search_start..search_end];
                if let Some(found) = rfind_with(slice, query, search_fn) {
                    let abs_pos = search_start + found;
                    let (line_at, col_at) = byte_pos_to_line_col(chunk, abs_pos, seg_relative_line);
                    return Some((vline_start + line_at, col_at, match_char_len));
                }
            }

            if chunk_start_byte <= read_start_byte {
                break;
            }
            chunk_end_byte = chunk_start_byte + overlap as u64;
        }

        None
    }

    // --- Selection ---

    pub fn clear_selection(&mut self) {
        self.selection_anchor = None;
        self.search_selection = false;
    }

    fn ensure_anchor(&mut self) {
        if self.selection_anchor.is_none() || self.search_selection {
            self.selection_anchor = Some((self.cursor_line, self.cursor_col));
            self.search_selection = false;
        }
    }

    /// Returns ordered ((start_line, start_col), (end_line, end_col)).
    pub fn selection_range(&self) -> Option<((usize, usize), (usize, usize))> {
        let anchor = self.selection_anchor?;
        let cursor = (self.cursor_line, self.cursor_col);
        if anchor <= cursor {
            Some((anchor, cursor))
        } else {
            Some((cursor, anchor))
        }
    }

    pub fn selected_text(&mut self) -> Option<String> {
        let ((sl, sc), (el, ec)) = self.selection_range()?;
        let mut result = String::new();
        for ln in sl..=el {
            let text = self.get_line_text(ln)?;
            let start = if ln == sl { sc } else { 0 };
            let end = if ln == el { ec } else { text.chars().count() };
            if ln > sl {
                result.push('\n');
            }
            let selected: String = text
                .chars()
                .skip(start)
                .take(end.saturating_sub(start))
                .collect();
            result.push_str(&selected);
        }
        Some(result)
    }

    pub fn select_up(&mut self) {
        self.ensure_anchor();
        self.cursor_up();
    }
    pub fn select_down(&mut self) {
        self.ensure_anchor();
        self.cursor_down();
    }
    pub fn select_left(&mut self) {
        self.ensure_anchor();
        self.cursor_left();
    }
    pub fn select_right(&mut self) {
        self.ensure_anchor();
        self.cursor_right();
    }
    pub fn select_line_start(&mut self) {
        self.ensure_anchor();
        self.cursor_line_start();
    }
    pub fn select_line_end(&mut self) {
        self.ensure_anchor();
        self.cursor_line_end();
    }
    pub fn select_page_up(&mut self) {
        self.ensure_anchor();
        self.page_up();
    }
    pub fn select_page_down(&mut self) {
        self.ensure_anchor();
        self.page_down();
    }
    pub fn select_all(&mut self) {
        self.selection_anchor = Some((0, 0));
        self.goto_bottom();
        self.cursor_line_end();
    }

    pub fn copy_to_clipboard(&mut self) {
        if let Some(text) = self.selected_text() {
            osc52_copy(&text);
            self.status_msg = Some(format!("Copied {} chars", text.len()));
        }
    }

    // --- Visible content ---

    /// Get lines in a range [start, end) for syntax highlighting context.
    pub fn get_lines_range(&mut self, start: usize, end: usize) -> Vec<String> {
        let mut result = Vec::with_capacity(end.saturating_sub(start));
        for vline in start..end {
            match self.get_line_text(vline) {
                Some(text) => result.push(text),
                None => break,
            }
        }
        result
    }

    pub fn visible_content(&mut self) -> Vec<(usize, String)> {
        let mut result = Vec::with_capacity(self.visible_lines);
        for i in 0..self.visible_lines {
            let vline = self.scroll_y + i;
            match self.get_line_text(vline) {
                Some(text) => result.push((vline, text)),
                None => break,
            }
        }
        result
    }

    // --- Save ---

    pub fn save(&mut self) -> Result<()> {
        self.scan_to_end();

        // Pre-compute byte ranges for Original segments (avoids borrow conflicts)
        let segments_snapshot = self.segments.clone();
        let mut byte_ranges: Vec<Option<(u64, u64)>> = Vec::with_capacity(segments_snapshot.len());
        for seg in &segments_snapshot {
            match seg {
                Segment::Original { start_line, count } => {
                    let real_count = (*count).min(self.lines_scanned.saturating_sub(*start_line));
                    if real_count == 0 {
                        byte_ranges.push(None);
                        continue;
                    }
                    let start_byte = self.get_byte_offset(*start_line);
                    let end_line = start_line + real_count;
                    let end_byte = if end_line >= self.lines_scanned {
                        self.file_size
                    } else {
                        self.get_byte_offset(end_line)
                    };
                    byte_ranges.push(Some((start_byte, end_byte)));
                }
                Segment::Buffer { .. } => {
                    byte_ranges.push(None);
                }
            }
        }

        let temp_path = self.path.with_extension("mm_tmp");
        {
            let mut writer =
                std::io::BufWriter::with_capacity(256 * 1024, File::create(&temp_path)?);
            let orig_file = File::open(&self.path)?;

            for (i, seg) in segments_snapshot.iter().enumerate() {
                match seg {
                    Segment::Original { .. } => {
                        let (start_byte, end_byte) = match byte_ranges[i] {
                            Some(r) => r,
                            None => continue,
                        };
                        let mut reader = BufReader::new(&orig_file);
                        reader.seek(SeekFrom::Start(start_byte))?;
                        let mut remaining = end_byte - start_byte;
                        let mut buf = vec![0u8; 256 * 1024];
                        while remaining > 0 {
                            let to_read = buf.len().min(remaining as usize);
                            let n = reader.read(&mut buf[..to_read])?;
                            if n == 0 {
                                break;
                            }
                            writer.write_all(&buf[..n])?;
                            remaining -= n as u64;
                        }
                    }
                    Segment::Buffer { lines } => {
                        for line in lines {
                            writer.write_all(line.as_bytes())?;
                            writer.write_all(b"\n")?;
                        }
                    }
                }
            }
            writer.flush()?;
        }

        std::fs::rename(&temp_path, &self.path)
            .with_context(|| format!("Failed to rename temp file to {:?}", self.path))?;

        // Reinitialize
        self.file_size = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        self.segments = vec![Segment::Original {
            start_line: 0,
            count: usize::MAX / 2,
        }];
        self.line_index = vec![0];
        self.lines_scanned = 0;
        self.scan_byte_offset = 0;
        self.scan_complete = false;
        self.modified = false;
        self.status_msg = Some("Saved.".to_string());

        // Re-scan around cursor
        self.scan_to_line(self.cursor_line + 100);
        Ok(())
    }

    // --- File reading helpers ---

    fn get_byte_offset(&mut self, orig_line: usize) -> u64 {
        self.scan_to_line(orig_line);

        let idx = (orig_line / INDEX_INTERVAL).min(self.line_index.len().saturating_sub(1));
        let mut current_line = idx * INDEX_INTERVAL;
        let mut offset = self.line_index[idx];

        if current_line == orig_line {
            return offset;
        }

        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return offset,
        };
        let mut reader = BufReader::new(file);
        if reader.seek(SeekFrom::Start(offset)).is_err() {
            return offset;
        }

        let mut buf = String::new();
        while current_line < orig_line {
            buf.clear();
            match reader.read_line(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    offset += n as u64;
                    current_line += 1;
                }
                Err(_) => break,
            }
        }
        offset
    }

    fn read_original_line(&mut self, orig_line: usize) -> Option<String> {
        if orig_line >= self.lines_scanned && !self.scan_complete {
            self.scan_to_line(orig_line + 100);
        }
        if orig_line >= self.lines_scanned {
            return None;
        }

        let idx = (orig_line / INDEX_INTERVAL).min(self.line_index.len().saturating_sub(1));
        let mut current_line = idx * INDEX_INTERVAL;
        let start_offset = self.line_index[idx];

        let file = File::open(&self.path).ok()?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(start_offset)).ok()?;

        let mut buf = String::new();
        while current_line <= orig_line {
            buf.clear();
            match reader.read_line(&mut buf) {
                Ok(0) => return None,
                Ok(_) => {
                    if current_line == orig_line {
                        let trimmed = buf.trim_end_matches('\n').trim_end_matches('\r');
                        return Some(trimmed.to_string());
                    }
                    current_line += 1;
                }
                Err(_) => return None,
            }
        }
        None
    }

    /// Compute the line number (relative to file start) at a given byte offset
    /// using the sparse line index. Reads at most INDEX_INTERVAL lines from disk.
    fn line_at_byte_offset(&self, byte_offset: u64) -> usize {
        if byte_offset == 0 {
            return 0;
        }
        // Binary search: find the largest index entry with byte offset <= target
        let idx = self
            .line_index
            .partition_point(|&b| b <= byte_offset)
            .saturating_sub(1);
        let base_line = idx * INDEX_INTERVAL;
        let base_byte = self.line_index[idx];

        if base_byte >= byte_offset {
            return base_line;
        }

        // Count newlines from base_byte to byte_offset
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return base_line,
        };
        let mut reader = BufReader::new(file);
        if reader.seek(SeekFrom::Start(base_byte)).is_err() {
            return base_line;
        }

        let mut count = 0;
        let mut remaining = (byte_offset - base_byte) as usize;
        let mut buf = [0u8; 64 * 1024];
        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            let n = match reader.read(&mut buf[..to_read]) {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };
            count += buf[..n].iter().filter(|&&b| b == b'\n').count();
            remaining -= n;
        }
        base_line + count
    }

    pub fn scan_to_line(&mut self, target: usize) {
        if self.scan_complete || self.lines_scanned >= target {
            return;
        }
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        if reader.seek(SeekFrom::Start(self.scan_byte_offset)).is_err() {
            return;
        }
        let mut buf = String::new();
        while self.lines_scanned < target {
            buf.clear();
            match reader.read_line(&mut buf) {
                Ok(0) => {
                    self.scan_complete = true;
                    return;
                }
                Ok(n) => {
                    self.scan_byte_offset += n as u64;
                    self.lines_scanned += 1;
                    if self.lines_scanned.is_multiple_of(INDEX_INTERVAL) {
                        self.line_index.push(self.scan_byte_offset);
                    }
                }
                Err(_) => {
                    self.scan_complete = true;
                    return;
                }
            }
        }
    }

    fn scan_to_end(&mut self) {
        if self.scan_complete {
            return;
        }
        // Scan in large batches
        self.scan_to_line(usize::MAX);
    }
}

/// Search a Buffer segment's in-memory lines.
/// `skip_to`: skip ahead to (line_offset, col) — search starts after this position.
/// `limit_to`: stop at (line_offset, col) — only search before this position.
#[allow(clippy::too_many_arguments)]
fn search_buffer_segment(
    lines: &[String],
    vline_start: usize,
    query: &[u8],
    case_sensitive: bool,
    reverse: bool,
    skip_to: Option<(usize, usize)>,
    limit_to: Option<(usize, usize)>,
) -> Option<(usize, usize, usize)> {
    let query_str = std::str::from_utf8(query).unwrap_or("");
    let match_char_len = query_str.chars().count();

    let iter: Box<dyn Iterator<Item = (usize, &String)>> = if reverse {
        Box::new(lines.iter().enumerate().rev())
    } else {
        Box::new(lines.iter().enumerate())
    };

    let mut best_reverse: Option<(usize, usize, usize)> = None;

    for (offset, line) in iter {
        let vline = vline_start + offset;
        let search_text = if case_sensitive {
            line.clone()
        } else {
            line.to_lowercase()
        };

        // Determine byte bounds on this line
        let start_byte = if let Some((sl, sc)) = skip_to {
            if offset < sl {
                if !reverse {
                    continue;
                } else {
                    0
                }
            } else if offset == sl {
                char_to_byte(&search_text, sc)
            } else {
                0
            }
        } else {
            0
        };

        let end_byte = if let Some((ll, lc)) = limit_to {
            if offset > ll {
                if reverse {
                    continue;
                } else {
                    break;
                }
            } else if offset == ll {
                char_to_byte(&search_text, lc)
            } else {
                search_text.len()
            }
        } else {
            search_text.len()
        };

        if start_byte >= end_byte {
            continue;
        }

        let slice = &search_text[start_byte..end_byte];

        if reverse {
            // Collect last match in valid range
            if let Some(byte_pos) = slice.rfind(query_str) {
                let col = search_text[..start_byte + byte_pos].chars().count();
                best_reverse = Some((vline, col, match_char_len));
                return best_reverse; // reverse iteration, first rfind is the best
            }
        } else if let Some(byte_pos) = slice.find(query_str) {
            let col = search_text[..start_byte + byte_pos].chars().count();
            return Some((vline, col, match_char_len));
        }
    }

    best_reverse
}

/// Case-sensitive byte search (forward). Uses first-byte scan to skip non-matching
/// positions, then verifies the full needle. Much faster than sliding window for
/// large haystacks with infrequent first-byte matches.
fn find_bytes_sensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    let first = needle[0];
    let mut i = 0;
    while i + needle.len() <= haystack.len() {
        match haystack[i..].iter().position(|&b| b == first) {
            Some(pos) => {
                let start = i + pos;
                if start + needle.len() > haystack.len() {
                    return None;
                }
                if haystack[start..start + needle.len()] == *needle {
                    return Some(start);
                }
                i = start + 1;
            }
            None => return None,
        }
    }
    None
}

/// Case-insensitive byte search (forward, ASCII). Pre-lowercased needle expected.
fn find_bytes_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    let first_lower = needle[0];
    let mut i = 0;
    while i + needle.len() <= haystack.len() {
        match haystack[i..]
            .iter()
            .position(|b| b.to_ascii_lowercase() == first_lower)
        {
            Some(pos) => {
                let start = i + pos;
                if start + needle.len() > haystack.len() {
                    return None;
                }
                if haystack[start..start + needle.len()]
                    .iter()
                    .zip(needle)
                    .all(|(a, b)| a.to_ascii_lowercase() == *b)
                {
                    return Some(start);
                }
                i = start + 1;
            }
            None => return None,
        }
    }
    None
}

/// Find the LAST match in a slice using the given forward search function.
fn rfind_with(
    haystack: &[u8],
    needle: &[u8],
    search_fn: fn(&[u8], &[u8]) -> Option<usize>,
) -> Option<usize> {
    let mut last = None;
    let mut pos = 0;
    while pos + needle.len() <= haystack.len() {
        if let Some(found) = search_fn(&haystack[pos..], needle) {
            last = Some(pos + found);
            pos += found + 1;
        } else {
            break;
        }
    }
    last
}

/// Convert a byte position in a chunk to (line_offset, char_column).
/// `base_line` is the line number at the start of the chunk.
/// Column counts only non-control characters (excluding \n), matching how the
/// editor loads lines (read_original_line strips trailing \r, and the display
/// strips control characters).
fn byte_pos_to_line_col(chunk: &[u8], byte_pos: usize, base_line: usize) -> (usize, usize) {
    let mut line = base_line;
    let mut last_newline_pos: Option<usize> = None;
    for (i, &b) in chunk[..byte_pos].iter().enumerate() {
        if b == b'\n' {
            line += 1;
            last_newline_pos = Some(i);
        }
    }
    let line_start = match last_newline_pos {
        Some(pos) => pos + 1,
        None => 0,
    };
    // Count chars from line start to match position, excluding trailing \r
    // before \n, matching how read_original_line strips trailing \r.
    let line_bytes = &chunk[line_start..byte_pos];
    let col = std::str::from_utf8(line_bytes)
        .map(|s| s.chars().count())
        .unwrap_or(byte_pos - line_start);
    (line, col)
}

/// Convert (line_offset_within_chunk, char_col) to a byte position in the chunk.
fn line_col_to_byte_pos(chunk: &[u8], line_offset: usize, char_col: usize) -> usize {
    let mut lines_seen = 0;
    let mut pos = 0;
    // Skip to the right line
    while lines_seen < line_offset && pos < chunk.len() {
        if chunk[pos] == b'\n' {
            lines_seen += 1;
        }
        pos += 1;
    }
    // Now skip char_col characters on this line
    let line_start = pos;
    if let Ok(text) = std::str::from_utf8(&chunk[line_start..]) {
        let byte_offset = text
            .char_indices()
            .nth(char_col)
            .map(|(i, _)| i)
            .unwrap_or(text.find('\n').unwrap_or(text.len()));
        line_start + byte_offset
    } else {
        (line_start + char_col).min(chunk.len())
    }
}

/// Count newlines in a byte slice.
fn bytecount_newlines(data: &[u8]) -> usize {
    data.iter().filter(|&&b| b == b'\n').count()
}

/// Count chars in a byte slice (UTF-8 aware).
fn char_count_in_bytes(data: &[u8]) -> usize {
    std::str::from_utf8(data)
        .map(|s| s.chars().count())
        .unwrap_or(data.len())
}

/// Convert a display column back to an original char column,
/// accounting for tab expansion and control characters.
fn display_col_to_orig_col(text: &str, display_col: usize, scroll_x: usize) -> usize {
    let mut dcol = 0;
    for (i, ch) in text.chars().enumerate() {
        if i < scroll_x {
            continue;
        }
        // Skip zero-width control chars — they don't occupy display space
        if ch.is_control() && ch != '\t' {
            continue;
        }
        if dcol >= display_col {
            return i;
        }
        if ch == '\t' {
            dcol += 4 - (dcol % 4);
        } else {
            dcol += 1;
        }
    }
    // Past end of line — clamp to line length
    text.chars().count()
}

fn char_to_byte(s: &str, char_pos: usize) -> usize {
    s.char_indices()
        .nth(char_pos)
        .map(|(i, _)| i)
        .unwrap_or(s.len())
}

pub(crate) fn osc52_copy(text: &str) {
    // Try native clipboard tools first (more reliable on Linux), fall back to OSC 52
    if try_native_clipboard(text) {
        return;
    }
    use std::io::Write;
    let encoded = base64_encode(text.as_bytes());
    let osc = format!("\x1b]52;c;{}\x1b\\", encoded);
    let _ = std::io::stdout().write_all(osc.as_bytes());
    let _ = std::io::stdout().flush();
}

/// Try to copy using native clipboard tools. Returns true on success.
fn try_native_clipboard(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    // Detect Wayland vs X11
    let is_wayland = std::env::var("WAYLAND_DISPLAY").is_ok();

    let tools: &[&[&str]] = if is_wayland {
        &[&["wl-copy"], &["xclip", "-selection", "clipboard"]]
    } else {
        &[
            &["xclip", "-selection", "clipboard"],
            &["xsel", "--clipboard", "--input"],
            &["wl-copy"],
        ]
    };

    for tool_args in tools {
        let program = tool_args[0];
        let args = &tool_args[1..];
        if let Ok(mut child) = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(text.as_bytes());
            }
            if let Ok(status) = child.wait() {
                if status.success() {
                    return true;
                }
            }
        }
    }
    false
}

// Exposed for tests
pub fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        result.push(if chunk.len() > 1 {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_editor(content: &str) -> EditorState {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("mm_test_{}_{}.txt", std::process::id(), id,));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        drop(f);
        let editor = EditorState::open(path.clone());
        // Clean up is best-effort; tests are short-lived
        editor
    }

    #[test]
    fn selection_range_none_when_no_anchor() {
        let editor = create_test_editor("hello\nworld\n");
        assert_eq!(editor.selection_range(), None);
    }

    #[test]
    fn selection_range_same_line_forward() {
        let mut editor = create_test_editor("hello world\nsecond\n");
        editor.cursor_line = 0;
        editor.cursor_col = 8;
        editor.selection_anchor = Some((0, 3));
        assert_eq!(editor.selection_range(), Some(((0, 3), (0, 8))));
    }

    #[test]
    fn selection_range_same_line_reversed() {
        let mut editor = create_test_editor("hello world\nsecond\n");
        editor.cursor_line = 0;
        editor.cursor_col = 2;
        editor.selection_anchor = Some((0, 9));
        assert_eq!(editor.selection_range(), Some(((0, 2), (0, 9))));
    }

    #[test]
    fn selection_range_multi_line_forward() {
        let mut editor = create_test_editor("aaa\nbbb\nccc\n");
        editor.selection_anchor = Some((0, 1));
        editor.cursor_line = 2;
        editor.cursor_col = 2;
        assert_eq!(editor.selection_range(), Some(((0, 1), (2, 2))));
    }

    #[test]
    fn selection_range_multi_line_reversed() {
        let mut editor = create_test_editor("aaa\nbbb\nccc\n");
        editor.selection_anchor = Some((2, 2));
        editor.cursor_line = 0;
        editor.cursor_col = 1;
        assert_eq!(editor.selection_range(), Some(((0, 1), (2, 2))));
    }

    #[test]
    fn selected_text_single_line() {
        let mut editor = create_test_editor("hello world\nsecond\n");
        editor.selection_anchor = Some((0, 6));
        editor.cursor_line = 0;
        editor.cursor_col = 11;
        assert_eq!(editor.selected_text(), Some("world".to_string()));
    }

    #[test]
    fn selected_text_multi_line() {
        let mut editor = create_test_editor("aaa\nbbb\nccc\n");
        editor.selection_anchor = Some((0, 1));
        editor.cursor_line = 2;
        editor.cursor_col = 2;
        assert_eq!(editor.selected_text(), Some("aa\nbbb\ncc".to_string()));
    }

    #[test]
    fn selected_text_reversed() {
        let mut editor = create_test_editor("hello world\nsecond line\n");
        editor.selection_anchor = Some((1, 6));
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        assert_eq!(editor.selected_text(), Some(" world\nsecond".to_string()));
    }

    #[test]
    fn select_right_then_up() {
        let mut editor = create_test_editor("short\nhello world here\n");
        // Position cursor at line 1, col 5
        editor.cursor_line = 1;
        editor.cursor_col = 5;
        editor.desired_col = 5;

        // Shift+Right 3 times
        editor.select_right();
        editor.select_right();
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((1, 5)));
        assert_eq!(editor.cursor_col, 8);

        // Shift+Up — cursor goes to line 0, col clamped to min(8, 5) = 5
        editor.select_up();
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 5); // "short" has 5 chars, desired=8 clamped
        assert_eq!(editor.selection_anchor, Some((1, 5)));
        assert_eq!(editor.selection_range(), Some(((0, 5), (1, 5))));
    }

    #[test]
    fn clear_selection_on_regular_move() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.selection_anchor = Some((0, 2));
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        assert!(editor.selection_range().is_some());

        editor.clear_selection();
        assert_eq!(editor.selection_range(), None);
    }

    #[test]
    fn select_all() {
        let mut editor = create_test_editor("aaa\nbbb\nccc\n");
        editor.select_all();
        assert_eq!(editor.selection_anchor, Some((0, 0)));
        // Cursor should be at end of last non-empty line
        assert!(editor.cursor_line >= 2);
        assert!(editor.cursor_col > 0);
    }

    // --- Search tests ---

    fn make_search(query: &str, direction: SearchDirection, case_sensitive: bool) -> SearchParams {
        SearchParams {
            query: query.to_string(),
            direction,
            case_sensitive,
        }
    }

    #[test]
    fn search_forward_basic() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Should find first "hello" at line 0, col 0; cursor at match start
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 5)));
    }

    #[test]
    fn search_forward_skips_cursor_position() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        // Simulate: just found first "hello" — cursor at match start (0), anchor at match end (5)
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.selection_anchor = Some((0, 5));
        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Should find second "hello" at line 2, col 0
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 5)));
    }

    #[test]
    fn search_forward_wraps_around() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        // Simulate: just found "hello" on line 2 — cursor at start (0), anchor at end (5)
        editor.cursor_line = 2;
        editor.cursor_col = 0;
        editor.selection_anchor = Some((2, 5));
        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Should wrap and find first "hello" at line 0
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 5)));
    }

    #[test]
    fn search_backward_basic() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        // Cursor at end of file
        editor.cursor_line = 2;
        editor.cursor_col = 11;
        let params = make_search("hello", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        // Should find "hello" on line 2; cursor at match start, anchor at match end
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 5)));
    }

    #[test]
    fn search_backward_does_not_refind_same_match() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        // Simulate: just found "hello" on line 2 (cursor at start, anchor at end)
        editor.cursor_line = 2;
        editor.cursor_col = 0;
        editor.selection_anchor = Some((2, 5));
        let params = make_search("hello", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        // Should find "hello" on line 0, NOT line 2 again
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 5)));
    }

    #[test]
    fn search_backward_wraps_around() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        // Cursor at beginning — nothing before it
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("hello", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        // Should wrap and find "hello" on line 2; cursor at match start, anchor at match end
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 5)));
    }

    #[test]
    fn search_case_insensitive() {
        let mut editor = create_test_editor("Hello World\nFOO BAR\nhello again\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("hello", SearchDirection::Forward, false);
        assert!(editor.find(&params));
        // Should find "Hello" at line 0 (case insensitive); cursor at match start, anchor at match end
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 5)));
    }

    #[test]
    fn search_case_sensitive_misses() {
        let mut editor = create_test_editor("Hello World\nFOO BAR\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(!editor.find(&params));
    }

    #[test]
    fn search_not_found() {
        let mut editor = create_test_editor("hello world\nfoo bar\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("zzz", SearchDirection::Forward, true);
        assert!(!editor.find(&params));
    }

    #[test]
    fn search_empty_query() {
        let mut editor = create_test_editor("hello\n");
        let params = make_search("", SearchDirection::Forward, true);
        assert!(!editor.find(&params));
    }

    #[test]
    fn search_mid_line_match() {
        let mut editor = create_test_editor("the quick brown fox\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("brown", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // cursor at match start (10), anchor at match end (15)
        assert_eq!(editor.cursor_col, 10);
        assert_eq!(editor.selection_anchor, Some((0, 15)));
    }

    #[test]
    fn search_forward_then_backward_cycles() {
        let mut editor = create_test_editor("aaa\nbbb\naaa\nbbb\naaa\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let fwd = make_search("aaa", SearchDirection::Forward, true);

        // First forward: line 0 — cursor at match start (0), anchor at match end (3)
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 3)));

        // Second forward: line 2
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 3)));

        // Third forward: line 4
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_line, 4);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((4, 3)));

        // Fourth forward: wraps to line 0
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 3)));

        // Now backward from line 0: should wrap to line 4
        let bwd = make_search("aaa", SearchDirection::Backward, true);
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_line, 4);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((4, 3)));

        // Backward again: line 2
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 3)));
    }

    #[test]
    fn search_multiple_matches_same_line() {
        let mut editor = create_test_editor("ab ab ab\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let fwd = make_search("ab", SearchDirection::Forward, true);

        // First: cursor at match start (0), anchor at match end (2)
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 2)));

        // Second: cursor at 3, anchor at 5
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_col, 3);
        assert_eq!(editor.selection_anchor, Some((0, 5)));

        // Third: cursor at 6, anchor at 8
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_col, 6);
        assert_eq!(editor.selection_anchor, Some((0, 8)));

        // Wraps back to col 0
        assert!(editor.find(&fwd));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 2)));
    }

    #[test]
    fn search_backward_multiple_same_line() {
        let mut editor = create_test_editor("ab ab ab\n");
        // Cursor at end of line
        editor.cursor_line = 0;
        editor.cursor_col = 8;

        let bwd = make_search("ab", SearchDirection::Backward, true);

        // First backward: cursor at match start (6), anchor at match end (8)
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_col, 6);
        assert_eq!(editor.selection_anchor, Some((0, 8)));

        // Second backward: cursor at 3, anchor at 5
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_col, 3);
        assert_eq!(editor.selection_anchor, Some((0, 5)));

        // Third backward: cursor at 0, anchor at 2
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 2)));

        // Wraps to col 6
        assert!(editor.find(&bwd));
        assert_eq!(editor.cursor_col, 6);
        assert_eq!(editor.selection_anchor, Some((0, 8)));
    }

    // --- Selection behavior tests ---

    #[test]
    fn select_right_single_char() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((0, 0)));
        assert_eq!(editor.cursor_col, 1);
        assert_eq!(editor.selected_text(), Some("h".to_string()));
    }

    #[test]
    fn select_right_multiple_chars() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.select_right();
        editor.select_right();
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((0, 0)));
        assert_eq!(editor.cursor_col, 3);
        assert_eq!(editor.selected_text(), Some("hel".to_string()));
    }

    #[test]
    fn select_right_from_middle() {
        let mut editor = create_test_editor("hello world\n");
        editor.cursor_line = 0;
        editor.cursor_col = 6;
        editor.select_right();
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((0, 6)));
        assert_eq!(editor.cursor_col, 8);
        assert_eq!(editor.selected_text(), Some("wo".to_string()));
    }

    #[test]
    fn select_right_at_line_end_wraps() {
        let mut editor = create_test_editor("hi\nthere\n");
        editor.cursor_line = 0;
        editor.cursor_col = 2; // end of "hi"
        editor.select_right();
        // Should wrap to start of next line
        assert_eq!(editor.selection_anchor, Some((0, 2)));
        assert_eq!(editor.cursor_line, 1);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selected_text(), Some("\n".to_string()));
    }

    #[test]
    fn select_left_single_char() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 3;
        editor.select_left();
        assert_eq!(editor.selection_anchor, Some((0, 3)));
        assert_eq!(editor.cursor_col, 2);
        assert_eq!(editor.selected_text(), Some("l".to_string()));
    }

    #[test]
    fn select_left_at_line_start_wraps() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 1;
        editor.cursor_col = 0;
        editor.select_left();
        assert_eq!(editor.selection_anchor, Some((1, 0)));
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 5); // end of "hello"
        assert_eq!(editor.selected_text(), Some("\n".to_string()));
    }

    #[test]
    fn select_right_then_left_cancels() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 2;
        editor.select_right();
        editor.select_right();
        assert_eq!(editor.selected_text(), Some("ll".to_string()));
        editor.select_left();
        assert_eq!(editor.selected_text(), Some("l".to_string()));
        editor.select_left();
        // Cursor back at anchor — empty selection
        assert_eq!(editor.cursor_col, 2);
        assert_eq!(editor.selection_anchor, Some((0, 2)));
    }

    #[test]
    fn select_down_selects_to_same_col() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 0;
        editor.cursor_col = 2;
        editor.desired_col = 2;
        editor.select_down();
        assert_eq!(editor.selection_anchor, Some((0, 2)));
        assert_eq!(editor.cursor_line, 1);
        assert_eq!(editor.cursor_col, 2);
        assert_eq!(editor.selected_text(), Some("llo\nwo".to_string()));
    }

    #[test]
    fn select_up_from_second_line() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 1;
        editor.cursor_col = 3;
        editor.desired_col = 3;
        editor.select_up();
        assert_eq!(editor.selection_anchor, Some((1, 3)));
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 3);
        assert_eq!(editor.selected_text(), Some("lo\nwor".to_string()));
    }

    #[test]
    fn select_line_end() {
        let mut editor = create_test_editor("hello world\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.select_line_end();
        assert_eq!(editor.selection_anchor, Some((0, 0)));
        assert_eq!(editor.cursor_col, 11);
        assert_eq!(editor.selected_text(), Some("hello world".to_string()));
    }

    #[test]
    fn select_line_start() {
        let mut editor = create_test_editor("hello world\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.select_line_start();
        assert_eq!(editor.selection_anchor, Some((0, 5)));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selected_text(), Some("hello".to_string()));
    }

    #[test]
    fn select_after_search_starts_fresh() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        // Search forward — cursor at match start (0), anchor at match end (5)
        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((0, 5)));

        // Now Shift+Right — ensure_anchor resets anchor to cursor (match start=0), then cursor moves to 1
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((0, 0)));
        assert_eq!(editor.cursor_col, 1);
        assert_eq!(editor.selected_text(), Some("h".to_string()));
    }

    #[test]
    fn select_after_reverse_search_starts_fresh() {
        let mut editor = create_test_editor("hello world\nfoo bar\nhello again\n");
        editor.cursor_line = 2;
        editor.cursor_col = 11;

        // Backward search — cursor at match start (0), anchor at match end (5)
        let params = make_search("hello", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((2, 5)));

        // Shift+Right — ensure_anchor resets anchor to cursor (match start=0), then cursor moves to 1
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((2, 0)));
        assert_eq!(editor.cursor_col, 1);
        assert_eq!(editor.selected_text(), Some("h".to_string()));
    }

    #[test]
    fn search_then_select_right_crlf_file() {
        // File with \r\n line endings — search column must match editor column
        let mut editor = create_test_editor("hello world\r\nfoo bar\r\nhello again\r\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let params = make_search("foo", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // cursor at match start (0), anchor at match end (3)
        assert_eq!(editor.cursor_line, 1);
        assert_eq!(editor.cursor_col, 0);
        assert_eq!(editor.selection_anchor, Some((1, 3)));

        // Shift+Right — ensure_anchor resets anchor to cursor (0), cursor moves to 1
        editor.select_right();
        assert_eq!(editor.selection_anchor, Some((1, 0)));
        assert_eq!(editor.cursor_col, 1);
        // Editor's line 1 is "foo bar" (stripped \r), char at col 0 = 'f'
        assert_eq!(editor.selected_text(), Some("f".to_string()));
    }

    #[test]
    fn select_left_after_forward_search() {
        let mut editor = create_test_editor("abcdef NEEDLE xyz\nsecond line\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let params = make_search("NEEDLE", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Match at col 7; cursor at match start (7), anchor at match end (13)
        assert_eq!(editor.cursor_col, 7);
        assert_eq!(editor.selection_anchor, Some((0, 13)));
        assert!(editor.search_selection);

        // Shift+Left: ensure_anchor resets anchor to cursor (7), cursor moves to 6
        // Selects char at col 6 = ' ' (space before NEEDLE)
        editor.select_left();
        assert_eq!(editor.selection_anchor, Some((0, 7)));
        assert_eq!(editor.cursor_col, 6);
        assert_eq!(editor.selected_text(), Some(" ".to_string()));
    }

    #[test]
    fn select_left_after_forward_search_cursor_was_elsewhere() {
        // Simulate: cursor was at col 50 before search, search finds match at col 11
        let mut editor = create_test_editor(
            "0123456789 NEEDLE rest of the line with lots of padding chars here\n",
        );
        editor.cursor_line = 0;
        editor.cursor_col = 50; // cursor was here before search

        let params = make_search("NEEDLE", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Match at col 11; cursor at match start (11), anchor at match end (17)
        assert_eq!(editor.cursor_col, 11);
        assert_eq!(editor.selection_anchor, Some((0, 17)));

        // Shift+Left: ensure_anchor resets anchor to cursor (11), cursor moves to 10
        // Selects char at col 10 = ' ' (space before NEEDLE)
        // NOT at old cursor pos (50)
        editor.select_left();
        assert_eq!(editor.selection_anchor, Some((0, 11)));
        assert_eq!(editor.cursor_col, 10);
        assert_eq!(editor.selected_text(), Some(" ".to_string()));
    }

    #[test]
    fn search_column_matches_editor_column_crlf() {
        // Verify search result column matches what the editor sees
        let mut editor = create_test_editor("abcdef\r\nXYZhello\r\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let params = make_search("hello", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        // Editor's line 1 is "XYZhello" (8 chars, \r stripped)
        // "hello" starts at col 3; cursor at match start, anchor at match end
        assert_eq!(editor.cursor_col, 3);
        assert_eq!(editor.selection_anchor, Some((1, 8))); // 3 + 5
    }

    #[test]
    fn clear_selection_resets_anchor() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_col = 0;
        editor.select_right();
        editor.select_right();
        assert!(editor.selection_anchor.is_some());
        editor.clear_selection();
        assert!(editor.selection_anchor.is_none());
        assert_eq!(editor.selected_text(), None);
    }

    #[test]
    fn select_right_exact_text_boundary() {
        // Verify selection of each character position matches expected text
        let mut editor = create_test_editor("abcde\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;

        let expected = ["a", "ab", "abc", "abcd", "abcde"];
        for (i, exp) in expected.iter().enumerate() {
            editor.select_right();
            assert_eq!(
                editor.selected_text(),
                Some(exp.to_string()),
                "After {} select_right calls, expected '{}' but got '{:?}'",
                i + 1,
                exp,
                editor.selected_text()
            );
        }
    }

    // --- Undo/Redo tests ---

    #[test]
    fn undo_insert_char() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.insert_char('!');
        assert_eq!(editor.get_line_text(0), Some("hello!".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.cursor_col, 5);
    }

    #[test]
    fn redo_insert_char() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.insert_char('!');
        editor.undo();
        editor.redo();
        assert_eq!(editor.get_line_text(0), Some("hello!".to_string()));
        assert_eq!(editor.cursor_col, 6);
    }

    #[test]
    fn undo_delete_backward() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.delete_char_backward();
        assert_eq!(editor.get_line_text(0), Some("hell".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.cursor_col, 5);
    }

    #[test]
    fn undo_delete_backward_join() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 1;
        editor.cursor_col = 0;
        editor.delete_char_backward();
        assert_eq!(editor.get_line_text(0), Some("helloworld".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.get_line_text(1), Some("world".to_string()));
        assert_eq!(editor.cursor_line, 1);
        assert_eq!(editor.cursor_col, 0);
    }

    #[test]
    fn undo_insert_newline() {
        let mut editor = create_test_editor("hello world\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.insert_newline();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.get_line_text(1), Some(" world".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello world".to_string()));
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 5);
    }

    #[test]
    fn undo_delete_line() {
        let mut editor = create_test_editor("aaa\nbbb\nccc\n");
        editor.cursor_line = 1;
        editor.cursor_col = 0;
        editor.delete_line();
        assert_eq!(editor.get_line_text(0), Some("aaa".to_string()));
        assert_eq!(editor.get_line_text(1), Some("ccc".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("aaa".to_string()));
        assert_eq!(editor.get_line_text(1), Some("bbb".to_string()));
        assert_eq!(editor.get_line_text(2), Some("ccc".to_string()));
    }

    #[test]
    fn undo_groups_consecutive_typing() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        // Type "abc" — should be one undo group
        editor.insert_char('a');
        editor.insert_char('b');
        editor.insert_char('c');
        assert_eq!(editor.get_line_text(0), Some("helloabc".to_string()));
        // Single undo should revert all three
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
    }

    #[test]
    fn undo_breaks_group_on_whitespace() {
        let mut editor = create_test_editor("\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.insert_char('a');
        editor.insert_char('b');
        editor.insert_char(' '); // whitespace breaks the group
        editor.insert_char('c');
        assert_eq!(editor.get_line_text(0), Some("ab c".to_string()));
        editor.undo(); // undoes "c"
        assert_eq!(editor.get_line_text(0), Some("ab ".to_string()));
        editor.undo(); // undoes " "
        assert_eq!(editor.get_line_text(0), Some("ab".to_string()));
        editor.undo(); // undoes "ab"
        assert_eq!(editor.get_line_text(0), Some("".to_string()));
    }

    #[test]
    fn redo_cleared_on_new_edit() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.insert_char('!');
        editor.undo();
        // New edit should clear redo stack
        editor.insert_char('?');
        editor.redo(); // should do nothing
        assert_eq!(editor.get_line_text(0), Some("hello?".to_string()));
    }

    #[test]
    fn multiple_undo_redo_cycle() {
        let mut editor = create_test_editor("abc\n");
        editor.cursor_line = 0;
        editor.cursor_col = 3;

        editor.insert_newline(); // "abc" → "abc\n" + new line
        editor.insert_char('d');
        editor.insert_char('e');

        editor.undo(); // undo "de"
        editor.undo(); // undo newline

        assert_eq!(editor.get_line_text(0), Some("abc".to_string()));

        editor.redo(); // redo newline
        assert_eq!(editor.get_line_text(0), Some("abc".to_string()));
        assert_eq!(editor.get_line_text(1), Some("".to_string()));

        editor.redo(); // redo "de"
        assert_eq!(editor.get_line_text(1), Some("de".to_string()));
    }

    #[test]
    fn undo_on_empty_stack() {
        let mut editor = create_test_editor("hello\n");
        // Undo with nothing to undo — should not panic
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
    }

    #[test]
    fn redo_on_empty_stack() {
        let mut editor = create_test_editor("hello\n");
        // Redo with nothing to redo — should not panic
        editor.redo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
    }

    #[test]
    fn undo_delete_line_single_line_file() {
        let mut editor = create_test_editor("only line\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.delete_line();
        // Single line file — line is cleared, not removed
        assert_eq!(editor.get_line_text(0), Some("".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("only line".to_string()));
    }

    #[test]
    fn undo_delete_line_last_line() {
        let mut editor = create_test_editor("aaa\nbbb\n");
        editor.cursor_line = 1;
        editor.delete_line();
        assert_eq!(editor.total_virtual_lines(), 1);
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("aaa".to_string()));
        assert_eq!(editor.get_line_text(1), Some("bbb".to_string()));
    }

    #[test]
    fn undo_insert_at_beginning_of_file() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.insert_char('X');
        assert_eq!(editor.get_line_text(0), Some("Xhello".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.cursor_col, 0);
    }

    #[test]
    fn undo_newline_at_end_of_file() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5;
        editor.insert_newline();
        assert_eq!(editor.total_virtual_lines(), 2);
        editor.undo();
        assert_eq!(editor.total_virtual_lines(), 1);
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
    }

    #[test]
    fn undo_all_then_redo_all() {
        let mut editor = create_test_editor("x\n");
        editor.cursor_line = 0;
        editor.cursor_col = 1;
        editor.insert_char('a');
        editor.insert_char(' '); // breaks group
        editor.insert_char('b');
        assert_eq!(editor.get_line_text(0), Some("xa b".to_string()));

        // Undo everything
        editor.undo(); // "b"
        editor.undo(); // " "
        editor.undo(); // "a"
        assert_eq!(editor.get_line_text(0), Some("x".to_string()));

        // Undo past the beginning — should be no-op
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("x".to_string()));

        // Redo everything
        editor.redo(); // "a"
        editor.redo(); // " "
        editor.redo(); // "b"
        assert_eq!(editor.get_line_text(0), Some("xa b".to_string()));

        // Redo past the end — should be no-op
        editor.redo();
        assert_eq!(editor.get_line_text(0), Some("xa b".to_string()));
    }

    #[test]
    fn undo_delete_forward_at_end_of_line() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5; // end of "hello"
        editor.delete_char_forward(); // joins with "world"
        assert_eq!(editor.get_line_text(0), Some("helloworld".to_string()));
        editor.undo();
        assert_eq!(editor.get_line_text(0), Some("hello".to_string()));
        assert_eq!(editor.get_line_text(1), Some("world".to_string()));
    }

    #[test]
    fn undo_preserves_cursor_position() {
        let mut editor = create_test_editor("abcdef\n");
        editor.cursor_line = 0;
        editor.cursor_col = 3;
        editor.insert_char('X');
        assert_eq!(editor.cursor_col, 4);
        editor.undo();
        assert_eq!(editor.cursor_col, 3); // cursor restored to before insert
        editor.redo();
        assert_eq!(editor.cursor_col, 4); // cursor at after position
    }

    #[test]
    fn no_undo_for_noop_operations() {
        let mut editor = create_test_editor("hello\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        // Backspace at position 0 on first line — no-op
        editor.delete_char_backward();
        // Undo stack should be empty
        assert_eq!(editor.undo_stack.len(), 0);

        // Delete forward at end of last line — no-op
        editor.cursor_col = 5;
        editor.cursor_line = 0;
        // Only one line, cursor at end
        let total = editor.total_virtual_lines();
        if editor.cursor_col >= editor.current_line_len() && editor.cursor_line + 1 >= total {
            editor.delete_char_forward(); // no-op
        }
        assert_eq!(editor.undo_stack.len(), 0);
    }

    // --- Word navigation tests ---

    #[test]
    fn word_right_basic() {
        let mut editor = create_test_editor("hello world foo\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.word_right();
        assert_eq!(editor.cursor_col, 6); // past "hello " → start of "world"
        editor.word_right();
        assert_eq!(editor.cursor_col, 12); // past "world " → start of "foo"
        editor.word_right();
        assert_eq!(editor.cursor_col, 15); // end of line
    }

    #[test]
    fn word_left_basic() {
        let mut editor = create_test_editor("hello world foo\n");
        editor.cursor_line = 0;
        editor.cursor_col = 15;
        editor.word_left();
        assert_eq!(editor.cursor_col, 12); // start of "foo"
        editor.word_left();
        assert_eq!(editor.cursor_col, 6); // start of "world"
        editor.word_left();
        assert_eq!(editor.cursor_col, 0); // start of "hello"
    }

    #[test]
    fn word_right_with_punctuation() {
        let mut editor = create_test_editor("foo.bar(baz)\n");
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        editor.word_right();
        assert_eq!(editor.cursor_col, 4); // past "foo." → start of "bar"
        editor.word_right();
        assert_eq!(editor.cursor_col, 8); // past "bar(" → start of "baz"
    }

    #[test]
    fn word_left_with_punctuation() {
        let mut editor = create_test_editor("foo.bar(baz)\n");
        editor.cursor_line = 0;
        editor.cursor_col = 12;
        editor.word_left();
        assert_eq!(editor.cursor_col, 8); // start of "baz"
        editor.word_left();
        assert_eq!(editor.cursor_col, 4); // start of "bar"
        editor.word_left();
        assert_eq!(editor.cursor_col, 0); // start of "foo"
    }

    #[test]
    fn word_right_wraps_to_next_line() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 0;
        editor.cursor_col = 5; // end of "hello"
        editor.word_right();
        assert_eq!(editor.cursor_line, 1);
        assert_eq!(editor.cursor_col, 0);
    }

    #[test]
    fn word_left_wraps_to_prev_line() {
        let mut editor = create_test_editor("hello\nworld\n");
        editor.cursor_line = 1;
        editor.cursor_col = 0;
        editor.word_left();
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 5);
    }

    // --- Mouse click / display_col_to_orig_col tests ---

    #[test]
    fn display_col_to_orig_col_basic() {
        assert_eq!(display_col_to_orig_col("hello", 0, 0), 0);
        assert_eq!(display_col_to_orig_col("hello", 3, 0), 3);
        assert_eq!(display_col_to_orig_col("hello", 5, 0), 5); // past end
        assert_eq!(display_col_to_orig_col("hello", 99, 0), 5); // way past end
    }

    #[test]
    fn display_col_to_orig_col_with_tab() {
        // "a\tb" → display "a   b" (tab expands to 3 spaces, reaching col 4)
        // display col 0 → orig 0 (a)
        // display col 1 → orig 1 (tab start)
        // display col 4 → orig 2 (b)
        assert_eq!(display_col_to_orig_col("a\tb", 0, 0), 0);
        assert_eq!(display_col_to_orig_col("a\tb", 1, 0), 1);
        assert_eq!(display_col_to_orig_col("a\tb", 4, 0), 2);
    }

    #[test]
    fn display_col_to_orig_col_with_scroll() {
        // "hello world" with scroll_x=6 → display "world"
        // display col 0 → orig 6 (w)
        // display col 3 → orig 9 (l)
        assert_eq!(display_col_to_orig_col("hello world", 0, 6), 6);
        assert_eq!(display_col_to_orig_col("hello world", 3, 6), 9);
    }

    #[test]
    fn display_col_to_orig_col_with_control_char() {
        // "ab\x01cd" → display "abcd" (control stripped)
        // display col 2 → orig 3 (c, skipping \x01)
        assert_eq!(display_col_to_orig_col("ab\x01cd", 0, 0), 0);
        assert_eq!(display_col_to_orig_col("ab\x01cd", 2, 0), 3);
        assert_eq!(display_col_to_orig_col("ab\x01cd", 3, 0), 4);
    }

    #[test]
    fn click_at_positions_cursor() {
        let mut editor = create_test_editor("hello world\nsecond line\nthird line\n");
        // Simulate viewport: inner starts at (1, 1), line_num_width=6
        editor.viewport_x = 1;
        editor.viewport_y = 1;
        editor.line_num_width = 6;
        editor.scroll_y = 0;
        editor.scroll_x = 0;
        editor.visible_lines = 10;

        // Click on first char of line 0: screen (7, 1) → text_x=7, display_col=0
        editor.click_at(7, 1);
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 0);

        // Click on col 5 of line 0: screen (12, 1) → display_col=5
        editor.click_at(12, 1);
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 5);

        // Click on line 2: screen row 3
        editor.click_at(7, 3);
        assert_eq!(editor.cursor_line, 2);
        assert_eq!(editor.cursor_col, 0);
    }

    #[test]
    fn click_at_with_scroll() {
        let mut editor = create_test_editor("line0\nline1\nline2\nline3\nline4\n");
        editor.viewport_x = 1;
        editor.viewport_y = 1;
        editor.line_num_width = 6;
        editor.scroll_y = 2; // scrolled: first visible line is line2
        editor.scroll_x = 0;
        editor.visible_lines = 3;

        // Click on first visible row → line 2
        editor.click_at(7, 1);
        assert_eq!(editor.cursor_line, 2);

        // Click on second visible row → line 3
        editor.click_at(7, 2);
        assert_eq!(editor.cursor_line, 3);
    }

    #[test]
    fn click_at_clears_selection() {
        let mut editor = create_test_editor("hello world\n");
        editor.viewport_x = 1;
        editor.viewport_y = 1;
        editor.line_num_width = 6;
        editor.scroll_y = 0;
        editor.scroll_x = 0;
        editor.visible_lines = 10;

        // Set a selection
        editor.selection_anchor = Some((0, 2));
        editor.cursor_col = 5;
        assert!(editor.selection_range().is_some());

        // Click clears it
        editor.click_at(10, 1);
        assert!(editor.selection_anchor.is_none());
    }

    #[test]
    fn click_on_gutter_ignored() {
        let mut editor = create_test_editor("hello\n");
        editor.viewport_x = 1;
        editor.viewport_y = 1;
        editor.line_num_width = 6;
        editor.scroll_y = 0;
        editor.scroll_x = 0;
        editor.visible_lines = 10;
        editor.cursor_line = 0;
        editor.cursor_col = 3;

        // Click on line number area (col < text_x=7)
        editor.click_at(3, 1);
        // Cursor should not move
        assert_eq!(editor.cursor_col, 3);
    }

    #[test]
    fn click_past_end_of_line_clamps() {
        let mut editor = create_test_editor("hi\n");
        editor.viewport_x = 1;
        editor.viewport_y = 1;
        editor.line_num_width = 6;
        editor.scroll_y = 0;
        editor.scroll_x = 0;
        editor.visible_lines = 10;

        // Click at display col 50 on a 2-char line → clamp to col 2
        editor.click_at(57, 1);
        assert_eq!(editor.cursor_line, 0);
        assert_eq!(editor.cursor_col, 2);
    }

    // --- Large file / multi-chunk search tests ---

    /// Create a test file large enough to force multi-chunk reads (>4MB).
    /// Returns (editor, line_number_of_needle) with a known "NEEDLE" on one line.
    fn create_large_test_editor(needle_line: usize, total_lines: usize) -> (EditorState, usize) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static LARGE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let id = LARGE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("mm_large_test_{}_{}.txt", std::process::id(), id,));
        let mut f = std::fs::File::create(&path).unwrap();
        for i in 0..total_lines {
            if i == needle_line {
                writeln!(f, "line {:06} NEEDLE_PATTERN_HERE", i).unwrap();
            } else {
                // ~80 chars per line to make the file large
                writeln!(
                    f,
                    "line {:06} padding text to make this line reasonably long for chunk testing",
                    i
                )
                .unwrap();
            }
        }
        drop(f);
        let editor = EditorState::open(path);
        (editor, needle_line)
    }

    #[test]
    fn search_forward_large_file() {
        // ~6MB file, needle near the end → forces multi-chunk forward search
        let (mut editor, needle_line) = create_large_test_editor(70_000, 80_000);
        editor.cursor_line = 0;
        editor.cursor_col = 0;
        let params = make_search("NEEDLE_PATTERN_HERE", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle_line,
            "Forward search should find needle at line {}",
            needle_line
        );
    }

    #[test]
    fn search_backward_large_file() {
        // Cursor past the needle → backward search reads backwards in chunks
        let (mut editor, needle_line) = create_large_test_editor(10_000, 80_000);
        editor.cursor_line = 79_999;
        editor.cursor_col = 0;
        let params = make_search("NEEDLE_PATTERN_HERE", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle_line,
            "Backward search should find needle at line {}",
            needle_line
        );
    }

    #[test]
    fn search_forward_wrap_large_file() {
        // Cursor AFTER the needle → forward search wraps from beginning
        let (mut editor, needle_line) = create_large_test_editor(5_000, 80_000);
        editor.cursor_line = 60_000;
        editor.cursor_col = 0;
        let params = make_search("NEEDLE_PATTERN_HERE", SearchDirection::Forward, true);
        assert!(editor.find(&params));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle_line,
            "Forward wrap should find needle at line {}",
            needle_line
        );
    }

    #[test]
    fn search_backward_wrap_large_file() {
        // Cursor BEFORE the needle → backward search wraps from end
        let (mut editor, needle_line) = create_large_test_editor(70_000, 80_000);
        editor.cursor_line = 5_000;
        editor.cursor_col = 0;
        let params = make_search("NEEDLE_PATTERN_HERE", SearchDirection::Backward, true);
        assert!(editor.find(&params));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle_line,
            "Backward wrap should find needle at line {}",
            needle_line
        );
    }

    #[test]
    fn search_backward_findnext_large_file() {
        // Two needles: cursor past both. Backward find should hit the later one first,
        // then the earlier one on repeat.
        use std::sync::atomic::{AtomicU64, Ordering};
        static LARGE2_COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let id = LARGE2_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("mm_large2_{}_{}.txt", std::process::id(), id,));
        let mut f = std::fs::File::create(&path).unwrap();
        let total = 80_000;
        let needle1 = 20_000;
        let needle2 = 60_000;
        for i in 0..total {
            if i == needle1 || i == needle2 {
                writeln!(f, "line {:06} NEEDLE_PATTERN_HERE", i).unwrap();
            } else {
                writeln!(
                    f,
                    "line {:06} padding text to make this line reasonably long for chunk testing",
                    i
                )
                .unwrap();
            }
        }
        drop(f);
        let mut editor = EditorState::open(path);
        editor.cursor_line = 79_999;
        editor.cursor_col = 0;

        let bwd = make_search("NEEDLE_PATTERN_HERE", SearchDirection::Backward, true);

        // First backward: should find needle2 (line 60000)
        assert!(editor.find(&bwd));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle2,
            "First backward should find needle at line {}",
            needle2
        );

        // Second backward: should find needle1 (line 20000)
        assert!(editor.find(&bwd));
        assert_eq!(
            editor.selection_anchor.unwrap().0,
            needle1,
            "Second backward should find needle at line {}",
            needle1
        );
    }

    #[test]
    fn base64_encode_basic() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"abc"), "YWJj");
    }
}
