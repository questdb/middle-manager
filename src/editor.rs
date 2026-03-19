use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write as IoWrite};
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::app::SearchDirection;

#[derive(Clone)]
pub struct SearchParams {
    pub query: String,
    pub direction: SearchDirection,
    pub case_sensitive: bool,
}

const INDEX_INTERVAL: usize = 1000;

#[derive(Clone)]
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
}

#[derive(Clone)]
enum Segment {
    Original { start_line: usize, count: usize },
    Buffer { lines: Vec<String> },
}

impl EditorState {
    pub fn open(path: PathBuf) -> Self {
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
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
            modified: false,
            status_msg: None,
            selection_anchor: None,
            search_selection: false,
            last_search: None,
        };
        // Pre-scan first batch of lines for immediate display
        state.scan_to_line(10_000);
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

    // --- Editing operations ---

    pub fn insert_char(&mut self, c: char) {
        let vline = self.cursor_line;
        self.materialize_line(vline);

        let (seg_idx, offset) = self.find_segment(vline).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            let line = &mut lines[offset];
            let byte_pos = char_to_byte(line, self.cursor_col);
            line.insert(byte_pos, c);
            self.cursor_col += 1;
            self.desired_col = self.cursor_col;
        }
        self.modified = true;
        self.scroll_to_cursor();
    }

    pub fn delete_char_backward(&mut self) {
        if self.cursor_col == 0 {
            // Join with previous line
            if self.cursor_line == 0 {
                return;
            }
            let prev = self.cursor_line - 1;
            self.materialize_line(prev);
            self.materialize_line(self.cursor_line);

            // Recalculate segment positions after materialization
            let prev_text = self.get_line_text(prev).unwrap_or_default();
            let cur_text = self.get_line_text(self.cursor_line).unwrap_or_default();
            let new_col = prev_text.chars().count();
            let joined = format!("{}{}", prev_text, cur_text);

            // Set the previous line to the joined text
            let (seg_idx, offset) = self.find_segment(prev).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset] = joined;
            }

            // Remove the current line
            self.remove_virtual_line(self.cursor_line);

            self.cursor_line = prev;
            self.cursor_col = new_col;
            self.desired_col = self.cursor_col;
            self.modified = true;
            self.scroll_to_cursor();
            return;
        }

        let vline = self.cursor_line;
        self.materialize_line(vline);
        let (seg_idx, offset) = self.find_segment(vline).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            let line = &mut lines[offset];
            let byte_pos = char_to_byte(line, self.cursor_col);
            let prev_byte = char_to_byte(line, self.cursor_col - 1);
            line.drain(prev_byte..byte_pos);
            self.cursor_col -= 1;
            self.desired_col = self.cursor_col;
        }
        self.modified = true;
    }

    pub fn delete_char_forward(&mut self) {
        let line_len = self.current_line_len();
        if self.cursor_col >= line_len {
            // Join with next line
            let next = self.cursor_line + 1;
            if next >= self.total_virtual_lines() {
                return;
            }
            self.materialize_line(self.cursor_line);
            self.materialize_line(next);

            let cur_text = self.get_line_text(self.cursor_line).unwrap_or_default();
            let next_text = self.get_line_text(next).unwrap_or_default();
            let joined = format!("{}{}", cur_text, next_text);

            let (seg_idx, offset) = self.find_segment(self.cursor_line).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset] = joined;
            }
            self.remove_virtual_line(next);
            self.modified = true;
            return;
        }

        let vline = self.cursor_line;
        self.materialize_line(vline);
        let (seg_idx, offset) = self.find_segment(vline).unwrap();
        if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
            let line = &mut lines[offset];
            let byte_start = char_to_byte(line, self.cursor_col);
            let byte_end = char_to_byte(line, self.cursor_col + 1);
            line.drain(byte_start..byte_end);
        }
        self.modified = true;
    }

    pub fn insert_newline(&mut self) {
        let vline = self.cursor_line;
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
        self.modified = true;
        self.scroll_to_cursor();
    }

    pub fn delete_line(&mut self) {
        let total = self.total_virtual_lines();
        if total == 0 {
            return;
        }

        if total == 1 {
            // Clear the only line
            self.materialize_line(0);
            let (seg_idx, offset) = self.find_segment(0).unwrap();
            if let Segment::Buffer { ref mut lines } = self.segments[seg_idx] {
                lines[offset] = String::new();
            }
            self.cursor_col = 0;
            self.desired_col = 0;
            self.modified = true;
            return;
        }

        self.remove_virtual_line(self.cursor_line);

        if self.cursor_line >= self.total_virtual_lines() {
            self.cursor_line = self.total_virtual_lines().saturating_sub(1);
        }
        self.clamp_cursor_col();
        self.modified = true;
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
            SearchDirection::Forward => self.find_streaming(&query_bytes, params.case_sensitive, false),
            SearchDirection::Backward => self.find_streaming(&query_bytes, params.case_sensitive, true),
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
    fn find_streaming(
        &mut self,
        query: &[u8],
        case_sensitive: bool,
        reverse: bool,
    ) -> bool {
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
                if pass == 0 { cursor_col } else { anchor_col }
            } else {
                if pass == 0 { anchor_col } else { cursor_col }
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
                    (false, 0) => (true, false),  // forward from cursor
                    (false, 1) => (false, true),  // forward wrap: start to cursor
                    (true, 0)  => (false, true),  // backward: start to cursor (find last)
                    (true, 1)  => (true, false),  // backward wrap: cursor to end (find last)
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
                    if use_skip { Some((skip_lines_in_seg, skip_col)) } else { None },
                    if use_limit { Some((skip_lines_in_seg, skip_col)) } else { None },
                )
            } else if let Segment::Buffer { lines } = &self.segments[seg_idx] {
                search_buffer_segment(
                    lines,
                    seg_vline_start,
                    query,
                    case_sensitive,
                    reverse,
                    if use_skip { Some((skip_lines_in_seg, skip_col)) } else { None },
                    if use_limit { Some((skip_lines_in_seg, skip_col)) } else { None },
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
                read_start_byte, effective_end_byte, start_line_offset, vline_start,
                orig_start_line, query, search_fn, match_char_len, skip_to, limit_to,
            )
        } else {
            self.search_original_forward(
                read_start_byte, effective_end_byte, start_line_offset, vline_start,
                query, search_fn, match_char_len, skip_to, limit_to,
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
            if n == 0 { break; }
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
                } else { 0 }
            } else { 0 };

            let search_end = if let Some((ll, lc)) = limit_to {
                if current_line <= ll {
                    line_col_to_byte_pos(chunk, ll - current_line, lc).min(chunk.len())
                } else { 0 }
            } else { chunk.len() };

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
        if total_bytes == 0 { return None; }

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
            if n == 0 { break; }
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
            } else { 0 };

            let search_end = if let Some((ll, lc)) = limit_to {
                if seg_relative_line <= ll {
                    line_col_to_byte_pos(chunk, ll - seg_relative_line, lc).min(n)
                } else {
                    0
                }
            } else { n };

            if search_start < search_end {
                let slice = &chunk[search_start..search_end];
                if let Some(found) = rfind_with(slice, query, search_fn) {
                    let abs_pos = search_start + found;
                    let (line_at, col_at) =
                        byte_pos_to_line_col(chunk, abs_pos, seg_relative_line);
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
                    if self.lines_scanned % INDEX_INTERVAL == 0 {
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
                if !reverse { continue } else { 0 }
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
                if reverse { continue } else { break }
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
        match haystack[i..].iter().position(|b| b.to_ascii_lowercase() == first_lower) {
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

fn char_to_byte(s: &str, char_pos: usize) -> usize {
    s.char_indices()
        .nth(char_pos)
        .map(|(i, _)| i)
        .unwrap_or(s.len())
}

fn osc52_copy(text: &str) {
    use std::io::Write;
    let encoded = base64_encode(text.as_bytes());
    let osc = format!("\x1b]52;c;{}\x1b\\", encoded);
    let _ = std::io::stdout().write_all(osc.as_bytes());
    let _ = std::io::stdout().flush();
}

// Exposed for tests
pub fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
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
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "mm_test_{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

    // --- Large file / multi-chunk search tests ---

    /// Create a test file large enough to force multi-chunk reads (>4MB).
    /// Returns (editor, line_number_of_needle) with a known "NEEDLE" on one line.
    fn create_large_test_editor(needle_line: usize, total_lines: usize) -> (EditorState, usize) {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "mm_large_test_{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "mm_large2_{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
