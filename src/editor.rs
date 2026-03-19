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

    pub fn find(&mut self, params: &SearchParams) -> bool {
        if params.query.is_empty() {
            return false;
        }
        self.scan_to_end();
        match params.direction {
            SearchDirection::Forward => self.find_forward(params),
            SearchDirection::Backward => self.find_backward(params),
        }
    }

    fn find_forward(&mut self, params: &SearchParams) -> bool {
        let query = if params.case_sensitive {
            params.query.clone()
        } else {
            params.query.to_lowercase()
        };
        let total = self.total_virtual_lines();
        let start_line = self.cursor_line;
        let start_col = self.cursor_col + 1;

        // Search from cursor to end
        for vline in start_line..total {
            if let Some(text) = self.get_line_text(vline) {
                let search_text = if params.case_sensitive {
                    text.clone()
                } else {
                    text.to_lowercase()
                };
                let byte_offset = if vline == start_line {
                    char_to_byte(&search_text, start_col.min(search_text.chars().count()))
                } else {
                    0
                };
                if let Some(byte_pos) = search_text[byte_offset..].find(&query) {
                    let col = search_text[..byte_offset + byte_pos].chars().count();
                    let match_len = query.chars().count();
                    self.selection_anchor = Some((vline, col));
                    self.cursor_line = vline;
                    self.cursor_col = col + match_len;
                    self.desired_col = self.cursor_col;
                    self.scroll_to_cursor();
                    return true;
                }
            }
        }

        // Wrap: search from beginning to cursor
        for vline in 0..=start_line.min(total.saturating_sub(1)) {
            if let Some(text) = self.get_line_text(vline) {
                let search_text = if params.case_sensitive {
                    text.clone()
                } else {
                    text.to_lowercase()
                };
                let limit = if vline == start_line {
                    char_to_byte(
                        &search_text,
                        self.cursor_col.min(search_text.chars().count()),
                    )
                } else {
                    search_text.len()
                };
                if let Some(byte_pos) = search_text[..limit].find(&query) {
                    let col = search_text[..byte_pos].chars().count();
                    let match_len = query.chars().count();
                    self.selection_anchor = Some((vline, col));
                    self.cursor_line = vline;
                    self.cursor_col = col + match_len;
                    self.desired_col = self.cursor_col;
                    self.scroll_to_cursor();
                    return true;
                }
            }
        }

        false
    }

    fn find_backward(&mut self, params: &SearchParams) -> bool {
        let query = if params.case_sensitive {
            params.query.clone()
        } else {
            params.query.to_lowercase()
        };
        let total = self.total_virtual_lines();
        let start_line = self.cursor_line;
        let start_col = self.cursor_col;

        // Search from cursor backward to beginning
        for vline in (0..=start_line).rev() {
            if let Some(text) = self.get_line_text(vline) {
                let search_text = if params.case_sensitive {
                    text.clone()
                } else {
                    text.to_lowercase()
                };
                let limit = if vline == start_line {
                    char_to_byte(
                        &search_text,
                        start_col.saturating_sub(1).min(search_text.chars().count()),
                    )
                } else {
                    search_text.len()
                };
                if let Some(byte_pos) = search_text[..limit].rfind(&query) {
                    let col = search_text[..byte_pos].chars().count();
                    let match_len = query.chars().count();
                    self.selection_anchor = Some((vline, col));
                    self.cursor_line = vline;
                    self.cursor_col = col + match_len;
                    self.desired_col = self.cursor_col;
                    self.scroll_to_cursor();
                    return true;
                }
            }
        }

        // Wrap: search from end backward to cursor
        for vline in (start_line..total).rev() {
            if let Some(text) = self.get_line_text(vline) {
                let search_text = if params.case_sensitive {
                    text.clone()
                } else {
                    text.to_lowercase()
                };
                if let Some(byte_pos) = search_text.rfind(&query) {
                    let col = search_text[..byte_pos].chars().count();
                    let match_len = query.chars().count();
                    self.selection_anchor = Some((vline, col));
                    self.cursor_line = vline;
                    self.cursor_col = col + match_len;
                    self.desired_col = self.cursor_col;
                    self.scroll_to_cursor();
                    return true;
                }
            }
        }

        false
    }

    // --- Selection ---

    pub fn clear_selection(&mut self) {
        self.selection_anchor = None;
    }

    fn ensure_anchor(&mut self) {
        if self.selection_anchor.is_none() {
            self.selection_anchor = Some((self.cursor_line, self.cursor_col));
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
