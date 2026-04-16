use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write as IoWrite};
use std::path::Path;
use std::path::PathBuf;

pub const BYTES_PER_ROW: usize = 16;
const BUFFER_BYTES: usize = 256 * 1024;
const BINARY_CHECK_BYTES: usize = 8192;
/// Chunk size for file search (1 MB).
const SEARCH_CHUNK: usize = 1024 * 1024;

#[derive(Clone)]
pub struct HexViewerState {
    pub path: PathBuf,
    pub file_size: u64,
    pub scroll_offset: usize, // top visible row index
    pub visible_rows: usize,

    buffer: Vec<u8>,
    buffer_start_row: usize,

    // Cursor
    pub cursor_offset: u64,
    /// 0 = high nibble, 1 = low nibble (hex side only).
    pub cursor_nibble: u8,
    /// Whether the cursor is on the ASCII pane.
    pub editing_ascii: bool,

    // Modifications overlay: offset → new byte value.
    pub modifications: HashMap<u64, u8>,
    pub modified: bool,

    // Undo / redo
    undo_stack: Vec<HexUndoEntry>,
    redo_stack: Vec<HexUndoEntry>,

    // Selection
    /// Byte offset of selection anchor. Selection spans from anchor to cursor.
    pub selection_anchor: Option<u64>,

    // Search
    pub last_search_pattern: Option<Vec<u8>>,
    /// Byte offset of the current search match (for highlighting / navigation).
    pub search_match: Option<(u64, usize)>, // (offset, pattern_len)

    pub status_msg: Option<String>,
}

#[derive(Clone)]
struct HexUndoEntry {
    offset: u64,
    /// Previous value in the modifications map (None = was unmodified / original file byte).
    old: Option<u8>,
    /// New value written.
    new: u8,
}

impl HexViewerState {
    pub fn open(path: PathBuf) -> Self {
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut state = Self {
            path,
            file_size,
            scroll_offset: 0,
            visible_rows: 0,
            buffer: Vec::new(),
            buffer_start_row: 0,
            cursor_offset: 0,
            cursor_nibble: 0,
            editing_ascii: false,
            modifications: HashMap::new(),
            modified: false,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            selection_anchor: None,
            last_search_pattern: None,
            search_match: None,
            status_msg: None,
        };
        state.load_buffer_at_byte(0);
        state
    }

    pub fn is_binary(path: &Path) -> bool {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mut buf = [0u8; BINARY_CHECK_BYTES];
        let n = file.read(&mut buf).unwrap_or(0);
        buf[..n].contains(&0)
    }

    pub fn total_rows(&self) -> usize {
        (self.file_size as usize).div_ceil(BYTES_PER_ROW)
    }

    fn max_scroll(&self) -> usize {
        self.total_rows().saturating_sub(self.visible_rows)
    }

    // --- Scrolling (viewport only, no cursor) ---

    pub fn scroll_up(&mut self, amount: usize) {
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_down(&mut self, amount: usize) {
        self.scroll_offset = (self.scroll_offset + amount).min(self.max_scroll());
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_top(&mut self) {
        self.cursor_offset = 0;
        self.cursor_nibble = 0;
        self.scroll_offset = 0;
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_bottom(&mut self) {
        self.cursor_offset = self.file_size.saturating_sub(1);
        self.cursor_nibble = 0;
        self.scroll_offset = self.max_scroll();
        self.ensure_buffer_covers_viewport();
    }

    // --- Cursor movement ---

    pub fn cursor_up(&mut self) {
        if self.cursor_offset >= BYTES_PER_ROW as u64 {
            self.cursor_offset -= BYTES_PER_ROW as u64;
        } else {
            self.cursor_offset = 0;
        }
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn cursor_down(&mut self) {
        let new = self.cursor_offset + BYTES_PER_ROW as u64;
        if new < self.file_size {
            self.cursor_offset = new;
        } else {
            self.cursor_offset = self.file_size.saturating_sub(1);
        }
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn cursor_left(&mut self) {
        if self.editing_ascii {
            if self.cursor_offset > 0 {
                self.cursor_offset -= 1;
            }
        } else if self.cursor_nibble == 1 {
            self.cursor_nibble = 0;
        } else if self.cursor_offset > 0 {
            self.cursor_offset -= 1;
            self.cursor_nibble = 1;
        }
        self.scroll_to_cursor();
    }

    pub fn cursor_right(&mut self) {
        if self.editing_ascii {
            if self.cursor_offset + 1 < self.file_size {
                self.cursor_offset += 1;
            }
        } else if self.cursor_nibble == 0 {
            self.cursor_nibble = 1;
        } else if self.cursor_offset + 1 < self.file_size {
            self.cursor_offset += 1;
            self.cursor_nibble = 0;
        }
        self.scroll_to_cursor();
    }

    pub fn cursor_row_start(&mut self) {
        self.cursor_offset &= !(BYTES_PER_ROW as u64 - 1);
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn cursor_row_end(&mut self) {
        let row_start = self.cursor_offset & !(BYTES_PER_ROW as u64 - 1);
        let row_end = (row_start + BYTES_PER_ROW as u64 - 1).min(self.file_size.saturating_sub(1));
        self.cursor_offset = row_end;
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn page_up(&mut self) {
        let page = (self.visible_rows.max(1) as u64) * BYTES_PER_ROW as u64;
        self.cursor_offset = self.cursor_offset.saturating_sub(page);
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn page_down(&mut self) {
        let page = (self.visible_rows.max(1) as u64) * BYTES_PER_ROW as u64;
        let new = self.cursor_offset + page;
        self.cursor_offset = if new < self.file_size {
            new
        } else {
            self.file_size.saturating_sub(1)
        };
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }

    pub fn toggle_ascii(&mut self) {
        self.editing_ascii = !self.editing_ascii;
        self.cursor_nibble = 0;
    }

    // --- Selection ---

    pub fn clear_selection(&mut self) {
        self.selection_anchor = None;
    }

    /// Set anchor at current cursor if not already set.
    fn ensure_anchor(&mut self) {
        if self.selection_anchor.is_none() {
            self.selection_anchor = Some(self.cursor_offset);
        }
    }

    /// Returns ordered selection range (inclusive): (start_offset, end_offset).
    pub fn selection_range(&self) -> Option<(u64, u64)> {
        let anchor = self.selection_anchor?;
        let cursor = self.cursor_offset;
        if anchor <= cursor {
            Some((anchor, cursor))
        } else {
            Some((cursor, anchor))
        }
    }

    /// Select-move variants: set anchor, then move cursor.
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
        // In select mode, always move by whole byte
        if self.cursor_offset > 0 {
            self.cursor_offset -= 1;
        }
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }
    pub fn select_right(&mut self) {
        self.ensure_anchor();
        if self.cursor_offset + 1 < self.file_size {
            self.cursor_offset += 1;
        }
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }
    pub fn select_all(&mut self) {
        if self.file_size == 0 {
            return;
        }
        self.selection_anchor = Some(0);
        self.cursor_offset = self.file_size - 1;
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
    }
    pub fn select_page_up(&mut self) {
        self.ensure_anchor();
        self.page_up();
    }
    pub fn select_page_down(&mut self) {
        self.ensure_anchor();
        self.page_down();
    }

    /// Get selected bytes as a hex string ("FF 00 AB").
    pub fn selected_text_hex(&self) -> Option<String> {
        let (start, end) = self.selection_range()?;
        let mut parts = Vec::new();
        for off in start..=end {
            if let Some(b) = self.get_byte(off) {
                parts.push(format!("{:02X}", b));
            }
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" "))
        }
    }

    /// Get selected bytes as ASCII text (non-printable → '.').
    pub fn selected_text_ascii(&self) -> Option<String> {
        let (start, end) = self.selection_range()?;
        let mut result = String::new();
        for off in start..=end {
            if let Some(b) = self.get_byte(off) {
                if (0x20..=0x7E).contains(&b) {
                    result.push(b as char);
                } else {
                    result.push('.');
                }
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Copy selection to clipboard. Uses hex format when on hex side, ASCII when on ASCII side.
    pub fn copy_to_clipboard(&self) -> Option<usize> {
        let text = if self.editing_ascii {
            self.selected_text_ascii()?
        } else {
            self.selected_text_hex()?
        };
        let len = text.len();
        crate::clipboard::copy(&text);
        Some(len)
    }

    /// Return numeric interpretations of selected bytes (1/2/4/8 bytes).
    /// Returns None if no selection or selection length isn't 1/2/4/8.
    pub fn selection_info(&self) -> Option<String> {
        let (start, end) = self.selection_range()?;
        let len = (end - start + 1) as usize;
        if !matches!(len, 1 | 2 | 4 | 8) {
            // Just show byte count for other sizes
            return Some(format!("{} bytes selected", len));
        }
        let mut bytes = Vec::with_capacity(len);
        for off in start..=end {
            bytes.push(self.get_byte(off).unwrap_or(0));
        }
        let mut parts = Vec::new();
        match len {
            1 => {
                let b = bytes[0];
                parts.push(format!("u8: {}  i8: {}", b, b as i8));
            }
            2 => {
                let le = u16::from_le_bytes([bytes[0], bytes[1]]);
                let be = u16::from_be_bytes([bytes[0], bytes[1]]);
                parts.push(format!(
                    "LE u16: {} i16: {} | BE u16: {} i16: {}",
                    le, le as i16, be, be as i16
                ));
            }
            4 => {
                let buf: [u8; 4] = bytes[..4].try_into().unwrap();
                let le_u = u32::from_le_bytes(buf);
                let be_u = u32::from_be_bytes(buf);
                let le_f = f32::from_le_bytes(buf);
                let be_f = f32::from_be_bytes(buf);
                parts.push(format!(
                    "LE u32: {} i32: {} f32: {:.6} | BE u32: {} i32: {} f32: {:.6}",
                    le_u, le_u as i32, le_f, be_u, be_u as i32, be_f
                ));
            }
            8 => {
                let buf: [u8; 8] = bytes[..8].try_into().unwrap();
                let le_u = u64::from_le_bytes(buf);
                let be_u = u64::from_be_bytes(buf);
                let le_f = f64::from_le_bytes(buf);
                let be_f = f64::from_be_bytes(buf);
                parts.push(format!(
                    "LE u64: {} i64: {} f64: {:.6} | BE u64: {} i64: {} f64: {:.6}",
                    le_u, le_u as i64, le_f, be_u, be_u as i64, be_f
                ));
            }
            _ => unreachable!(),
        }
        Some(parts.join(""))
    }

    /// Ensure cursor is visible, adjusting scroll_offset if needed.
    pub fn scroll_to_cursor(&mut self) {
        let cursor_row = (self.cursor_offset as usize) / BYTES_PER_ROW;
        if cursor_row < self.scroll_offset {
            self.scroll_offset = cursor_row;
        } else if self.visible_rows > 0 && cursor_row >= self.scroll_offset + self.visible_rows {
            self.scroll_offset = cursor_row - self.visible_rows + 1;
        }
        self.scroll_offset = self.scroll_offset.min(self.max_scroll());
        self.ensure_buffer_covers_viewport();
    }

    // --- Byte access (with overlay) ---

    /// Get the effective byte at the given file offset (overlay or file).
    pub fn get_byte(&self, offset: u64) -> Option<u8> {
        if offset >= self.file_size {
            return None;
        }
        if let Some(&b) = self.modifications.get(&offset) {
            return Some(b);
        }
        // Read from buffer
        let row = (offset as usize) / BYTES_PER_ROW;
        if row < self.buffer_start_row {
            return None;
        }
        let buf_offset = (offset as usize) - self.buffer_start_row * BYTES_PER_ROW;
        self.buffer.get(buf_offset).copied()
    }

    /// Returns (byte_offset, row_bytes) for each visible row, with modifications applied.
    pub fn visible_rows_data(&mut self) -> Vec<(u64, Vec<u8>)> {
        self.ensure_buffer_covers_viewport();

        let mut rows = Vec::with_capacity(self.visible_rows);
        for i in 0..self.visible_rows {
            let row = self.scroll_offset + i;
            let byte_offset = (row * BYTES_PER_ROW) as u64;
            if byte_offset >= self.file_size {
                break;
            }
            let mut bytes = self.get_row_bytes(row);
            // Apply modifications overlay
            for (j, b) in bytes.iter_mut().enumerate() {
                let off = byte_offset + j as u64;
                if let Some(&modified) = self.modifications.get(&off) {
                    *b = modified;
                }
            }
            if bytes.is_empty() {
                break;
            }
            rows.push((byte_offset, bytes));
        }
        rows
    }

    // --- Editing ---

    /// Write a hex nibble at the cursor position.
    pub fn input_hex_nibble(&mut self, nibble: u8) {
        if self.file_size == 0 {
            return;
        }
        let current = self.get_byte(self.cursor_offset).unwrap_or(0);
        let new_byte = if self.cursor_nibble == 0 {
            (nibble << 4) | (current & 0x0F)
        } else {
            (current & 0xF0) | nibble
        };
        self.set_byte(self.cursor_offset, new_byte);

        // Advance: high → low → next byte high
        if self.cursor_nibble == 0 {
            self.cursor_nibble = 1;
        } else {
            self.cursor_nibble = 0;
            if self.cursor_offset + 1 < self.file_size {
                self.cursor_offset += 1;
            }
        }
        self.scroll_to_cursor();
    }

    /// Write an ASCII byte at the cursor position.
    pub fn input_ascii(&mut self, ch: u8) {
        if self.file_size == 0 {
            return;
        }
        self.set_byte(self.cursor_offset, ch);
        if self.cursor_offset + 1 < self.file_size {
            self.cursor_offset += 1;
        }
        self.scroll_to_cursor();
    }

    fn set_byte(&mut self, offset: u64, value: u8) {
        let old = self.modifications.get(&offset).copied();
        self.undo_stack.push(HexUndoEntry {
            offset,
            old,
            new: value,
        });
        self.redo_stack.clear();
        self.modifications.insert(offset, value);
        self.modified = true;
    }

    pub fn undo(&mut self) {
        if let Some(entry) = self.undo_stack.pop() {
            let current = self.modifications.get(&entry.offset).copied();
            self.redo_stack.push(HexUndoEntry {
                offset: entry.offset,
                old: current,
                new: entry.old.unwrap_or(0), // doesn't matter, we use old
            });
            if let Some(prev) = entry.old {
                self.modifications.insert(entry.offset, prev);
            } else {
                self.modifications.remove(&entry.offset);
            }
            self.modified = !self.modifications.is_empty();
            self.cursor_offset = entry.offset;
            self.cursor_nibble = 0;
            self.scroll_to_cursor();
        }
    }

    pub fn redo(&mut self) {
        if let Some(entry) = self.redo_stack.pop() {
            let current = self.modifications.get(&entry.offset).copied();
            self.undo_stack.push(HexUndoEntry {
                offset: entry.offset,
                old: current,
                new: entry.new,
            });
            if let Some(prev) = entry.old {
                self.modifications.insert(entry.offset, prev);
            } else {
                self.modifications.remove(&entry.offset);
            }
            self.modified = !self.modifications.is_empty();
            self.cursor_offset = entry.offset;
            self.cursor_nibble = 0;
            self.scroll_to_cursor();
        }
    }

    // --- Save ---

    pub fn save(&mut self) -> Result<(), String> {
        if self.modifications.is_empty() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| format!("Cannot open file for writing: {}", e))?;

        // Sort by offset for sequential I/O
        let mut edits: Vec<_> = self.modifications.iter().collect();
        edits.sort_unstable_by_key(|(&off, _)| off);
        for (&offset, &byte) in edits {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| format!("Seek failed: {}", e))?;
            file.write_all(&[byte])
                .map_err(|e| format!("Write failed: {}", e))?;
        }
        file.flush().map_err(|e| format!("Flush failed: {}", e))?;

        // Reload buffer to reflect saved state
        self.modifications.clear();
        self.undo_stack.clear();
        self.redo_stack.clear();
        self.modified = false;
        let byte_start = (self.buffer_start_row * BYTES_PER_ROW) as u64;
        self.load_buffer_at_byte(byte_start);
        self.status_msg = Some("Saved".to_string());
        Ok(())
    }

    // --- Search ---

    /// Parse a hex pattern string like "FF 00 AB" or "ff00ab" into bytes.
    pub fn parse_hex_pattern(input: &str) -> Result<Vec<u8>, String> {
        let clean: String = input.chars().filter(|c| !c.is_whitespace()).collect();
        if clean.is_empty() {
            return Err("Empty pattern".to_string());
        }
        if !clean.len().is_multiple_of(2) {
            return Err("Odd number of hex digits".to_string());
        }
        let mut bytes = Vec::with_capacity(clean.len() / 2);
        for i in (0..clean.len()).step_by(2) {
            let byte_str = &clean[i..i + 2];
            let b = u8::from_str_radix(byte_str, 16)
                .map_err(|_| format!("Invalid hex: '{}'", byte_str))?;
            bytes.push(b);
        }
        Ok(bytes)
    }

    /// Search forward from cursor_offset + 1 for the pattern. Wraps around.
    pub fn find_next(&mut self, pattern: &[u8]) -> bool {
        if pattern.is_empty() || self.file_size == 0 {
            return false;
        }
        self.last_search_pattern = Some(pattern.to_vec());
        let start = self.cursor_offset + 1;
        // Search from start to end of file
        if let Some(offset) = self.search_range(pattern, start, self.file_size) {
            self.goto_match(offset, pattern.len());
            return true;
        }
        // Wrap: search from 0 to start
        if let Some(offset) = self.search_range(pattern, 0, start.min(self.file_size)) {
            self.goto_match(offset, pattern.len());
            self.status_msg = Some("Search wrapped".to_string());
            return true;
        }
        self.search_match = None;
        self.status_msg = Some("Pattern not found".to_string());
        false
    }

    /// Search backward from cursor_offset - 1 for the pattern. Wraps around.
    pub fn find_prev(&mut self, pattern: &[u8]) -> bool {
        if pattern.is_empty() || self.file_size == 0 {
            return false;
        }
        self.last_search_pattern = Some(pattern.to_vec());
        let start = self.cursor_offset;
        // Search from start backward to 0
        if let Some(offset) = self.search_range_reverse(pattern, 0, start) {
            self.goto_match(offset, pattern.len());
            return true;
        }
        // Wrap: search from file end backward to start
        if let Some(offset) = self.search_range_reverse(pattern, start, self.file_size) {
            self.goto_match(offset, pattern.len());
            self.status_msg = Some("Search wrapped".to_string());
            return true;
        }
        self.search_match = None;
        self.status_msg = Some("Pattern not found".to_string());
        false
    }

    fn goto_match(&mut self, offset: u64, len: usize) {
        self.cursor_offset = offset;
        self.cursor_nibble = 0;
        self.search_match = Some((offset, len));
        self.scroll_to_cursor();
    }

    /// Search [from, to) for pattern. Returns first match offset.
    fn search_range(&self, pattern: &[u8], from: u64, to: u64) -> Option<u64> {
        if from >= to || pattern.is_empty() {
            return None;
        }
        let file = File::open(&self.path).ok()?;
        let mut reader = BufReader::with_capacity(SEARCH_CHUNK, file);
        reader.seek(SeekFrom::Start(from)).ok()?;

        let pat_len = pattern.len();
        // Keep overlap of pat_len - 1 bytes between chunks to catch boundary matches
        let overlap = pat_len.saturating_sub(1);
        let mut carry = Vec::new();
        let mut file_pos = from;

        loop {
            if file_pos >= to {
                break;
            }
            let read_size = SEARCH_CHUNK.min((to - file_pos) as usize + overlap);
            let mut chunk = vec![0u8; carry.len() + read_size];
            chunk[..carry.len()].copy_from_slice(&carry);
            let n = reader.read(&mut chunk[carry.len()..]).ok()?;
            if n == 0 {
                break;
            }
            let chunk_len = carry.len() + n;
            chunk.truncate(chunk_len);

            // Apply modifications overlay to chunk
            let chunk_start_offset = file_pos - carry.len() as u64;
            for (i, byte) in chunk[..chunk_len].iter_mut().enumerate() {
                let off = chunk_start_offset + i as u64;
                if let Some(&b) = self.modifications.get(&off) {
                    *byte = b;
                }
            }

            // Search within chunk
            let search_end = if file_pos + n as u64 > to {
                // Don't search past `to`
                (to - chunk_start_offset) as usize
            } else {
                chunk_len
            };
            for i in 0..search_end {
                let abs = chunk_start_offset + i as u64;
                if abs < from || abs + pat_len as u64 > to {
                    continue;
                }
                if i + pat_len <= chunk_len && chunk[i..i + pat_len] == *pattern {
                    return Some(abs);
                }
            }

            file_pos += n as u64;
            // Keep overlap for next iteration
            if chunk_len >= overlap {
                carry = chunk[chunk_len - overlap..].to_vec();
            } else {
                carry = chunk;
            }
        }
        None
    }

    /// Search [from, to) in reverse for pattern. Returns last match offset before `to`.
    fn search_range_reverse(&self, pattern: &[u8], from: u64, to: u64) -> Option<u64> {
        if from >= to || pattern.is_empty() {
            return None;
        }
        // Simple approach: scan forward but keep track of the last match < to
        let file = File::open(&self.path).ok()?;
        let mut reader = BufReader::with_capacity(SEARCH_CHUNK, file);

        let pat_len = pattern.len();
        let overlap = pat_len.saturating_sub(1);

        // Read backward in chunks
        let mut chunk_end = to;

        while chunk_end > from {
            let chunk_size = SEARCH_CHUNK.min((chunk_end - from) as usize);
            let chunk_start = chunk_end - chunk_size as u64;
            // Read extra overlap after chunk_end for boundary matches
            let extra = overlap.min((self.file_size - chunk_end) as usize);
            let read_len = chunk_size + extra;

            reader.seek(SeekFrom::Start(chunk_start)).ok()?;
            let mut chunk = vec![0u8; read_len];
            let n = reader.read(&mut chunk).ok()?;
            chunk.truncate(n);

            // Apply modifications
            for (i, byte) in chunk.iter_mut().enumerate() {
                let off = chunk_start + i as u64;
                if let Some(&b) = self.modifications.get(&off) {
                    *byte = b;
                }
            }

            // Search within this chunk, find the last match
            let search_end = chunk_size.min(chunk.len());
            for i in (0..search_end).rev() {
                let abs = chunk_start + i as u64;
                if abs < from || abs >= to {
                    continue;
                }
                if i + pat_len <= chunk.len() && chunk[i..i + pat_len] == *pattern {
                    return Some(abs);
                }
            }

            chunk_end = chunk_start;
        }
        None
    }

    // --- Goto offset ---

    /// Parse a hex offset string and navigate to it.
    pub fn goto_offset(&mut self, input: &str) -> Result<(), String> {
        let trimmed = input
            .trim()
            .trim_start_matches("0x")
            .trim_start_matches("0X");
        let offset = u64::from_str_radix(trimmed, 16)
            .map_err(|_| format!("Invalid hex offset: '{}'", input))?;
        if offset >= self.file_size {
            return Err(format!(
                "Offset 0x{:X} exceeds file size 0x{:X}",
                offset, self.file_size
            ));
        }
        self.cursor_offset = offset;
        self.cursor_nibble = 0;
        self.scroll_to_cursor();
        Ok(())
    }

    // --- Buffer internals ---

    fn get_row_bytes(&self, row: usize) -> Vec<u8> {
        if row < self.buffer_start_row {
            return Vec::new();
        }
        let offset_in_buf = (row - self.buffer_start_row) * BYTES_PER_ROW;
        if offset_in_buf >= self.buffer.len() {
            return Vec::new();
        }
        let end = (offset_in_buf + BYTES_PER_ROW).min(self.buffer.len());
        self.buffer[offset_in_buf..end].to_vec()
    }

    fn ensure_buffer_covers_viewport(&mut self) {
        let vp_start = self.scroll_offset * BYTES_PER_ROW;
        let vp_end = vp_start + self.visible_rows * BYTES_PER_ROW;

        let buf_start = self.buffer_start_row * BYTES_PER_ROW;
        let buf_end = buf_start + self.buffer.len();

        if vp_start >= buf_start && vp_end <= buf_end {
            return;
        }

        // Center buffer around viewport
        let center = vp_start + (self.visible_rows * BYTES_PER_ROW) / 2;
        let new_start = center.saturating_sub(BUFFER_BYTES / 2);
        // Align to row boundary
        let new_start = (new_start / BYTES_PER_ROW) * BYTES_PER_ROW;
        self.load_buffer_at_byte(new_start as u64);
    }

    fn load_buffer_at_byte(&mut self, start_byte: u64) {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        if reader.seek(SeekFrom::Start(start_byte)).is_err() {
            return;
        }

        self.buffer.resize(BUFFER_BYTES, 0);
        let mut total_read = 0;
        while total_read < BUFFER_BYTES {
            match reader.read(&mut self.buffer[total_read..]) {
                Ok(0) => break,
                Ok(n) => total_read += n,
                Err(_) => break,
            }
        }
        self.buffer.truncate(total_read);
        self.buffer_start_row = (start_byte as usize) / BYTES_PER_ROW;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_file(data: &[u8]) -> (tempfile::NamedTempFile, PathBuf) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        let path = f.path().to_path_buf();
        (f, path)
    }

    fn open(data: &[u8]) -> (tempfile::NamedTempFile, HexViewerState) {
        let (f, path) = temp_file(data);
        let mut h = HexViewerState::open(path);
        h.visible_rows = 10;
        (f, h)
    }

    // ===== parse_hex_pattern =====

    #[test]
    fn parse_hex_pattern_spaced() {
        assert_eq!(
            HexViewerState::parse_hex_pattern("FF 00 AB").unwrap(),
            vec![0xFF, 0x00, 0xAB]
        );
    }

    #[test]
    fn parse_hex_pattern_packed() {
        assert_eq!(
            HexViewerState::parse_hex_pattern("ff00ab").unwrap(),
            vec![0xFF, 0x00, 0xAB]
        );
    }

    #[test]
    fn parse_hex_pattern_mixed_case() {
        assert_eq!(
            HexViewerState::parse_hex_pattern("aAbBcC").unwrap(),
            vec![0xAA, 0xBB, 0xCC]
        );
    }

    #[test]
    fn parse_hex_pattern_single_byte() {
        assert_eq!(HexViewerState::parse_hex_pattern("00").unwrap(), vec![0x00]);
    }

    #[test]
    fn parse_hex_pattern_tabs_and_spaces() {
        assert_eq!(
            HexViewerState::parse_hex_pattern("  FF \t 00  ").unwrap(),
            vec![0xFF, 0x00]
        );
    }

    #[test]
    fn parse_hex_pattern_odd_digits() {
        assert!(HexViewerState::parse_hex_pattern("FFA").is_err());
    }

    #[test]
    fn parse_hex_pattern_invalid_chars() {
        assert!(HexViewerState::parse_hex_pattern("GGGG").is_err());
    }

    #[test]
    fn parse_hex_pattern_empty() {
        assert!(HexViewerState::parse_hex_pattern("").is_err());
    }

    #[test]
    fn parse_hex_pattern_whitespace_only() {
        assert!(HexViewerState::parse_hex_pattern("   ").is_err());
    }

    // ===== open / basic state =====

    #[test]
    fn open_empty_file() {
        let (_f, h) = open(&[]);
        assert_eq!(h.file_size, 0);
        assert_eq!(h.cursor_offset, 0);
        assert_eq!(h.total_rows(), 0);
        assert!(!h.modified);
    }

    #[test]
    fn open_single_byte_file() {
        let (_f, h) = open(&[0x42]);
        assert_eq!(h.file_size, 1);
        assert_eq!(h.total_rows(), 1);
    }

    #[test]
    fn open_exact_row_boundary() {
        let (_f, h) = open(&[0u8; 16]);
        assert_eq!(h.total_rows(), 1);
        let (_f, h) = open(&[0u8; 17]);
        assert_eq!(h.total_rows(), 2);
    }

    #[test]
    fn is_binary_detects_null() {
        let (_f, path) = temp_file(&[0x48, 0x65, 0x00, 0x6C]);
        assert!(HexViewerState::is_binary(&path));
    }

    #[test]
    fn is_binary_text_file() {
        let (_f, path) = temp_file(b"Hello World\n");
        assert!(!HexViewerState::is_binary(&path));
    }

    // ===== cursor navigation (hex mode) =====

    #[test]
    fn cursor_right_nibble_alternation() {
        let (_f, mut h) = open(&[0u8; 64]);
        assert_eq!(h.cursor_nibble, 0);
        h.cursor_right(); // → nibble 1
        assert_eq!((h.cursor_offset, h.cursor_nibble), (0, 1));
        h.cursor_right(); // → byte 1, nibble 0
        assert_eq!((h.cursor_offset, h.cursor_nibble), (1, 0));
        h.cursor_right(); // → nibble 1
        assert_eq!((h.cursor_offset, h.cursor_nibble), (1, 1));
    }

    #[test]
    fn cursor_left_nibble_alternation() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 2;
        h.cursor_nibble = 0;
        h.cursor_left(); // → byte 1, nibble 1
        assert_eq!((h.cursor_offset, h.cursor_nibble), (1, 1));
        h.cursor_left(); // → byte 1, nibble 0
        assert_eq!((h.cursor_offset, h.cursor_nibble), (1, 0));
    }

    #[test]
    fn cursor_left_at_origin_stays() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_left();
        assert_eq!((h.cursor_offset, h.cursor_nibble), (0, 0));
    }

    #[test]
    fn cursor_right_at_last_byte_nibble1_stays() {
        let (_f, mut h) = open(&[0u8; 4]);
        h.cursor_offset = 3;
        h.cursor_nibble = 1;
        h.cursor_right();
        assert_eq!((h.cursor_offset, h.cursor_nibble), (3, 1));
    }

    #[test]
    fn cursor_right_at_last_byte_nibble0_goes_to_nibble1() {
        let (_f, mut h) = open(&[0u8; 4]);
        h.cursor_offset = 3;
        h.cursor_nibble = 0;
        h.cursor_right();
        assert_eq!((h.cursor_offset, h.cursor_nibble), (3, 1));
    }

    #[test]
    fn cursor_up_at_first_row_clamps_to_zero() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 5; // within first row
        h.cursor_up();
        assert_eq!(h.cursor_offset, 0);
    }

    #[test]
    fn cursor_up_normal() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 20; // row 1, col 4
        h.cursor_up();
        assert_eq!(h.cursor_offset, 4); // row 0, col 4
    }

    #[test]
    fn cursor_down_at_last_row_clamps() {
        let (_f, mut h) = open(&[0u8; 32]); // 2 rows
        h.cursor_offset = 20; // row 1
        h.cursor_down();
        assert_eq!(h.cursor_offset, 31); // clamps to last byte
    }

    #[test]
    fn cursor_down_normal() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 5;
        h.cursor_down();
        assert_eq!(h.cursor_offset, 21);
    }

    #[test]
    fn cursor_row_start() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 20; // row 1, byte 4
        h.cursor_row_start();
        assert_eq!(h.cursor_offset, 16); // row 1, byte 0
    }

    #[test]
    fn cursor_row_end() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 16;
        h.cursor_row_end();
        assert_eq!(h.cursor_offset, 31); // last byte of row 1
    }

    #[test]
    fn cursor_row_end_last_partial_row() {
        let (_f, mut h) = open(&[0u8; 20]); // row 0: 16 bytes, row 1: 4 bytes
        h.cursor_offset = 16;
        h.cursor_row_end();
        assert_eq!(h.cursor_offset, 19); // clamped to file_size - 1
    }

    #[test]
    fn page_up_at_top() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.cursor_offset = 5;
        h.page_up();
        assert_eq!(h.cursor_offset, 0);
    }

    #[test]
    fn page_down_at_bottom() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.cursor_offset = 250;
        h.page_down();
        assert_eq!(h.cursor_offset, 255); // clamped to last byte
    }

    #[test]
    fn scroll_to_top_and_bottom() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.cursor_offset = 100;
        h.scroll_to_top();
        assert_eq!(h.cursor_offset, 0);
        assert_eq!(h.scroll_offset, 0);

        h.scroll_to_bottom();
        assert_eq!(h.cursor_offset, 255);
    }

    // ===== cursor navigation (ASCII mode) =====

    #[test]
    fn ascii_cursor_right_moves_byte() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.toggle_ascii();
        assert!(h.editing_ascii);
        h.cursor_right();
        assert_eq!(h.cursor_offset, 1);
        h.cursor_right();
        assert_eq!(h.cursor_offset, 2);
    }

    #[test]
    fn ascii_cursor_left_moves_byte() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.toggle_ascii();
        h.cursor_offset = 5;
        h.cursor_left();
        assert_eq!(h.cursor_offset, 4);
    }

    #[test]
    fn ascii_cursor_left_at_zero_stays() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.toggle_ascii();
        h.cursor_left();
        assert_eq!(h.cursor_offset, 0);
    }

    #[test]
    fn ascii_cursor_right_at_end_stays() {
        let (_f, mut h) = open(&[0u8; 4]);
        h.toggle_ascii();
        h.cursor_offset = 3;
        h.cursor_right();
        assert_eq!(h.cursor_offset, 3);
    }

    #[test]
    fn toggle_ascii_clears_nibble() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_nibble = 1;
        h.toggle_ascii();
        assert_eq!(h.cursor_nibble, 0);
        assert!(h.editing_ascii);
        h.toggle_ascii();
        assert!(!h.editing_ascii);
        assert_eq!(h.cursor_nibble, 0);
    }

    // ===== scroll_to_cursor (viewport tracking) =====

    #[test]
    fn scroll_follows_cursor_down() {
        let (_f, mut h) = open(&[0u8; 512]);
        h.visible_rows = 4;
        // Move cursor below visible area
        for _ in 0..10 {
            h.cursor_down();
        }
        let cursor_row = h.cursor_offset as usize / BYTES_PER_ROW;
        assert!(cursor_row >= h.scroll_offset);
        assert!(cursor_row < h.scroll_offset + h.visible_rows);
    }

    #[test]
    fn scroll_follows_cursor_up() {
        let (_f, mut h) = open(&[0u8; 512]);
        h.visible_rows = 4;
        h.cursor_offset = 200;
        h.scroll_to_cursor();
        // Now move up past visible area
        for _ in 0..20 {
            h.cursor_up();
        }
        let cursor_row = h.cursor_offset as usize / BYTES_PER_ROW;
        assert!(cursor_row >= h.scroll_offset);
    }

    // ===== get_byte =====

    #[test]
    fn get_byte_from_file() {
        let (_f, h) = open(&[0x41, 0x42, 0x43]);
        assert_eq!(h.get_byte(0), Some(0x41));
        assert_eq!(h.get_byte(1), Some(0x42));
        assert_eq!(h.get_byte(2), Some(0x43));
    }

    #[test]
    fn get_byte_out_of_range() {
        let (_f, h) = open(&[0x41, 0x42]);
        assert_eq!(h.get_byte(2), None);
        assert_eq!(h.get_byte(100), None);
    }

    #[test]
    fn get_byte_with_modification_overlay() {
        let (_f, mut h) = open(&[0x00, 0x00, 0x00]);
        h.modifications.insert(1, 0xFF);
        assert_eq!(h.get_byte(0), Some(0x00));
        assert_eq!(h.get_byte(1), Some(0xFF));
        assert_eq!(h.get_byte(2), Some(0x00));
    }

    // ===== visible_rows_data =====

    #[test]
    fn visible_rows_data_applies_modifications() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.visible_rows = 2;
        h.modifications.insert(0, 0xAA);
        h.modifications.insert(17, 0xBB);
        let rows = h.visible_rows_data();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[0], 0xAA);
        assert_eq!(rows[1].1[1], 0xBB);
    }

    #[test]
    fn visible_rows_data_empty_file() {
        let (_f, mut h) = open(&[]);
        let rows = h.visible_rows_data();
        assert!(rows.is_empty());
    }

    // ===== editing: input_hex_nibble =====

    #[test]
    fn input_hex_nibble_sequence() {
        let (_f, mut h) = open(&[0x00; 32]);
        h.input_hex_nibble(0xA); // high nibble → 0xA0
        assert_eq!(h.get_byte(0), Some(0xA0));
        assert_eq!(h.cursor_nibble, 1);
        h.input_hex_nibble(0xB); // low nibble → 0xAB, advance to byte 1
        assert_eq!(h.get_byte(0), Some(0xAB));
        assert_eq!(h.cursor_offset, 1);
        assert_eq!(h.cursor_nibble, 0);
    }

    #[test]
    fn input_hex_nibble_at_last_byte() {
        let (_f, mut h) = open(&[0x00; 2]);
        h.cursor_offset = 1; // last byte
        h.input_hex_nibble(0xF);
        h.input_hex_nibble(0xF);
        // Should not advance past end
        assert_eq!(h.cursor_offset, 1);
        assert_eq!(h.get_byte(1), Some(0xFF));
    }

    #[test]
    fn input_hex_nibble_empty_file_noop() {
        let (_f, mut h) = open(&[]);
        h.input_hex_nibble(0xA);
        assert!(!h.modified);
    }

    #[test]
    fn input_hex_nibble_overwrites_existing() {
        let (_f, mut h) = open(&[0xAB; 4]);
        h.input_hex_nibble(0x1); // high: 0x1B
        assert_eq!(h.get_byte(0), Some(0x1B));
        h.input_hex_nibble(0x2); // low: 0x12
        assert_eq!(h.get_byte(0), Some(0x12));
    }

    // ===== editing: input_ascii =====

    #[test]
    fn input_ascii_writes_and_advances() {
        let (_f, mut h) = open(&[0x00; 8]);
        h.toggle_ascii();
        h.input_ascii(b'A');
        assert_eq!(h.get_byte(0), Some(0x41));
        assert_eq!(h.cursor_offset, 1);
        h.input_ascii(b'B');
        assert_eq!(h.get_byte(1), Some(0x42));
        assert_eq!(h.cursor_offset, 2);
    }

    #[test]
    fn input_ascii_at_last_byte_no_advance() {
        let (_f, mut h) = open(&[0x00; 2]);
        h.toggle_ascii();
        h.cursor_offset = 1;
        h.input_ascii(b'Z');
        assert_eq!(h.get_byte(1), Some(b'Z'));
        assert_eq!(h.cursor_offset, 1); // stays at last byte
    }

    #[test]
    fn input_ascii_empty_file_noop() {
        let (_f, mut h) = open(&[]);
        h.toggle_ascii();
        h.input_ascii(b'A');
        assert!(!h.modified);
    }

    // ===== undo/redo =====

    #[test]
    fn undo_empty_stack_noop() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.undo();
        assert_eq!(h.cursor_offset, 0);
        assert!(!h.modified);
    }

    #[test]
    fn redo_empty_stack_noop() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.redo();
        assert_eq!(h.cursor_offset, 0);
        assert!(!h.modified);
    }

    #[test]
    fn undo_single_edit() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.input_hex_nibble(0xA);
        assert!(h.modified);
        h.undo();
        assert_eq!(h.get_byte(0), Some(0x00));
        assert!(!h.modified);
    }

    #[test]
    fn redo_after_undo() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.input_hex_nibble(0xA);
        h.undo();
        h.redo();
        assert_eq!(h.get_byte(0), Some(0xA0));
        assert!(h.modified);
    }

    #[test]
    fn redo_cleared_on_new_edit() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.input_hex_nibble(0xA);
        h.undo();
        // Now edit something else — redo stack should be cleared
        h.input_hex_nibble(0xB);
        h.redo(); // should be noop
        assert_eq!(h.get_byte(0), Some(0xB0));
    }

    #[test]
    fn undo_all_then_redo_all() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.input_hex_nibble(0xA);
        h.input_hex_nibble(0xB); // byte 0 = 0xAB
        h.input_hex_nibble(0xC); // byte 1 high nibble
                                 // 3 operations on undo stack
        h.undo();
        h.undo();
        h.undo();
        assert_eq!(h.get_byte(0), Some(0x00));
        assert!(!h.modified);

        h.redo();
        h.redo();
        h.redo();
        assert_eq!(h.get_byte(0), Some(0xAB));
    }

    #[test]
    fn undo_moves_cursor_to_edit_location() {
        let (_f, mut h) = open(&[0x00; 64]);
        h.cursor_offset = 50;
        h.input_hex_nibble(0xA);
        h.cursor_offset = 10; // move cursor away
        h.undo();
        assert_eq!(h.cursor_offset, 50); // back to where the edit was
    }

    #[test]
    fn multiple_edits_same_byte_undo_sequence() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.set_byte(0, 0xAA);
        h.set_byte(0, 0xBB);
        h.set_byte(0, 0xCC);
        assert_eq!(h.get_byte(0), Some(0xCC));
        h.undo();
        assert_eq!(h.get_byte(0), Some(0xBB));
        h.undo();
        assert_eq!(h.get_byte(0), Some(0xAA));
        h.undo();
        assert_eq!(h.get_byte(0), Some(0x00));
    }

    // ===== save =====

    #[test]
    fn save_no_modifications_noop() {
        let (_f, path) = temp_file(&[0x41; 4]);
        let mut h = HexViewerState::open(path.clone());
        h.visible_rows = 1;
        h.save().unwrap();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, vec![0x41; 4]);
    }

    #[test]
    fn save_clears_undo_redo() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.set_byte(0, 0xFF);
        h.save().unwrap();
        assert!(h.undo_stack.is_empty());
        assert!(h.redo_stack.is_empty());
        assert!(h.modifications.is_empty());
    }

    #[test]
    fn save_multiple_scattered_modifications() {
        let data = vec![0x00; 64];
        let (_f, path) = temp_file(&data);
        let mut h = HexViewerState::open(path.clone());
        h.visible_rows = 4;
        h.set_byte(0, 0x11);
        h.set_byte(31, 0x22);
        h.set_byte(63, 0x33);
        h.save().unwrap();
        let saved = std::fs::read(&path).unwrap();
        assert_eq!(saved[0], 0x11);
        assert_eq!(saved[31], 0x22);
        assert_eq!(saved[63], 0x33);
        assert_eq!(saved[1], 0x00);
    }

    #[test]
    fn save_then_get_byte_reads_saved_value() {
        let (_f, mut h) = open(&[0x00; 16]);
        h.set_byte(5, 0xAB);
        h.save().unwrap();
        // After save, modifications are cleared; get_byte reads from reloaded buffer
        assert_eq!(h.get_byte(5), Some(0xAB));
    }

    // ===== search forward =====

    #[test]
    fn search_pattern_at_offset_zero_from_elsewhere() {
        let mut data = vec![0x00; 64];
        data[0] = 0xDE;
        data[1] = 0xAD;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 10; // start away from the pattern
        assert!(h.find_next(&[0xDE, 0xAD]));
        assert_eq!(h.cursor_offset, 0); // wraps to find it
    }

    #[test]
    fn search_only_match_is_at_cursor_not_found() {
        let mut data = vec![0x00; 64];
        data[0] = 0xDE;
        data[1] = 0xAD;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 0; // sitting on the only match
                             // find_next starts at cursor+1; wrap range [0,1) is too short for 2-byte pattern
        assert!(!h.find_next(&[0xDE, 0xAD]));
    }

    #[test]
    fn search_single_byte_at_cursor_wraps_to_self() {
        let mut data = vec![0x00; 64];
        data[5] = 0xAA;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 5; // sitting on the only match
                             // Single-byte pattern: wrap range [0,6) includes offset 5
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 5);
    }

    #[test]
    fn search_pattern_at_end_of_file() {
        let mut data = vec![0x00; 64];
        data[62] = 0xBE;
        data[63] = 0xEF;
        let (_f, mut h) = open(&data);
        assert!(h.find_next(&[0xBE, 0xEF]));
        assert_eq!(h.cursor_offset, 62);
    }

    #[test]
    fn search_single_byte_pattern() {
        let mut data = vec![0x00; 64];
        data[30] = 0xFF;
        let (_f, mut h) = open(&data);
        assert!(h.find_next(&[0xFF]));
        assert_eq!(h.cursor_offset, 30);
    }

    #[test]
    fn search_pattern_longer_than_file() {
        let data = vec![0xFF; 4];
        let (_f, mut h) = open(&data);
        assert!(!h.find_next(&[0xFF; 5]));
    }

    #[test]
    fn search_empty_pattern() {
        let (_f, mut h) = open(&[0x00; 64]);
        assert!(!h.find_next(&[]));
    }

    #[test]
    fn search_empty_file() {
        let (_f, mut h) = open(&[]);
        assert!(!h.find_next(&[0xFF]));
    }

    #[test]
    fn search_stores_last_pattern() {
        let mut data = vec![0x00; 64];
        data[10] = 0xAA;
        let (_f, mut h) = open(&data);
        h.find_next(&[0xAA]);
        assert_eq!(h.last_search_pattern, Some(vec![0xAA]));
    }

    #[test]
    fn search_forward_skips_current_offset() {
        let mut data = vec![0x00; 64];
        data[0] = 0xAA;
        data[10] = 0xAA;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 0; // sitting on first match
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 10); // should find SECOND match
    }

    #[test]
    fn search_forward_successive_finds_all() {
        let mut data = vec![0x00; 64];
        data[5] = 0xAA;
        data[20] = 0xAA;
        data[40] = 0xAA;
        let (_f, mut h) = open(&data);
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 5);
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 20);
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 40);
        // Wraps to first
        assert!(h.find_next(&[0xAA]));
        assert_eq!(h.cursor_offset, 5);
    }

    #[test]
    fn search_wraps_around() {
        let mut data = vec![0x00; 256];
        data[10] = 0xBE;
        data[11] = 0xEF;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 50;
        assert!(h.find_next(&[0xBE, 0xEF]));
        assert_eq!(h.cursor_offset, 10);
    }

    #[test]
    fn search_not_found_clears_match() {
        let (_f, mut h) = open(&[0x00; 64]);
        h.search_match = Some((10, 2)); // pretend previous match
        assert!(!h.find_next(&[0xDE, 0xAD]));
        assert_eq!(h.search_match, None);
    }

    #[test]
    fn search_not_found_sets_status() {
        let (_f, mut h) = open(&[0x00; 64]);
        h.find_next(&[0xDE, 0xAD]);
        assert!(h.status_msg.is_some());
        assert!(h.status_msg.unwrap().contains("not found"));
    }

    // ===== search backward =====

    #[test]
    fn search_backward_basic() {
        let mut data = vec![0x00; 64];
        data[10] = 0xAA;
        data[40] = 0xAA;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 50;
        assert!(h.find_prev(&[0xAA]));
        assert_eq!(h.cursor_offset, 40);
    }

    #[test]
    fn search_backward_wraps() {
        let mut data = vec![0x00; 64];
        data[50] = 0xAA;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 10;
        assert!(h.find_prev(&[0xAA]));
        assert_eq!(h.cursor_offset, 50); // wrapped to end
    }

    #[test]
    fn search_backward_not_found() {
        let (_f, mut h) = open(&[0x00; 64]);
        assert!(!h.find_prev(&[0xAA]));
    }

    #[test]
    fn search_backward_from_start() {
        let mut data = vec![0x00; 64];
        data[60] = 0xBB;
        let (_f, mut h) = open(&data);
        h.cursor_offset = 0;
        assert!(h.find_prev(&[0xBB]));
        assert_eq!(h.cursor_offset, 60); // wraps to find it near end
    }

    // ===== search with modifications =====

    #[test]
    fn search_finds_modified_bytes() {
        let (_f, mut h) = open(&[0x00; 64]);
        h.modifications.insert(30, 0xCA);
        h.modifications.insert(31, 0xFE);
        assert!(h.find_next(&[0xCA, 0xFE]));
        assert_eq!(h.cursor_offset, 30);
    }

    #[test]
    fn search_modification_hides_original_pattern() {
        let mut data = vec![0x00; 64];
        data[10] = 0xDE;
        data[11] = 0xAD;
        let (_f, mut h) = open(&data);
        // Overwrite the pattern with something else
        h.modifications.insert(10, 0x00);
        assert!(!h.find_next(&[0xDE, 0xAD]));
    }

    // ===== goto_offset =====

    #[test]
    fn goto_offset_zero() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.cursor_offset = 100;
        h.goto_offset("0").unwrap();
        assert_eq!(h.cursor_offset, 0);
    }

    #[test]
    fn goto_offset_last_byte() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.goto_offset("FF").unwrap();
        assert_eq!(h.cursor_offset, 0xFF);
    }

    #[test]
    fn goto_offset_with_0x_prefix() {
        let (_f, mut h) = open(&[0u8; 4096]);
        h.goto_offset("0x100").unwrap();
        assert_eq!(h.cursor_offset, 0x100);
    }

    #[test]
    fn goto_offset_with_upper_0x_prefix() {
        let (_f, mut h) = open(&[0u8; 4096]);
        h.goto_offset("0X1FF").unwrap();
        assert_eq!(h.cursor_offset, 0x1FF);
    }

    #[test]
    fn goto_offset_no_prefix() {
        let (_f, mut h) = open(&[0u8; 4096]);
        h.goto_offset("A0").unwrap();
        assert_eq!(h.cursor_offset, 0xA0);
    }

    #[test]
    fn goto_offset_with_whitespace() {
        let (_f, mut h) = open(&[0u8; 4096]);
        h.goto_offset("  0x10  ").unwrap();
        assert_eq!(h.cursor_offset, 0x10);
    }

    #[test]
    fn goto_offset_at_file_size_fails() {
        let (_f, mut h) = open(&[0u8; 256]); // offsets 0..255
        assert!(h.goto_offset("100").is_err()); // 0x100 = 256, which == file_size
    }

    #[test]
    fn goto_offset_past_file_size_fails() {
        let (_f, mut h) = open(&[0u8; 16]);
        assert!(h.goto_offset("FFFF").is_err());
    }

    #[test]
    fn goto_offset_invalid_hex_fails() {
        let (_f, mut h) = open(&[0u8; 256]);
        assert!(h.goto_offset("ZZZZ").is_err());
    }

    #[test]
    fn goto_offset_empty_string_fails() {
        let (_f, mut h) = open(&[0u8; 256]);
        assert!(h.goto_offset("").is_err());
    }

    #[test]
    fn goto_offset_resets_nibble() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.cursor_nibble = 1;
        h.goto_offset("10").unwrap();
        assert_eq!(h.cursor_nibble, 0);
    }

    // ===== scroll_up / scroll_down (viewport only) =====

    #[test]
    fn scroll_down_clamped() {
        let (_f, mut h) = open(&[0u8; 32]); // 2 rows
        h.visible_rows = 10;
        h.scroll_down(100);
        assert_eq!(h.scroll_offset, 0); // can't scroll when all rows visible
    }

    #[test]
    fn scroll_up_at_zero() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.scroll_up(100);
        assert_eq!(h.scroll_offset, 0);
    }

    // ===== edge cases: single-byte file =====

    #[test]
    fn single_byte_cursor_stays() {
        let (_f, mut h) = open(&[0x42]);
        h.cursor_right();
        assert_eq!(h.cursor_offset, 0);
        assert_eq!(h.cursor_nibble, 1);
        h.cursor_right();
        assert_eq!(h.cursor_offset, 0); // can't go to byte 1
        h.cursor_down();
        assert_eq!(h.cursor_offset, 0);
    }

    #[test]
    fn single_byte_edit_and_save() {
        let (_f, path) = temp_file(&[0x00]);
        let mut h = HexViewerState::open(path.clone());
        h.visible_rows = 1;
        h.input_hex_nibble(0xF);
        h.input_hex_nibble(0xF);
        assert_eq!(h.get_byte(0), Some(0xFF));
        h.save().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), vec![0xFF]);
    }

    // ===== modified flag =====

    #[test]
    fn modified_flag_set_on_edit() {
        let (_f, mut h) = open(&[0x00; 4]);
        assert!(!h.modified);
        h.set_byte(0, 0xFF);
        assert!(h.modified);
    }

    #[test]
    fn modified_flag_cleared_on_full_undo() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.set_byte(0, 0xFF);
        h.set_byte(1, 0xAA);
        h.undo();
        assert!(h.modified); // still has one modification
        h.undo();
        assert!(!h.modified); // all undone
    }

    #[test]
    fn modified_flag_cleared_on_save() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.set_byte(0, 0xFF);
        h.save().unwrap();
        assert!(!h.modified);
    }

    // ===== selection =====

    #[test]
    fn selection_none_by_default() {
        let (_f, h) = open(&[0u8; 32]);
        assert_eq!(h.selection_range(), None);
    }

    #[test]
    fn select_right_creates_selection() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.select_right();
        assert_eq!(h.selection_anchor, Some(0));
        assert_eq!(h.cursor_offset, 1);
        assert_eq!(h.selection_range(), Some((0, 1)));
    }

    #[test]
    fn select_left_creates_selection() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.cursor_offset = 5;
        h.select_left();
        assert_eq!(h.selection_anchor, Some(5));
        assert_eq!(h.cursor_offset, 4);
        assert_eq!(h.selection_range(), Some((4, 5)));
    }

    #[test]
    fn select_down_creates_selection() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 5;
        h.select_down();
        assert_eq!(h.selection_anchor, Some(5));
        assert_eq!(h.cursor_offset, 21);
        assert_eq!(h.selection_range(), Some((5, 21)));
    }

    #[test]
    fn select_up_creates_selection() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.cursor_offset = 20;
        h.select_up();
        assert_eq!(h.selection_anchor, Some(20));
        assert_eq!(h.cursor_offset, 4);
        assert_eq!(h.selection_range(), Some((4, 20)));
    }

    #[test]
    fn select_right_then_left_shrinks() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.select_right();
        h.select_right();
        h.select_right();
        assert_eq!(h.selection_range(), Some((0, 3)));
        h.select_left();
        assert_eq!(h.selection_range(), Some((0, 2)));
    }

    #[test]
    fn select_all_covers_entire_file() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.select_all();
        assert_eq!(h.selection_range(), Some((0, 63)));
    }

    #[test]
    fn select_all_empty_file_noop() {
        let (_f, mut h) = open(&[]);
        h.select_all();
        assert_eq!(h.selection_range(), None);
    }

    #[test]
    fn clear_selection_removes_anchor() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.select_right();
        assert!(h.selection_anchor.is_some());
        h.clear_selection();
        assert_eq!(h.selection_anchor, None);
        assert_eq!(h.selection_range(), None);
    }

    #[test]
    fn cursor_move_clears_selection() {
        let (_f, mut h) = open(&[0u8; 64]);
        h.select_right();
        h.select_right();
        assert!(h.selection_anchor.is_some());
        // Simulate what the action handler does: clear on normal move
        h.clear_selection();
        h.cursor_right();
        assert_eq!(h.selection_anchor, None);
    }

    #[test]
    fn select_page_down() {
        let (_f, mut h) = open(&[0u8; 512]);
        h.select_page_down();
        assert!(h.selection_anchor.is_some());
        assert!(h.cursor_offset > 0);
        let (s, e) = h.selection_range().unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, h.cursor_offset);
    }

    #[test]
    fn select_page_up_from_end() {
        let (_f, mut h) = open(&[0u8; 512]);
        h.cursor_offset = 500;
        h.scroll_to_cursor();
        h.select_page_up();
        assert!(h.selection_anchor.is_some());
        let (s, e) = h.selection_range().unwrap();
        assert!(s < 500);
        assert_eq!(e, 500);
    }

    // ===== selected text =====

    #[test]
    fn selected_text_hex_format() {
        let data: Vec<u8> = (0..16).collect();
        let (_f, mut h) = open(&data);
        h.selection_anchor = Some(0);
        h.cursor_offset = 3;
        assert_eq!(h.selected_text_hex(), Some("00 01 02 03".to_string()));
    }

    #[test]
    fn selected_text_ascii_format() {
        let (_f, mut h) = open(b"Hello World!");
        h.selection_anchor = Some(0);
        h.cursor_offset = 4;
        assert_eq!(h.selected_text_ascii(), Some("Hello".to_string()));
    }

    #[test]
    fn selected_text_ascii_non_printable() {
        let (_f, mut h) = open(&[0x41, 0x00, 0x42]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 2;
        assert_eq!(h.selected_text_ascii(), Some("A.B".to_string()));
    }

    #[test]
    fn selected_text_no_selection() {
        let (_f, h) = open(&[0u8; 32]);
        assert_eq!(h.selected_text_hex(), None);
        assert_eq!(h.selected_text_ascii(), None);
    }

    #[test]
    fn selected_text_single_byte() {
        let (_f, mut h) = open(&[0xAB; 4]);
        h.selection_anchor = Some(2);
        h.cursor_offset = 2;
        assert_eq!(h.selected_text_hex(), Some("AB".to_string()));
    }

    #[test]
    fn selected_text_reversed_selection() {
        let data: Vec<u8> = (0..16).collect();
        let (_f, mut h) = open(&data);
        // Anchor after cursor (reverse selection)
        h.selection_anchor = Some(5);
        h.cursor_offset = 2;
        assert_eq!(h.selected_text_hex(), Some("02 03 04 05".to_string()));
    }

    #[test]
    fn selected_text_with_modifications() {
        let (_f, mut h) = open(&[0x00; 8]);
        h.modifications.insert(1, 0xFF);
        h.selection_anchor = Some(0);
        h.cursor_offset = 2;
        assert_eq!(h.selected_text_hex(), Some("00 FF 00".to_string()));
    }

    // ===== selection_info (numeric interpretations) =====

    #[test]
    fn selection_info_no_selection() {
        let (_f, h) = open(&[0u8; 32]);
        assert_eq!(h.selection_info(), None);
    }

    #[test]
    fn selection_info_1_byte() {
        let (_f, mut h) = open(&[0xFF; 4]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 0;
        let info = h.selection_info().unwrap();
        assert!(info.contains("u8: 255"));
        assert!(info.contains("i8: -1"));
    }

    #[test]
    fn selection_info_2_bytes_le_be() {
        // bytes: [0x01, 0x00] → LE u16: 1, BE u16: 256
        let (_f, mut h) = open(&[0x01, 0x00, 0x00, 0x00]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 1;
        let info = h.selection_info().unwrap();
        assert!(info.contains("LE u16: 1"));
        assert!(info.contains("BE u16: 256"));
    }

    #[test]
    fn selection_info_4_bytes() {
        // bytes: [0x00, 0x00, 0x80, 0x3F] → LE f32: 1.0
        let (_f, mut h) = open(&[0x00, 0x00, 0x80, 0x3F, 0x00, 0x00]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 3;
        let info = h.selection_info().unwrap();
        assert!(info.contains("f32: 1.0"));
        assert!(info.contains("LE u32:"));
        assert!(info.contains("BE u32:"));
    }

    #[test]
    fn selection_info_8_bytes() {
        // bytes: [0x00..0x00, 0x00, 0xF0, 0x3F] (at the right offsets) → LE f64: 1.0
        let le_bytes = 1.0_f64.to_le_bytes();
        let mut data = vec![0u8; 16];
        data[..8].copy_from_slice(&le_bytes);
        let (_f, mut h) = open(&data);
        h.selection_anchor = Some(0);
        h.cursor_offset = 7;
        let info = h.selection_info().unwrap();
        assert!(info.contains("f64: 1.0"));
        assert!(info.contains("LE u64:"));
        assert!(info.contains("BE u64:"));
    }

    #[test]
    fn selection_info_3_bytes_shows_count() {
        let (_f, mut h) = open(&[0u8; 32]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 2;
        let info = h.selection_info().unwrap();
        assert_eq!(info, "3 bytes selected");
    }

    #[test]
    fn selection_info_large_selection_shows_count() {
        let (_f, mut h) = open(&[0u8; 256]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 99;
        let info = h.selection_info().unwrap();
        assert_eq!(info, "100 bytes selected");
    }

    #[test]
    fn selection_info_signed_negative() {
        // 0x80 as i8 = -128
        let (_f, mut h) = open(&[0x80, 0x00]);
        h.selection_anchor = Some(0);
        h.cursor_offset = 0;
        let info = h.selection_info().unwrap();
        assert!(info.contains("u8: 128"));
        assert!(info.contains("i8: -128"));
    }

    #[test]
    fn selection_info_with_modifications() {
        let (_f, mut h) = open(&[0x00; 4]);
        h.modifications.insert(0, 0x01);
        h.modifications.insert(1, 0x00);
        h.selection_anchor = Some(0);
        h.cursor_offset = 1;
        let info = h.selection_info().unwrap();
        assert!(info.contains("LE u16: 1")); // [0x01, 0x00] LE = 1
        assert!(info.contains("BE u16: 256")); // [0x01, 0x00] BE = 256
    }
}
