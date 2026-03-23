use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::path::PathBuf;

pub const BYTES_PER_ROW: usize = 16;
const BUFFER_BYTES: usize = 256 * 1024;
const BINARY_CHECK_BYTES: usize = 8192;

#[derive(Clone)]
pub struct HexViewerState {
    pub path: PathBuf,
    pub file_size: u64,
    pub scroll_offset: usize, // top visible row index
    pub visible_rows: usize,

    buffer: Vec<u8>,
    buffer_start_row: usize,
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
        };
        state.load_buffer_at_byte(0);
        state
    }

    pub fn is_binary(path: &Path) -> bool {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mut buf = vec![0u8; BINARY_CHECK_BYTES];
        let n = file.read(&mut buf).unwrap_or(0);
        buf[..n].contains(&0)
    }

    pub fn total_rows(&self) -> usize {
        (self.file_size as usize).div_ceil(BYTES_PER_ROW)
    }

    fn max_scroll(&self) -> usize {
        self.total_rows().saturating_sub(self.visible_rows)
    }

    pub fn scroll_up(&mut self, amount: usize) {
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_down(&mut self, amount: usize) {
        self.scroll_offset = (self.scroll_offset + amount).min(self.max_scroll());
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_top(&mut self) {
        self.scroll_offset = 0;
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_bottom(&mut self) {
        self.scroll_offset = self.max_scroll();
        self.ensure_buffer_covers_viewport();
    }

    /// Returns (byte_offset, row_bytes) for each visible row.
    pub fn visible_rows_data(&mut self) -> Vec<(u64, Vec<u8>)> {
        self.ensure_buffer_covers_viewport();

        let mut rows = Vec::with_capacity(self.visible_rows);
        for i in 0..self.visible_rows {
            let row = self.scroll_offset + i;
            let byte_offset = (row * BYTES_PER_ROW) as u64;
            if byte_offset >= self.file_size {
                break;
            }
            let bytes = self.get_row_bytes(row);
            if bytes.is_empty() {
                break;
            }
            rows.push((byte_offset, bytes));
        }
        rows
    }

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
