use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

/// How many lines to keep in the in-memory buffer.
const BUFFER_LINES: usize = 10_000;
/// Store a byte-offset checkpoint every N lines for fast seeking.
const INDEX_INTERVAL: usize = 1000;
/// Re-center the buffer when the viewport is within this many lines of the buffer edge.
const BUFFER_MARGIN: usize = 500;

#[derive(Clone)]
pub struct ViewerState {
    pub path: PathBuf,

    /// Sparse line-offset index: entry `i` holds the byte offset of line `i * INDEX_INTERVAL`.
    line_index: Vec<u64>,
    /// How many lines we have discovered so far (by scanning from the start).
    lines_scanned: usize,
    /// Byte offset just past the last byte we scanned.
    scan_byte_offset: u64,
    /// True once we have scanned to EOF.
    scan_complete: bool,

    /// The currently loaded window of lines.
    buffer: Vec<String>,
    /// The global line number of `buffer[0]`.
    buffer_first_line: usize,

    /// Current top-of-viewport line number.
    pub scroll_offset: usize,
    pub visible_lines: usize,

    /// Horizontal scroll offset (display columns) for unwrapped mode.
    pub scroll_x: usize,
    /// Whether lines wrap at the viewport edge.
    pub wrap_mode: bool,

    pub error: Option<String>,
}

impl ViewerState {
    pub fn open(path: PathBuf) -> Self {
        let mut state = Self {
            path,
            line_index: vec![0],
            lines_scanned: 0,
            scan_byte_offset: 0,
            scan_complete: false,
            buffer: Vec::new(),
            buffer_first_line: 0,
            scroll_offset: 0,
            visible_lines: 0,
            scroll_x: 0,
            wrap_mode: false,
            error: None,
        };

        // Load the initial buffer (first BUFFER_LINES lines).
        state.load_buffer_at_line(0);
        state
    }

    // --- public scrolling API ---------------------------------------------------

    pub fn scroll_up(&mut self, amount: usize) {
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_down(&mut self, amount: usize) {
        let max = self.max_scroll();
        self.scroll_offset = (self.scroll_offset + amount).min(max);
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_top(&mut self) {
        self.scroll_offset = 0;
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_to_bottom(&mut self) {
        // We must discover the total line count first.
        self.scan_to_end();
        self.scroll_offset = self.max_scroll();
        self.ensure_buffer_covers_viewport();
    }

    pub fn scroll_left(&mut self, amount: usize) {
        self.scroll_x = self.scroll_x.saturating_sub(amount);
    }

    pub fn scroll_right(&mut self, amount: usize) {
        self.scroll_x = self.scroll_x.saturating_add(amount);
    }

    pub fn toggle_wrap(&mut self) {
        self.wrap_mode = !self.wrap_mode;
        self.scroll_x = 0;
    }

    /// Returns `(line_number, text)` pairs for the current viewport.
    /// `count` specifies how many file lines to fetch (may differ from `visible_lines`
    /// when wrapping is enabled and extra lines are needed).
    pub fn visible_line_iter(&mut self, count: usize) -> Vec<(usize, String)> {
        self.ensure_buffer_covers(self.scroll_offset, count);

        let start = self.scroll_offset;

        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let global_line = start + i;
            if let Some(text) = self.get_line(global_line) {
                out.push((global_line, text.to_owned()));
            } else {
                break;
            }
        }
        out
    }

    /// Total lines if known, otherwise the count so far with a `+` suffix hint.
    pub fn total_lines_display(&self) -> String {
        if self.scan_complete {
            format!("{}", self.lines_scanned)
        } else {
            format!("{}+", self.lines_scanned)
        }
    }

    // --- internals ---------------------------------------------------------------

    fn max_scroll(&self) -> usize {
        if self.scan_complete {
            self.lines_scanned.saturating_sub(self.visible_lines)
        } else {
            // We don't know the end yet; allow scrolling as far as we've scanned.
            self.lines_scanned.saturating_sub(self.visible_lines)
        }
    }

    fn get_line(&self, global_line: usize) -> Option<&str> {
        if global_line < self.buffer_first_line {
            return None;
        }
        let idx = global_line - self.buffer_first_line;
        self.buffer.get(idx).map(|s| s.as_str())
    }

    /// Make sure the buffer contains the current viewport, reloading from disk if needed.
    fn ensure_buffer_covers_viewport(&mut self) {
        self.ensure_buffer_covers(self.scroll_offset, self.visible_lines);
    }

    fn ensure_buffer_covers(&mut self, start: usize, count: usize) {
        let vp_start = start;
        // Clamp to known line count so we don't re-read past EOF every frame.
        let vp_end = if self.scan_complete {
            (vp_start + count).min(self.lines_scanned)
        } else {
            vp_start + count
        };

        let buf_start = self.buffer_first_line;
        let buf_end = buf_start + self.buffer.len();

        // Check if viewport is within the buffer with enough margin.
        let need_reload = vp_start < buf_start
            || vp_end > buf_end
            || (vp_start > buf_start && vp_start - buf_start < BUFFER_MARGIN && buf_start > 0)
            || (buf_end > vp_end && buf_end - vp_end < BUFFER_MARGIN && !self.scan_complete);

        if need_reload {
            // Center the buffer around the viewport.
            let center = vp_start + count / 2;
            let target = center.saturating_sub(BUFFER_LINES / 2);
            self.load_buffer_at_line(target);
        }
    }

    /// Load `BUFFER_LINES` lines starting from `target_line` into the buffer.
    fn load_buffer_at_line(&mut self, target_line: usize) {
        // Make sure we've scanned far enough to know where target_line is.
        if target_line >= self.lines_scanned && !self.scan_complete {
            self.scan_to_line(target_line + BUFFER_LINES);
        }

        // Find the byte offset for target_line using the sparse index.
        let (start_line, start_offset) = self.nearest_index_before(target_line);

        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) => {
                self.error = Some(format!("Cannot open file: {}", e));
                return;
            }
        };
        let mut reader = BufReader::with_capacity(64 * 1024, file);

        if reader.seek(SeekFrom::Start(start_offset)).is_err() {
            return;
        }

        // Skip lines from start_line to target_line.
        let mut current_line = start_line;
        if current_line < target_line {
            let mut skip_buf = String::new();
            while current_line < target_line {
                skip_buf.clear();
                match reader.read_line(&mut skip_buf) {
                    Ok(0) => break, // EOF
                    Ok(_) => current_line += 1,
                    Err(_) => break,
                }
            }
        }

        // Read BUFFER_LINES lines into the buffer.
        self.buffer.clear();
        self.buffer_first_line = current_line;

        let mut line_buf = String::new();
        for _ in 0..BUFFER_LINES {
            line_buf.clear();
            match reader.read_line(&mut line_buf) {
                Ok(0) => break,
                Ok(_) => {
                    // Strip the trailing newline.
                    let trimmed = line_buf.trim_end_matches('\n').trim_end_matches('\r');
                    self.buffer.push(trimmed.to_owned());
                }
                Err(_) => break,
            }
        }
    }

    /// Scan forward from where we left off, building the sparse index, until we have
    /// discovered at least `target_line` lines (or hit EOF).
    fn scan_to_line(&mut self, target_line: usize) {
        if self.scan_complete || self.lines_scanned >= target_line {
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

        let mut line_buf = String::new();
        while self.lines_scanned < target_line {
            line_buf.clear();
            match reader.read_line(&mut line_buf) {
                Ok(0) => {
                    self.scan_complete = true;
                    return;
                }
                Ok(n) => {
                    self.scan_byte_offset += n as u64;
                    self.lines_scanned += 1;

                    // Record sparse index checkpoint.
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

    /// Scan all the way to EOF so we know the total line count.
    fn scan_to_end(&mut self) {
        if self.scan_complete {
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

        let mut line_buf = String::new();
        loop {
            line_buf.clear();
            match reader.read_line(&mut line_buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.scan_byte_offset += n as u64;
                    self.lines_scanned += 1;
                    if self.lines_scanned % INDEX_INTERVAL == 0 {
                        self.line_index.push(self.scan_byte_offset);
                    }
                }
                Err(_) => break,
            }
        }
        self.scan_complete = true;
    }

    /// Return the closest sparse-index entry at or before `line`.
    fn nearest_index_before(&self, line: usize) -> (usize, u64) {
        let idx = (line / INDEX_INTERVAL).min(self.line_index.len() - 1);
        (idx * INDEX_INTERVAL, self.line_index[idx])
    }
}
