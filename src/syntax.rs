use ratatui::style::Color;
use std::path::Path;
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

use crate::theme::theme;

/// Highlight capture names recognized by the theme.
/// Order matters — index into this array maps to a theme color.
const HIGHLIGHT_NAMES: &[&str] = &[
    "keyword",           // 0
    "function",          // 1
    "function.method",   // 2
    "type",              // 3
    "type.builtin",      // 4
    "string",            // 5
    "string.special",    // 6
    "number",            // 7
    "comment",           // 8
    "variable",          // 9
    "variable.builtin",  // 10
    "variable.parameter",// 11
    "constant",          // 12
    "constant.builtin",  // 13
    "operator",          // 14
    "punctuation",       // 15
    "punctuation.bracket",// 16
    "punctuation.delimiter",// 17
    "attribute",         // 18
    "tag",               // 19
    "property",          // 20
    "label",             // 21
    "escape",            // 22
    "constructor",       // 23
];

/// Map highlight capture index to a theme color.
fn highlight_color(idx: usize) -> Color {
    let t = theme();
    match idx {
        0 => t.syn_keyword,
        1 | 2 => t.syn_function,
        3 | 4 => t.syn_type,
        5 | 6 => t.syn_string,
        7 => t.syn_number,
        8 => t.syn_comment,
        9 | 10 | 11 => t.syn_variable,
        12 | 13 => t.syn_constant,
        14 => t.syn_operator,
        15 | 16 | 17 => t.syn_punctuation,
        18 => t.syn_attribute,
        19 => t.syn_tag,
        20 => t.syn_property,
        21 | 22 => t.syn_escape,
        23 => t.syn_constructor,
        _ => Color::LightCyan,
    }
}

/// A colored span within a line: (start_byte, end_byte, color).
pub type HighlightSpan = (usize, usize, Color);

/// Files under this size get a full parse cached in memory.
/// Above this, we fall back to context-window parsing.
const FULL_PARSE_THRESHOLD: u64 = 10 * 1024 * 1024; // 10 MB

/// Syntax highlighter with two modes:
/// - Small files (< 10MB): full file parsed once, cached. Incremental re-parse on edits.
/// - Large files: context-window parsing (200 lines before viewport).
pub struct SyntaxHighlighter {
    highlighter: Highlighter,
    config: Option<HighlightConfiguration>,
    default_color: Color,
    /// Cached full-file highlights (byte_start, byte_end, color).
    /// Only populated for files under FULL_PARSE_THRESHOLD.
    cached_highlights: Option<Vec<HighlightSpan>>,
    /// Byte offset of the start of each line in the cached text.
    cached_line_offsets: Vec<usize>,
    /// The source text hash used to detect when re-parsing is needed (used by reparse_full).
    #[allow(dead_code)]
    cached_text_hash: u64,
}

impl SyntaxHighlighter {
    pub fn new(path: &Path, default_color: Color) -> Self {
        let config = detect_language(path).map(|mut cfg| {
            cfg.configure(HIGHLIGHT_NAMES);
            cfg
        });
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let use_full_parse = config.is_some() && file_size < FULL_PARSE_THRESHOLD;

        // For small files, read and parse the full file immediately
        let (cached_highlights, cached_line_offsets, cached_text_hash) = if use_full_parse {
            if let Ok(text) = std::fs::read_to_string(path) {
                let hash = simple_hash(&text);
                let mut highlighter = Highlighter::new();
                let spans = run_highlight(&mut highlighter, config.as_ref().unwrap(), &text, default_color);
                let offsets = build_line_offsets(&text);
                (Some(spans), offsets, hash)
            } else {
                (None, Vec::new(), 0)
            }
        } else {
            (None, Vec::new(), 0)
        };

        Self {
            highlighter: Highlighter::new(),
            config,
            default_color,
            cached_highlights,
            cached_line_offsets,
            cached_text_hash,
        }
    }

    /// Returns true if syntax highlighting is available for this file.
    pub fn is_active(&self) -> bool {
        self.config.is_some()
    }

    /// Returns true if this file has a cached full parse.
    pub fn has_full_parse(&self) -> bool {
        self.cached_highlights.is_some()
    }

    /// Get cached highlights for a byte range (for the full-parse path).
    /// Returns spans that overlap [start_byte, end_byte).
    /// Uses binary search since spans are sorted by start byte.
    pub fn get_cached_spans(&self, start_byte: usize, end_byte: usize) -> &[HighlightSpan] {
        match &self.cached_highlights {
            Some(spans) => {
                // Binary search for the first span that could overlap:
                // find the first span whose end > start_byte
                let first = spans.partition_point(|(_, e, _)| *e <= start_byte);
                // Find the last span whose start < end_byte
                let last = spans[first..].partition_point(|(s, _, _)| *s < end_byte);
                &spans[first..first + last]
            }
            None => &[],
        }
    }

    /// Invalidate the cached parse (e.g., after editing).
    /// Falls back to context-window highlighting until reparse_full is called.
    pub fn invalidate_cache(&mut self) {
        self.cached_highlights = None;
        self.cached_line_offsets.clear();
    }

    /// Invalidate cache and re-parse the full file text.
    /// Called after edits when the editor has the full content available.
    #[allow(dead_code)]
    pub fn reparse_full(&mut self, full_text: &str) {
        if self.config.is_none() {
            return;
        }
        let hash = simple_hash(full_text);
        if hash == self.cached_text_hash {
            return; // no change
        }
        self.cached_text_hash = hash;
        self.cached_highlights = Some(run_highlight(
            &mut self.highlighter,
            self.config.as_ref().unwrap(),
            full_text,
            self.default_color,
        ));
        self.cached_line_offsets = build_line_offsets(full_text);
    }

    /// Highlight a block of text (context-window path for large files).
    /// Returns a flat list of (byte_start, byte_end, color) spans.
    pub fn highlight_text(&mut self, text: &str) -> Vec<HighlightSpan> {
        let config = match &self.config {
            Some(c) => c,
            None => return vec![(0, text.len(), self.default_color)],
        };
        run_highlight(&mut self.highlighter, config, text, self.default_color)
    }

    /// Get the byte offset of a given line number in the cached text.
    pub fn line_byte_offset(&self, line: usize) -> usize {
        self.cached_line_offsets
            .get(line)
            .copied()
            .unwrap_or(self.cached_line_offsets.last().copied().unwrap_or(0))
    }
}

/// Build a list of byte offsets for the start of each line.
fn build_line_offsets(text: &str) -> Vec<usize> {
    let mut offsets = vec![0];
    for (i, b) in text.bytes().enumerate() {
        if b == b'\n' {
            offsets.push(i + 1);
        }
    }
    offsets
}

/// Run tree-sitter highlighting on text and collect spans.
fn run_highlight(
    highlighter: &mut Highlighter,
    config: &HighlightConfiguration,
    text: &str,
    default_color: Color,
) -> Vec<HighlightSpan> {
    let result = highlighter.highlight(config, text.as_bytes(), None, |_| None);

    let mut highlights = match result {
        Ok(h) => h,
        Err(_) => return vec![(0, text.len(), default_color)],
    };

    let mut spans = Vec::new();
    let mut current_color = default_color;
    let mut byte_offset = 0;
    let mut color_stack: Vec<Color> = vec![default_color];

    while let Some(Ok(event)) = highlights.next() {
        match event {
            HighlightEvent::Source { start, end } => {
                if start < end {
                    spans.push((start, end, current_color));
                }
                byte_offset = end;
            }
            HighlightEvent::HighlightStart(h) => {
                current_color = highlight_color(h.0);
                color_stack.push(current_color);
            }
            HighlightEvent::HighlightEnd => {
                color_stack.pop();
                current_color = *color_stack.last().unwrap_or(&default_color);
            }
        }
    }

    if byte_offset < text.len() {
        spans.push((byte_offset, text.len(), default_color));
    }

    spans
}

/// Simple non-crypto hash for change detection.
fn simple_hash(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for byte in text.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    hash
}

/// Detect language from file extension and return a configured HighlightConfiguration.
fn detect_language(path: &Path) -> Option<HighlightConfiguration> {
    let ext = path.extension()?.to_str()?;
    match ext {
        "rs" => make_config(
            tree_sitter_rust::LANGUAGE.into(),
            tree_sitter_rust::HIGHLIGHTS_QUERY,
            "",
            tree_sitter_rust::INJECTIONS_QUERY,
        ),
        "java" => make_config(
            tree_sitter_java::LANGUAGE.into(),
            tree_sitter_java::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
        "py" | "pyi" => make_config(
            tree_sitter_python::LANGUAGE.into(),
            tree_sitter_python::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
        "js" | "jsx" | "mjs" | "cjs" | "ts" | "tsx" => make_config(
            tree_sitter_javascript::LANGUAGE.into(),
            tree_sitter_javascript::HIGHLIGHT_QUERY,
            tree_sitter_javascript::INJECTIONS_QUERY,
            tree_sitter_javascript::LOCALS_QUERY,
        ),
        "json" | "jsonl" => make_config(
            tree_sitter_json::LANGUAGE.into(),
            tree_sitter_json::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
        "go" => make_config(
            tree_sitter_go::LANGUAGE.into(),
            tree_sitter_go::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
        "c" | "h" => make_config(
            tree_sitter_c::LANGUAGE.into(),
            tree_sitter_c::HIGHLIGHT_QUERY,
            "",
            "",
        ),
        "sh" | "bash" | "zsh" => make_config(
            tree_sitter_bash::LANGUAGE.into(),
            tree_sitter_bash::HIGHLIGHT_QUERY,
            "",
            "",
        ),
        "toml" => make_config(
            tree_sitter_toml_ng::LANGUAGE.into(),
            tree_sitter_toml_ng::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
        _ => None,
    }
}

fn make_config(
    language: tree_sitter::Language,
    highlights: &str,
    injections: &str,
    locals: &str,
) -> Option<HighlightConfiguration> {
    HighlightConfiguration::new(language, "highlight", highlights, injections, locals).ok()
}
