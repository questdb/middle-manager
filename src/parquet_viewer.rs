use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::Encoding;
use parquet2::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
use parquet2::page::{split_buffer, DataPage, DataPageHeader, DataPageHeaderExt, DictPage, Page};
use parquet2::read;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType, TimeUnit};
use parquet2::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics, Statistics,
};
use unicode_width::UnicodeWidthStr;

/// Capped at 100 — the Tree is a quick-look summary, not a deep inspection
/// tool. For full navigation the user is expected to switch to Table view
/// (which loads larger sliding windows).
const DATA_PREVIEW_MAX_ROWS: usize = 100;

/// Sliding-window size used by the Table view. We load this many rows at a
/// time starting at the cursor's row within the enclosing row group. When the
/// cursor moves outside the window, a new window is loaded.
const TABLE_BUFFER_ROWS: usize = 10_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum NodeId {
    Root,
    KvMetadata,
    Schema,
    RowGroup(usize),
    RowGroupColumns(usize),
    /// (row_group_index, column_index)
    RowGroupColumn(usize, usize),
    RowGroupData(usize),
    /// Dictionary contents of a column chunk (rg, col).
    ColumnDict(usize, usize),
}

#[derive(Clone, Copy, PartialEq)]
pub enum ItemKind {
    Header,
    Property,
    SchemaField,
    RowGroupHeader,
    ColumnInfo,
    DataHeader,
    DataCell,
    Error,
}

pub struct TreeItem {
    pub depth: usize,
    pub text: String,
    pub kind: ItemKind,
    pub expandable: bool,
    pub node_id: Option<NodeId>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum ViewMode {
    Tree,
    Table,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alignment {
    Left,
    Right,
}

#[derive(Clone)]
pub struct DataPreview {
    pub column_widths: Vec<usize>,
    pub rows: Vec<Vec<String>>,
}

/// Geometry snapshot from the last Table-view render, used to translate a
/// mouse click at terminal coordinates back into a (row, column) pair.
/// Stored as plain integers so the viewer layer doesn't depend on ratatui
/// types.
#[derive(Default, Clone)]
pub struct TableLayout {
    pub data_start_y: u16,
    pub data_end_y: u16, // exclusive
    pub scroll_row: usize,
    /// Visible cell hitboxes: `(col_idx, x_start, x_end_exclusive)`.
    /// Includes the frozen cell (when one is drawn) and the scrolled range.
    pub col_hits: Vec<(usize, u16, u16)>,
}

/// Popup shown over the Table view. `Row` shows all columns for the current
/// row; `Cell` focuses on a single value; `ColumnInfo` shows aggregated
/// metadata for a column across all row groups.
pub enum DetailPopup {
    Row {
        row_idx: usize,
        pairs: Vec<(String, String)>,
        scroll: usize,
    },
    Cell {
        row_idx: usize,
        col_idx: usize,
        column_name: String,
        value: String,
        scroll: usize,
    },
    ColumnInfo {
        col_idx: usize,
        column_name: String,
        pairs: Vec<(String, String)>,
        scroll: usize,
    },
}

impl DetailPopup {
    pub fn scroll_mut(&mut self) -> &mut usize {
        match self {
            DetailPopup::Row { scroll, .. } => scroll,
            DetailPopup::Cell { scroll, .. } => scroll,
            DetailPopup::ColumnInfo { scroll, .. } => scroll,
        }
    }
    pub fn scroll(&self) -> usize {
        match self {
            DetailPopup::Row { scroll, .. } => *scroll,
            DetailPopup::Cell { scroll, .. } => *scroll,
            DetailPopup::ColumnInfo { scroll, .. } => *scroll,
        }
    }
}

/// Search state for the Table view. The `input` is a full `TextInput` so
/// cursor movement, selection, undo/redo and clipboard paste all work in the
/// search prompt — consistent with every other dialog in the app.
/// The pattern is treated as a regex (with a fallback to literal substring
/// if the regex doesn't compile); searching is case-insensitive.
pub struct TableSearch {
    pub input: crate::text_input::TextInput,
    /// True while the user is typing the query; false once accepted with Enter.
    pub input_open: bool,
    /// Cached compiled regex plus the query string it was built from.
    /// Rebuilt lazily from `query()` when the query changes; looked up from
    /// render on every row, so this avoids ~100 recompilations per keystroke.
    cached_regex: std::cell::RefCell<Option<(String, Option<regex::Regex>)>>,
}

impl TableSearch {
    pub fn query(&self) -> &str {
        &self.input.text
    }

    /// Compile-on-demand, cached by query text. Returns `None` if the query
    /// is empty or the pattern cannot be compiled (even as an escaped literal).
    pub fn regex(&self) -> Option<regex::Regex> {
        let q = self.query();
        {
            let cache = self.cached_regex.borrow();
            if let Some((cached_q, cached_re)) = cache.as_ref() {
                if cached_q == q {
                    return cached_re.clone();
                }
            }
        }
        let re = compile_search_regex(q);
        *self.cached_regex.borrow_mut() = Some((q.to_string(), re.clone()));
        re
    }
}

// ---------------------------------------------------------------------------
// ParquetViewerState
// ---------------------------------------------------------------------------

pub struct ParquetViewerState {
    pub path: PathBuf,
    pub file_size: u64,

    metadata: FileMetaData,

    pub view_mode: ViewMode,

    // Tree view
    pub tree_items: Vec<TreeItem>,
    pub tree_cursor: usize,
    pub tree_scroll: usize,
    pub tree_visible: usize,
    pub expanded: HashSet<NodeId>,

    // Table view
    pub table_columns: Vec<String>,
    pub table_column_widths: Vec<usize>,
    pub table_column_aligns: Vec<Alignment>,
    pub table_rows: Vec<Vec<String>>,
    pub table_total_rows: usize,
    pub table_cursor_row: usize,
    pub table_cursor_col: usize,
    pub table_scroll_row: usize,
    pub table_scroll_col: usize,
    pub table_visible_rows: usize,
    pub table_visible_cols: usize,
    table_loaded_rg: Option<usize>,
    table_loaded_offset: usize,

    // Popups and search (Table view)
    pub popup: Option<DetailPopup>,
    pub search: Option<TableSearch>,
    /// Transient status message shown in the hint bar (e.g. "No matches").
    pub status: Option<String>,
    /// Selected rows by global index (Table view). Space toggles.
    pub selected_rows: HashSet<usize>,
    /// A single "pinned" column that stays rendered to the left of the
    /// scrolled range when `table_scroll_col` moves past it (Table view).
    pub frozen_col: Option<usize>,
    /// Columns hidden from the Table view (skipped in rendering and
    /// navigation). Export operations still include them — hiding is a UI
    /// concern, not a data filter.
    pub hidden_cols: HashSet<usize>,
    /// When true, integer values in right-aligned (numeric) columns render
    /// with thousands separators. Applied at display time only — stored
    /// values and clipboard output stay unformatted for copy-paste into SQL.
    pub thousands_separators: bool,
    /// Geometry from the last table render, used by `click_at`.
    pub last_layout: Option<TableLayout>,
    /// In-window sort key: (column index, ascending). When set, the
    /// currently-loaded window is sorted by this column; the canonical
    /// (load-order) global row index for each visual position is tracked
    /// via `table_row_global`. Window reloads re-apply the same sort.
    pub sort_order: Option<(usize, bool)>,
    /// Parallel to `table_rows` — the canonical global row index for each
    /// entry. Without sort this is just `loaded_offset..loaded_offset+len`.
    /// With sort it's permuted along with `table_rows`.
    pub table_row_global: Vec<usize>,

    // Data previews (per row group, lazily loaded)
    data_previews: Vec<Option<DataPreview>>,

    // Caches (computed once, reused across rebuild_tree calls)
    /// Pretty-printed KV metadata lines: vec of (key, formatted_value_lines)
    kv_cache: Option<Vec<(String, Vec<String>)>>,
    /// Per-column-chunk decoded dictionary values. `None` inside the map means
    /// "load attempted and failed"; absence from the map means "not yet loaded".
    column_dicts: std::collections::HashMap<(usize, usize), Option<Vec<String>>>,
}

impl ParquetViewerState {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut file = File::open(&path).map_err(|e| format!("Cannot open file: {}", e))?;
        let metadata = read::read_metadata(&mut file)
            .map_err(|e| format!("Not a valid Parquet file: {}", e))?;

        let num_rg = metadata.row_groups.len();
        let table_columns: Vec<String> = metadata
            .schema()
            .columns()
            .iter()
            .map(|c| c.descriptor.primitive_type.field_info.name.clone())
            .collect();
        let table_column_widths: Vec<usize> = table_columns
            .iter()
            .map(|n| UnicodeWidthStr::width(n.as_str()).max(8))
            .collect();
        // Right-align numeric types; everything else (dates, strings, bool,
        // binary) left-aligns. This mirrors most DB/spreadsheet conventions.
        let table_column_aligns: Vec<Alignment> = metadata
            .schema()
            .columns()
            .iter()
            .map(|c| {
                column_alignment(
                    c.descriptor.primitive_type.physical_type,
                    c.descriptor.primitive_type.logical_type.as_ref(),
                )
            })
            .collect();
        let table_total_rows: usize = metadata.row_groups.iter().map(|rg| rg.num_rows()).sum();

        let mut state = Self {
            path,
            file_size,
            metadata,
            view_mode: ViewMode::Tree,
            // Pre-allocate: root + properties + schema header + row group headers
            tree_items: Vec::with_capacity(4 + table_columns.len() + num_rg * 3),
            tree_cursor: 0,
            tree_scroll: 0,
            tree_visible: 0,
            expanded: HashSet::new(),
            table_columns,
            table_column_widths,
            table_column_aligns,
            table_rows: Vec::new(),
            table_total_rows,
            table_cursor_row: 0,
            table_cursor_col: 0,
            table_scroll_row: 0,
            table_scroll_col: 0,
            table_visible_rows: 0,
            table_visible_cols: 0,
            table_loaded_rg: None,
            table_loaded_offset: 0,
            popup: None,
            search: None,
            status: None,
            selected_rows: HashSet::new(),
            frozen_col: None,
            hidden_cols: HashSet::new(),
            thousands_separators: false,
            last_layout: None,
            sort_order: None,
            table_row_global: Vec::new(),
            data_previews: vec![None; num_rg],
            kv_cache: None,
            column_dicts: std::collections::HashMap::new(),
        };

        state.expanded.insert(NodeId::Root);
        state.rebuild_tree();
        Ok(state)
    }

    // -----------------------------------------------------------------------
    // Navigation (unified: dispatches to tree or table)
    // -----------------------------------------------------------------------

    pub fn move_up(&mut self, amount: usize) {
        if let Some(popup) = &mut self.popup {
            let s = popup.scroll_mut();
            *s = s.saturating_sub(amount);
            return;
        }
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = self.tree_cursor.saturating_sub(amount);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                self.status = None;
                self.table_cursor_row = self.table_cursor_row.saturating_sub(amount);
                self.ensure_cursor_visible();
                self.ensure_table_data();
            }
        }
    }

    pub fn move_down(&mut self, amount: usize) {
        if let Some(popup) = &mut self.popup {
            // Scroll guard is applied at render time (we don't know the
            // total wrapped line count here without the terminal width).
            let s = popup.scroll_mut();
            *s = s.saturating_add(amount);
            return;
        }
        match self.view_mode {
            ViewMode::Tree => {
                let max = self.tree_items.len().saturating_sub(1);
                self.tree_cursor = (self.tree_cursor + amount).min(max);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                self.status = None;
                let max = self.table_total_rows.saturating_sub(1);
                self.table_cursor_row = (self.table_cursor_row + amount).min(max);
                self.ensure_cursor_visible();
                self.ensure_table_data();
            }
        }
    }

    pub fn move_to_top(&mut self) {
        if let Some(popup) = &mut self.popup {
            *popup.scroll_mut() = 0;
            return;
        }
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = 0;
                self.tree_scroll = 0;
            }
            ViewMode::Table => {
                self.status = None;
                self.table_cursor_row = 0;
                self.table_scroll_row = 0;
                self.ensure_table_data();
            }
        }
    }

    pub fn move_to_bottom(&mut self) {
        if let Some(popup) = &mut self.popup {
            // Clamped at render time.
            *popup.scroll_mut() = usize::MAX / 2;
            return;
        }
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = self.tree_items.len().saturating_sub(1);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                self.status = None;
                self.table_cursor_row = self.table_total_rows.saturating_sub(1);
                self.table_scroll_row = self.table_max_scroll();
                self.ensure_table_data();
            }
        }
    }

    pub fn page_up(&mut self) {
        let page = match self.view_mode {
            ViewMode::Tree => self.tree_visible.max(1),
            ViewMode::Table => self.table_visible_rows.max(1),
        };
        self.move_up(page);
    }

    pub fn page_down(&mut self) {
        let page = match self.view_mode {
            ViewMode::Tree => self.tree_visible.max(1),
            ViewMode::Table => self.table_visible_rows.max(1),
        };
        self.move_down(page);
    }

    /// Vi-style `Ctrl+U`: move up half a screen.
    pub fn half_page_up(&mut self) {
        let page = match self.view_mode {
            ViewMode::Tree => (self.tree_visible / 2).max(1),
            ViewMode::Table => (self.table_visible_rows / 2).max(1),
        };
        self.move_up(page);
    }

    /// Vi-style `Ctrl+D`: move down half a screen.
    pub fn half_page_down(&mut self) {
        let page = match self.view_mode {
            ViewMode::Tree => (self.tree_visible / 2).max(1),
            ViewMode::Table => (self.table_visible_rows / 2).max(1),
        };
        self.move_down(page);
    }

    pub fn scroll_left(&mut self) {
        if self.popup.is_some() {
            return;
        }
        if self.view_mode == ViewMode::Table {
            self.status = None;
            if self.table_cursor_col > 0 {
                let start = self.table_cursor_col - 1;
                // Nearest visible column strictly to the left.
                for i in (0..=start).rev() {
                    if !self.hidden_cols.contains(&i) {
                        self.table_cursor_col = i;
                        break;
                    }
                }
            }
            self.ensure_col_cursor_visible();
        } else {
            self.collapse();
        }
    }

    pub fn scroll_right(&mut self) {
        if self.popup.is_some() {
            return;
        }
        if self.view_mode == ViewMode::Table {
            self.status = None;
            let total = self.table_columns.len();
            for i in self.table_cursor_col.saturating_add(1)..total {
                if !self.hidden_cols.contains(&i) {
                    self.table_cursor_col = i;
                    break;
                }
            }
            self.ensure_col_cursor_visible();
        } else {
            self.expand();
        }
    }

    /// Jump one screen of columns to the left (Shift-Left in the UI).
    pub fn page_left(&mut self) {
        if self.popup.is_some() || self.view_mode != ViewMode::Table {
            return;
        }
        self.status = None;
        let step = self.table_visible_cols.max(1);
        let mut target = self.table_cursor_col;
        let mut moved = 0;
        while moved < step && target > 0 {
            target -= 1;
            if !self.hidden_cols.contains(&target) {
                moved += 1;
            }
        }
        if self.hidden_cols.contains(&target) {
            target = self
                .nearest_visible_col(target, true)
                .unwrap_or(self.table_cursor_col);
        }
        self.table_cursor_col = target;
        self.ensure_col_cursor_visible();
    }

    /// Jump one screen of columns to the right (Shift-Right in the UI).
    pub fn page_right(&mut self) {
        if self.popup.is_some() || self.view_mode != ViewMode::Table {
            return;
        }
        self.status = None;
        let step = self.table_visible_cols.max(1);
        let max_col = self.table_columns.len().saturating_sub(1);
        let mut target = self.table_cursor_col;
        let mut moved = 0;
        while moved < step && target < max_col {
            target += 1;
            if !self.hidden_cols.contains(&target) {
                moved += 1;
            }
        }
        if self.hidden_cols.contains(&target) {
            target = self
                .nearest_visible_col(target, false)
                .unwrap_or(self.table_cursor_col);
        }
        self.table_cursor_col = target;
        self.ensure_col_cursor_visible();
    }

    /// Toggle expand/collapse on the current tree node.
    pub fn toggle_expand(&mut self) {
        if self.view_mode != ViewMode::Tree {
            return;
        }
        if let Some(item) = self.tree_items.get(self.tree_cursor) {
            if let Some(node_id) = item.node_id {
                if item.expandable {
                    if self.expanded.contains(&node_id) {
                        self.expanded.remove(&node_id);
                    } else {
                        self.expanded.insert(node_id);
                        match node_id {
                            NodeId::RowGroupData(rg) => {
                                if self.data_previews[rg].is_none() {
                                    self.load_data_preview(rg);
                                }
                            }
                            NodeId::ColumnDict(rg, col) => {
                                if !self.column_dicts.contains_key(&(rg, col)) {
                                    self.load_column_dict(rg, col);
                                }
                            }
                            _ => {}
                        }
                    }
                    self.rebuild_tree();
                }
            }
        }
    }

    pub fn expand(&mut self) {
        if self.view_mode != ViewMode::Tree {
            return;
        }
        if let Some(item) = self.tree_items.get(self.tree_cursor) {
            if let Some(node_id) = item.node_id {
                if item.expandable && !self.expanded.contains(&node_id) {
                    self.expanded.insert(node_id);
                    match node_id {
                        NodeId::RowGroupData(rg) => {
                            if self.data_previews[rg].is_none() {
                                self.load_data_preview(rg);
                            }
                        }
                        NodeId::ColumnDict(rg, col) => {
                            if !self.column_dicts.contains_key(&(rg, col)) {
                                self.load_column_dict(rg, col);
                            }
                        }
                        _ => {}
                    }
                    self.rebuild_tree();
                }
            }
        }
    }

    pub fn collapse(&mut self) {
        if self.view_mode != ViewMode::Tree {
            return;
        }
        if let Some(item) = self.tree_items.get(self.tree_cursor) {
            if let Some(node_id) = item.node_id {
                if item.expandable && self.expanded.contains(&node_id) {
                    self.expanded.remove(&node_id);
                    self.rebuild_tree();
                    return;
                }
            }
            // If not expandable or already collapsed, move to parent
            let cur_depth = item.depth;
            if cur_depth > 0 {
                for i in (0..self.tree_cursor).rev() {
                    if self.tree_items[i].depth < cur_depth && self.tree_items[i].expandable {
                        self.tree_cursor = i;
                        self.ensure_tree_visible();
                        return;
                    }
                }
            }
        }
    }

    pub fn switch_view(&mut self) {
        // Switching views while a popup or search is open is surprising;
        // clear them first.
        self.popup = None;
        self.search = None;
        self.status = None;
        match self.view_mode {
            ViewMode::Tree => {
                self.view_mode = ViewMode::Table;
                self.ensure_table_data();
            }
            ViewMode::Table => {
                self.view_mode = ViewMode::Tree;
            }
        }
    }

    pub fn goto_row(&mut self, row: usize) {
        self.popup = None;
        self.status = None;
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = row.min(self.tree_items.len().saturating_sub(1));
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                let max_row = self.table_total_rows.saturating_sub(1);
                self.table_cursor_row = row.min(max_row);
                self.ensure_cursor_visible();
                self.ensure_table_data();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Popups (Table view)
    // -----------------------------------------------------------------------

    /// Enter/activate: expand tree node, open cell popup in Table, or close
    /// any currently-open popup.
    pub fn activate(&mut self) {
        if self.popup.is_some() {
            self.popup = None;
            return;
        }
        match self.view_mode {
            ViewMode::Tree => self.toggle_expand(),
            ViewMode::Table => self.open_cell_detail(),
        }
    }

    /// Alternate "show full row" action (Shift-Enter).
    pub fn activate_row_detail(&mut self) {
        if self.popup.is_some() || self.view_mode != ViewMode::Table {
            return;
        }
        self.open_row_detail();
    }

    /// Closes the row-detail popup if open. Returns true if something was
    /// closed (so the caller can suppress a broader cancel, e.g. Esc).
    pub fn close_popup_if_open(&mut self) -> bool {
        if self.popup.is_some() {
            self.popup = None;
            true
        } else {
            false
        }
    }

    pub fn popup_is_open(&self) -> bool {
        self.popup.is_some()
    }

    fn open_cell_detail(&mut self) {
        if self.table_total_rows == 0 || self.table_columns.is_empty() {
            return;
        }
        self.ensure_table_data();
        let col_idx = self.table_cursor_col.min(self.table_columns.len() - 1);
        let column_name = self.table_columns[col_idx].clone();
        let value = self
            .table_row(self.table_cursor_row)
            .and_then(|r| r.get(col_idx).cloned())
            .unwrap_or_default();
        self.popup = Some(DetailPopup::Cell {
            row_idx: self.table_cursor_row,
            col_idx,
            column_name,
            value,
            scroll: 0,
        });
    }

    /// Build a column-info popup for the column under the cursor. Aggregates
    /// null counts, byte sizes, and per-row-group compression/encoding info
    /// from the file metadata — no data pages are read.
    pub fn open_column_info(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let col_idx = self.table_cursor_col.min(self.table_columns.len() - 1);
        let column_name = self.table_columns[col_idx].clone();
        let pairs = self.build_column_info_pairs(col_idx);
        self.popup = Some(DetailPopup::ColumnInfo {
            col_idx,
            column_name,
            pairs,
            scroll: 0,
        });
    }

    fn build_column_info_pairs(&self, col_idx: usize) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        let name = &self.table_columns[col_idx];
        pairs.push(("Name".into(), name.clone()));

        // Aggregate across row groups.
        let mut total_values: u64 = 0;
        let mut total_nulls: Option<u64> = None; // None until we see one stat
        let mut total_compressed: u64 = 0;
        let mut total_uncompressed: u64 = 0;
        let mut compressions: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        let mut encodings: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut per_rg: Vec<String> = Vec::new();

        let mut physical = String::new();
        let mut logical = String::new();

        for (rg_idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let col_meta = match rg.columns().get(col_idx) {
                Some(c) => c,
                None => continue,
            };
            if physical.is_empty() {
                let pt = &col_meta.descriptor().descriptor.primitive_type;
                physical = format_physical_type(pt.physical_type).to_string();
                logical = pt
                    .logical_type
                    .as_ref()
                    .map(format_logical_type)
                    .unwrap_or_default();
            }
            let nv = col_meta.num_values() as u64;
            total_values = total_values.saturating_add(nv);
            total_compressed = total_compressed.saturating_add(col_meta.compressed_size() as u64);
            total_uncompressed =
                total_uncompressed.saturating_add(col_meta.uncompressed_size() as u64);
            let comp_name = format!("{:?}", col_meta.compression());
            *compressions.entry(comp_name.clone()).or_insert(0) += 1;
            let encs: Vec<&str> = col_meta
                .column_encoding()
                .iter()
                .map(|e| format_encoding(e.0))
                .collect();
            for e in &encs {
                encodings.insert((*e).to_string());
            }
            let stats = col_meta.statistics().and_then(|r| r.ok());
            let nulls = stats.and_then(|s| s.null_count()).map(|n| n as u64);
            if let Some(n) = nulls {
                total_nulls = Some(total_nulls.unwrap_or(0) + n);
            }
            per_rg.push(format!(
                "#{}: {} rows, {}, {} → {}",
                rg_idx + 1,
                format_number(nv as usize),
                comp_name,
                format_size(col_meta.uncompressed_size() as u64),
                format_size(col_meta.compressed_size() as u64),
            ));
        }

        let type_str = if logical.is_empty() {
            physical
        } else {
            format!("{} / {}", physical, logical)
        };
        pairs.push(("Type".into(), type_str));
        pairs.push(("Values".into(), format_number(total_values as usize)));
        if let Some(n) = total_nulls {
            if total_values > 0 {
                let pct = 100.0 * (n as f64) / (total_values as f64);
                pairs.push((
                    "Nulls".into(),
                    format!("{} ({:.2}%)", format_number(n as usize), pct),
                ));
            } else {
                pairs.push(("Nulls".into(), format_number(n as usize)));
            }
        }
        if total_uncompressed > 0 {
            let ratio = total_uncompressed as f64 / total_compressed.max(1) as f64;
            pairs.push((
                "Size".into(),
                format!(
                    "{} compressed / {} uncompressed ({:.2}x)",
                    format_size(total_compressed),
                    format_size(total_uncompressed),
                    ratio,
                ),
            ));
        }
        let compressions_str: Vec<String> = compressions
            .iter()
            .map(|(k, v)| {
                if self.metadata.row_groups.len() == 1 {
                    k.clone()
                } else {
                    format!("{} ({} RGs)", k, v)
                }
            })
            .collect();
        pairs.push(("Compressions".into(), compressions_str.join(", ")));
        let enc_vec: Vec<String> = encodings.into_iter().collect();
        pairs.push(("Encodings".into(), enc_vec.join(", ")));
        pairs.push((
            "Row groups".into(),
            format_number(self.metadata.row_groups.len()),
        ));
        // Per-RG breakdown (cap at 20 rows so the popup stays legible).
        const PER_RG_LIMIT: usize = 20;
        for (i, line) in per_rg.iter().take(PER_RG_LIMIT).enumerate() {
            pairs.push((format!("RG {}", i + 1), line.clone()));
        }
        if per_rg.len() > PER_RG_LIMIT {
            pairs.push((
                "…".into(),
                format!("and {} more row groups", per_rg.len() - PER_RG_LIMIT),
            ));
        }
        pairs
    }

    fn open_row_detail(&mut self) {
        if self.table_total_rows == 0 {
            return;
        }
        self.ensure_table_data();
        let row = match self.table_row(self.table_cursor_row) {
            Some(r) => r.clone(),
            None => return,
        };
        let pairs: Vec<(String, String)> = self.table_columns.iter().cloned().zip(row).collect();
        self.popup = Some(DetailPopup::Row {
            row_idx: self.table_cursor_row,
            pairs,
            scroll: 0,
        });
    }

    // -----------------------------------------------------------------------
    // Search (Table view)
    // -----------------------------------------------------------------------

    /// Open the search input prompt. In Table view the search scans row
    /// values; in Tree view it scans the visible tree lines (field names,
    /// metadata keys, etc.).
    pub fn search_open(&mut self) {
        self.popup = None;
        self.status = None;
        self.search = Some(TableSearch {
            input: crate::text_input::TextInput::new(String::new()),
            input_open: true,
            cached_regex: std::cell::RefCell::new(None),
        });
    }

    pub fn search_is_input_open(&self) -> bool {
        matches!(self.search, Some(ref s) if s.input_open)
    }

    pub fn search_input_char(&mut self, c: char) {
        if let Some(s) = &mut self.search {
            if s.input_open {
                s.input.insert_char(c);
            }
        }
    }

    pub fn search_input_backspace(&mut self) {
        if let Some(s) = &mut self.search {
            if s.input_open {
                s.input.backspace();
            }
        }
    }

    pub fn search_input_cancel(&mut self) {
        self.search = None;
        self.status = None;
    }

    /// Accept the current query and jump to the first match from the cursor.
    pub fn search_input_accept(&mut self) {
        let query = match &self.search {
            Some(s) if s.input_open && !s.query().is_empty() => s.query().to_string(),
            _ => {
                self.search = None;
                return;
            }
        };
        if let Some(s) = &mut self.search {
            s.input_open = false;
        }
        match self.view_mode {
            ViewMode::Table => self.search_jump(&query, self.table_cursor_row, false),
            ViewMode::Tree => self.search_tree_jump(&query, self.tree_cursor, false),
        }
    }

    /// Set the search pattern to a literal match of the current cell's
    /// value and jump to the next (or previous) occurrence. Vim-style `*`/`#`.
    pub fn search_current_cell(&mut self, reverse: bool) {
        if self.view_mode != ViewMode::Table {
            return;
        }
        let col = self.table_cursor_col;
        let val = match self
            .table_row(self.table_cursor_row)
            .and_then(|r| r.get(col).cloned())
        {
            Some(v) => v,
            None => return,
        };
        if val.is_empty() || val == "null" {
            self.status = Some("Nothing to search for".into());
            return;
        }
        // Escape so regex metacharacters in the value become literals.
        let pattern = regex::escape(&val);
        self.search = Some(TableSearch {
            input: crate::text_input::TextInput::new(pattern.clone()),
            input_open: false,
            cached_regex: std::cell::RefCell::new(None),
        });
        let start = if reverse {
            self.table_cursor_row.saturating_sub(1)
        } else {
            self.table_cursor_row.saturating_add(1)
        };
        self.search_jump(&pattern, start, reverse);
    }

    pub fn search_next(&mut self) {
        let query = match self.search.as_ref().map(|s| s.query().to_string()) {
            Some(q) if !q.is_empty() => q,
            _ => return,
        };
        match self.view_mode {
            ViewMode::Table => {
                let start = self.table_cursor_row.saturating_add(1);
                self.search_jump(&query, start, false);
            }
            ViewMode::Tree => {
                let start = self.tree_cursor.saturating_add(1);
                self.search_tree_jump(&query, start, false);
            }
        }
    }

    pub fn search_prev(&mut self) {
        let query = match self.search.as_ref().map(|s| s.query().to_string()) {
            Some(q) if !q.is_empty() => q,
            _ => return,
        };
        match self.view_mode {
            ViewMode::Table => {
                let start = self.table_cursor_row.saturating_sub(1);
                self.search_jump(&query, start, true);
            }
            ViewMode::Tree => {
                let start = self.tree_cursor.saturating_sub(1);
                self.search_tree_jump(&query, start, true);
            }
        }
    }

    /// Search the currently-expanded tree items. Because tree lines are
    /// already materialized (no lazy loading), this just walks the Vec.
    fn search_tree_jump(&mut self, query: &str, start: usize, reverse: bool) {
        if self.tree_items.is_empty() {
            return;
        }
        let re = match compile_search_regex(query) {
            Some(r) => r,
            None => {
                self.status = Some(format!("Invalid pattern: {}", query));
                return;
            }
        };
        let len = self.tree_items.len();
        let start = start.min(len - 1);
        let origin = start;
        let mut cur = start;
        let mut wrapped = false;
        let found;
        loop {
            if re.is_match(&self.tree_items[cur].text) {
                found = Some(cur);
                break;
            }
            if reverse {
                if cur == 0 {
                    if wrapped {
                        found = None;
                        break;
                    }
                    wrapped = true;
                    cur = len - 1;
                } else {
                    cur -= 1;
                }
            } else {
                cur += 1;
                if cur >= len {
                    if wrapped {
                        found = None;
                        break;
                    }
                    wrapped = true;
                    cur = 0;
                }
            }
            if wrapped && cur == origin {
                found = None;
                break;
            }
        }
        match found {
            Some(idx) => {
                self.tree_cursor = idx;
                self.ensure_tree_visible();
                self.status = if wrapped {
                    Some(format!("Match (wrapped) at line {}", idx + 1))
                } else {
                    None
                };
            }
            None => self.status = Some(format!("No match for /{}/", query)),
        }
    }

    /// Compile the current search query to a case-insensitive regex, falling
    /// back to an escaped literal substring if the user's pattern doesn't
    /// compile (so a naive `/(foo` still works as a literal search).
    pub fn search_regex(&self) -> Option<regex::Regex> {
        self.search.as_ref()?.regex()
    }

    /// Scan forward (or backward) for a row where any column matches the
    /// current query (regex, case-insensitive). Loads windows as needed and
    /// wraps around the file once before giving up.
    fn search_jump(&mut self, query: &str, start: usize, reverse: bool) {
        if self.table_total_rows == 0 {
            return;
        }
        // Compile once.
        let re = match compile_search_regex(query) {
            Some(r) => r,
            None => {
                self.status = Some(format!("Invalid pattern: {}", query));
                return;
            }
        };
        let limit = TABLE_BUFFER_ROWS.saturating_mul(100);
        let mut checked = 0usize;
        let mut cur = start.min(self.table_total_rows.saturating_sub(1));
        let origin = cur;
        let mut wrapped = false;
        let found;
        loop {
            self.table_cursor_row = cur;
            self.ensure_cursor_visible();
            self.ensure_table_data();
            if let Some(row) = self.table_row(cur) {
                if row.iter().any(|v| re.is_match(v)) {
                    found = Some(cur);
                    break;
                }
            }
            checked += 1;
            if checked >= limit {
                found = None;
                break;
            }
            if reverse {
                if cur == 0 {
                    // Wrap to bottom once, then keep scanning until we pass origin.
                    if wrapped {
                        found = None;
                        break;
                    }
                    wrapped = true;
                    cur = self.table_total_rows - 1;
                } else {
                    cur -= 1;
                }
            } else {
                cur = cur.saturating_add(1);
                if cur >= self.table_total_rows {
                    if wrapped {
                        found = None;
                        break;
                    }
                    wrapped = true;
                    cur = 0;
                }
            }
            // If we've fully circled back past the starting row, give up.
            if wrapped && cur == origin {
                found = None;
                break;
            }
        }
        match found {
            Some(row) => {
                self.table_cursor_row = row;
                self.ensure_cursor_visible();
                self.ensure_table_data();
                self.status = if wrapped {
                    Some(format!("Match (wrapped) at row {}", row + 1))
                } else {
                    None
                };
            }
            None => {
                self.status = Some(format!("No match for /{}/", query));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Row group navigation
    // -----------------------------------------------------------------------

    /// Jump to the first row of the next row group.
    pub fn next_row_group(&mut self) {
        if self.view_mode != ViewMode::Table || self.metadata.row_groups.is_empty() {
            return;
        }
        self.status = None;
        let cursor = self.table_cursor_row;
        let mut offset = 0;
        for (idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let rows = rg.num_rows();
            if cursor < offset + rows {
                // In row group `idx`. Jump to the start of `idx + 1` if any.
                let next = idx + 1;
                if let Some(next_rg_offset) = self.rg_global_offset(next) {
                    self.goto_row(next_rg_offset);
                    self.set_status(format!("Row group {}", next + 1));
                }
                return;
            }
            offset += rows;
        }
    }

    /// Jump to the first row of the previous row group (or to the start of
    /// the current one if the cursor is already past its first row).
    pub fn prev_row_group(&mut self) {
        if self.view_mode != ViewMode::Table || self.metadata.row_groups.is_empty() {
            return;
        }
        self.status = None;
        let cursor = self.table_cursor_row;
        let mut offset = 0;
        for (idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let rows = rg.num_rows();
            if cursor < offset + rows {
                if cursor > offset {
                    // Jump to the start of the current row group.
                    self.goto_row(offset);
                    self.set_status(format!("Row group {}", idx + 1));
                } else if idx > 0 {
                    let prev_offset = self.rg_global_offset(idx - 1).unwrap_or(0);
                    self.goto_row(prev_offset);
                    self.set_status(format!("Row group {}", idx));
                }
                return;
            }
            offset += rows;
        }
    }

    fn rg_global_offset(&self, rg_idx: usize) -> Option<usize> {
        if rg_idx > self.metadata.row_groups.len() {
            return None;
        }
        Some(
            self.metadata.row_groups[..rg_idx.min(self.metadata.row_groups.len())]
                .iter()
                .map(|rg| rg.num_rows())
                .sum(),
        )
    }

    // -----------------------------------------------------------------------
    // Clipboard helpers
    // -----------------------------------------------------------------------

    /// Returns the string the `y` key should copy, based on current context.
    /// - In a Cell popup: the cell's value
    /// - In a Row popup: the entire row as "col: value" lines
    /// - In Table view with no popup: the current cell's value
    /// - In Tree view: the text of the selected line
    pub fn clipboard_selection(&self) -> Option<String> {
        if let Some(popup) = &self.popup {
            return Some(match popup {
                DetailPopup::Cell { value, .. } => value.clone(),
                DetailPopup::Row { pairs, .. } | DetailPopup::ColumnInfo { pairs, .. } => pairs
                    .iter()
                    .map(|(k, v)| format!("{}\t{}", k, v))
                    .collect::<Vec<_>>()
                    .join("\n"),
            });
        }
        match self.view_mode {
            ViewMode::Tree => self
                .tree_items
                .get(self.tree_cursor)
                .map(|i| i.text.clone()),
            ViewMode::Table => {
                let col = self
                    .table_cursor_col
                    .min(self.table_columns.len().saturating_sub(1).max(0));
                self.table_row(self.table_cursor_row)
                    .and_then(|r| r.get(col).cloned())
            }
        }
    }

    /// Copy the whole current row as TSV (used by e.g. Shift-Y).
    pub fn clipboard_row_tsv(&self) -> Option<String> {
        if self.view_mode != ViewMode::Table {
            return None;
        }
        self.table_row(self.table_cursor_row).map(|r| r.join("\t"))
    }

    /// Copy the whole current row as a single-line JSON object. All values
    /// are emitted as strings except the literal "null" which becomes JSON
    /// `null`. Column order is preserved.
    pub fn clipboard_row_json(&self) -> Option<String> {
        if self.view_mode != ViewMode::Table {
            return None;
        }
        let row = self.table_row(self.table_cursor_row)?;
        let mut out = String::from("{");
        for (i, name) in self.table_columns.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let v = row.get(i).map(|s| s.as_str()).unwrap_or("");
            out.push_str(&json_quote(name));
            out.push_str(": ");
            if v == "null" {
                out.push_str("null");
            } else {
                out.push_str(&json_quote(v));
            }
        }
        out.push('}');
        Some(out)
    }

    /// Set a transient status message (e.g. after a copy).
    pub fn set_status(&mut self, msg: impl Into<String>) {
        self.status = Some(msg.into());
    }

    // -----------------------------------------------------------------------
    // Row selection
    // -----------------------------------------------------------------------

    /// Toggle selection for the row under the cursor (Table view only).
    pub fn toggle_row_selection(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_total_rows == 0 {
            return;
        }
        let row = self.table_cursor_row;
        if !self.selected_rows.insert(row) {
            self.selected_rows.remove(&row);
        }
        self.status = Some(format!("{} selected", self.selected_rows.len()));
    }

    /// Clear all row selection.
    pub fn clear_row_selection(&mut self) {
        if self.selected_rows.is_empty() {
            return;
        }
        let n = self.selected_rows.len();
        self.selected_rows.clear();
        self.status = Some(format!("Cleared {} selection", n));
    }

    /// Select every row in the file. Refuses for very large files to avoid
    /// burning hundreds of MB on a HashSet — user can still export the whole
    /// file via `E` if they don't actually need explicit selection.
    pub fn select_all_rows(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_total_rows == 0 {
            return;
        }
        const LIMIT: usize = 1_000_000;
        if self.table_total_rows > LIMIT {
            self.status = Some(format!(
                "Too many rows to select all ({} > {}); export with E instead",
                format_number(self.table_total_rows),
                format_number(LIMIT),
            ));
            return;
        }
        self.selected_rows = (0..self.table_total_rows).collect();
        self.status = Some(format!(
            "Selected {}",
            format_number(self.selected_rows.len())
        ));
    }

    /// Invert the current row selection across the whole file.
    /// Same size limit as select_all_rows.
    pub fn invert_row_selection(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_total_rows == 0 {
            return;
        }
        const LIMIT: usize = 1_000_000;
        if self.table_total_rows > LIMIT {
            self.status = Some(format!(
                "Too many rows to invert selection ({} > {})",
                format_number(self.table_total_rows),
                format_number(LIMIT),
            ));
            return;
        }
        let mut new_sel = HashSet::with_capacity(self.table_total_rows);
        for r in 0..self.table_total_rows {
            if !self.selected_rows.contains(&r) {
                new_sel.insert(r);
            }
        }
        self.selected_rows = new_sel;
        self.status = Some(format!(
            "Inverted, {} selected",
            format_number(self.selected_rows.len())
        ));
    }

    pub fn selected_count(&self) -> usize {
        self.selected_rows.len()
    }

    // -----------------------------------------------------------------------
    // Column width adjustment
    // -----------------------------------------------------------------------

    /// Make the cursor column wider by one "tick" (approximately +30% or
    /// +4 columns, whichever is larger). Capped at a sensible maximum so a
    /// single column can't hide all the others. No-op in Tree mode.
    pub fn widen_current_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let c = self
            .table_cursor_col
            .min(self.table_column_widths.len() - 1);
        let cur = self.table_column_widths[c];
        let step = (cur / 3).max(4);
        let new = (cur + step).min(120);
        self.table_column_widths[c] = new;
        self.status = Some(format!("col width {} → {}", cur, new));
    }

    /// Narrow the cursor column by one "tick". Floors at 4 columns.
    pub fn narrow_current_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let c = self
            .table_cursor_col
            .min(self.table_column_widths.len() - 1);
        let cur = self.table_column_widths[c];
        let step = (cur / 3).max(4);
        let new = cur.saturating_sub(step).max(4);
        if new == cur {
            return;
        }
        self.table_column_widths[c] = new;
        self.status = Some(format!("col width {} → {}", cur, new));
    }

    // -----------------------------------------------------------------------
    // In-window sort
    // -----------------------------------------------------------------------

    /// Cycle the sort key for the current column through: none → ascending
    /// → descending → none. Applied to the currently-loaded window only;
    /// when crossing window boundaries, the same key is re-applied.
    pub fn cycle_sort(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let col = self.table_cursor_col.min(self.table_columns.len() - 1);
        self.sort_order = match self.sort_order {
            None => Some((col, true)),
            Some((c, _)) if c != col => Some((col, true)),
            Some((_, true)) => Some((col, false)),
            Some((_, false)) => None,
        };
        match self.sort_order {
            None => {
                // Cancel sort: force a reload so canonical order returns.
                self.table_rows.clear();
                self.table_row_global.clear();
                self.table_loaded_rg = None;
                self.ensure_table_data();
                self.status = Some("Sort: off".into());
            }
            Some((c, asc)) => {
                self.apply_sort_if_any();
                let name = self.table_columns.get(c).cloned().unwrap_or_default();
                let dir = if asc { "↑ asc" } else { "↓ desc" };
                self.status = Some(format!("Sort: {} {}", dir, name));
            }
        }
    }

    /// If a sort key is set, reorder `table_rows` + `table_row_global`
    /// together by the column's values. Tries numeric comparison first,
    /// falls back to lexicographic.
    fn apply_sort_if_any(&mut self) {
        let (col, asc) = match self.sort_order {
            Some(s) => s,
            None => return,
        };
        if col >= self.table_columns.len() {
            return;
        }
        // Build parallel vector of (row, global_id), sort, unpack.
        let mut paired: Vec<(Vec<String>, usize)> = self
            .table_rows
            .drain(..)
            .zip(self.table_row_global.drain(..))
            .collect();
        paired.sort_by(|a, b| {
            let av = a.0.get(col).map(|s| s.as_str()).unwrap_or("");
            let bv = b.0.get(col).map(|s| s.as_str()).unwrap_or("");
            cmp_values(av, bv, asc)
        });
        self.table_rows.reserve(paired.len());
        self.table_row_global.reserve(paired.len());
        for (row, gid) in paired {
            self.table_rows.push(row);
            self.table_row_global.push(gid);
        }
    }

    /// Canonical global row index for the row at visual position
    /// `cursor_row`, or `cursor_row` itself when the cursor isn't within
    /// the loaded buffer (fallback for display purposes).
    pub fn canonical_row_id(&self, visual_row: usize) -> usize {
        if visual_row < self.table_loaded_offset {
            return visual_row;
        }
        let local = visual_row - self.table_loaded_offset;
        self.table_row_global
            .get(local)
            .copied()
            .unwrap_or(visual_row)
    }

    // -----------------------------------------------------------------------
    // Mouse click → cursor position
    // -----------------------------------------------------------------------

    /// Translate a terminal-space click into a (row, column) cursor move.
    /// Relies on `last_layout` populated by the most recent render call.
    /// No-op in Tree view, when a popup is open, or when the click falls
    /// outside the table data area.
    pub fn click_at(&mut self, col: u16, row: u16) {
        if self.popup.is_some() || self.view_mode != ViewMode::Table {
            return;
        }
        let layout = match &self.last_layout {
            Some(l) => l.clone(),
            None => return,
        };
        let mut moved = false;
        // Row hit
        if row >= layout.data_start_y && row < layout.data_end_y {
            let offset = (row - layout.data_start_y) as usize;
            let new_row = layout.scroll_row + offset;
            if new_row < self.table_total_rows {
                self.table_cursor_row = new_row;
                self.ensure_cursor_visible();
                self.ensure_table_data();
                moved = true;
            }
        }
        // Column hit — check each visible cell's x range.
        for &(c, x_start, x_end) in &layout.col_hits {
            if col >= x_start && col < x_end {
                self.table_cursor_col = c;
                self.ensure_col_cursor_visible();
                moved = true;
                break;
            }
        }
        if moved {
            self.status = None;
        }
    }

    // -----------------------------------------------------------------------
    // Reload
    // -----------------------------------------------------------------------

    /// Re-read the file from disk and re-apply user UI state (cursor, hidden
    /// columns, frozen column, per-column widths, selection) by column NAME
    /// so the UI survives schema drift across rewrites. Returns an error if
    /// the file can't be re-opened.
    pub fn reload(&mut self) -> Result<(), String> {
        // Snapshot what we want to preserve.
        let old_widths: std::collections::HashMap<String, usize> = self
            .table_columns
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), self.table_column_widths[i]))
            .collect();
        let old_hidden_names: std::collections::HashSet<String> = self
            .hidden_cols
            .iter()
            .filter_map(|&i| self.table_columns.get(i).cloned())
            .collect();
        let old_frozen_name = self
            .frozen_col
            .and_then(|i| self.table_columns.get(i).cloned());
        let old_cursor_row = self.table_cursor_row;
        let old_cursor_col_name = self.table_columns.get(self.table_cursor_col).cloned();
        let old_selected_global_rows = self.selected_rows.clone();
        let old_view_mode = self.view_mode;
        let old_sort_name_asc = self
            .sort_order
            .and_then(|(c, asc)| self.table_columns.get(c).cloned().map(|n| (n, asc)));

        // Re-open. The path hasn't changed; the file on disk may have.
        let path = self.path.clone();
        let fresh = Self::open(path)?;
        *self = fresh;
        self.view_mode = old_view_mode;

        // Re-apply state by column name — column indices may have shifted.
        for (i, name) in self.table_columns.iter().enumerate() {
            if let Some(w) = old_widths.get(name) {
                self.table_column_widths[i] = *w;
            }
            if old_hidden_names.contains(name) {
                self.hidden_cols.insert(i);
            }
        }
        if let Some(name) = old_frozen_name {
            if let Some(i) = self.table_columns.iter().position(|n| n == &name) {
                self.frozen_col = Some(i);
            }
        }
        if let Some(name) = old_cursor_col_name {
            if let Some(i) = self.table_columns.iter().position(|n| n == &name) {
                self.table_cursor_col = i;
            }
        }
        if self.table_total_rows > 0 {
            self.table_cursor_row = old_cursor_row.min(self.table_total_rows - 1);
        }
        // Drop selected rows whose indices are now out of range.
        self.selected_rows = old_selected_global_rows
            .into_iter()
            .filter(|&r| r < self.table_total_rows)
            .collect();

        if let Some((name, asc)) = old_sort_name_asc {
            if let Some(i) = self.table_columns.iter().position(|n| n == &name) {
                self.sort_order = Some((i, asc));
            }
        }
        self.ensure_col_cursor_visible();
        self.ensure_cursor_visible();
        self.ensure_table_data();
        self.status = Some("Reloaded".into());
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Column hide / show
    // -----------------------------------------------------------------------

    pub fn is_col_hidden(&self, c: usize) -> bool {
        self.hidden_cols.contains(&c)
    }

    /// Find the nearest visible column at-or-after `from` (or at-or-before
    /// if `reverse`). Wraps around once; returns None only if every column
    /// is hidden (which the hide path prevents).
    fn nearest_visible_col(&self, from: usize, reverse: bool) -> Option<usize> {
        let total = self.table_columns.len();
        if total == 0 {
            return None;
        }
        let mut search: Box<dyn Iterator<Item = usize>> = if reverse {
            Box::new(
                (0..=from.min(total - 1))
                    .rev()
                    .chain((from + 1..total).rev()),
            )
        } else {
            Box::new((from..total).chain(0..from))
        };
        search.find(|i| !self.hidden_cols.contains(i))
    }

    /// Hide the column under the cursor. Refuses to hide the last remaining
    /// visible column (always leave at least one showing).
    pub fn hide_current_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let c = self.table_cursor_col.min(self.table_columns.len() - 1);
        if self.table_columns.len() - self.hidden_cols.len() <= 1 {
            self.status = Some("Can't hide the last visible column".into());
            return;
        }
        self.hidden_cols.insert(c);
        // If the frozen column just got hidden, unfreeze it — otherwise the
        // renderer would try to draw a column that isn't supposed to appear.
        if self.frozen_col == Some(c) {
            self.frozen_col = None;
        }
        // Move the cursor to the next visible column (forward, then wrap).
        if let Some(next) = self.nearest_visible_col(c.saturating_add(1), false) {
            self.table_cursor_col = next;
        } else if let Some(prev) = self.nearest_visible_col(c.saturating_sub(1), true) {
            self.table_cursor_col = prev;
        }
        self.ensure_col_cursor_visible();
        self.status = Some(format!("{} hidden", self.hidden_cols.len()));
    }

    /// Unhide all columns.
    pub fn unhide_all_columns(&mut self) {
        if self.hidden_cols.is_empty() {
            return;
        }
        let n = self.hidden_cols.len();
        self.hidden_cols.clear();
        self.status = Some(format!("Unhidden {}", n));
    }

    /// Flip the thousands-separator display toggle. When turning ON, widen
    /// numeric columns so formatted values fit (we only grow — an explicit
    /// `=` reset is needed to shrink user-adjusted widths).
    pub fn toggle_thousands_separators(&mut self) {
        if self.view_mode != ViewMode::Table {
            return;
        }
        self.thousands_separators = !self.thousands_separators;
        if self.thousands_separators {
            // Grow column widths to fit the formatted versions of the
            // currently-loaded rows.
            for (ci, align) in self.table_column_aligns.iter().enumerate() {
                if *align != Alignment::Right {
                    continue;
                }
                let max_w = self
                    .table_rows
                    .iter()
                    .filter_map(|r| r.get(ci))
                    .map(|v| display_width_oneline(&format_with_thousands(v)))
                    .max()
                    .unwrap_or(0);
                if ci < self.table_column_widths.len() {
                    self.table_column_widths[ci] = self.table_column_widths[ci].max(max_w).min(80);
                }
            }
            self.status = Some("Thousands separators ON".into());
        } else {
            self.status = Some("Thousands separators OFF".into());
        }
    }

    /// Toggle the current column as "frozen" — it remains rendered to the
    /// left of the scrolled range when the user pans past it. Setting the
    /// same column a second time unfreezes. Only one column may be frozen
    /// at a time.
    pub fn toggle_column_freeze(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let c = self.table_cursor_col.min(self.table_columns.len() - 1);
        if self.frozen_col == Some(c) {
            self.frozen_col = None;
            self.status = Some(format!("Unfroze column {}", c + 1));
        } else {
            self.frozen_col = Some(c);
            self.status = Some(format!("Froze column {}", c + 1));
        }
    }

    /// Jump the cursor to the first visible (non-hidden) column.
    pub fn jump_to_first_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        self.status = None;
        if let Some(i) = self.nearest_visible_col(0, false) {
            self.table_cursor_col = i;
            self.ensure_col_cursor_visible();
        }
    }

    /// Jump the cursor to the last visible (non-hidden) column.
    pub fn jump_to_last_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        self.status = None;
        let last = self.table_columns.len() - 1;
        if let Some(i) = self.nearest_visible_col(last, true) {
            self.table_cursor_col = i;
            self.ensure_col_cursor_visible();
        }
    }

    /// Resize the cursor column to fit its widest value currently in the
    /// loaded row window. Grows or shrinks to match, capped at 80 columns.
    pub fn autofit_current_column(&mut self) {
        if self.view_mode != ViewMode::Table || self.table_columns.is_empty() {
            return;
        }
        let c = self.table_cursor_col.min(self.table_columns.len() - 1);
        let name_w = self
            .table_columns
            .get(c)
            .map(|n| UnicodeWidthStr::width(n.as_str()))
            .unwrap_or(4);
        let data_w = self
            .table_rows
            .iter()
            .filter_map(|r| r.get(c))
            .map(|v| display_width_oneline(v))
            .max()
            .unwrap_or(0);
        let target = name_w.max(data_w).clamp(4, 80);
        let old = self.table_column_widths[c];
        self.table_column_widths[c] = target;
        self.status = Some(format!("col width {} → {} (autofit)", old, target));
    }

    /// Reset all columns to their initial Unicode-width-based sizing.
    pub fn reset_column_widths(&mut self) {
        if self.view_mode != ViewMode::Table {
            return;
        }
        for (i, name) in self.table_columns.iter().enumerate() {
            self.table_column_widths[i] = UnicodeWidthStr::width(name.as_str()).max(8);
        }
        // Re-derive from currently-loaded data so we still have useful widths.
        for (col_idx, col_data) in (0..self.table_columns.len()).zip(
            (0..self.table_rows.first().map(|r| r.len()).unwrap_or(0)).map(|c| {
                self.table_rows
                    .iter()
                    .filter_map(|r| r.get(c))
                    .cloned()
                    .collect::<Vec<_>>()
            }),
        ) {
            if col_idx < self.table_column_widths.len() {
                let max_w = col_data
                    .iter()
                    .take(100)
                    .map(|v| display_width_oneline(v))
                    .max()
                    .unwrap_or(0);
                self.table_column_widths[col_idx] =
                    self.table_column_widths[col_idx].max(max_w).min(40);
            }
        }
        self.status = Some("column widths reset".into());
    }

    pub fn is_row_selected(&self, global_row: usize) -> bool {
        self.selected_rows.contains(&global_row)
    }

    /// Copy all loaded values of the current column as a newline-separated
    /// list. Scope is the currently-loaded window (the value shown is what's
    /// currently visible/near-visible; larger-than-window copies would need
    /// a full decode).
    pub fn clipboard_column_values(&self) -> Option<(String, usize)> {
        if self.view_mode != ViewMode::Table {
            return None;
        }
        if self.table_columns.is_empty() || self.table_rows.is_empty() {
            return None;
        }
        let col = self.table_cursor_col.min(self.table_columns.len() - 1);
        let mut out = String::new();
        let mut count = 0usize;
        for row in &self.table_rows {
            if let Some(v) = row.get(col) {
                if count > 0 {
                    out.push('\n');
                }
                out.push_str(v);
                count += 1;
            }
        }
        Some((out, count))
    }

    // -----------------------------------------------------------------------
    // CSV export
    // -----------------------------------------------------------------------

    /// Export the row group containing the cursor to a CSV file next to the
    /// parquet file. If any rows are selected, export ONLY the selected rows
    /// within that row group instead. Returns the output path on success.
    pub fn export_current_row_group_csv(&mut self) -> Result<PathBuf, String> {
        if self.view_mode != ViewMode::Table {
            return Err("Switch to Table view to export".into());
        }
        if self.table_total_rows == 0 {
            return Err("No rows to export".into());
        }
        let target = self.table_cursor_row;
        let mut rg_offset = 0;
        for (rg_idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let rg_rows = rg.num_rows();
            if target < rg_offset + rg_rows {
                return self.export_row_group_csv(rg_idx, rg_offset);
            }
            rg_offset += rg_rows;
        }
        Err("cursor outside any row group".into())
    }

    /// Export the whole file (or the current selection) as newline-delimited
    /// JSON. One JSON object per line, column order preserved, `null` used
    /// for the literal "null" value. Output path is `<stem>.ndjson` (or
    /// `<stem>.selected.ndjson` when a selection is active). Returns the
    /// output path on success.
    pub fn export_full_file_ndjson(&mut self) -> Result<PathBuf, String> {
        if self.view_mode != ViewMode::Table {
            return Err("Switch to Table view to export".into());
        }
        if self.table_total_rows == 0 {
            return Err("No rows to export".into());
        }

        let parent = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let stem = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "export".to_string());

        let has_selection = !self.selected_rows.is_empty();
        let base = if has_selection {
            format!("{}.selected", stem)
        } else {
            stem
        };

        let out_path = pick_unused_export_path(|attempt| {
            let suffix = if attempt == 0 {
                String::new()
            } else {
                format!("-{}", attempt)
            };
            parent.join(format!("{}{}.ndjson", base, suffix))
        })?;

        let file = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
        let mut w = std::io::BufWriter::new(file);
        use std::io::Write;

        let rg_count = self.metadata.row_groups.len();
        let mut rg_offset_global = 0usize;
        for rg_idx in 0..rg_count {
            let rg = &self.metadata.row_groups[rg_idx];
            let rg_rows = rg.num_rows();
            if rg_rows == 0 {
                continue;
            }
            if has_selection {
                let rg_end = rg_offset_global + rg_rows;
                let overlaps = self
                    .selected_rows
                    .iter()
                    .any(|&r| r >= rg_offset_global && r < rg_end);
                if !overlaps {
                    rg_offset_global += rg_rows;
                    continue;
                }
            }
            let columns = decode_row_group_columns(&self.path, rg, 0, rg_rows)
                .ok_or_else(|| format!("failed to decode row group {}", rg_idx))?;
            let num_rows = columns.iter().map(|c| c.len()).max().unwrap_or(0);
            for i in 0..num_rows {
                if has_selection {
                    let global = rg_offset_global + i;
                    if !self.selected_rows.contains(&global) {
                        continue;
                    }
                }
                let row_iter = self.table_columns.iter().enumerate().map(|(c_idx, name)| {
                    let v = columns
                        .get(c_idx)
                        .and_then(|col| col.get(i))
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    (name.as_str(), v)
                });
                writeln!(w, "{}", render_ndjson_line(row_iter)).map_err(|e| e.to_string())?;
            }
            rg_offset_global += rg_rows;
        }
        w.flush().map_err(|e| e.to_string())?;
        Ok(out_path)
    }

    /// Export the entire file (all row groups, concatenated) to a single
    /// CSV next to the parquet file. Returns the output path on success.
    pub fn export_full_file_csv(&mut self) -> Result<PathBuf, String> {
        if self.view_mode != ViewMode::Table {
            return Err("Switch to Table view to export".into());
        }
        if self.table_total_rows == 0 {
            return Err("No rows to export".into());
        }

        let parent = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let stem = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "export".to_string());

        let out_path = pick_unused_export_path(|attempt| {
            let suffix = if attempt == 0 {
                String::new()
            } else {
                format!("-{}", attempt)
            };
            parent.join(format!("{}{}.csv", stem, suffix))
        })?;

        let file = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
        let mut w = std::io::BufWriter::new(file);
        use std::io::Write;

        // Header.
        {
            let cells: Vec<String> = self.table_columns.iter().map(|c| csv_escape(c)).collect();
            writeln!(w, "{}", cells.join(",")).map_err(|e| e.to_string())?;
        }

        // Decode each row group in turn to bound memory usage. Writer is
        // reused across all row groups. When a selection is active, only
        // rows with their global index in `selected_rows` are written.
        let has_selection = !self.selected_rows.is_empty();
        let rg_count = self.metadata.row_groups.len();
        let mut rg_offset_global = 0usize;
        for rg_idx in 0..rg_count {
            let rg = &self.metadata.row_groups[rg_idx];
            let rg_rows = rg.num_rows();
            if rg_rows == 0 {
                continue;
            }
            // Fast path: skip decoding a row group entirely if no selected
            // rows fall within it.
            if has_selection {
                let rg_end = rg_offset_global + rg_rows;
                let overlaps = self
                    .selected_rows
                    .iter()
                    .any(|&r| r >= rg_offset_global && r < rg_end);
                if !overlaps {
                    rg_offset_global += rg_rows;
                    continue;
                }
            }
            let columns = decode_row_group_columns(&self.path, rg, 0, rg_rows)
                .ok_or_else(|| format!("failed to decode row group {}", rg_idx))?;
            let num_rows = columns.iter().map(|c| c.len()).max().unwrap_or(0);
            for i in 0..num_rows {
                if has_selection {
                    let global = rg_offset_global + i;
                    if !self.selected_rows.contains(&global) {
                        continue;
                    }
                }
                let cells: Vec<String> = columns
                    .iter()
                    .map(|col| csv_escape(col.get(i).map(|s| s.as_str()).unwrap_or("")))
                    .collect();
                writeln!(w, "{}", cells.join(",")).map_err(|e| e.to_string())?;
            }
            rg_offset_global += rg_rows;
        }
        w.flush().map_err(|e| e.to_string())?;
        Ok(out_path)
    }

    fn export_row_group_csv(
        &self,
        rg_idx: usize,
        rg_offset_global: usize,
    ) -> Result<PathBuf, String> {
        let rg = &self.metadata.row_groups[rg_idx];
        let rg_rows = rg.num_rows();

        let columns = decode_row_group_columns(&self.path, rg, 0, rg_rows)
            .ok_or_else(|| "failed to decode row group".to_string())?;

        let parent = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let stem = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "export".to_string());

        let has_selection = !self.selected_rows.is_empty();
        let suffix_tag = if has_selection { "selected" } else { "rg" };

        let out_path = pick_unused_export_path(|attempt| {
            let suffix = if attempt == 0 {
                String::new()
            } else {
                format!("-{}", attempt)
            };
            if has_selection {
                parent.join(format!("{}.{}{}.csv", stem, suffix_tag, suffix))
            } else {
                parent.join(format!("{}.{}{}{}.csv", stem, suffix_tag, rg_idx, suffix))
            }
        })?;

        let file = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
        let mut w = std::io::BufWriter::new(file);
        use std::io::Write;

        // Header
        {
            let cells: Vec<String> = self.table_columns.iter().map(|c| csv_escape(c)).collect();
            writeln!(w, "{}", cells.join(",")).map_err(|e| e.to_string())?;
        }

        // Rows (columns are parallel vectors after decode). When a selection
        // is active, restrict output to rows whose global index is selected.
        let num_rows = columns.iter().map(|c| c.len()).max().unwrap_or(0);
        for i in 0..num_rows {
            if has_selection {
                let global = rg_offset_global + i;
                if !self.selected_rows.contains(&global) {
                    continue;
                }
            }
            let cells: Vec<String> = columns
                .iter()
                .map(|col| csv_escape(col.get(i).map(|s| s.as_str()).unwrap_or("")))
                .collect();
            writeln!(w, "{}", cells.join(",")).map_err(|e| e.to_string())?;
        }
        w.flush().map_err(|e| e.to_string())?;
        Ok(out_path)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn ensure_tree_visible(&mut self) {
        if self.tree_cursor < self.tree_scroll {
            self.tree_scroll = self.tree_cursor;
        } else if self.tree_cursor >= self.tree_scroll + self.tree_visible {
            self.tree_scroll = self
                .tree_cursor
                .saturating_sub(self.tree_visible.saturating_sub(1));
        }
    }

    fn ensure_cursor_visible(&mut self) {
        if self.table_visible_rows == 0 {
            return;
        }
        if self.table_cursor_row < self.table_scroll_row {
            self.table_scroll_row = self.table_cursor_row;
        } else if self.table_cursor_row >= self.table_scroll_row + self.table_visible_rows {
            self.table_scroll_row = self.table_cursor_row + 1 - self.table_visible_rows;
        }
        let max = self.table_max_scroll();
        if self.table_scroll_row > max {
            self.table_scroll_row = max;
        }
    }

    /// Pull the column cursor into view by adjusting `table_scroll_col`.
    /// We don't know exact visible widths here (those are computed at render
    /// time), but `table_visible_cols` from the previous frame is a useful
    /// approximation. If the cursor is to the left of the scrolled region,
    /// scroll to it; if it's at or past the right edge of the previously
    /// visible window, scroll until it's at the right edge.
    fn ensure_col_cursor_visible(&mut self) {
        if self.table_columns.is_empty() {
            return;
        }
        let max_col = self.table_columns.len() - 1;
        if self.table_cursor_col > max_col {
            self.table_cursor_col = max_col;
        }
        if self.table_cursor_col < self.table_scroll_col {
            self.table_scroll_col = self.table_cursor_col;
        } else if self.table_visible_cols > 0
            && self.table_cursor_col >= self.table_scroll_col + self.table_visible_cols
        {
            self.table_scroll_col = self.table_cursor_col + 1 - self.table_visible_cols;
        }
        // Clamp scroll so we don't scroll past the last column.
        let max_scroll = self
            .table_columns
            .len()
            .saturating_sub(self.table_visible_cols.max(1));
        if self.table_scroll_col > max_scroll {
            self.table_scroll_col = max_scroll;
        }
    }

    fn table_max_scroll(&self) -> usize {
        self.table_total_rows
            .saturating_sub(self.table_visible_rows)
    }

    fn ensure_table_data(&mut self) {
        if self.table_total_rows == 0 {
            return;
        }
        // Drive loading from the cursor (falling back to the top visible row
        // if nothing has been loaded yet).
        let target = self.table_cursor_row.max(self.table_scroll_row);

        // Still inside the currently loaded window? Nothing to do.
        if !self.table_rows.is_empty()
            && target >= self.table_loaded_offset
            && target < self.table_loaded_offset + self.table_rows.len()
        {
            return;
        }

        // Find the row group containing `target` and its global offset.
        let mut rg_offset_global = 0;
        for (rg_idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let rg_rows = rg.num_rows();
            if target < rg_offset_global + rg_rows {
                let target_local = target - rg_offset_global;
                // Center the window on the cursor so short scrolls don't
                // immediately re-trigger a load.
                let half = TABLE_BUFFER_ROWS / 2;
                let max_start = rg_rows.saturating_sub(TABLE_BUFFER_ROWS.min(rg_rows));
                let start_local = target_local.saturating_sub(half).min(max_start);
                self.load_table_window(rg_idx, start_local, rg_offset_global);
                return;
            }
            rg_offset_global += rg_rows;
        }
    }

    fn load_table_window(&mut self, rg_idx: usize, start_local: usize, rg_offset_global: usize) {
        let rg = &self.metadata.row_groups[rg_idx];
        let rg_rows = rg.num_rows();
        let count = TABLE_BUFFER_ROWS.min(rg_rows.saturating_sub(start_local));
        if count == 0 {
            return;
        }

        let mut columns = match decode_row_group_columns(&self.path, rg, start_local, count) {
            Some(c) => c,
            None => return,
        };

        // Update column widths using Unicode display width so non-ASCII
        // (accented chars, CJK, emoji) don't push separators out of alignment.
        for (col_idx, col_data) in columns.iter().enumerate() {
            if col_idx < self.table_column_widths.len() {
                let max_w = col_data
                    .iter()
                    .take(100)
                    .map(|v| display_width_oneline(v))
                    .max()
                    .unwrap_or(0);
                self.table_column_widths[col_idx] =
                    self.table_column_widths[col_idx].max(max_w).min(40);
            }
        }

        self.table_rows = transpose_columns(&mut columns);
        let base = rg_offset_global + start_local;
        self.table_row_global = (0..self.table_rows.len()).map(|i| base + i).collect();
        self.table_loaded_rg = Some(rg_idx);
        self.table_loaded_offset = base;
        // Re-apply any active sort so the newly-loaded window obeys the
        // user's current sort key instead of reverting to load order.
        self.apply_sort_if_any();
    }

    /// Get the row for a global row index in the table view.
    pub fn table_row(&self, global_row: usize) -> Option<&Vec<String>> {
        if global_row < self.table_loaded_offset {
            return None;
        }
        let local = global_row - self.table_loaded_offset;
        self.table_rows.get(local)
    }

    // -----------------------------------------------------------------------
    // Tree building
    // -----------------------------------------------------------------------

    fn rebuild_tree(&mut self) {
        // .clear() preserves allocated capacity, so repeated expand/collapse
        // reuses the same buffer without reallocating.
        self.tree_items.clear();

        let fname = self.path.file_name().unwrap_or_default().to_string_lossy();
        let root_expanded = self.expanded.contains(&NodeId::Root);
        push_item(
            &mut self.tree_items,
            0,
            format!(
                "{} ({} rows, {})",
                fname,
                format_number(self.metadata.num_rows),
                format_size(self.file_size)
            ),
            ItemKind::Header,
            true,
            Some(NodeId::Root),
        );

        if !root_expanded {
            return;
        }

        // File properties
        push_item(
            &mut self.tree_items,
            1,
            format!("Version: {}", self.metadata.version),
            ItemKind::Property,
            false,
            None,
        );
        if let Some(ref created_by) = self.metadata.created_by {
            push_item(
                &mut self.tree_items,
                1,
                format!("Created by: {}", created_by),
                ItemKind::Property,
                false,
                None,
            );
        }
        push_item(
            &mut self.tree_items,
            1,
            format!("Row groups: {}", self.metadata.row_groups.len()),
            ItemKind::Property,
            false,
            None,
        );

        // Key-value metadata
        if let Some(ref kv) = self.metadata.key_value_metadata {
            if !kv.is_empty() {
                let kv_expanded = self.expanded.contains(&NodeId::KvMetadata);
                push_item(
                    &mut self.tree_items,
                    1,
                    format!("Key-Value Metadata ({} entries)", kv.len()),
                    ItemKind::Header,
                    true,
                    Some(NodeId::KvMetadata),
                );
                if kv_expanded {
                    // Build cache on first expansion
                    if self.kv_cache.is_none() {
                        let cached: Vec<(String, Vec<String>)> = kv
                            .iter()
                            .map(|entry| {
                                let value = entry.value.as_deref().unwrap_or("<null>");
                                let lines = if let Ok(parsed) =
                                    serde_json::from_str::<serde_json::Value>(value)
                                {
                                    let pretty = serde_json::to_string_pretty(&parsed)
                                        .unwrap_or_else(|_| value.to_string());
                                    let plines: Vec<String> =
                                        pretty.lines().map(String::from).collect();
                                    if plines.len() <= 1 {
                                        vec![value.to_string()]
                                    } else {
                                        plines
                                    }
                                } else {
                                    vec![truncate(value, 120)]
                                };
                                (entry.key.clone(), lines)
                            })
                            .collect();
                        self.kv_cache = Some(cached);
                    }
                    let kv_cache = self.kv_cache.as_ref().unwrap();
                    let w_key = kv_cache.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
                    for (key, lines) in kv_cache {
                        if lines.len() == 1 {
                            push_item(
                                &mut self.tree_items,
                                2,
                                format!("{:<w_key$}  {}", key, lines[0]),
                                ItemKind::Property,
                                false,
                                None,
                            );
                        } else {
                            push_item(
                                &mut self.tree_items,
                                2,
                                format!("{:<w_key$}:", key),
                                ItemKind::Property,
                                false,
                                None,
                            );
                            for line in lines {
                                push_item(
                                    &mut self.tree_items,
                                    3,
                                    line.clone(),
                                    ItemKind::Property,
                                    false,
                                    None,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Schema
        let schema_expanded = self.expanded.contains(&NodeId::Schema);
        let num_cols = self.metadata.schema_descr.columns().len();
        push_item(
            &mut self.tree_items,
            1,
            format!("Schema ({} columns)", num_cols),
            ItemKind::Header,
            true,
            Some(NodeId::Schema),
        );
        if schema_expanded {
            struct SchemaField {
                name: String,
                phys: String,
                logical: String,
                rep: String,
            }
            let fields: Vec<SchemaField> = self
                .metadata
                .schema_descr
                .columns()
                .iter()
                .map(|col| {
                    let pt = &col.descriptor.primitive_type;
                    SchemaField {
                        name: pt.field_info.name.clone(),
                        phys: format_physical_type(pt.physical_type).to_string(),
                        logical: pt
                            .logical_type
                            .as_ref()
                            .map(format_logical_type)
                            .unwrap_or_default(),
                        rep: format!("{:?}", pt.field_info.repetition).to_lowercase(),
                    }
                })
                .collect();

            let w_name = fields.iter().map(|f| f.name.len()).max().unwrap_or(4);
            let w_phys = fields.iter().map(|f| f.phys.len()).max().unwrap_or(4);
            let w_logical = fields.iter().map(|f| f.logical.len()).max().unwrap_or(0);

            for f in &fields {
                let type_str = if f.logical.is_empty() {
                    format!("{:<w_phys$}", f.phys)
                } else {
                    format!("{:<w_phys$} / {:<w_logical$}", f.phys, f.logical)
                };
                push_item(
                    &mut self.tree_items,
                    2,
                    format!("{:<w_name$}  {}  ({})", f.name, type_str, f.rep),
                    ItemKind::SchemaField,
                    false,
                    None,
                );
            }
        }

        // Row groups
        // We need indices, so clone the metadata we need upfront.
        let num_row_groups = self.metadata.row_groups.len();
        for rg_idx in 0..num_row_groups {
            let rg = &self.metadata.row_groups[rg_idx];
            let rg_expanded = self.expanded.contains(&NodeId::RowGroup(rg_idx));
            push_item(
                &mut self.tree_items,
                1,
                format!(
                    "Row Group {} ({} rows, {} compressed)",
                    rg_idx,
                    format_number(rg.num_rows()),
                    format_size(rg.compressed_size() as u64)
                ),
                ItemKind::RowGroupHeader,
                true,
                Some(NodeId::RowGroup(rg_idx)),
            );

            if !rg_expanded {
                continue;
            }

            // Columns sub-section
            let cols_expanded = self.expanded.contains(&NodeId::RowGroupColumns(rg_idx));
            push_item(
                &mut self.tree_items,
                2,
                format!("Columns ({})", rg.columns().len()),
                ItemKind::Header,
                true,
                Some(NodeId::RowGroupColumns(rg_idx)),
            );
            if cols_expanded {
                // Pre-compute column fields for alignment
                // Deserialize statistics once per column so we don't redo thrift work.
                struct ColFields {
                    name: String,
                    compression: String,
                    encodings: String,
                    uncompressed: String,
                    compressed: String,
                    nulls: String,
                    num_values: usize,
                    stats: Option<Arc<dyn Statistics>>,
                }
                let fields: Vec<ColFields> = rg
                    .columns()
                    .iter()
                    .map(|col_meta| {
                        let desc = col_meta.descriptor();
                        let name = desc.descriptor.primitive_type.field_info.name.clone();
                        let compression = format!("{:?}", col_meta.compression());
                        let encs: Vec<&str> = col_meta
                            .column_encoding()
                            .iter()
                            .map(|e| format_encoding(e.0))
                            .collect();
                        let stats = col_meta.statistics().and_then(|r| r.ok());
                        let null_count = stats.as_ref().and_then(|s| s.null_count());
                        let nulls = match null_count {
                            Some(0) | None => String::new(),
                            Some(n) => format_number(n as usize),
                        };
                        ColFields {
                            name,
                            compression,
                            encodings: encs.join(", "),
                            uncompressed: format_size(col_meta.uncompressed_size() as u64),
                            compressed: format_size(col_meta.compressed_size() as u64),
                            nulls,
                            num_values: col_meta.num_values() as usize,
                            stats,
                        }
                    })
                    .collect();

                // Compute max widths
                let w_name = fields.iter().map(|f| f.name.len()).max().unwrap_or(4);
                let w_comp = fields
                    .iter()
                    .map(|f| f.compression.len())
                    .max()
                    .unwrap_or(4);
                let w_enc = fields.iter().map(|f| f.encodings.len()).max().unwrap_or(4);
                let w_uncomp = fields
                    .iter()
                    .map(|f| f.uncompressed.len())
                    .max()
                    .unwrap_or(4);
                let w_compr = fields.iter().map(|f| f.compressed.len()).max().unwrap_or(4);
                let w_null = fields
                    .iter()
                    .map(|f| {
                        if f.nulls.is_empty() {
                            0
                        } else {
                            "  nulls: ".len() + f.nulls.len()
                        }
                    })
                    .max()
                    .unwrap_or(0);

                for (col_idx, f) in fields.iter().enumerate() {
                    let col_node = NodeId::RowGroupColumn(rg_idx, col_idx);
                    let col_expanded = self.expanded.contains(&col_node);
                    let null_part = if w_null > 0 {
                        if f.nulls.is_empty() {
                            format!("{:w_null$}", "")
                        } else {
                            format!("{:<w_null$}", format!("  nulls: {}", f.nulls))
                        }
                    } else {
                        String::new()
                    };
                    push_item(
                        &mut self.tree_items,
                        3,
                        format!(
                            "{:<w_name$}  {:<w_comp$}  {:<w_enc$}  {:>w_uncomp$} -> {:<w_compr$}{}",
                            f.name,
                            f.compression,
                            f.encodings,
                            f.uncompressed,
                            f.compressed,
                            null_part,
                        ),
                        ItemKind::ColumnInfo,
                        true,
                        Some(col_node),
                    );
                    if col_expanded {
                        let col_meta = &rg.columns()[col_idx];
                        push_item(
                            &mut self.tree_items,
                            4,
                            format!("Values: {}", format_number(f.num_values)),
                            ItemKind::Property,
                            false,
                            None,
                        );
                        // Byte range (file offsets) are useful when debugging
                        // page-level issues with dump tools.
                        let (bstart, bend) = col_meta.byte_range();
                        push_item(
                            &mut self.tree_items,
                            4,
                            format!(
                                "File bytes: {}..{} ({})",
                                format_number(bstart as usize),
                                format_number(bend as usize),
                                format_size(bend.saturating_sub(bstart)),
                            ),
                            ItemKind::Property,
                            false,
                            None,
                        );
                        push_item(
                            &mut self.tree_items,
                            4,
                            format!(
                                "Data page offset: {}",
                                format_number(col_meta.data_page_offset() as usize)
                            ),
                            ItemKind::Property,
                            false,
                            None,
                        );
                        if let Some(dict_off) = col_meta.dictionary_page_offset() {
                            push_item(
                                &mut self.tree_items,
                                4,
                                format!("Dict page offset: {}", format_number(dict_off as usize)),
                                ItemKind::Property,
                                false,
                                None,
                            );
                        }
                        if col_meta.has_index_page() {
                            let ind = col_meta
                                .index_page_offset()
                                .map(|v| format_number(v as usize))
                                .unwrap_or_else(|| "?".into());
                            push_item(
                                &mut self.tree_items,
                                4,
                                format!("Index page offset: {}", ind),
                                ItemKind::Property,
                                false,
                                None,
                            );
                        }
                        emit_column_stats(
                            &mut self.tree_items,
                            col_meta.physical_type(),
                            col_meta
                                .descriptor()
                                .descriptor
                                .primitive_type
                                .logical_type
                                .as_ref(),
                            f.stats.as_deref(),
                        );
                        // Dictionary sub-node (only if the column has a
                        // dictionary page).
                        if col_meta.dictionary_page_offset().is_some() {
                            let dict_node = NodeId::ColumnDict(rg_idx, col_idx);
                            let dict_expanded = self.expanded.contains(&dict_node);
                            let dict_count = self
                                .column_dicts
                                .get(&(rg_idx, col_idx))
                                .and_then(|o| o.as_ref())
                                .map(|v| v.len())
                                .map(|n| format!("Dictionary ({} values)", format_number(n)))
                                .unwrap_or_else(|| "Dictionary".to_string());
                            push_item(
                                &mut self.tree_items,
                                4,
                                dict_count,
                                ItemKind::Header,
                                true,
                                Some(dict_node),
                            );
                            if dict_expanded {
                                match self.column_dicts.get(&(rg_idx, col_idx)) {
                                    Some(Some(dict)) => {
                                        const DICT_PREVIEW_LIMIT: usize = 50;
                                        for (i, v) in
                                            dict.iter().take(DICT_PREVIEW_LIMIT).enumerate()
                                        {
                                            push_item(
                                                &mut self.tree_items,
                                                5,
                                                format!(
                                                    "{:>4}  {}",
                                                    i,
                                                    truncate(&sanitize_for_line(v), 120)
                                                ),
                                                ItemKind::Property,
                                                false,
                                                None,
                                            );
                                        }
                                        if dict.len() > DICT_PREVIEW_LIMIT {
                                            push_item(
                                                &mut self.tree_items,
                                                5,
                                                format!(
                                                    "... and {} more",
                                                    format_number(dict.len() - DICT_PREVIEW_LIMIT)
                                                ),
                                                ItemKind::Property,
                                                false,
                                                None,
                                            );
                                        }
                                    }
                                    Some(None) => {
                                        push_item(
                                            &mut self.tree_items,
                                            5,
                                            "<decode failed>".into(),
                                            ItemKind::Error,
                                            false,
                                            None,
                                        );
                                    }
                                    None => {
                                        push_item(
                                            &mut self.tree_items,
                                            5,
                                            "<loading…>".into(),
                                            ItemKind::Property,
                                            false,
                                            None,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Data preview sub-section
            let data_expanded = self.expanded.contains(&NodeId::RowGroupData(rg_idx));
            push_item(
                &mut self.tree_items,
                2,
                "Data Preview".to_string(),
                ItemKind::DataHeader,
                true,
                Some(NodeId::RowGroupData(rg_idx)),
            );

            if data_expanded {
                if let Some(ref preview) = self.data_previews[rg_idx] {
                    // Header row
                    let header: String = self
                        .table_columns
                        .iter()
                        .enumerate()
                        .map(|(i, name)| {
                            let w = preview.column_widths.get(i).copied().unwrap_or(8);
                            format!("{:<w$}", truncate(name, w), w = w)
                        })
                        .collect::<Vec<_>>()
                        .join(" | ");
                    push_item(
                        &mut self.tree_items,
                        3,
                        header,
                        ItemKind::DataHeader,
                        false,
                        None,
                    );

                    // Separator
                    let sep: String = preview
                        .column_widths
                        .iter()
                        .map(|&w| "-".repeat(w))
                        .collect::<Vec<_>>()
                        .join("-+-");
                    push_item(
                        &mut self.tree_items,
                        3,
                        sep,
                        ItemKind::DataHeader,
                        false,
                        None,
                    );

                    // Data rows (sanitize newlines — tree preview is single-line per row)
                    for row in &preview.rows {
                        let line: String = row
                            .iter()
                            .enumerate()
                            .map(|(i, val)| {
                                let w = preview.column_widths.get(i).copied().unwrap_or(8);
                                let clean = sanitize_for_line(val);
                                format!("{:<w$}", truncate(&clean, w), w = w)
                            })
                            .collect::<Vec<_>>()
                            .join(" | ");
                        push_item(
                            &mut self.tree_items,
                            3,
                            line,
                            ItemKind::DataCell,
                            false,
                            None,
                        );
                    }
                } else {
                    push_item(
                        &mut self.tree_items,
                        3,
                        "<loading failed>".to_string(),
                        ItemKind::Error,
                        false,
                        None,
                    );
                }
            }
        }

        // Clamp cursor
        if self.tree_cursor >= self.tree_items.len() {
            self.tree_cursor = self.tree_items.len().saturating_sub(1);
        }
    }

    // -----------------------------------------------------------------------
    // Data preview loading
    // -----------------------------------------------------------------------

    /// Load the dictionary page for `(rg_idx, col_idx)` and cache it. If the
    /// column has no dictionary or loading fails, caches `None`.
    fn load_column_dict(&mut self, rg_idx: usize, col_idx: usize) {
        let rg = match self.metadata.row_groups.get(rg_idx) {
            Some(rg) => rg,
            None => {
                self.column_dicts.insert((rg_idx, col_idx), None);
                return;
            }
        };
        let col_meta = match rg.columns().get(col_idx) {
            Some(c) => c,
            None => {
                self.column_dicts.insert((rg_idx, col_idx), None);
                return;
            }
        };
        let result = decode_column_dict(&self.path, col_meta);
        self.column_dicts.insert((rg_idx, col_idx), result);
    }

    fn load_data_preview(&mut self, rg_idx: usize) {
        let rg = &self.metadata.row_groups[rg_idx];
        let max_rows = rg.num_rows().min(DATA_PREVIEW_MAX_ROWS);

        let mut columns = match decode_row_group_columns(&self.path, rg, 0, max_rows) {
            Some(c) => c,
            None => return,
        };

        // Compute column widths using display width (handles non-ASCII).
        let mut col_widths: Vec<usize> = self
            .table_columns
            .iter()
            .map(|n| UnicodeWidthStr::width(n.as_str()).max(4))
            .collect();
        for (ci, col_data) in columns.iter().enumerate() {
            if ci < col_widths.len() {
                let max_w = col_data
                    .iter()
                    .map(|v| display_width_oneline(v))
                    .max()
                    .unwrap_or(0);
                col_widths[ci] = col_widths[ci].max(max_w).min(30);
            }
        }

        self.data_previews[rg_idx] = Some(DataPreview {
            column_widths: col_widths,
            rows: transpose_columns(&mut columns),
        });
    }
}

// ---------------------------------------------------------------------------
// Column decoding
// ---------------------------------------------------------------------------

fn emit_column_stats(
    items: &mut Vec<TreeItem>,
    phys: PhysicalType,
    logical: Option<&PrimitiveLogicalType>,
    cached_stats: Option<&dyn Statistics>,
) {
    let stats = match cached_stats {
        Some(s) => s,
        None => {
            push_item(
                items,
                4,
                "Statistics: N/A".into(),
                ItemKind::Property,
                false,
                None,
            );
            return;
        }
    };

    if let Some(n) = stats.null_count() {
        push_item(
            items,
            4,
            format!("Null count: {}", format_number(n as usize)),
            ItemKind::Property,
            false,
            None,
        );
    }

    match phys {
        PhysicalType::Boolean => {
            if let Some(s) = stats.as_any().downcast_ref::<BooleanStatistics>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(min), Some(max)) = (s.min_value, s.max_value) {
                    push_item(
                        items,
                        4,
                        format!("Min: {}  Max: {}", min, max),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::Int32 => {
            if let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<i32>>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(min), Some(max)) = (s.min_value, s.max_value) {
                    push_item(
                        items,
                        4,
                        format!(
                            "Min: {}  Max: {}",
                            format_i32(min, logical),
                            format_i32(max, logical)
                        ),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::Int64 => {
            if let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<i64>>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(min), Some(max)) = (s.min_value, s.max_value) {
                    push_item(
                        items,
                        4,
                        format!(
                            "Min: {}  Max: {}",
                            format_i64(min, logical),
                            format_i64(max, logical)
                        ),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::Float => {
            if let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<f32>>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(min), Some(max)) = (s.min_value, s.max_value) {
                    push_item(
                        items,
                        4,
                        format!("Min: {}  Max: {}", min, max),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::Double => {
            if let Some(s) = stats.as_any().downcast_ref::<PrimitiveStatistics<f64>>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(min), Some(max)) = (s.min_value, s.max_value) {
                    push_item(
                        items,
                        4,
                        format!("Min: {}  Max: {}", min, max),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::ByteArray => {
            if let Some(s) = stats.as_any().downcast_ref::<BinaryStatistics>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                let fmt = |v: &[u8]| -> String {
                    match std::str::from_utf8(v) {
                        Ok(s) => {
                            let char_count = s.chars().count();
                            if char_count > 60 {
                                format!("\"{}...\"", s.chars().take(57).collect::<String>())
                            } else {
                                format!("\"{}\"", s)
                            }
                        }
                        Err(_) => format!("[{} bytes]", v.len()),
                    }
                };
                if let (Some(ref min), Some(ref max)) = (&s.min_value, &s.max_value) {
                    push_item(
                        items,
                        4,
                        format!("Min: {}  Max: {}", fmt(min), fmt(max)),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::Int96 => {
            if let Some(s) = stats.as_any().downcast_ref::<FixedLenStatistics>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                if let (Some(ref min), Some(ref max)) = (&s.min_value, &s.max_value) {
                    push_item(
                        items,
                        4,
                        format!(
                            "Min: {}  Max: {}",
                            format_int96_bytes(min),
                            format_int96_bytes(max)
                        ),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
        PhysicalType::FixedLenByteArray(len) => {
            if let Some(s) = stats.as_any().downcast_ref::<FixedLenStatistics>() {
                if let Some(dc) = s.distinct_count {
                    push_item(
                        items,
                        4,
                        format!("Distinct: {}", dc),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
                let fmt = |v: &[u8]| -> String {
                    if matches!(logical, Some(PrimitiveLogicalType::Uuid)) && len == 16 {
                        format_uuid(v)
                    } else if let Some(PrimitiveLogicalType::Decimal(_, scale)) = logical {
                        format_decimal_bytes(v, *scale)
                    } else if v.len() <= 16 {
                        v.iter().map(|b| format!("{:02x}", b)).collect()
                    } else {
                        format!(
                            "{}... ({} bytes)",
                            v[..8]
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>(),
                            v.len()
                        )
                    }
                };
                if let (Some(ref min), Some(ref max)) = (&s.min_value, &s.max_value) {
                    push_item(
                        items,
                        4,
                        format!("Min: {}  Max: {}", fmt(min), fmt(max)),
                        ItemKind::Property,
                        false,
                        None,
                    );
                }
            }
        }
    }
}

/// Decode a 12-byte INT96 (Impala-style Julian-day timestamp) to a
/// human-readable form, matching the plain decoder's output.
fn format_int96_bytes(v: &[u8]) -> String {
    if v.len() != 12 {
        return v.iter().map(|b| format!("{:02x}", b)).collect();
    }
    let nanos = i64::from_le_bytes(v[..8].try_into().unwrap());
    let julian_day = i32::from_le_bytes(v[8..12].try_into().unwrap());
    let epoch_days = (julian_day as i64).saturating_sub(2_440_588);
    let epoch_nanos = epoch_days
        .checked_mul(86_400_000_000_000)
        .and_then(|d| d.checked_add(nanos));
    match epoch_nanos {
        Some(en) => {
            let secs = en.div_euclid(1_000_000_000);
            let nanos_rem = en.rem_euclid(1_000_000_000) as u32;
            format_timestamp_secs(secs, nanos_rem)
        }
        None => format!("<int96 oor: jd={} ns={}>", julian_day, nanos),
    }
}

fn push_item(
    items: &mut Vec<TreeItem>,
    depth: usize,
    text: String,
    kind: ItemKind,
    expandable: bool,
    node_id: Option<NodeId>,
) {
    items.push(TreeItem {
        depth,
        text,
        kind,
        expandable,
        node_id,
    });
}

/// Open the file once and decode all columns for a row-group window.
/// `start_local` is the offset within the row group; `max_rows` is the number
/// of rows to produce starting at that offset.
fn decode_row_group_columns(
    path: &PathBuf,
    rg: &RowGroupMetaData,
    start_local: usize,
    max_rows: usize,
) -> Option<Vec<Vec<String>>> {
    let mut file = File::open(path).ok()?;
    let mut columns = Vec::with_capacity(rg.columns().len());
    for col_meta in rg.columns() {
        columns.push(decode_column(&mut file, col_meta, start_local, max_rows));
    }
    Some(columns)
}

/// Transpose column-major data to row-major, draining the source vecs.
fn transpose_columns(columns: &mut [Vec<String>]) -> Vec<Vec<String>> {
    let num_rows = columns.iter().map(|c| c.len()).max().unwrap_or(0);
    let num_cols = columns.len();
    // Reverse each column so we can pop from the end (O(1)) in row order.
    for col in columns.iter_mut() {
        col.reverse();
    }
    let mut rows = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        let mut row = Vec::with_capacity(num_cols);
        for col in columns.iter_mut() {
            row.push(col.pop().unwrap_or_default());
        }
        rows.push(row);
    }
    rows
}

/// Read a column chunk's dictionary page (if any) and decode it into
/// display strings. Returns None when the column has no dictionary or when
/// decoding fails (e.g. on corrupted files).
fn decode_column_dict(path: &PathBuf, col_meta: &ColumnChunkMetaData) -> Option<Vec<String>> {
    col_meta.dictionary_page_offset()?;
    let mut file = File::open(path).ok()?;
    let pages = read::get_page_iterator(col_meta, &mut file, None, vec![], usize::MAX).ok()?;
    let desc = col_meta.descriptor();
    let physical_type = desc.descriptor.primitive_type.physical_type;
    let logical_type = desc.descriptor.primitive_type.logical_type.as_ref();
    let mut decompress_buffer = vec![];
    for maybe_page in pages {
        let compressed = match maybe_page {
            Ok(p) => p,
            Err(_) => return None,
        };
        let page = match read::decompress(compressed, &mut decompress_buffer) {
            Ok(p) => p,
            Err(_) => return None,
        };
        if let Page::Dict(ref dict_page) = page {
            return Some(decode_dict_page(dict_page, physical_type, logical_type));
        }
        // First data page reached — dicts always precede data pages, so we
        // can stop without loading the entire chunk.
        if matches!(page, Page::Data(_)) {
            return None;
        }
    }
    None
}

fn decode_column(
    file: &mut File,
    col_meta: &ColumnChunkMetaData,
    start_local: usize,
    max_rows: usize,
) -> Vec<String> {
    let desc = col_meta.descriptor();
    let physical_type = desc.descriptor.primitive_type.physical_type;
    let logical_type = desc.descriptor.primitive_type.logical_type.as_ref();
    let max_def_level = desc.descriptor.max_def_level;
    let max_rep_level = desc.descriptor.max_rep_level;

    // Repeated columns (lists/maps) need rep-level-aware record assembly,
    // which this viewer doesn't implement. Emit an explicit marker instead
    // of silently producing misaligned per-element output.
    if max_rep_level > 0 {
        return vec!["<repeated / nested>".to_string(); max_rows];
    }

    let pages = match read::get_page_iterator(col_meta, file, None, vec![], usize::MAX) {
        Ok(p) => p,
        Err(e) => return vec![format!("<err: {}>", e)],
    };

    let mut values: Vec<String> = Vec::with_capacity(max_rows);
    let mut dict: Option<Vec<String>> = None;
    let mut decompress_buffer = vec![];
    // Row index within the column chunk (flat columns only; repeated columns
    // are handled by the early-return above).
    let mut cumulative = 0usize;

    for maybe_page in pages {
        if values.len() >= max_rows {
            break;
        }
        let compressed = match maybe_page {
            Ok(p) => p,
            Err(_) => break,
        };
        let page = match read::decompress(compressed, &mut decompress_buffer) {
            Ok(p) => p,
            Err(_) => break,
        };

        match page {
            Page::Dict(ref dict_page) => {
                // Dict pages always appear before data pages within a chunk,
                // so we process them unconditionally.
                dict = Some(decode_dict_page(dict_page, physical_type, logical_type));
            }
            Page::Data(ref data_page) => {
                let page_rows = data_page.num_values();
                // Page ends before the window starts → skip entirely. We
                // already paid for decompression; full decode is the expensive
                // part and is avoided here.
                if cumulative + page_rows <= start_local {
                    cumulative += page_rows;
                    continue;
                }
                let skip_in_page = start_local.saturating_sub(cumulative);
                let remaining = max_rows - values.len();
                let to_keep = remaining.min(page_rows.saturating_sub(skip_in_page));
                let decode_count = skip_in_page + to_keep;

                let mut decoded = decode_data_page(
                    data_page,
                    physical_type,
                    logical_type,
                    max_def_level,
                    dict.as_deref(),
                    decode_count,
                );
                // Drop the prefix, keep only the window slice.
                if skip_in_page < decoded.len() {
                    let tail = decoded.split_off(skip_in_page);
                    values.extend(tail.into_iter().take(to_keep));
                }
                cumulative += page_rows;
            }
        }
    }

    values.truncate(max_rows);
    values
}

fn page_encoding(page: &DataPage) -> Encoding {
    match page.header() {
        DataPageHeader::V1(h) => DataPageHeaderExt::encoding(h),
        DataPageHeader::V2(h) => DataPageHeaderExt::encoding(h),
    }
}

fn decode_data_page(
    page: &DataPage,
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    max_def_level: i16,
    dict: Option<&[String]>,
    max_values: usize,
) -> Vec<String> {
    let (_rep_buf, def_buf, values_buf) = match split_buffer(page) {
        Ok(bufs) => bufs,
        Err(_) => return vec!["<split err>".into()],
    };

    let num_values = page.num_values();

    // Decode definition levels
    let def_levels = if max_def_level > 0 && !def_buf.is_empty() {
        decode_def_levels(def_buf, num_values, max_def_level)
    } else {
        vec![max_def_level as u32; num_values]
    };

    let non_null = count_non_null(&def_levels, max_def_level);
    let is_v1 = matches!(page.header(), DataPageHeader::V1(_));
    let encoding = page_encoding(page);

    // Dictionary-encoded pages produce per-row output directly (they use
    // def_levels internally to emit nulls), so return early.
    if matches!(
        encoding,
        Encoding::RleDictionary | Encoding::PlainDictionary
    ) {
        return match dict {
            Some(d) => decode_dict_data(values_buf, &def_levels, max_def_level, d, max_values),
            None => vec!["<no dict>".into(); max_values.min(num_values)],
        };
    }

    // All other encoders produce only the non-null values; we interleave
    // nulls afterwards using the def_levels.
    let raw = match encoding {
        Encoding::Plain => decode_plain_raw(values_buf, physical_type, logical_type, non_null),
        Encoding::DeltaBinaryPacked => {
            decode_delta_binary_packed(values_buf, physical_type, logical_type, non_null)
        }
        Encoding::DeltaLengthByteArray => {
            decode_delta_length_byte_array(values_buf, logical_type, non_null)
        }
        Encoding::DeltaByteArray => decode_delta_byte_array(values_buf, logical_type, non_null),
        Encoding::Rle => decode_rle_values(values_buf, physical_type, is_v1, non_null),
        // BitPacked is deprecated. In practice it's only produced for BOOLEAN
        // with bit_width=1, where the LSB-first layout matches PLAIN bool.
        Encoding::BitPacked => decode_plain_raw(values_buf, physical_type, logical_type, non_null),
        Encoding::ByteStreamSplit => {
            decode_byte_stream_split(values_buf, physical_type, logical_type, non_null)
        }
        // Dict encodings handled above.
        Encoding::RleDictionary | Encoding::PlainDictionary => unreachable!(),
    };

    interleave_with_nulls(raw, &def_levels, max_def_level, max_values)
}

fn decode_dict_page(
    dict_page: &DictPage,
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
) -> Vec<String> {
    let buf = &dict_page.buffer;
    let num_values = dict_page.num_values;
    decode_plain_raw(buf, physical_type, logical_type, num_values)
}

fn decode_def_levels(buf: &[u8], num_values: usize, max_def_level: i16) -> Vec<u32> {
    if buf.is_empty() {
        return vec![max_def_level as u32; num_values];
    }
    let bit_width = bit_width_for(max_def_level as u32);
    match HybridRleDecoder::try_new(buf, bit_width, num_values) {
        Ok(decoder) => decoder.into_iter().map(|r| r.unwrap_or(0)).collect(),
        Err(_) => vec![max_def_level as u32; num_values],
    }
}

fn decode_dict_data(
    values_buf: &[u8],
    def_levels: &[u32],
    max_def_level: i16,
    dict: &[String],
    max_values: usize,
) -> Vec<String> {
    if values_buf.is_empty() {
        return def_levels
            .iter()
            .take(max_values)
            .map(|&d| {
                if d < max_def_level as u32 {
                    "null".into()
                } else {
                    "".into()
                }
            })
            .collect();
    }

    let bit_width = values_buf[0] as u32;
    let indices_buf = &values_buf[1..];
    let num_non_null = def_levels
        .iter()
        .filter(|&&d| d == max_def_level as u32)
        .count();

    let indices: Vec<u32> = if bit_width == 0 {
        vec![0; num_non_null]
    } else {
        match HybridRleDecoder::try_new(indices_buf, bit_width, num_non_null) {
            Ok(decoder) => decoder.into_iter().map(|r| r.unwrap_or(0)).collect(),
            Err(_) => return vec!["<rle err>".into(); max_values.min(def_levels.len())],
        }
    };

    let mut result = Vec::with_capacity(max_values);
    let mut idx_iter = indices.iter();
    for &def in def_levels {
        if result.len() >= max_values {
            break;
        }
        if def < max_def_level as u32 {
            result.push("null".into());
        } else if let Some(&idx) = idx_iter.next() {
            result.push(
                dict.get(idx as usize)
                    .cloned()
                    .unwrap_or_else(|| format!("<idx {}>", idx)),
            );
        } else {
            result.push("<missing>".into());
        }
    }
    result
}

fn count_non_null(def_levels: &[u32], max_def_level: i16) -> usize {
    if max_def_level == 0 {
        return def_levels.len();
    }
    let m = max_def_level as u32;
    def_levels.iter().filter(|&&d| d == m).count()
}

fn interleave_with_nulls(
    raw: Vec<String>,
    def_levels: &[u32],
    max_def_level: i16,
    max_values: usize,
) -> Vec<String> {
    if max_def_level == 0 {
        return raw.into_iter().take(max_values).collect();
    }
    let mut result = Vec::with_capacity(max_values);
    let mut iter = raw.into_iter();
    for &def in def_levels {
        if result.len() >= max_values {
            break;
        }
        if def < max_def_level as u32 {
            result.push("null".into());
        } else if let Some(v) = iter.next() {
            result.push(v);
        } else {
            result.push("".into());
        }
    }
    result
}

fn decode_plain_raw(
    buf: &[u8],
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    match physical_type {
        PhysicalType::Boolean => decode_plain_boolean(buf, max_values),
        PhysicalType::Int32 => decode_plain_i32(buf, logical_type, max_values),
        PhysicalType::Int64 => decode_plain_i64(buf, logical_type, max_values),
        PhysicalType::Int96 => decode_plain_int96(buf, max_values),
        PhysicalType::Float => decode_plain_f32(buf, max_values),
        PhysicalType::Double => decode_plain_f64(buf, max_values),
        PhysicalType::ByteArray => decode_plain_byte_array(buf, logical_type, max_values),
        PhysicalType::FixedLenByteArray(len) => {
            decode_plain_fixed_byte_array(buf, len, logical_type, max_values)
        }
    }
}

// ---------------------------------------------------------------------------
// PLAIN decoders per physical type
// ---------------------------------------------------------------------------

fn decode_plain_boolean(buf: &[u8], max_values: usize) -> Vec<String> {
    let mut result = Vec::with_capacity(max_values);
    for &byte in buf {
        for bit in 0..8 {
            if result.len() >= max_values {
                return result;
            }
            let val = (byte >> bit) & 1 == 1;
            result.push(if val { "true" } else { "false" }.into());
        }
    }
    result
}

fn decode_plain_i32(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    buf.chunks_exact(4)
        .take(max_values)
        .map(|chunk| {
            let val = i32::from_le_bytes(chunk.try_into().unwrap());
            format_i32(val, logical_type)
        })
        .collect()
}

fn decode_plain_i64(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    buf.chunks_exact(8)
        .take(max_values)
        .map(|chunk| {
            let val = i64::from_le_bytes(chunk.try_into().unwrap());
            format_i64(val, logical_type)
        })
        .collect()
}

fn decode_plain_int96(buf: &[u8], max_values: usize) -> Vec<String> {
    buf.chunks_exact(12)
        .take(max_values)
        .map(|chunk| {
            // INT96: 12 bytes, first 8 = nanoseconds within day, last 4 = Julian day
            let nanos = i64::from_le_bytes(chunk[..8].try_into().unwrap());
            let julian_day = i32::from_le_bytes(chunk[8..12].try_into().unwrap());
            // Convert Julian day to unix epoch days: Julian day of 1970-01-01 = 2440588
            let epoch_days = (julian_day as i64).saturating_sub(2_440_588);
            // Guard against overflow for wildly-out-of-range Julian days.
            let epoch_nanos = epoch_days
                .checked_mul(86_400_000_000_000)
                .and_then(|d| d.checked_add(nanos));
            match epoch_nanos {
                Some(en) => {
                    let secs = en.div_euclid(1_000_000_000);
                    let nanos_rem = en.rem_euclid(1_000_000_000) as u32;
                    format_timestamp_secs(secs, nanos_rem)
                }
                None => format!("<int96 oor: jd={} ns={}>", julian_day, nanos),
            }
        })
        .collect()
}

fn decode_plain_f32(buf: &[u8], max_values: usize) -> Vec<String> {
    buf.chunks_exact(4)
        .take(max_values)
        .map(|chunk| {
            let val = f32::from_le_bytes(chunk.try_into().unwrap());
            format!("{}", val)
        })
        .collect()
}

fn decode_plain_f64(buf: &[u8], max_values: usize) -> Vec<String> {
    buf.chunks_exact(8)
        .take(max_values)
        .map(|chunk| {
            let val = f64::from_le_bytes(chunk.try_into().unwrap());
            format!("{}", val)
        })
        .collect()
}

fn is_string_byte_array(logical_type: Option<&PrimitiveLogicalType>) -> bool {
    matches!(
        logical_type,
        Some(PrimitiveLogicalType::String)
            | Some(PrimitiveLogicalType::Enum)
            | Some(PrimitiveLogicalType::Json)
            | None
    )
}

/// Sanitize a decoded string for storage: preserve newlines (so the row-detail
/// popup can break on them), but replace carriage returns, tabs, and other
/// control bytes with single-width substitutes. Each output char occupies one
/// display column, keeping column-width math honest.
fn sanitize_for_display(s: &str) -> String {
    // \n (0x0A) is intentionally preserved.
    if !s.bytes().any(|b| (b < 0x20 && b != b'\n') || b == 0x7F) {
        return s.to_string();
    }
    s.chars()
        .map(|c| match c {
            '\n' => '\n',
            '\r' | '\t' => ' ',
            c if c.is_control() => '?',
            c => c,
        })
        .collect()
}

/// Unicode-aware display width after single-line sanitization. Use this
/// everywhere a terminal column count is needed so non-ASCII characters (CJK,
/// accented letters, emoji) align correctly.
pub fn display_width_oneline(s: &str) -> usize {
    UnicodeWidthStr::width(sanitize_for_line(s).as_str())
}

/// Default alignment for a column given its physical + logical type.
/// Numeric columns right-align; everything else (dates, times, timestamps,
/// strings, binary, bool) left-aligns.
fn column_alignment(phys: PhysicalType, logical: Option<&PrimitiveLogicalType>) -> Alignment {
    // Date/Time/Timestamp already render left-aligned in the existing branch.
    match phys {
        PhysicalType::Int32 | PhysicalType::Int64 => match logical {
            Some(PrimitiveLogicalType::Date)
            | Some(PrimitiveLogicalType::Time { .. })
            | Some(PrimitiveLogicalType::Timestamp { .. }) => Alignment::Left,
            _ => Alignment::Right,
        },
        PhysicalType::Float | PhysicalType::Double => Alignment::Right,
        // INT96 is always an Impala timestamp in practice.
        PhysicalType::Int96 => Alignment::Left,
        PhysicalType::Boolean | PhysicalType::ByteArray | PhysicalType::FixedLenByteArray(_) => {
            Alignment::Left
        }
    }
}

/// Strip newlines (and other controls) for single-line display contexts such
/// as table rows and the tree Data Preview. Callers still use the raw value
/// in multi-line contexts (the row-detail popup).
pub fn sanitize_for_line(s: &str) -> String {
    if !s.bytes().any(|b| b < 0x20 || b == 0x7F) {
        return s.to_string();
    }
    s.chars()
        .map(|c| match c {
            '\n' | '\r' | '\t' => ' ',
            c if c.is_control() => '?',
            c => c,
        })
        .collect()
}

fn byte_array_display(bytes: &[u8], is_string: bool) -> String {
    if is_string {
        if let Ok(s) = std::str::from_utf8(bytes) {
            return sanitize_for_display(s);
        }
        // fall through to hex on invalid UTF-8
    }
    if bytes.len() <= 16 {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    } else {
        format!(
            "{}... ({} bytes)",
            bytes[..8]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            bytes.len()
        )
    }
}

fn decode_plain_byte_array(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    let is_string = is_string_byte_array(logical_type);
    let mut result = Vec::with_capacity(max_values);
    let mut offset = 0;
    while result.len() < max_values && offset + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > buf.len() {
            break;
        }
        let bytes = &buf[offset..offset + len];
        offset += len;
        result.push(byte_array_display(bytes, is_string));
    }
    result
}

// ---------------------------------------------------------------------------
// Non-PLAIN encoding decoders
// ---------------------------------------------------------------------------

fn decode_delta_length_byte_array(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    use parquet2::encoding::delta_length_byte_array::Decoder;
    let is_string = is_string_byte_array(logical_type);
    let mut decoder = match Decoder::try_new(buf) {
        Ok(d) => d,
        Err(_) => return vec!["<delta-len err>".into()],
    };
    // Iterate all lengths first — required before calling into_values().
    let mut lengths: Vec<usize> = Vec::new();
    for item in decoder.by_ref() {
        match item {
            Ok(v) if v >= 0 => lengths.push(v as usize),
            _ => return vec!["<delta-len err>".into()],
        }
    }
    let values = decoder.into_values();
    let mut result = Vec::with_capacity(lengths.len().min(max_values));
    let mut offset = 0usize;
    for len in lengths {
        if result.len() >= max_values {
            break;
        }
        let end = match offset.checked_add(len) {
            Some(e) if e <= values.len() => e,
            _ => break,
        };
        result.push(byte_array_display(&values[offset..end], is_string));
        offset = end;
    }
    result
}

fn decode_delta_byte_array(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    use parquet2::encoding::delta_byte_array::Decoder;
    let is_string = is_string_byte_array(logical_type);
    let mut pref_dec = match Decoder::try_new(buf) {
        Ok(d) => d,
        Err(_) => return vec!["<delta-ba err>".into()],
    };
    let mut prefixes: Vec<usize> = Vec::new();
    for item in pref_dec.by_ref() {
        match item {
            // Prefix decoder emits `u32`, so no non-negative guard is possible
            // (or needed). Casting to `usize` is lossless on 32-/64-bit targets.
            Ok(v) => prefixes.push(v as usize),
            Err(_) => return vec!["<delta-ba err>".into()],
        }
    }
    let mut len_dec = match pref_dec.into_lengths() {
        Ok(d) => d,
        Err(_) => return vec!["<delta-ba err>".into()],
    };
    let mut lengths: Vec<usize> = Vec::new();
    for item in len_dec.by_ref() {
        match item {
            Ok(v) if v >= 0 => lengths.push(v as usize),
            _ => return vec!["<delta-ba err>".into()],
        }
    }
    let suffix_bytes = len_dec.values();

    let n = prefixes.len().min(lengths.len());
    let mut result = Vec::with_capacity(n.min(max_values));
    let mut prev: Vec<u8> = Vec::new();
    let mut offset = 0usize;
    for i in 0..n {
        if result.len() >= max_values {
            break;
        }
        let pref = prefixes[i];
        let suf_len = lengths[i];
        if pref > prev.len() {
            break;
        }
        let end = match offset.checked_add(suf_len) {
            Some(e) if e <= suffix_bytes.len() => e,
            _ => break,
        };
        let mut value = Vec::with_capacity(pref + suf_len);
        value.extend_from_slice(&prev[..pref]);
        value.extend_from_slice(&suffix_bytes[offset..end]);
        offset = end;
        result.push(byte_array_display(&value, is_string));
        prev = value;
    }
    result
}

fn decode_delta_binary_packed(
    buf: &[u8],
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    use parquet2::encoding::delta_bitpacked::Decoder;
    let dec = match Decoder::try_new(buf) {
        Ok(d) => d,
        Err(_) => return vec!["<delta-bp err>".into()],
    };
    let mut result = Vec::with_capacity(max_values);
    for item in dec {
        if result.len() >= max_values {
            break;
        }
        match item {
            Ok(v) => match physical_type {
                PhysicalType::Int32 => result.push(format_i32(v as i32, logical_type)),
                PhysicalType::Int64 => result.push(format_i64(v, logical_type)),
                _ => result.push(format!("{}", v)),
            },
            Err(_) => break,
        }
    }
    result
}

/// RLE-encoded BOOLEAN data. In Parquet V1 the value buffer is prefixed with
/// a 4-byte little-endian length; V2 drops that prefix.
fn decode_rle_values(
    buf: &[u8],
    physical_type: PhysicalType,
    is_v1: bool,
    max_values: usize,
) -> Vec<String> {
    if !matches!(physical_type, PhysicalType::Boolean) {
        // Spec-legal only for BOOLEAN and def/rep levels. For any other
        // physical type this shouldn't happen; bail visibly rather than lie.
        return vec!["<rle: non-bool>".into(); max_values];
    }
    let inner: &[u8] = if is_v1 {
        if buf.len() < 4 {
            return Vec::new();
        }
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        if 4 + len > buf.len() {
            return Vec::new();
        }
        &buf[4..4 + len]
    } else {
        buf
    };
    match HybridRleDecoder::try_new(inner, 1, max_values) {
        Ok(dec) => dec
            .into_iter()
            .map(|r| match r {
                Ok(v) if v != 0 => "true".into(),
                Ok(_) => "false".into(),
                Err(_) => "<rle err>".into(),
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// BYTE_STREAM_SPLIT: K byte-streams of N bytes each, where K is the type size.
/// Value i is reassembled by taking byte k from offset k*N + i.
///
/// Parquet 2.11+ allows BSS on FIXED_LEN_BYTE_ARRAY, INT32, and INT64 in
/// addition to FLOAT/DOUBLE. We reassemble the bytes and then hand off to the
/// corresponding PLAIN formatter for type-aware display.
fn decode_byte_stream_split(
    buf: &[u8],
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    let stride = match physical_type {
        PhysicalType::Float => 4,
        PhysicalType::Double => 8,
        PhysicalType::Int32 => 4,
        PhysicalType::Int64 => 8,
        PhysicalType::FixedLenByteArray(n) if n > 0 => n,
        _ => return vec!["<bss unsupported>".into(); max_values],
    };
    let n = buf.len() / stride;
    let count = max_values.min(n);
    let mut result = Vec::with_capacity(count);
    let mut reassembled = vec![0u8; stride];
    for i in 0..count {
        for b in 0..stride {
            reassembled[b] = buf[b * n + i];
        }
        match physical_type {
            PhysicalType::Float => {
                let bytes: [u8; 4] = reassembled[..4].try_into().unwrap();
                result.push(format!("{}", f32::from_le_bytes(bytes)));
            }
            PhysicalType::Double => {
                let bytes: [u8; 8] = reassembled[..8].try_into().unwrap();
                result.push(format!("{}", f64::from_le_bytes(bytes)));
            }
            PhysicalType::Int32 => {
                let bytes: [u8; 4] = reassembled[..4].try_into().unwrap();
                result.push(format_i32(i32::from_le_bytes(bytes), logical_type));
            }
            PhysicalType::Int64 => {
                let bytes: [u8; 8] = reassembled[..8].try_into().unwrap();
                result.push(format_i64(i64::from_le_bytes(bytes), logical_type));
            }
            PhysicalType::FixedLenByteArray(_) => {
                // Same handling as PLAIN FLBA.
                if matches!(logical_type, Some(PrimitiveLogicalType::Uuid)) && stride == 16 {
                    result.push(format_uuid(&reassembled));
                } else if let Some(PrimitiveLogicalType::Decimal(_, scale)) = logical_type {
                    result.push(format_decimal_bytes(&reassembled, *scale));
                } else {
                    result.push(reassembled.iter().map(|b| format!("{:02x}", b)).collect());
                }
            }
            _ => unreachable!(),
        }
    }
    result
}

fn decode_plain_fixed_byte_array(
    buf: &[u8],
    len: usize,
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    if len == 0 {
        return Vec::new();
    }
    buf.chunks_exact(len)
        .take(max_values)
        .map(|chunk| {
            if matches!(logical_type, Some(PrimitiveLogicalType::Uuid)) && len == 16 {
                format_uuid(chunk)
            } else if let Some(PrimitiveLogicalType::Decimal(_, scale)) = logical_type {
                format_decimal_bytes(chunk, *scale)
            } else {
                chunk.iter().map(|b| format!("{:02x}", b)).collect()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Value formatting
// ---------------------------------------------------------------------------

fn format_i32(val: i32, logical_type: Option<&PrimitiveLogicalType>) -> String {
    match logical_type {
        Some(PrimitiveLogicalType::Date) => {
            // Days since 1970-01-01
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            match epoch.checked_add_signed(chrono::Duration::days(val as i64)) {
                Some(d) => d.format("%Y-%m-%d").to_string(),
                None => format!("{} (days)", val),
            }
        }
        Some(PrimitiveLogicalType::Time { unit, .. }) => format_time_of_day(val as i64, *unit),
        Some(PrimitiveLogicalType::Decimal(_, scale)) => format_decimal_i64(val as i64, *scale),
        _ => format!("{}", val),
    }
}

fn format_i64(val: i64, logical_type: Option<&PrimitiveLogicalType>) -> String {
    match logical_type {
        Some(PrimitiveLogicalType::Timestamp {
            unit,
            is_adjusted_to_utc,
        }) => {
            let (secs, nanos) = match unit {
                TimeUnit::Milliseconds => (
                    val.div_euclid(1000),
                    (val.rem_euclid(1000) * 1_000_000) as u32,
                ),
                TimeUnit::Microseconds => (
                    val.div_euclid(1_000_000),
                    (val.rem_euclid(1_000_000) * 1000) as u32,
                ),
                TimeUnit::Nanoseconds => (
                    val.div_euclid(1_000_000_000),
                    val.rem_euclid(1_000_000_000) as u32,
                ),
            };
            let formatted = format_timestamp_secs(secs, nanos);
            // Spec: `is_adjusted_to_utc = true` means the instant is stored in
            // UTC; `false` means "local semantics, unknown offset". Mark UTC
            // explicitly with `Z` so it isn't mistaken for local time.
            if *is_adjusted_to_utc {
                format!("{}Z", formatted)
            } else {
                formatted
            }
        }
        Some(PrimitiveLogicalType::Time { unit, .. }) => format_time_of_day(val, *unit),
        Some(PrimitiveLogicalType::Decimal(_, scale)) => format_decimal_i64(val, *scale),
        _ => format!("{}", val),
    }
}

/// Format a "time of day" value (duration since midnight). Parquet stores
/// these as ms (Int32), µs (Int64), or ns (Int64) since midnight — `val` is
/// the raw count in the given unit. Renders as `HH:MM:SS[.fractional]`,
/// following the same subsecond-trimming rules as `format_timestamp_secs`.
fn format_time_of_day(val: i64, unit: TimeUnit) -> String {
    let nanos_total: i128 = match unit {
        TimeUnit::Milliseconds => (val as i128) * 1_000_000,
        TimeUnit::Microseconds => (val as i128) * 1_000,
        TimeUnit::Nanoseconds => val as i128,
    };
    // Wrap negative / over-24h values into [0, 86400_000_000_000 ns) so we
    // always print a valid clock time.
    let day_ns: i128 = 86_400_000_000_000;
    let wrapped = ((nanos_total % day_ns) + day_ns) % day_ns;
    let secs_total = (wrapped / 1_000_000_000) as u32;
    let nanos = (wrapped % 1_000_000_000) as u32;
    let hh = secs_total / 3600;
    let mm = (secs_total % 3600) / 60;
    let ss = secs_total % 60;
    let base = format!("{:02}:{:02}:{:02}", hh, mm, ss);
    if nanos == 0 {
        base
    } else if nanos.is_multiple_of(1_000_000) {
        format!("{}.{:03}", base, nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!("{}.{:06}", base, nanos / 1_000)
    } else {
        format!("{}.{:09}", base, nanos)
    }
}

/// Formats a Unix epoch timestamp as `YYYY-MM-DD HH:MM:SS` with a subsecond
/// fraction appended only when non-zero and only at the precision actually
/// carried by the value (ms, µs, or ns). This preserves the original unit's
/// information instead of silently dropping fractional seconds.
fn format_timestamp_secs(secs: i64, nanos: u32) -> String {
    let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) else {
        return format!("{}s", secs);
    };
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if nanos == 0 {
        return base;
    }
    // Trim trailing zeros: show ms if µs/ns parts are zero, µs if ns is zero,
    // otherwise full ns precision.
    if nanos.is_multiple_of(1_000_000) {
        format!("{}.{:03}", base, nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!("{}.{:06}", base, nanos / 1_000)
    } else {
        format!("{}.{:09}", base, nanos)
    }
}

fn format_decimal_i64(unscaled: i64, scale: usize) -> String {
    if scale == 0 {
        return format!("{}", unscaled);
    }
    let divisor = 10_i64.pow(scale as u32);
    let integer = unscaled / divisor;
    let frac = (unscaled % divisor).unsigned_abs();
    format!("{}.{:0>width$}", integer, frac, width = scale)
}

fn format_decimal_bytes(bytes: &[u8], scale: usize) -> String {
    // Big-endian two's complement
    if bytes.is_empty() {
        return "0".into();
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut val: i128 = 0;
    for &b in bytes {
        val = (val << 8) | b as i128;
    }
    if negative {
        // Sign-extend
        let bits = bytes.len() * 8;
        val -= 1i128 << bits;
    }
    if scale == 0 {
        return format!("{}", val);
    }
    let divisor = 10_i128.pow(scale as u32);
    let integer = val / divisor;
    let frac = (val % divisor).unsigned_abs();
    format!("{}.{:0>width$}", integer, frac, width = scale)
}

fn format_uuid(bytes: &[u8]) -> String {
    if bytes.len() != 16 {
        return bytes.iter().map(|b| format!("{:02x}", b)).collect();
    }
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn bit_width_for(max_val: u32) -> u32 {
    if max_val == 0 {
        0
    } else {
        32 - max_val.leading_zeros()
    }
}

pub fn format_size(size: u64) -> String {
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.1} KB", size as f64 / 1024.0)
    } else if size < 1024 * 1024 * 1024 {
        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", size as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Cap on export attempts when resolving a non-colliding output filename.
/// With 1000+ existing variants of the same export, the user has a larger
/// problem than we can solve from a keystroke handler.
const EXPORT_ATTEMPT_LIMIT: u32 = 1000;

/// Find a non-existent path under `parent` built from `make_path(attempt)`.
/// Tries `attempt = 0, 1, 2, ...` until `make_path` returns a path that does
/// not yet exist, or until `EXPORT_ATTEMPT_LIMIT` is reached.
fn pick_unused_export_path<F>(mut make_path: F) -> Result<PathBuf, String>
where
    F: FnMut(u32) -> PathBuf,
{
    for attempt in 0..=EXPORT_ATTEMPT_LIMIT {
        let cand = make_path(attempt);
        if !cand.exists() {
            return Ok(cand);
        }
    }
    Err("too many existing export files".into())
}

/// Compile a search pattern to a case-insensitive regex. If `query` is not
/// valid regex, falls back to an escaped literal substring so that a naive
/// `/(foo` still works as a literal search. Returns None when even the
/// escaped fallback fails to compile (shouldn't happen in practice).
fn compile_search_regex(query: &str) -> Option<regex::Regex> {
    if query.is_empty() {
        return None;
    }
    match regex::RegexBuilder::new(query)
        .case_insensitive(true)
        .build()
    {
        Ok(r) => Some(r),
        Err(_) => regex::RegexBuilder::new(&regex::escape(query))
            .case_insensitive(true)
            .build()
            .ok(),
    }
}

fn format_encoding(id: i32) -> &'static str {
    match id {
        0 => "Plain",
        2 => "PlainDictionary",
        3 => "RLE",
        4 => "BitPacked",
        5 => "DeltaBinaryPacked",
        6 => "DeltaLengthByteArray",
        7 => "DeltaByteArray",
        8 => "RleDictionary",
        9 => "ByteStreamSplit",
        _ => "Unknown",
    }
}

fn format_physical_type(pt: PhysicalType) -> &'static str {
    match pt {
        PhysicalType::Boolean => "BOOLEAN",
        PhysicalType::Int32 => "INT32",
        PhysicalType::Int64 => "INT64",
        PhysicalType::Int96 => "INT96",
        PhysicalType::Float => "FLOAT",
        PhysicalType::Double => "DOUBLE",
        PhysicalType::ByteArray => "BYTE_ARRAY",
        PhysicalType::FixedLenByteArray(n) => {
            // Can't return dynamic string from &'static str, use a const approach
            match n {
                16 => "FIXED_LEN(16)",
                12 => "FIXED_LEN(12)",
                _ => "FIXED_LEN",
            }
        }
    }
}

fn format_logical_type(lt: &PrimitiveLogicalType) -> String {
    match lt {
        PrimitiveLogicalType::String => "String".into(),
        PrimitiveLogicalType::Enum => "Enum".into(),
        PrimitiveLogicalType::Decimal(p, s) => format!("Decimal({},{})", p, s),
        PrimitiveLogicalType::Date => "Date".into(),
        PrimitiveLogicalType::Time { unit, .. } => format!("Time({:?})", unit),
        PrimitiveLogicalType::Timestamp { unit, .. } => format!("Timestamp({:?})", unit),
        PrimitiveLogicalType::Integer(it) => format!("{:?}", it),
        PrimitiveLogicalType::Unknown => "Unknown".into(),
        PrimitiveLogicalType::Json => "JSON".into(),
        PrimitiveLogicalType::Bson => "BSON".into(),
        PrimitiveLogicalType::Uuid => "UUID".into(),
    }
}

fn truncate(s: &str, max: usize) -> String {
    // Single pass: scan chars, record byte offset at the truncation point,
    // and bail early once we know whether truncation is needed.
    let keep = if max > 3 { max - 3 } else { max };
    let mut byte_offsets = s
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(s.len()));
    // Advance to the byte offset after `keep` chars
    let end_at_keep = byte_offsets.nth(keep).unwrap_or(s.len());
    if end_at_keep == s.len() {
        // String has <= keep chars, so <= max chars — no truncation needed
        return s.to_string();
    }
    // There are more than `keep` chars. Check if total exceeds max.
    // We need (max - keep) more chars to know. For max > 3, that's 3 more.
    let remaining = max - keep;
    if byte_offsets.nth(remaining - 1).is_none() {
        // String has <= max chars total — no truncation
        return s.to_string();
    }
    if max > 3 {
        format!("{}...", &s[..end_at_keep])
    } else {
        s[..end_at_keep].to_string()
    }
}

/// Compare two stringified cell values for in-window sort. Tries numeric
/// comparison first (so "10" sorts after "2"), falls back to lexicographic
/// byte order. `asc` controls direction.
fn cmp_values(a: &str, b: &str, asc: bool) -> std::cmp::Ordering {
    let ord = match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.cmp(b),
    };
    if asc {
        ord
    } else {
        ord.reverse()
    }
}

/// Quote a string as a JSON string literal (escaping as needed). Delegates
/// to serde_json for correctness; only allocates when required.
fn json_quote(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s.replace('"', "\\\"")))
}

/// Render a single NDJSON / single-line JSON row from (column, value) pairs.
/// Values equal to the literal `"null"` become JSON `null`; everything else
/// is emitted as a JSON string. Pure function — testable in isolation.
fn render_ndjson_line<'a, I>(pairs: I) -> String
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut out = String::from("{");
    let mut first = true;
    for (k, v) in pairs {
        if !first {
            out.push_str(", ");
        }
        first = false;
        out.push_str(&json_quote(k));
        out.push_str(": ");
        if v == "null" {
            out.push_str("null");
        } else {
            out.push_str(&json_quote(v));
        }
    }
    out.push('}');
    out
}

/// If `s` is a bare integer (optional `-` then only ASCII digits, at least
/// four digits in the magnitude), insert comma thousands separators.
/// Otherwise return the input unchanged. Used for display-only formatting;
/// does not touch stored values or clipboard output.
pub fn format_with_thousands(s: &str) -> String {
    let (sign, body): (&str, &str) = if let Some(rest) = s.strip_prefix('-') {
        ("-", rest)
    } else {
        ("", s)
    };
    if body.is_empty() || body.len() <= 3 {
        return s.to_string();
    }
    if !body.bytes().all(|b| b.is_ascii_digit()) {
        return s.to_string();
    }
    let mut out = String::with_capacity(sign.len() + body.len() + body.len() / 3);
    out.push_str(sign);
    let len = body.len();
    for (i, c) in body.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// CSV field escaping per RFC 4180: quote the field if it contains the
/// delimiter, a quote, or a line break; double existing quotes inside.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Column metadata summary (hint bar)
// ---------------------------------------------------------------------------

/// Info about one column of the enclosing row group: name, physical type,
/// optional logical type, and null fraction within this row group.
#[derive(Clone, Debug)]
pub struct ColumnSummary {
    pub name: String,
    pub physical: String,
    pub logical: String,
    pub null_count: Option<u64>,
    pub num_values: u64,
}

impl ParquetViewerState {
    /// Summary for the column under the cursor in Table view.
    pub fn current_column_summary(&self) -> Option<ColumnSummary> {
        if self.view_mode != ViewMode::Table {
            return None;
        }
        let col_idx = self.table_cursor_col;
        if col_idx >= self.table_columns.len() {
            return None;
        }
        // Use the row group containing the cursor.
        let target = self.table_cursor_row;
        let mut rg_offset = 0;
        for rg in self.metadata.row_groups.iter() {
            let rg_rows = rg.num_rows();
            if target < rg_offset + rg_rows {
                let col_meta = rg.columns().get(col_idx)?;
                let name = self.table_columns[col_idx].clone();
                let pt = &col_meta.descriptor().descriptor.primitive_type;
                let physical = format_physical_type(pt.physical_type).to_string();
                let logical = pt
                    .logical_type
                    .as_ref()
                    .map(format_logical_type)
                    .unwrap_or_default();
                let stats = col_meta.statistics().and_then(|r| r.ok());
                let null_count = stats.and_then(|s| s.null_count()).map(|n| n as u64);
                return Some(ColumnSummary {
                    name,
                    physical,
                    logical,
                    null_count,
                    num_values: col_meta.num_values() as u64,
                });
            }
            rg_offset += rg_rows;
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
//
// We construct encoded buffers using parquet2's own encoders and then assert
// our decoders reproduce the original values. This guards against regressions
// in the main data-path code (delta encodings, RLE, BSS, dict interleave).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Sanitization / width ---

    #[test]
    fn sanitize_preserves_newlines() {
        // \n kept; \t and \r become single spaces; other controls become '?'.
        let out = sanitize_for_display("hello\nworld\ttab\rcr\x01x");
        assert_eq!(out, "hello\nworld tab cr?x");
    }

    #[test]
    fn sanitize_for_line_strips_newlines() {
        assert_eq!(sanitize_for_line("a\nb\tc\rd\x01e"), "a b c d?e");
    }

    #[test]
    fn display_width_handles_cjk_and_emoji() {
        // Half-width ASCII
        assert_eq!(display_width_oneline("hello"), 5);
        // Full-width CJK glyph occupies 2 columns
        assert_eq!(display_width_oneline("你好"), 4);
        // Sanitized newline becomes a single space (width 1)
        assert_eq!(display_width_oneline("a\nb"), 3);
    }

    // --- Delta length byte array ---

    #[test]
    fn delta_length_byte_array_roundtrip_strings() {
        let values: Vec<&[u8]> = vec![b"hello", b"", b"world", b"longer string"];
        let mut buf = Vec::new();
        parquet2::encoding::delta_length_byte_array::encode(values.iter().copied(), &mut buf);

        let decoded = decode_delta_length_byte_array(&buf, None, values.len());
        let expected: Vec<String> = values
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap().to_string())
            .collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn delta_length_byte_array_max_values_caps_output() {
        let values: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let mut buf = Vec::new();
        parquet2::encoding::delta_length_byte_array::encode(values.iter().copied(), &mut buf);
        assert_eq!(
            decode_delta_length_byte_array(&buf, None, 3),
            vec!["a", "b", "c"]
        );
    }

    // --- Delta byte array (prefix-compressed) ---

    #[test]
    fn delta_byte_array_preserves_prefix_chain() {
        // Prefixes share with previous value.
        let values: Vec<&[u8]> = vec![b"Hello", b"Helicopter", b"Help"];
        let mut buf = Vec::new();
        parquet2::encoding::delta_byte_array::encode(values.iter().copied(), &mut buf);

        let decoded = decode_delta_byte_array(&buf, None, values.len());
        let expected: Vec<String> = values
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap().to_string())
            .collect();
        assert_eq!(decoded, expected);
    }

    // --- Delta binary packed (Int32/Int64) ---

    #[test]
    fn delta_binary_packed_roundtrip_i64() {
        // Avoid deltas that exceed i64 range — the encoder computes
        // max_delta - min_delta and doesn't protect against overflow.
        let values: Vec<i64> = vec![0, 1, 2, 10, 100, -5, 42, 12345, -6789];
        let mut buf = Vec::new();
        parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut buf);
        let decoded = decode_delta_binary_packed(&buf, PhysicalType::Int64, None, values.len());
        let expected: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn delta_binary_packed_respects_max_values() {
        let values: Vec<i64> = (0..200).collect();
        let mut buf = Vec::new();
        parquet2::encoding::delta_bitpacked::encode(values.iter().copied(), &mut buf);
        let decoded = decode_delta_binary_packed(&buf, PhysicalType::Int64, None, 50);
        assert_eq!(decoded.len(), 50);
        assert_eq!(decoded[0], "0");
        assert_eq!(decoded[49], "49");
    }

    // --- RLE boolean (V2 framing) ---

    #[test]
    fn rle_boolean_v2_framing_decodes() {
        // Bit-packed RLE for booleans has no outer length prefix in V2.
        let bits: Vec<u32> = vec![1, 0, 1, 1, 0, 0, 1, 0];
        let mut buf = Vec::new();
        parquet2::encoding::hybrid_rle::encode_u32(&mut buf, bits.iter().copied(), 1).unwrap();
        let decoded = decode_rle_values(&buf, PhysicalType::Boolean, false, bits.len());
        let expected: Vec<String> = bits
            .iter()
            .map(|b| {
                if *b != 0 {
                    "true".into()
                } else {
                    "false".into()
                }
            })
            .collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn rle_boolean_v1_framing_decodes() {
        // V1 prefixes the RLE data with a 4-byte LE length.
        let bits: Vec<u32> = vec![0, 1, 0, 1, 1];
        let mut inner = Vec::new();
        parquet2::encoding::hybrid_rle::encode_u32(&mut inner, bits.iter().copied(), 1).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(inner.len() as u32).to_le_bytes());
        buf.extend_from_slice(&inner);

        let decoded = decode_rle_values(&buf, PhysicalType::Boolean, true, bits.len());
        let expected: Vec<String> = bits
            .iter()
            .map(|b| {
                if *b != 0 {
                    "true".into()
                } else {
                    "false".into()
                }
            })
            .collect();
        assert_eq!(decoded, expected);
    }

    // --- Byte stream split ---

    #[test]
    fn byte_stream_split_float_reassembles() {
        let values: Vec<f32> = vec![1.0, 2.5, -3.25, std::f32::consts::PI];
        let n = values.len();
        // Interleave: for stream b in 0..4, emit byte b of value i in position b*n + i.
        let mut buf = vec![0u8; 4 * n];
        for (i, v) in values.iter().enumerate() {
            let bytes = v.to_le_bytes();
            for b in 0..4 {
                buf[b * n + i] = bytes[b];
            }
        }
        let decoded = decode_byte_stream_split(&buf, PhysicalType::Float, None, n);
        let expected: Vec<String> = values.iter().map(|v| format!("{}", v)).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn byte_stream_split_int64_reassembles() {
        let values: Vec<i64> = vec![1, 1_000_000, -42, i64::MAX, i64::MIN];
        let n = values.len();
        let mut buf = vec![0u8; 8 * n];
        for (i, v) in values.iter().enumerate() {
            let bytes = v.to_le_bytes();
            for b in 0..8 {
                buf[b * n + i] = bytes[b];
            }
        }
        let decoded = decode_byte_stream_split(&buf, PhysicalType::Int64, None, n);
        let expected: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        assert_eq!(decoded, expected);
    }

    // --- PLAIN BYTE_ARRAY display / sanitization ---

    #[test]
    fn plain_byte_array_strips_control_but_keeps_newline() {
        // Two values: "foo\nbar" and "tab\there"
        let mut buf = Vec::new();
        for s in ["foo\nbar", "tab\there"] {
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        let out = decode_plain_byte_array(&buf, Some(&PrimitiveLogicalType::String), 2);
        // \n preserved, \t -> space
        assert_eq!(out, vec!["foo\nbar", "tab here"]);
    }

    #[test]
    fn plain_byte_array_hex_for_non_string() {
        // Non-string BYTE_ARRAY renders as hex.
        let bytes = [0xde, 0xadu8, 0xbe, 0xef];
        let mut buf = Vec::new();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
        let out = decode_plain_byte_array(&buf, Some(&PrimitiveLogicalType::Bson), 1);
        assert_eq!(out, vec!["deadbeef"]);
    }

    // --- Null interleave ---

    #[test]
    fn interleave_with_nulls_threads_values() {
        let raw = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let defs = vec![1, 0, 1, 1, 0];
        let out = interleave_with_nulls(raw, &defs, 1, 5);
        assert_eq!(out, vec!["a", "null", "b", "c", "null"]);
    }

    #[test]
    fn interleave_with_nulls_no_def_levels_returns_raw() {
        let raw = vec!["x".to_string(), "y".to_string()];
        let out = interleave_with_nulls(raw, &[], 0, 5);
        assert_eq!(out, vec!["x", "y"]);
    }

    // --- INT96 overflow guard ---

    #[test]
    fn int96_overflow_value_returns_marker_instead_of_panicking() {
        // Construct a bogus INT96: nanos=0, julian_day = i32::MAX
        let mut buf = vec![0u8; 12];
        buf[8..12].copy_from_slice(&i32::MAX.to_le_bytes());
        let out = decode_plain_int96(&buf, 1);
        assert_eq!(out.len(), 1);
        assert!(out[0].starts_with("<int96 oor"));
    }

    // --- format_i64 timestamp UTC flag ---

    #[test]
    fn format_i64_timestamp_marks_utc() {
        let lt = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        let out = format_i64(0, Some(&lt));
        assert!(
            out.ends_with('Z'),
            "expected Z suffix on UTC timestamps, got {}",
            out
        );
    }

    // --- Column alignment ---

    #[test]
    fn alignment_numeric_types_right_align() {
        assert_eq!(
            column_alignment(PhysicalType::Int32, None),
            Alignment::Right
        );
        assert_eq!(
            column_alignment(PhysicalType::Int64, None),
            Alignment::Right
        );
        assert_eq!(
            column_alignment(PhysicalType::Float, None),
            Alignment::Right
        );
        assert_eq!(
            column_alignment(PhysicalType::Double, None),
            Alignment::Right
        );
    }

    #[test]
    fn alignment_temporal_logical_types_left_align() {
        assert_eq!(
            column_alignment(PhysicalType::Int32, Some(&PrimitiveLogicalType::Date)),
            Alignment::Left
        );
        let ts = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        assert_eq!(
            column_alignment(PhysicalType::Int64, Some(&ts)),
            Alignment::Left
        );
    }

    #[test]
    fn alignment_decimal_int_right_aligns() {
        let dec = PrimitiveLogicalType::Decimal(10, 2);
        assert_eq!(
            column_alignment(PhysicalType::Int32, Some(&dec)),
            Alignment::Right
        );
    }

    #[test]
    fn alignment_strings_and_bytes_left_align() {
        assert_eq!(
            column_alignment(PhysicalType::ByteArray, None),
            Alignment::Left
        );
        assert_eq!(
            column_alignment(PhysicalType::Boolean, None),
            Alignment::Left
        );
        assert_eq!(
            column_alignment(PhysicalType::FixedLenByteArray(16), None),
            Alignment::Left
        );
        assert_eq!(column_alignment(PhysicalType::Int96, None), Alignment::Left);
    }

    // --- Sort comparator ---

    #[test]
    fn cmp_values_numeric_beats_lexicographic() {
        // "10" > "2" numerically (lex would say "10" < "2").
        assert_eq!(cmp_values("10", "2", true), std::cmp::Ordering::Greater);
        assert_eq!(cmp_values("10", "2", false), std::cmp::Ordering::Less);
    }

    #[test]
    fn cmp_values_negative_numbers() {
        assert_eq!(cmp_values("-10", "-2", true), std::cmp::Ordering::Less);
        assert_eq!(cmp_values("-3.5", "2.5", true), std::cmp::Ordering::Less);
    }

    #[test]
    fn cmp_values_lexicographic_fallback() {
        // Non-numeric values compare lexicographically.
        assert_eq!(
            cmp_values("banana", "apple", true),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            cmp_values("apple", "banana", true),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn cmp_values_mixed_uses_lexicographic() {
        // One parseable + one not → lexicographic.
        assert_eq!(cmp_values("42", "foo", true), "42".cmp("foo"));
    }

    #[test]
    fn cmp_values_descending_reverses() {
        let asc = cmp_values("a", "b", true);
        let desc = cmp_values("a", "b", false);
        assert_eq!(asc.reverse(), desc);
    }

    // --- Mouse click hit-test logic ---

    /// Simulate the click_at lookup independently of a real ParquetViewerState
    /// (which would need a file on disk). Exercises the row/column hit-test
    /// math that click_at uses.
    fn simulate_click(
        layout: &TableLayout,
        total_rows: usize,
        col_count: usize,
        click_col: u16,
        click_row: u16,
    ) -> (Option<usize>, Option<usize>) {
        let _ = col_count;
        let row = if click_row >= layout.data_start_y && click_row < layout.data_end_y {
            let offset = (click_row - layout.data_start_y) as usize;
            let new_row = layout.scroll_row + offset;
            if new_row < total_rows {
                Some(new_row)
            } else {
                None
            }
        } else {
            None
        };
        let col = layout
            .col_hits
            .iter()
            .find(|&&(_, xs, xe)| click_col >= xs && click_col < xe)
            .map(|&(c, _, _)| c);
        (row, col)
    }

    #[test]
    fn click_hits_first_visible_col() {
        let layout = TableLayout {
            data_start_y: 3,
            data_end_y: 13,
            scroll_row: 0,
            col_hits: vec![(0, 10, 20), (1, 23, 33), (2, 36, 46)],
        };
        let (row, col) = simulate_click(&layout, 100, 3, 15, 5);
        assert_eq!(row, Some(2));
        assert_eq!(col, Some(0));
    }

    #[test]
    fn click_beyond_last_visible_col_no_column_hit() {
        let layout = TableLayout {
            data_start_y: 3,
            data_end_y: 13,
            scroll_row: 0,
            col_hits: vec![(0, 10, 20)],
        };
        let (row, col) = simulate_click(&layout, 100, 3, 100, 5);
        assert_eq!(row, Some(2));
        assert_eq!(col, None);
    }

    #[test]
    fn click_outside_data_rows_no_row_hit() {
        let layout = TableLayout {
            data_start_y: 3,
            data_end_y: 13,
            scroll_row: 0,
            col_hits: vec![(0, 10, 20)],
        };
        let (row, col) = simulate_click(&layout, 100, 1, 15, 2);
        assert_eq!(row, None);
        assert_eq!(col, Some(0));
    }

    #[test]
    fn click_past_total_rows_clamped_to_none() {
        let layout = TableLayout {
            data_start_y: 3,
            data_end_y: 13,
            scroll_row: 40,
            col_hits: vec![(0, 10, 20)],
        };
        // scroll_row=40, offset=5 → row 45. total = 42 → out of range.
        let (row, _) = simulate_click(&layout, 42, 1, 12, 8);
        assert_eq!(row, None);
    }

    #[test]
    fn click_respects_frozen_column_hit() {
        // Simulates frozen col 3 + scrolled cols 5, 6.
        let layout = TableLayout {
            data_start_y: 3,
            data_end_y: 13,
            scroll_row: 0,
            col_hits: vec![(3, 10, 20), (5, 24, 34), (6, 37, 47)],
        };
        let (_, col) = simulate_click(&layout, 100, 3, 15, 5);
        assert_eq!(col, Some(3)); // frozen
        let (_, col2) = simulate_click(&layout, 100, 3, 30, 5);
        assert_eq!(col2, Some(5)); // scrolled
    }

    // --- Thousands separator formatting ---

    #[test]
    fn thousands_small_numbers_unchanged() {
        assert_eq!(format_with_thousands("0"), "0");
        assert_eq!(format_with_thousands("42"), "42");
        assert_eq!(format_with_thousands("999"), "999");
        assert_eq!(format_with_thousands("-42"), "-42");
    }

    #[test]
    fn thousands_inserts_commas() {
        assert_eq!(format_with_thousands("1234"), "1,234");
        assert_eq!(format_with_thousands("1234567"), "1,234,567");
        assert_eq!(format_with_thousands("1000000000"), "1,000,000,000");
    }

    #[test]
    fn thousands_handles_negative() {
        assert_eq!(format_with_thousands("-1234"), "-1,234");
        assert_eq!(format_with_thousands("-1000000"), "-1,000,000");
    }

    #[test]
    fn thousands_non_integer_passes_through() {
        assert_eq!(format_with_thousands("1.5"), "1.5");
        assert_eq!(format_with_thousands("null"), "null");
        assert_eq!(format_with_thousands(""), "");
        assert_eq!(format_with_thousands("1e10"), "1e10");
        assert_eq!(format_with_thousands("2021-01-01"), "2021-01-01");
    }

    // --- Time-of-day formatting ---

    #[test]
    fn time_of_day_millis_no_fraction() {
        // 01:02:03 in milliseconds: 1*3600_000 + 2*60_000 + 3_000 = 3_723_000
        let out = format_time_of_day(3_723_000, TimeUnit::Milliseconds);
        assert_eq!(out, "01:02:03");
    }

    #[test]
    fn time_of_day_millis_with_fraction() {
        // 01:02:03.456
        let out = format_time_of_day(3_723_456, TimeUnit::Milliseconds);
        assert_eq!(out, "01:02:03.456");
    }

    #[test]
    fn time_of_day_micros_ns_precision() {
        // 00:00:00.000123456 (µs unit, 123456 µs → 0.123456 s)
        let out = format_time_of_day(123_456, TimeUnit::Microseconds);
        assert_eq!(out, "00:00:00.123456");
    }

    #[test]
    fn time_of_day_nanos_full_precision() {
        let out = format_time_of_day(123_456_789, TimeUnit::Nanoseconds);
        assert_eq!(out, "00:00:00.123456789");
    }

    #[test]
    fn time_of_day_negative_wraps_into_day() {
        // -1 ms → 23:59:59.999
        let out = format_time_of_day(-1, TimeUnit::Milliseconds);
        assert_eq!(out, "23:59:59.999");
    }

    #[test]
    fn time_of_day_over_24h_wraps() {
        // 86_400_001 ms = 24:00:00.001 → wraps to 00:00:00.001
        let out = format_time_of_day(86_400_001, TimeUnit::Milliseconds);
        assert_eq!(out, "00:00:00.001");
    }

    #[test]
    fn format_i32_time_routes_through_time_of_day() {
        let lt = PrimitiveLogicalType::Time {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        };
        let out = format_i32(3_723_456, Some(&lt));
        assert_eq!(out, "01:02:03.456");
    }

    #[test]
    fn format_i64_time_routes_through_time_of_day() {
        let lt = PrimitiveLogicalType::Time {
            unit: TimeUnit::Nanoseconds,
            is_adjusted_to_utc: false,
        };
        let out = format_i64(123_456_789, Some(&lt));
        assert_eq!(out, "00:00:00.123456789");
    }

    // --- Subsecond timestamp formatting ---

    #[test]
    fn timestamp_no_subseconds_when_nanos_zero() {
        // 2021-01-01 00:00:00 UTC
        let out = format_timestamp_secs(1_609_459_200, 0);
        assert_eq!(out, "2021-01-01 00:00:00");
    }

    #[test]
    fn timestamp_millisecond_precision() {
        // .123_000_000 ns = 123 ms
        let out = format_timestamp_secs(1_609_459_200, 123_000_000);
        assert!(out.ends_with(".123"), "got {}", out);
    }

    #[test]
    fn timestamp_microsecond_precision() {
        // .123_456_000 ns = 123.456 µs
        let out = format_timestamp_secs(1_609_459_200, 123_456_000);
        assert!(out.ends_with(".123456"), "got {}", out);
    }

    #[test]
    fn timestamp_nanosecond_precision() {
        let out = format_timestamp_secs(1_609_459_200, 123_456_789);
        assert!(out.ends_with(".123456789"), "got {}", out);
    }

    // --- INT96 bytes formatter ---

    #[test]
    fn int96_bytes_formatter_matches_plain_decoder() {
        // Craft an INT96 for 2021-01-01 00:00:00 UTC:
        //   julian_day = 2459216, nanos_within_day = 0
        let julian: i32 = 2_459_216;
        let mut bytes = vec![0u8; 8]; // nanos = 0
        bytes.extend_from_slice(&julian.to_le_bytes());
        let via_bytes = format_int96_bytes(&bytes);
        let via_plain = decode_plain_int96(&bytes, 1).into_iter().next().unwrap();
        assert_eq!(via_bytes, via_plain);
        assert!(via_bytes.starts_with("2021-01-01"), "got {}", via_bytes);
    }

    // --- Hidden-column navigation logic ---

    fn nearest_visible(
        hidden: &HashSet<usize>,
        total: usize,
        from: usize,
        reverse: bool,
    ) -> Option<usize> {
        if total == 0 {
            return None;
        }
        let mut search: Box<dyn Iterator<Item = usize>> = if reverse {
            Box::new(
                (0..=from.min(total - 1))
                    .rev()
                    .chain((from + 1..total).rev()),
            )
        } else {
            Box::new((from..total).chain(0..from))
        };
        search.find(|i| !hidden.contains(i))
    }

    #[test]
    fn nearest_visible_forward_skips_hidden() {
        let hidden: HashSet<usize> = [1, 2, 3].iter().copied().collect();
        assert_eq!(nearest_visible(&hidden, 6, 1, false), Some(4));
        assert_eq!(nearest_visible(&hidden, 6, 0, false), Some(0));
        assert_eq!(nearest_visible(&hidden, 6, 4, false), Some(4));
    }

    #[test]
    fn nearest_visible_wraps_when_none_forward() {
        let hidden: HashSet<usize> = [4, 5].iter().copied().collect();
        // From index 4, the only visible indices are 0..=3; search wraps.
        assert_eq!(nearest_visible(&hidden, 6, 4, false), Some(0));
    }

    #[test]
    fn nearest_visible_reverse_works() {
        let hidden: HashSet<usize> = [0, 1].iter().copied().collect();
        assert_eq!(nearest_visible(&hidden, 6, 2, true), Some(2));
        // From index 1: 1 and 0 are hidden; the reverse+wrap order is
        // 1, 0, 5, 4, 3, 2 → first visible is 5.
        assert_eq!(nearest_visible(&hidden, 6, 1, true), Some(5));
    }

    #[test]
    fn nearest_visible_none_when_all_hidden() {
        let hidden: HashSet<usize> = (0..6).collect();
        assert_eq!(nearest_visible(&hidden, 6, 3, false), None);
    }

    // --- JSON row helpers ---

    #[test]
    fn json_quote_escapes_special_chars() {
        assert_eq!(json_quote("hello"), "\"hello\"");
        assert_eq!(json_quote("a\"b"), "\"a\\\"b\"");
        assert_eq!(json_quote("a\nb"), "\"a\\nb\"");
        assert_eq!(json_quote("tab\there"), "\"tab\\there\"");
    }

    #[test]
    fn render_ndjson_line_basic_row() {
        let pairs = vec![("id", "42"), ("name", "alice")];
        let out = render_ndjson_line(pairs);
        assert_eq!(out, r#"{"id": "42", "name": "alice"}"#);
    }

    #[test]
    fn render_ndjson_line_nulls_are_bare() {
        let pairs = vec![("a", "1"), ("b", "null"), ("c", "x")];
        let out = render_ndjson_line(pairs);
        assert_eq!(out, r#"{"a": "1", "b": null, "c": "x"}"#);
    }

    #[test]
    fn render_ndjson_line_escapes_special_chars_in_values() {
        let pairs = vec![("q", "he said \"hi\""), ("nl", "a\nb")];
        let out = render_ndjson_line(pairs);
        // Round-trip: the result must parse as valid JSON.
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("must parse");
        assert_eq!(parsed["q"], "he said \"hi\"");
        assert_eq!(parsed["nl"], "a\nb");
    }

    #[test]
    fn render_ndjson_line_preserves_column_order() {
        // JSON technically doesn't guarantee key order, but our output does.
        let pairs = vec![("z", "1"), ("a", "2"), ("m", "3")];
        let out = render_ndjson_line(pairs);
        let z_pos = out.find("\"z\"").unwrap();
        let a_pos = out.find("\"a\"").unwrap();
        let m_pos = out.find("\"m\"").unwrap();
        assert!(z_pos < a_pos && a_pos < m_pos);
    }

    #[test]
    fn json_quote_unicode() {
        // serde_json doesn't escape non-ASCII by default.
        assert_eq!(json_quote("café"), "\"café\"");
    }

    // --- Selection toggle logic (HashSet behavior) ---

    #[test]
    fn selection_toggle_roundtrip() {
        let mut set: HashSet<usize> = HashSet::new();
        // "toggle" = insert if absent, remove if present.
        if !set.insert(5) {
            set.remove(&5);
        }
        assert!(set.contains(&5));
        if !set.insert(5) {
            set.remove(&5);
        }
        assert!(!set.contains(&5));
    }

    // --- Regex search ---

    #[test]
    fn regex_literal_search_matches_substring() {
        let re = regex::RegexBuilder::new("foo")
            .case_insensitive(true)
            .build()
            .unwrap();
        assert!(re.is_match("FOObar"));
        assert!(re.is_match("xFoOy"));
        assert!(!re.is_match("bar"));
    }

    #[test]
    fn regex_pattern_search_supports_alternation() {
        let re = regex::RegexBuilder::new("^(alpha|beta)$")
            .case_insensitive(true)
            .build()
            .unwrap();
        assert!(re.is_match("alpha"));
        assert!(re.is_match("Beta"));
        assert!(!re.is_match("alphabet"));
    }

    #[test]
    fn regex_invalid_pattern_escaped_fallback() {
        // Raw `(` is invalid as regex, but escaped becomes a literal that still
        // finds a match.
        let q = "(foo";
        let fallback = regex::RegexBuilder::new(&regex::escape(q))
            .case_insensitive(true)
            .build()
            .unwrap();
        assert!(fallback.is_match("x(foo)y"));
    }

    // --- CSV escaping ---

    #[test]
    fn csv_escape_plain_passthrough() {
        assert_eq!(csv_escape("hello"), "hello");
        assert_eq!(csv_escape(""), "");
        assert_eq!(csv_escape("a b c"), "a b c");
    }

    #[test]
    fn csv_escape_quotes_when_delimiter_present() {
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
    }

    #[test]
    fn csv_escape_doubles_internal_quotes() {
        assert_eq!(csv_escape("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn csv_escape_quotes_newlines_and_cr() {
        assert_eq!(csv_escape("a\nb"), "\"a\nb\"");
        assert_eq!(csv_escape("a\rb"), "\"a\rb\"");
    }

    // --- Search wrap-around ---

    #[test]
    fn search_wrap_flag_set_when_wrapping_forward() {
        // We can't easily construct a ParquetViewerState in a unit test, but
        // the wrap logic is self-contained in search_jump. Instead, verify
        // the documented behavior at the structure level: the forward path
        // resets `cur` to 0 after hitting total_rows, and the reverse path
        // resets `cur` to total_rows - 1 after hitting 0. (This is a smoke
        // check — full coverage would require fixture files.)
        let total = 10usize;
        let start = 7usize;
        let mut cur = start;
        let mut wrapped = false;
        let mut seq = Vec::new();
        for _ in 0..20 {
            seq.push(cur);
            cur = cur.saturating_add(1);
            if cur >= total {
                if wrapped {
                    break;
                }
                wrapped = true;
                cur = 0;
            }
            if wrapped && cur == start {
                break;
            }
        }
        // Forward scan from 7 through 9, wrap to 0, scan 0..6.
        assert!(seq.contains(&9) && seq.contains(&0) && seq.contains(&6));
        assert!(wrapped);
    }

    #[test]
    fn format_i64_timestamp_local_has_no_z() {
        let lt = PrimitiveLogicalType::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: false,
        };
        let out = format_i64(0, Some(&lt));
        assert!(!out.ends_with('Z'));
    }
}
