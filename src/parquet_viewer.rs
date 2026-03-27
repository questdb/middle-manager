use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;

use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::Encoding;
use parquet2::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
use parquet2::page::{split_buffer, DataPage, DataPageHeader, DataPageHeaderExt, DictPage, Page};
use parquet2::read;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType, TimeUnit};

const DATA_PREVIEW_MAX_ROWS: usize = 100;
const TABLE_BUFFER_ROWS: usize = 1000;

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
    RowGroupData(usize),
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

#[derive(Clone)]
pub struct DataPreview {
    pub column_widths: Vec<usize>,
    pub rows: Vec<Vec<String>>,
}

// ---------------------------------------------------------------------------
// ParquetViewerState
// ---------------------------------------------------------------------------

pub struct ParquetViewerState {
    pub path: PathBuf,
    pub file_size: u64,
    #[allow(dead_code)]
    pub error: Option<String>,

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
    pub table_rows: Vec<Vec<String>>,
    pub table_total_rows: usize,
    pub table_scroll_row: usize,
    pub table_scroll_col: usize,
    pub table_visible_rows: usize,
    pub table_visible_cols: usize,
    table_loaded_rg: Option<usize>,
    table_loaded_offset: usize,

    // Data previews (per row group, lazily loaded)
    data_previews: Vec<Option<DataPreview>>,
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
        let table_column_widths: Vec<usize> =
            table_columns.iter().map(|n| n.len().max(8)).collect();
        let table_total_rows: usize = metadata.row_groups.iter().map(|rg| rg.num_rows()).sum();

        let mut state = Self {
            path,
            file_size,
            error: None,
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
            table_rows: Vec::new(),
            table_total_rows,
            table_scroll_row: 0,
            table_scroll_col: 0,
            table_visible_rows: 0,
            table_visible_cols: 0,
            table_loaded_rg: None,
            table_loaded_offset: 0,
            data_previews: vec![None; num_rg],
        };

        state.expanded.insert(NodeId::Root);
        state.rebuild_tree();
        Ok(state)
    }

    // -----------------------------------------------------------------------
    // Navigation (unified: dispatches to tree or table)
    // -----------------------------------------------------------------------

    pub fn move_up(&mut self, amount: usize) {
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = self.tree_cursor.saturating_sub(amount);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                self.table_scroll_row = self.table_scroll_row.saturating_sub(amount);
                self.ensure_table_data();
            }
        }
    }

    pub fn move_down(&mut self, amount: usize) {
        match self.view_mode {
            ViewMode::Tree => {
                let max = self.tree_items.len().saturating_sub(1);
                self.tree_cursor = (self.tree_cursor + amount).min(max);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                let max = self.table_max_scroll();
                self.table_scroll_row = (self.table_scroll_row + amount).min(max);
                self.ensure_table_data();
            }
        }
    }

    pub fn move_to_top(&mut self) {
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = 0;
                self.tree_scroll = 0;
            }
            ViewMode::Table => {
                self.table_scroll_row = 0;
                self.ensure_table_data();
            }
        }
    }

    pub fn move_to_bottom(&mut self) {
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = self.tree_items.len().saturating_sub(1);
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
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

    pub fn scroll_left(&mut self) {
        if self.view_mode == ViewMode::Table {
            self.table_scroll_col = self.table_scroll_col.saturating_sub(1);
        } else {
            self.collapse();
        }
    }

    pub fn scroll_right(&mut self) {
        if self.view_mode == ViewMode::Table {
            let max_col = self
                .table_columns
                .len()
                .saturating_sub(self.table_visible_cols);
            self.table_scroll_col = (self.table_scroll_col + 1).min(max_col);
        } else {
            self.expand();
        }
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
                        // Trigger data preview load if expanding a data node
                        if let NodeId::RowGroupData(rg) = node_id {
                            if self.data_previews[rg].is_none() {
                                self.load_data_preview(rg);
                            }
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
                    if let NodeId::RowGroupData(rg) = node_id {
                        if self.data_previews[rg].is_none() {
                            self.load_data_preview(rg);
                        }
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
        match self.view_mode {
            ViewMode::Tree => {
                self.tree_cursor = row.min(self.tree_items.len().saturating_sub(1));
                self.ensure_tree_visible();
            }
            ViewMode::Table => {
                self.table_scroll_row = row.min(self.table_max_scroll());
                self.ensure_table_data();
            }
        }
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

    fn table_max_scroll(&self) -> usize {
        self.table_total_rows
            .saturating_sub(self.table_visible_rows)
    }

    fn ensure_table_data(&mut self) {
        if self.table_total_rows == 0 {
            return;
        }
        let target = self.table_scroll_row;
        let mut offset = 0;
        for (rg_idx, rg) in self.metadata.row_groups.iter().enumerate() {
            let rg_rows = rg.num_rows();
            if target < offset + rg_rows {
                if self.table_loaded_rg != Some(rg_idx) {
                    self.load_table_row_group(rg_idx);
                }
                return;
            }
            offset += rg_rows;
        }
    }

    fn load_table_row_group(&mut self, rg_idx: usize) {
        let offset: usize = self.metadata.row_groups[..rg_idx]
            .iter()
            .map(|rg| rg.num_rows())
            .sum();

        let rg = &self.metadata.row_groups[rg_idx];
        let max_rows = rg.num_rows().min(TABLE_BUFFER_ROWS);

        let columns = match decode_row_group_columns(&self.path, rg, max_rows) {
            Some(c) => c,
            None => return,
        };

        // Update column widths
        for (col_idx, col_data) in columns.iter().enumerate() {
            if col_idx < self.table_column_widths.len() {
                let max_w = col_data
                    .iter()
                    .take(100)
                    .map(|v| v.len())
                    .max()
                    .unwrap_or(0);
                self.table_column_widths[col_idx] =
                    self.table_column_widths[col_idx].max(max_w).min(40);
            }
        }

        self.table_rows = transpose_columns(&columns);
        self.table_loaded_rg = Some(rg_idx);
        self.table_loaded_offset = offset;
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
                    for entry in kv.iter() {
                        let value = entry.value.as_deref().unwrap_or("<null>");
                        push_item(
                            &mut self.tree_items,
                            2,
                            format!("{} = {}", entry.key, truncate(value, 80)),
                            ItemKind::Property,
                            false,
                            None,
                        );
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
            for col in self.metadata.schema_descr.columns() {
                let pt = &col.descriptor.primitive_type;
                let name = &pt.field_info.name;
                let phys = format_physical_type(pt.physical_type);
                let logical = pt
                    .logical_type
                    .as_ref()
                    .map(|lt| format!(" / {}", format_logical_type(lt)))
                    .unwrap_or_default();
                let rep = format!("{:?}", pt.field_info.repetition).to_lowercase();
                push_item(
                    &mut self.tree_items,
                    2,
                    format!("{:<24} {}{:<20} ({})", name, phys, logical, rep),
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
                for col_meta in rg.columns() {
                    let desc = col_meta.descriptor();
                    let name = &desc.descriptor.primitive_type.field_info.name;
                    let compression = format!("{:?}", col_meta.compression());
                    let uncompressed = format_size(col_meta.uncompressed_size() as u64);
                    let compressed = format_size(col_meta.compressed_size() as u64);
                    push_item(
                        &mut self.tree_items,
                        3,
                        format!(
                            "{:<24} {:<12} {} -> {}",
                            name, compression, uncompressed, compressed
                        ),
                        ItemKind::ColumnInfo,
                        false,
                        None,
                    );
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

                    // Data rows
                    for row in &preview.rows {
                        let line: String = row
                            .iter()
                            .enumerate()
                            .map(|(i, val)| {
                                let w = preview.column_widths.get(i).copied().unwrap_or(8);
                                format!("{:<w$}", truncate(val, w), w = w)
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

    fn load_data_preview(&mut self, rg_idx: usize) {
        let rg = &self.metadata.row_groups[rg_idx];
        let max_rows = rg.num_rows().min(DATA_PREVIEW_MAX_ROWS);

        let columns = match decode_row_group_columns(&self.path, rg, max_rows) {
            Some(c) => c,
            None => return,
        };

        // Compute column widths
        let mut col_widths: Vec<usize> =
            self.table_columns.iter().map(|n| n.len().max(4)).collect();
        for (ci, col_data) in columns.iter().enumerate() {
            if ci < col_widths.len() {
                let max_w = col_data.iter().map(|v| v.len()).max().unwrap_or(0);
                col_widths[ci] = col_widths[ci].max(max_w).min(30);
            }
        }

        self.data_previews[rg_idx] = Some(DataPreview {
            column_widths: col_widths,
            rows: transpose_columns(&columns),
        });
    }
}

// ---------------------------------------------------------------------------
// Column decoding
// ---------------------------------------------------------------------------

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

/// Open the file once and decode all columns for a row group.
fn decode_row_group_columns(
    path: &PathBuf,
    rg: &RowGroupMetaData,
    max_rows: usize,
) -> Option<Vec<Vec<String>>> {
    let mut file = File::open(path).ok()?;
    let mut columns = Vec::with_capacity(rg.columns().len());
    for col_meta in rg.columns() {
        columns.push(decode_column(&mut file, col_meta, max_rows));
    }
    Some(columns)
}

/// Transpose column-major data to row-major.
fn transpose_columns(columns: &[Vec<String>]) -> Vec<Vec<String>> {
    let num_rows = columns.iter().map(|c| c.len()).max().unwrap_or(0);
    (0..num_rows)
        .map(|r| {
            columns
                .iter()
                .map(|col| col.get(r).cloned().unwrap_or_default())
                .collect()
        })
        .collect()
}

fn decode_column(file: &mut File, col_meta: &ColumnChunkMetaData, max_rows: usize) -> Vec<String> {
    let pages = match read::get_page_iterator(col_meta, file, None, vec![], usize::MAX) {
        Ok(p) => p,
        Err(e) => return vec![format!("<err: {}>", e)],
    };

    let desc = col_meta.descriptor();
    let physical_type = desc.descriptor.primitive_type.physical_type;
    let logical_type = desc.descriptor.primitive_type.logical_type.as_ref();
    let max_def_level = desc.descriptor.max_def_level;

    let mut values: Vec<String> = Vec::new();
    let mut dict: Option<Vec<String>> = None;
    let mut decompress_buffer = vec![];

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
                dict = Some(decode_dict_page(dict_page, physical_type, logical_type));
            }
            Page::Data(ref data_page) => {
                let remaining = max_rows - values.len();
                let mut page_values = decode_data_page(
                    data_page,
                    physical_type,
                    logical_type,
                    max_def_level,
                    dict.as_deref(),
                    remaining,
                );
                values.append(&mut page_values);
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

    let encoding = page_encoding(page);
    let is_dict_encoded = matches!(
        encoding,
        Encoding::RleDictionary | Encoding::PlainDictionary
    );

    if is_dict_encoded {
        if let Some(dict) = dict {
            return decode_dict_data(values_buf, &def_levels, max_def_level, dict, max_values);
        }
        return vec!["<no dict>".into(); max_values.min(num_values)];
    }

    // PLAIN encoding
    decode_plain_values(
        values_buf,
        physical_type,
        logical_type,
        &def_levels,
        max_def_level,
        max_values,
    )
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

fn decode_plain_values(
    buf: &[u8],
    physical_type: PhysicalType,
    logical_type: Option<&PrimitiveLogicalType>,
    def_levels: &[u32],
    max_def_level: i16,
    max_values: usize,
) -> Vec<String> {
    let raw_values = decode_plain_raw(buf, physical_type, logical_type, def_levels.len());

    if max_def_level == 0 {
        return raw_values.into_iter().take(max_values).collect();
    }

    // Interleave nulls
    let mut result = Vec::with_capacity(max_values);
    let mut val_iter = raw_values.into_iter();
    for &def in def_levels {
        if result.len() >= max_values {
            break;
        }
        if def < max_def_level as u32 {
            result.push("null".into());
        } else if let Some(v) = val_iter.next() {
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
            let epoch_days = julian_day as i64 - 2_440_588;
            let epoch_nanos = epoch_days * 86_400_000_000_000 + nanos;
            let secs = epoch_nanos / 1_000_000_000;
            let nanos_rem = (epoch_nanos % 1_000_000_000) as u32;
            format_timestamp_secs(secs, nanos_rem)
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

fn decode_plain_byte_array(
    buf: &[u8],
    logical_type: Option<&PrimitiveLogicalType>,
    max_values: usize,
) -> Vec<String> {
    let is_string = matches!(
        logical_type,
        Some(PrimitiveLogicalType::String)
            | Some(PrimitiveLogicalType::Enum)
            | Some(PrimitiveLogicalType::Json)
            | None
    );

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

        if is_string {
            match std::str::from_utf8(bytes) {
                Ok(s) => result.push(s.to_owned()),
                Err(_) => {
                    // Not valid UTF-8 despite being typed as string — show hex
                    if bytes.len() <= 16 {
                        result.push(bytes.iter().map(|b| format!("{:02x}", b)).collect());
                    } else {
                        result.push(format!(
                            "{}... ({} bytes)",
                            bytes[..8]
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>(),
                            bytes.len()
                        ));
                    }
                }
            }
        } else {
            // Show as hex for non-string binary
            if bytes.len() <= 16 {
                result.push(bytes.iter().map(|b| format!("{:02x}", b)).collect());
            } else {
                result.push(format!(
                    "{}... ({} bytes)",
                    bytes[..8]
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>(),
                    bytes.len()
                ));
            }
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
        Some(PrimitiveLogicalType::Decimal(_, scale)) => format_decimal_i64(val as i64, *scale),
        _ => format!("{}", val),
    }
}

fn format_i64(val: i64, logical_type: Option<&PrimitiveLogicalType>) -> String {
    match logical_type {
        Some(PrimitiveLogicalType::Timestamp { unit, .. }) => {
            let (secs, nanos) = match unit {
                TimeUnit::Milliseconds => (val / 1000, ((val % 1000) * 1_000_000) as u32),
                TimeUnit::Microseconds => (val / 1_000_000, ((val % 1_000_000) * 1000) as u32),
                TimeUnit::Nanoseconds => (val / 1_000_000_000, (val % 1_000_000_000) as u32),
            };
            format_timestamp_secs(secs, nanos)
        }
        Some(PrimitiveLogicalType::Decimal(_, scale)) => format_decimal_i64(val, *scale),
        _ => format!("{}", val),
    }
}

fn format_timestamp_secs(secs: i64, nanos: u32) -> String {
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => format!("{}s", secs),
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
    if s.chars().count() <= max {
        s.to_string()
    } else if max > 3 {
        let truncated: String = s.chars().take(max - 3).collect();
        format!("{}...", truncated)
    } else {
        s.chars().take(max).collect()
    }
}
