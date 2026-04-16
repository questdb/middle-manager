use std::collections::HashMap;
use std::path::PathBuf;

use super::entry::FileEntry;
use super::DirSizeState;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortField {
    Name,
    Size,
    Date,
}

impl SortField {
    pub fn next(self) -> Self {
        match self {
            SortField::Name => SortField::Size,
            SortField::Size => SortField::Date,
            SortField::Date => SortField::Name,
        }
    }
}

pub fn sort_entries(
    entries: &mut [FileEntry],
    field: SortField,
    ascending: bool,
    dir_sizes: &HashMap<PathBuf, DirSizeState>,
) {
    entries.sort_by(|a, b| {
        // Directories always come first
        match (a.is_dir, b.is_dir) {
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            _ => {}
        }

        let ord = match field {
            SortField::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            SortField::Size => {
                let a_size = effective_size(a, dir_sizes);
                let b_size = effective_size(b, dir_sizes);
                a_size.cmp(&b_size)
            }
            SortField::Date => a.modified.cmp(&b.modified),
        };

        if ascending {
            ord
        } else {
            ord.reverse()
        }
    });
}

/// Get the effective size for sorting: use calculated dir size if available.
fn effective_size(entry: &FileEntry, dir_sizes: &HashMap<PathBuf, DirSizeState>) -> u64 {
    if entry.is_dir {
        if let Some(DirSizeState::Done(size)) = dir_sizes.get(&entry.path) {
            return *size;
        }
    }
    entry.size
}
