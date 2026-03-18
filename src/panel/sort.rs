use super::entry::FileEntry;

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

pub fn sort_entries(entries: &mut [FileEntry], field: SortField, ascending: bool) {
    entries.sort_by(|a, b| {
        // Directories always come first
        match (a.is_dir, b.is_dir) {
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            _ => {}
        }

        let ord = match field {
            SortField::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            SortField::Size => a.size.cmp(&b.size),
            SortField::Date => a.modified.cmp(&b.modified),
        };

        if ascending {
            ord
        } else {
            ord.reverse()
        }
    });
}
