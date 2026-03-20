use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GitFileStatus {
    Modified,
    Added,
    Deleted,
    Renamed,
    Untracked,
    Conflict,
}

impl GitFileStatus {
    pub fn marker(self) -> &'static str {
        match self {
            Self::Modified => "\u{25CF}",  // ●
            Self::Added => "+",
            Self::Deleted => "-",
            Self::Renamed => "\u{2192}",  // →
            Self::Untracked => "?",
            Self::Conflict => "!",
        }
    }

    /// Priority for directory status aggregation: higher = more important.
    fn priority(self) -> u8 {
        match self {
            Self::Conflict => 5,
            Self::Modified => 4,
            Self::Deleted => 3,
            Self::Added => 2,
            Self::Renamed => 1,
            Self::Untracked => 0,
        }
    }

    fn from_porcelain(x: u8, y: u8) -> Option<Self> {
        match (x, y) {
            (b'?', b'?') => Some(Self::Untracked),
            (b'U', _) | (_, b'U') | (b'A', b'A') | (b'D', b'D') => Some(Self::Conflict),
            (b'A', _) | (_, b'A') => Some(Self::Added),
            (b'R', _) => Some(Self::Renamed),
            (b'D', _) | (_, b'D') => Some(Self::Deleted),
            (b'M', _) | (_, b'M') | (b'T', _) | (_, b'T') => Some(Self::Modified),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GitInfo {
    pub branch: String,
    pub statuses: HashMap<String, GitFileStatus>,
    pub ahead: usize,
    pub behind: usize,
    /// Repo-wide status counts for the header summary.
    pub total_modified: usize,
    pub total_added: usize,
    pub total_deleted: usize,
    pub total_untracked: usize,
    pub total_renamed: usize,
    pub total_conflict: usize,
}

/// Cached git state per repository root.
/// Shared between panels that are in the same repo.
pub struct GitCache {
    /// Repo root path → cached data.
    cache: HashMap<PathBuf, CacheEntry>,
}

struct CacheEntry {
    branch: String,
    ahead: usize,
    behind: usize,
    /// All statuses keyed by repo-relative path.
    all_statuses: HashMap<String, GitFileStatus>,
    /// When this entry was last refreshed.
    last_refresh: Instant,
}

/// Minimum time between re-queries to the same repo.
const REFRESH_INTERVAL_MS: u128 = 2000;

impl GitCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Get git info for a directory view. Uses cached data if fresh enough.
    pub fn get_info(&mut self, dir: &Path) -> Option<GitInfo> {
        // Canonicalize to handle symlinks (e.g., /tmp → /private/tmp on macOS)
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());

        let repo_root = get_repo_root(&dir)?;

        // Check cache freshness
        let needs_refresh = self
            .cache
            .get(&repo_root)
            .map(|e| e.last_refresh.elapsed().as_millis() > REFRESH_INTERVAL_MS)
            .unwrap_or(true);

        if needs_refresh {
            if let Some(entry) = query_repo(&dir, &repo_root) {
                self.cache.insert(repo_root.clone(), entry);
            } else {
                self.cache.remove(&repo_root);
                return None;
            }
        }

        let entry = self.cache.get(&repo_root)?;

        // Filter statuses to entries visible in `dir`
        let dir_relative = dir.strip_prefix(&repo_root).unwrap_or(Path::new(""));
        let dir_prefix = if dir_relative == Path::new("") {
            String::new()
        } else {
            let mut s = dir_relative.to_string_lossy().to_string();
            if !s.ends_with('/') {
                s.push('/');
            }
            s
        };

        let mut statuses = HashMap::new();
        for (path, &status) in &entry.all_statuses {
            let name = if dir_prefix.is_empty() {
                // At repo root: first path component
                path.split('/').next().unwrap_or(path).to_string()
            } else if let Some(rest) = path.strip_prefix(&dir_prefix) {
                // In a subdirectory: first component after the prefix
                rest.split('/').next().unwrap_or(rest).to_string()
            } else {
                continue; // not in this directory
            };

            if name.is_empty() {
                continue;
            }

            // Keep the highest-priority status for directory aggregation
            statuses
                .entry(name)
                .and_modify(|existing: &mut GitFileStatus| {
                    if status.priority() > existing.priority() {
                        *existing = status;
                    }
                })
                .or_insert(status);
        }

        // Compute repo-wide totals
        let mut total_modified = 0;
        let mut total_added = 0;
        let mut total_deleted = 0;
        let mut total_untracked = 0;
        let mut total_renamed = 0;
        let mut total_conflict = 0;
        for status in entry.all_statuses.values() {
            match status {
                GitFileStatus::Modified => total_modified += 1,
                GitFileStatus::Added => total_added += 1,
                GitFileStatus::Deleted => total_deleted += 1,
                GitFileStatus::Untracked => total_untracked += 1,
                GitFileStatus::Renamed => total_renamed += 1,
                GitFileStatus::Conflict => total_conflict += 1,
            }
        }

        Some(GitInfo {
            branch: entry.branch.clone(),
            statuses,
            ahead: entry.ahead,
            behind: entry.behind,
            total_modified,
            total_added,
            total_deleted,
            total_untracked,
            total_renamed,
            total_conflict,
        })
    }

    /// Force refresh for a specific directory's repo.
    pub fn invalidate(&mut self, dir: &Path) {
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        if let Some(root) = get_repo_root(&dir) {
            self.cache.remove(&root);
        }
    }
}

/// Query ahead/behind counts against origin/<branch> using rev-list.
/// Falls back when porcelain output doesn't include tracking info.
fn query_ahead_behind(dir: &Path, branch: &str) -> Option<(usize, usize)> {
    // Try origin/<branch> as the remote ref
    let remote_ref = format!("origin/{}", branch);
    let output = Command::new("git")
        .args(["rev-list", "--left-right", "--count", &format!("{}...{}", branch, remote_ref)])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = text.trim().split('\t').collect();
    if parts.len() == 2 {
        let ahead = parts[0].parse::<usize>().unwrap_or(0);
        let behind = parts[1].parse::<usize>().unwrap_or(0);
        Some((ahead, behind))
    } else {
        None
    }
}

/// Get the repo root for a directory. Returns None if not in a git repo.
fn get_repo_root(dir: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    // Canonicalize the root too for consistent comparison
    Some(std::fs::canonicalize(&root).unwrap_or_else(|_| PathBuf::from(root)))
}

/// Query a repo for branch and all file statuses. Single git call.
fn query_repo(dir: &Path, _repo_root: &Path) -> Option<CacheEntry> {
    // `git status --branch --porcelain=v1` combines branch + status in one call.
    // Use -unormal (not -uall) to avoid listing every file in untracked dirs.
    let output = Command::new("git")
        .args(["status", "--branch", "--porcelain=v1", "-unormal"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();

    // First line: "## branch...tracking [ahead N, behind M]"
    let first_line = lines.next().unwrap_or("");
    let header = first_line.strip_prefix("## ").unwrap_or(first_line);

    // Parse branch name first (needed for ahead/behind fallback)
    let branch_part = header.split("...").next().unwrap_or(header);
    let branch_part = branch_part.split(' ').next().unwrap_or(branch_part);
    let branch = if branch_part == "HEAD" {
        Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()
            .map(|o| {
                let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                if s.is_empty() { "HEAD".to_string() } else { s }
            })
            .unwrap_or_else(|| "HEAD".to_string())
    } else {
        branch_part.to_string()
    };

    // Parse ahead/behind from "[ahead N, behind M]" in porcelain output
    let mut ahead = 0;
    let mut behind = 0;
    if let Some(bracket_start) = header.find('[') {
        let bracket_content = &header[bracket_start..];
        if let Some(n) = bracket_content
            .find("ahead ")
            .and_then(|i| bracket_content[i + 6..].split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|s| s.parse::<usize>().ok())
        {
            ahead = n;
        }
        if let Some(n) = bracket_content
            .find("behind ")
            .and_then(|i| bracket_content[i + 7..].split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|s| s.parse::<usize>().ok())
        {
            behind = n;
        }
    } else {
        // No tracking info in porcelain — fall back to rev-list against origin/<branch>
        if let Some((a, b)) = query_ahead_behind(dir, &branch) {
            ahead = a;
            behind = b;
        }
    }

    // Remaining lines: status entries
    let mut all_statuses = HashMap::new();
    for line in lines {
        if line.len() < 4 {
            continue;
        }
        let bytes = line.as_bytes();
        let x = bytes[0];
        let y = bytes[1];
        let path_str = &line[3..];

        // Handle renames: "R  old -> new"
        let file_path = if let Some(arrow_pos) = path_str.find(" -> ") {
            &path_str[arrow_pos + 4..]
        } else {
            path_str
        };

        // Unquote git-quoted paths
        let file_path = file_path.trim_matches('"');

        if let Some(status) = GitFileStatus::from_porcelain(x, y) {
            all_statuses
                .entry(file_path.to_string())
                .and_modify(|existing: &mut GitFileStatus| {
                    if status.priority() > existing.priority() {
                        *existing = status;
                    }
                })
                .or_insert(status);
        }
    }

    Some(CacheEntry {
        branch,
        ahead,
        behind,
        all_statuses,
        last_refresh: Instant::now(),
    })
}
