use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// A CI check run (workflow job).
#[derive(Debug, Clone)]
pub struct CiCheck {
    pub workflow_name: String,
    pub job_name: String,
    pub status: CiStatus,
    pub details_url: String,
    /// GitHub Actions: runs/{run_id}/job/{job_id}
    pub run_id: u64,
    pub job_id: u64,
    /// Azure DevOps: org, project, buildId, jobId
    pub azure_info: Option<AzureInfo>,
}

#[derive(Debug, Clone)]
pub struct AzureInfo {
    pub org: String,
    pub project: String,
    pub build_id: String,
    pub job_id: String,
}

impl CiCheck {
    pub fn display_name(&self) -> String {
        if self.workflow_name.is_empty() {
            self.job_name.clone()
        } else {
            format!("{} / {}", self.workflow_name, self.job_name)
        }
    }

    pub fn is_github_actions(&self) -> bool {
        self.details_url.contains("github.com") && self.job_id > 0
    }
}

/// A step within a CI job.
#[derive(Debug, Clone)]
pub struct CiStep {
    pub number: u64,
    pub name: String,
    pub status: CiStatus,
    /// Direct log URL (Azure DevOps provides this per-step).
    pub log_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CiStatus {
    Success,
    Failure,
    Pending,
    Skipped,
    Cancelled,
}

impl CiStatus {
    pub fn marker(&self) -> &'static str {
        match self {
            Self::Success => "\u{2713}",   // ✓
            Self::Failure => "\u{2717}",   // ✗
            Self::Pending => "\u{25cb}",   // ○
            Self::Skipped => "\u{2013}",   // –
            Self::Cancelled => "\u{2717}", // ✗
        }
    }

    pub fn sort_priority(&self) -> u8 {
        match self {
            Self::Failure | Self::Cancelled => 0,
            Self::Pending => 1,
            Self::Success => 2,
            Self::Skipped => 3,
        }
    }
}

/// A tree item in the CI panel — either a check or a step under a check.
#[derive(Debug, Clone)]
pub enum TreeItem {
    Check {
        check: CiCheck,
        expanded: bool,
        loading: bool,
    },
    Step {
        step: CiStep,
        check_idx: usize, // index of parent check in the checks vec
    },
}

/// What the CI panel is showing.
#[derive(Debug, Clone)]
pub enum CiView {
    /// Tree of checks with expandable steps.
    Tree {
        checks: Vec<CiCheck>,
        items: Vec<TreeItem>,
        selected: usize,
        scroll: usize,
    },
    /// Loading state.
    Loading(String),
    /// Error state.
    Error(String),
}

use crate::azure_auth::SPINNER;

/// Active log download state.
pub struct LogDownload {
    pub step_name: String,
    pub output_path: PathBuf,
    rx: std::sync::mpsc::Receiver<Result<(), String>>,
}

impl LogDownload {
    /// Start an async download with optional Azure auth. Returns immediately.
    pub fn start(url: &str, output_path: PathBuf, step_name: String) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let url = url.to_string();
        let path = output_path.clone();
        // Capture auth args on the calling thread (before spawning)
        let auth = if url.contains("dev.azure.com") {
            azure_auth_args()
        } else {
            vec![]
        };

        std::thread::spawn(move || {
            let mut cmd = Command::new("curl");
            cmd.args(["-s", "-L", "--compressed", "--max-time", "60", "-o"]);
            cmd.arg(&path);
            for arg in &auth {
                cmd.arg(arg);
            }
            cmd.arg(&url);
            let result = cmd
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(|e| format!("curl failed: {}", e))
                .and_then(|s| {
                    if s.success() {
                        Ok(())
                    } else {
                        Err("Download failed".to_string())
                    }
                });
            let _ = tx.send(result);
        });

        Self {
            step_name,
            output_path,
            rx,
        }
    }

    /// Start download for a GitHub Actions log (zip, then extract specific step).
    /// Start download for a GitHub Actions job log (plain text, per-job API).
    pub fn start_github(
        repo: &str,
        _run_id: u64,
        _step_number: u64,
        step_name: &str,
        output_path: PathBuf,
        job_id: u64,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let path = output_path.clone();

        // Use per-job log API: plain text, no zip, much faster
        let url = format!("repos/{}/actions/jobs/{}/logs", repo, job_id);

        std::thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let out_file = std::fs::File::create(&path)
                    .map_err(|e| format!("Failed to create file: {}", e))?;

                let output = Command::new("gh")
                    .args(["api", &url, "-H", "Accept: application/vnd.github+json"])
                    .stdout(out_file)
                    .stderr(Stdio::null())
                    .output()
                    .map_err(|e| format!("Failed to download: {}", e))?;

                if !output.status.success() {
                    return Err("Failed to download job log".to_string());
                }
                Ok(())
            })();
            let _ = tx.send(result);
        });

        Self {
            step_name: step_name.to_string(),
            output_path,
            rx,
        }
    }

    /// Check if download is complete. Returns Some(Ok(())) on success, Some(Err) on failure, None if still downloading.
    pub fn poll(&self) -> Option<Result<(), String>> {
        self.rx.try_recv().ok()
    }

    /// Get current downloaded bytes.
    pub fn downloaded_bytes(&self) -> u64 {
        std::fs::metadata(&self.output_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    pub fn progress_text(&self, tick: usize) -> String {
        let downloaded = self.downloaded_bytes();
        let spinner = SPINNER[tick % SPINNER.len()];
        format!(
            "{} Downloading {} — {}",
            spinner,
            self.step_name,
            format_size(downloaded)
        )
    }
}

fn format_size(bytes: u64) -> String {
    crate::remote_fs::format_size(bytes)
}

type ChecksReceiver = std::sync::mpsc::Receiver<Result<(Option<u64>, Vec<CiCheck>), String>>;
type PendingStepFetch = (
    usize,
    usize,
    CiCheck,
    std::sync::mpsc::Receiver<Result<Vec<CiStep>, String>>,
);

/// CI panel state.
pub struct CiPanel {
    pub view: CiView,
    pub repo: String,
    pub branch: String,
    pub pr_number: Option<u64>,
    /// Receiver for async check fetches.
    pending_checks: Option<ChecksReceiver>,
    /// Spinner tick counter.
    pub spinner_tick: usize,
    /// Visible height (set by renderer).
    pub visible_height: usize,
    /// Active log download.
    pub download: Option<LogDownload>,
    /// Pending async step fetch: (item_index, check_idx, check, receiver)
    pending_steps: Option<PendingStepFetch>,
    /// Active failure extraction.
    pub failure_extraction: Option<FailureExtraction>,
    /// The check that last failed with an Azure auth error — used to auto-retry
    /// after the user completes authentication in the dialog.
    pending_auth_retry: Option<CiCheck>,
    /// Substring filter applied to check display names (case-insensitive).
    /// When non-empty, hides non-matching checks (and steps under them).
    pub filter: String,
    /// True while the user is editing the filter (typed chars go into it).
    pub filter_editing: bool,
}

/// Indices of items that should be shown given the current filter.
/// When filter is empty, returns 0..items.len(). When non-empty, includes
/// each Check whose display_name contains the filter, plus any Steps whose
/// parent check matches.
pub fn compute_visible(filter: &str, items: &[TreeItem], checks: &[CiCheck]) -> Vec<usize> {
    if filter.is_empty() {
        return (0..items.len()).collect();
    }
    let needle = filter.to_lowercase();
    items
        .iter()
        .enumerate()
        .filter(|(_, item)| match item {
            TreeItem::Check { check, .. } => check.display_name().to_lowercase().contains(&needle),
            TreeItem::Step { check_idx, .. } => checks
                .get(*check_idx)
                .map(|c| c.display_name().to_lowercase().contains(&needle))
                .unwrap_or(false),
        })
        .map(|(i, _)| i)
        .collect()
}

impl CiPanel {
    /// Create the panel and start fetching checks asynchronously.
    pub fn for_branch(dir: &Path, branch: &str) -> Self {
        let repo = detect_repo(dir).unwrap_or_default();
        if repo.is_empty() {
            return Self {
                view: CiView::Error("Not a GitHub repository".to_string()),
                repo,
                branch: branch.to_string(),
                pr_number: None,
                pending_checks: None,
                spinner_tick: 0,
                visible_height: 0,
                download: None,
                pending_steps: None,
                failure_extraction: None,
                pending_auth_retry: None,
                filter: String::new(),
                filter_editing: false,
            };
        }

        // Spawn async fetch
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = dir.to_path_buf();
        let branch_str = branch.to_string();
        let branch_clone = branch_str.clone();
        std::thread::spawn(move || {
            let result = query_checks(&dir, &branch_clone);
            let _ = tx.send(result);
        });

        Self {
            view: CiView::Loading("Fetching checks...".to_string()),
            repo,
            branch: branch_str,
            pr_number: None,
            pending_checks: Some(rx),
            spinner_tick: 0,
            visible_height: 0,
            download: None,
            pending_steps: None,
            failure_extraction: None,
            pending_auth_retry: None,
            filter: String::new(),
            filter_editing: false,
        }
    }

    /// Poll for async results. Call on each tick.
    /// Returns an error message if step expansion failed (for status bar display).
    pub fn poll(&mut self) -> Option<String> {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);
        let step_err = self.poll_steps();

        if let Some(ref rx) = self.pending_checks {
            if let Ok(result) = rx.try_recv() {
                self.pending_checks = None;
                match result {
                    Ok((pr_number, mut checks)) => {
                        self.pr_number = pr_number;
                        checks.sort_by(|a, b| {
                            a.status.sort_priority().cmp(&b.status.sort_priority())
                        });
                        let items = checks
                            .iter()
                            .map(|c| TreeItem::Check {
                                check: c.clone(),
                                expanded: false,
                                loading: false,
                            })
                            .collect();
                        self.view = CiView::Tree {
                            checks,
                            items,
                            selected: 0,
                            scroll: 0,
                        };
                    }
                    Err(e) => {
                        self.view = CiView::Error(e);
                    }
                }
            } else {
                let c = SPINNER[self.spinner_tick % SPINNER.len()];
                self.view = CiView::Loading(format!("{} Fetching checks...", c));
            }
        }
        step_err
    }

    /// Poll for async step fetch results.
    /// Returns an error message if expansion failed.
    fn poll_steps(&mut self) -> Option<String> {
        if let Some((item_idx, check_idx, ref check, ref rx)) = self.pending_steps {
            if let Ok(result) = rx.try_recv() {
                let check = check.clone();
                self.pending_steps = None;

                if let CiView::Tree { items, .. } = &mut self.view {
                    match result {
                        Ok(steps) => {
                            items[item_idx] = TreeItem::Check {
                                check: check.clone(),
                                expanded: true,
                                loading: false,
                            };
                            let step_items: Vec<TreeItem> = steps
                                .into_iter()
                                .map(|step| TreeItem::Step { step, check_idx })
                                .collect();
                            for (i, item) in step_items.into_iter().enumerate() {
                                items.insert(item_idx + 1 + i, item);
                            }
                            return None;
                        }
                        Err(e) => {
                            // Revert to collapsed
                            let failed_check = check.clone();
                            items[item_idx] = TreeItem::Check {
                                check,
                                expanded: false,
                                loading: false,
                            };
                            // Remember the check if this looks like an auth failure,
                            // so we can auto-retry after the user authenticates.
                            let lower = e.to_lowercase();
                            if lower.contains("authentication required")
                                || lower.contains("authentication failed")
                            {
                                self.pending_auth_retry = Some(failed_check);
                            }
                            return Some(e);
                        }
                    }
                }
            } else {
                // Still loading — update the check's loading state
                if let CiView::Tree { items, .. } = &mut self.view {
                    if let Some(TreeItem::Check { loading, .. }) = items.get_mut(item_idx) {
                        *loading = true;
                    }
                }
            }
        }
        None
    }

    /// Check if a download completed. Returns the output path on success.
    pub fn poll_download(&mut self) -> Option<Result<PathBuf, String>> {
        if let Some(ref dl) = self.download {
            if let Some(result) = dl.poll() {
                let path = dl.output_path.clone();
                self.download = None;
                return Some(result.map(|_| path));
            }
        }
        None
    }

    /// Toggle expand/collapse on a check, or view log on a step.
    /// Returns Some((run_id, step)) if a step was activated for log viewing.
    pub fn enter(&mut self) -> Option<(u64, CiStep)> {
        let repo = self.repo.clone();
        let filter = self.filter.clone();
        match &mut self.view {
            CiView::Tree {
                checks,
                items,
                selected,
                ..
            } => {
                let visible = compute_visible(&filter, items, checks);
                let sel = visible.get(*selected).copied()?;
                match items.get(sel) {
                    Some(TreeItem::Check { expanded: true, .. }) => {
                        // Collapse: remove step items below
                        Self::collapse_at(items, sel);
                        None
                    }
                    Some(TreeItem::Check { check, .. }) => {
                        let check = check.clone();

                        if !check.is_github_actions() && check.azure_info.is_none() {
                            // Can't fetch steps — hint to use browser
                            return None;
                        }

                        let check_idx = checks
                            .iter()
                            .position(|c| c.details_url == check.details_url)
                            .unwrap_or(0);

                        // Mark as loading
                        items[sel] = TreeItem::Check {
                            check: check.clone(),
                            expanded: false,
                            loading: true,
                        };

                        // Spawn async step fetch
                        let (tx, rx) = std::sync::mpsc::channel();
                        let azure_info = check.azure_info.clone();
                        let job_id = check.job_id;
                        std::thread::spawn(move || {
                            let result = if let Some(ref azure) = azure_info {
                                query_azure_steps(azure)
                            } else {
                                query_steps(&repo, job_id)
                            };
                            let _ = tx.send(result);
                        });

                        self.pending_steps = Some((sel, check_idx, check, rx));
                        None
                    }
                    Some(TreeItem::Step { step, check_idx }) => {
                        // Activate step for log viewing
                        checks
                            .get(*check_idx)
                            .map(|check| (check.run_id, step.clone()))
                    }
                    None => None,
                }
            }
            _ => None,
        }
    }

    /// If a previous step expansion failed due to Azure auth, restart it.
    /// Returns true if a retry was started.
    pub fn retry_pending_auth(&mut self) -> bool {
        let Some(check) = self.pending_auth_retry.take() else {
            return false;
        };
        let CiView::Tree { checks, items, .. } = &mut self.view else {
            return false;
        };
        // Find the item index for this check (URLs are unique)
        let item_idx = match items
            .iter()
            .position(|it| matches!(it, TreeItem::Check { check: c, .. } if c.details_url == check.details_url))
        {
            Some(i) => i,
            None => return false,
        };
        let check_idx = checks
            .iter()
            .position(|c| c.details_url == check.details_url)
            .unwrap_or(0);

        // Mark as loading
        items[item_idx] = TreeItem::Check {
            check: check.clone(),
            expanded: false,
            loading: true,
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let azure_info = check.azure_info.clone();
        let job_id = check.job_id;
        let repo = self.repo.clone();
        std::thread::spawn(move || {
            let result = if let Some(ref azure) = azure_info {
                query_azure_steps(azure)
            } else {
                query_steps(&repo, job_id)
            };
            let _ = tx.send(result);
        });
        self.pending_steps = Some((item_idx, check_idx, check, rx));
        true
    }

    fn collapse_at(items: &mut Vec<TreeItem>, check_pos: usize) {
        // Mark as collapsed
        if let Some(TreeItem::Check { check, .. }) = items.get(check_pos) {
            let check = check.clone();
            items[check_pos] = TreeItem::Check {
                check,
                expanded: false,
                loading: false,
            };
        }
        // Remove all Step items immediately following
        while items.len() > check_pos + 1 {
            if matches!(items[check_pos + 1], TreeItem::Step { .. }) {
                items.remove(check_pos + 1);
            } else {
                break;
            }
        }
    }

    /// Get the details URL for the selected item (for 'o' browser open).
    pub fn selected_url(&self) -> Option<&str> {
        match &self.view {
            CiView::Tree {
                checks,
                items,
                selected,
                ..
            } => {
                let visible = compute_visible(&self.filter, items, checks);
                let item_idx = visible.get(*selected).copied()?;
                match items.get(item_idx)? {
                    TreeItem::Check { check, .. } => Some(&check.details_url),
                    TreeItem::Step { check_idx, .. } => {
                        checks.get(*check_idx).map(|c| c.details_url.as_str())
                    }
                }
            }
            _ => None,
        }
    }

    /// Left arrow: collapse if on expanded check, jump to parent if on step.
    pub fn collapse_or_parent(&mut self) {
        let filter = self.filter.clone();
        if let CiView::Tree {
            items,
            checks,
            selected,
            scroll,
        } = &mut self.view
        {
            let visible = compute_visible(&filter, items, checks);
            let Some(item_idx) = visible.get(*selected).copied() else {
                return;
            };
            match &items[item_idx] {
                TreeItem::Check { expanded: true, .. } => {
                    Self::collapse_at(items, item_idx);
                }
                TreeItem::Step { .. } => {
                    // Jump to parent check (previous Check item in visible list)
                    for vp in (0..*selected).rev() {
                        if let Some(&i) = visible.get(vp) {
                            if matches!(items[i], TreeItem::Check { .. }) {
                                *selected = vp;
                                if *selected < *scroll {
                                    *scroll = *selected;
                                }
                                break;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn move_up(&mut self) {
        if let CiView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            if *selected > 0 {
                *selected -= 1;
                if *selected < *scroll {
                    *scroll = *selected;
                }
            }
        }
    }

    pub fn move_down(&mut self) {
        let vh = self.visible_height;
        let filter = self.filter.clone();
        if let CiView::Tree {
            selected,
            scroll,
            items,
            checks,
        } = &mut self.view
        {
            let n = compute_visible(&filter, items, checks).len();
            if *selected + 1 < n {
                *selected += 1;
                if vh > 0 && *selected >= *scroll + vh {
                    *scroll = *selected - vh + 1;
                }
            }
        }
    }

    pub fn page_up(&mut self) {
        let vh = self.visible_height.max(1);
        if let CiView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            *selected = selected.saturating_sub(vh);
            if *selected < *scroll {
                *scroll = *selected;
            }
        }
    }

    pub fn page_down(&mut self) {
        let vh = self.visible_height.max(1);
        let filter = self.filter.clone();
        if let CiView::Tree {
            selected,
            scroll,
            items,
            checks,
        } = &mut self.view
        {
            let n = compute_visible(&filter, items, checks).len();
            let max = n.saturating_sub(1);
            *selected = (*selected + vh).min(max);
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }

    pub fn move_to_top(&mut self) {
        if let CiView::Tree {
            selected, scroll, ..
        } = &mut self.view
        {
            *selected = 0;
            *scroll = 0;
        }
    }

    pub fn move_to_bottom(&mut self) {
        let vh = self.visible_height;
        let filter = self.filter.clone();
        if let CiView::Tree {
            selected,
            scroll,
            items,
            checks,
        } = &mut self.view
        {
            let n = compute_visible(&filter, items, checks).len();
            let max = n.saturating_sub(1);
            *selected = max;
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }

    // ---- Filter editing ----

    /// Begin editing the filter (keeps existing filter text).
    pub fn filter_open(&mut self) {
        self.filter_editing = true;
    }

    /// Append a character to the filter; auto-starts editing so plain typing
    /// (without a leading F7) opens the filter on the first keystroke.
    pub fn filter_input(&mut self, c: char) {
        self.filter_editing = true;
        self.filter.push(c);
        self.clamp_to_visible();
    }

    /// Remove the last character of the filter while editing.
    pub fn filter_backspace(&mut self) {
        if !self.filter_editing {
            return;
        }
        self.filter.pop();
        self.clamp_to_visible();
    }

    /// Commit the filter and exit editing mode (filter remains active).
    pub fn filter_accept(&mut self) {
        self.filter_editing = false;
    }

    /// Clear the filter and exit editing mode.
    pub fn filter_cancel(&mut self) {
        self.filter.clear();
        self.filter_editing = false;
        self.clamp_to_visible();
    }

    /// Clamp selected/scroll to the current visible list length.
    fn clamp_to_visible(&mut self) {
        let filter = self.filter.clone();
        let vh = self.visible_height;
        if let CiView::Tree {
            items,
            checks,
            selected,
            scroll,
        } = &mut self.view
        {
            let n = compute_visible(&filter, items, checks).len();
            if n == 0 {
                *selected = 0;
                *scroll = 0;
                return;
            }
            *selected = (*selected).min(n - 1);
            if *scroll > *selected {
                *scroll = *selected;
            }
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }
}

// ============================================================
// CI Failure Extraction
// ============================================================

/// A single extracted test failure.
#[derive(Debug, Clone)]
pub struct TestFailure {
    pub check_name: String,
    pub test_name: String,
    /// Error message or failure context (if available).
    pub failure_info: Option<String>,
}

/// Batch failure extraction state.
pub struct FailureExtraction {
    rx: std::sync::mpsc::Receiver<Result<Vec<TestFailure>, String>>,
    /// Progress updates: "Downloading log 3/14: Run tests (linux-arm64)"
    progress_rx: std::sync::mpsc::Receiver<String>,
    pub progress: String,
}

impl FailureExtraction {
    /// Start extracting failures from all failed checks in the background.
    pub fn start(repo: String, checks: Vec<CiCheck>) -> Self {
        let failed_count = checks
            .iter()
            .filter(|c| c.status == CiStatus::Failure)
            .count();
        crate::debug_log::log(&format!(
            "FailureExtraction::start: {} total checks, {} failed",
            checks.len(),
            failed_count
        ));

        let (tx, rx) = std::sync::mpsc::channel();
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let result = extract_all_failures(&repo, &checks, &progress_tx);
            crate::debug_log::log(&format!(
                "FailureExtraction complete: {:?}",
                result.as_ref().map(|v| v.len())
            ));
            let _ = tx.send(result);
        });

        Self {
            rx,
            progress_rx,
            progress: "Starting extraction...".to_string(),
        }
    }

    /// Poll for completion. Returns Some when done. Also updates progress.
    pub fn poll(&mut self) -> Option<Result<Vec<TestFailure>, String>> {
        // Drain progress updates
        while let Ok(msg) = self.progress_rx.try_recv() {
            self.progress = msg;
        }
        self.rx.try_recv().ok()
    }
}

/// Check if `gh` CLI is authenticated. Returns true if auth is configured.
pub fn check_gh_auth() -> bool {
    Command::new("gh")
        .args(["auth", "status"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Check if Azure DevOps credentials are available (PAT or Bearer token).
pub fn has_azure_pat() -> bool {
    get_azure_pat().is_some() || crate::azure_auth::get_bearer_token().is_some()
}

/// Check if a PAT is specifically stored (env var or keychain).
pub fn has_stored_pat() -> bool {
    get_azure_pat().is_some()
}

/// Check GitHub API rate limit. Returns (remaining, limit) or None if check fails.
fn check_github_rate_limit() -> Option<(u32, u32)> {
    let output = Command::new("gh")
        .args([
            "api",
            "rate_limit",
            "--jq",
            ".resources.core | \"\\(.remaining) \\(.limit)\"",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut parts = text.split_whitespace();
    let remaining: u32 = parts.next()?.parse().ok()?;
    let limit: u32 = parts.next()?.parse().ok()?;
    Some((remaining, limit))
}

fn extract_all_failures(
    repo: &str,
    checks: &[CiCheck],
    progress: &std::sync::mpsc::Sender<String>,
) -> Result<Vec<TestFailure>, String> {
    // Check GitHub rate limit before starting
    let has_github_checks = checks.iter().any(|c| c.details_url.contains("github.com"));
    if has_github_checks {
        if let Some((remaining, limit)) = check_github_rate_limit() {
            crate::debug_log::log(&format!(
                "GitHub rate limit: {}/{} remaining",
                remaining, limit
            ));
            let _ = progress.send(format!(
                "GitHub API: {}/{} requests remaining",
                remaining, limit
            ));
            if remaining < 10 {
                return Err(format!(
                    "GitHub API rate limit nearly exhausted ({}/{}). Try again later.",
                    remaining, limit
                ));
            }
        }
    }

    let mut all_failures = Vec::new();
    let total = checks
        .iter()
        .filter(|c| c.status == CiStatus::Failure)
        .count();
    let mut current = 0;

    for check in checks {
        if check.status != CiStatus::Failure {
            continue;
        }

        current += 1;
        let check_name = check.display_name();

        crate::debug_log::log(&format!(
            "Extracting: {} (job_id={}, run_id={}, is_gh={}, has_azure={})",
            check_name,
            check.job_id,
            check.run_id,
            check.is_github_actions(),
            check.azure_info.is_some(),
        ));

        // Azure DevOps: use test results API (structured test names + error messages)
        if let Some(ref azure) = check.azure_info {
            let _ = progress.send(format!(
                "Check {}/{}: {} (querying test results)",
                current, total, check_name
            ));
            if let Some(failures) = query_azure_test_results(azure, &check_name) {
                crate::debug_log::log(&format!(
                    "Got {} failures from Azure test results API for {}",
                    failures.len(),
                    check_name
                ));
                all_failures.extend(failures);
            } else {
                crate::debug_log::log(&format!(
                    "Azure test results API returned no results for {}",
                    check_name
                ));
            }
        }
        // GitHub: use check run annotations API (structured error annotations)
        else if check.details_url.contains("github.com") && check.job_id > 0 {
            let _ = progress.send(format!(
                "Check {}/{}: {} (querying annotations)",
                current, total, check_name
            ));
            let failures = query_github_annotations(repo, check.job_id, &check_name);
            crate::debug_log::log(&format!(
                "Got {} failures from GitHub annotations for {}",
                failures.len(),
                check_name
            ));
            all_failures.extend(failures);
        } else {
            crate::debug_log::log(&format!("Skipping {}: no API method available", check_name));
        }
    }

    // Report final rate limit status
    if has_github_checks {
        if let Some((remaining, limit)) = check_github_rate_limit() {
            let _ = progress.send(format!(
                "Done. GitHub API: {}/{} requests remaining",
                remaining, limit
            ));
            crate::debug_log::log(&format!(
                "GitHub rate limit after extraction: {}/{}",
                remaining, limit
            ));
        }
    }

    Ok(all_failures)
}

/// Query GitHub check run annotations for failure info.
/// Uses `gh api` to fetch annotations which contain test failure details.
fn query_github_annotations(repo: &str, job_id: u64, check_name: &str) -> Vec<TestFailure> {
    let url = format!("repos/{}/check-runs/{}/annotations", repo, job_id);
    let output = Command::new("gh")
        .args(["api", &url, "--paginate"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(),
    };

    let text = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let annotations = match json.as_array() {
        Some(a) => a,
        None => return Vec::new(),
    };

    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for ann in annotations {
        let level = ann
            .get("annotation_level")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if level != "failure" && level != "error" {
            continue;
        }

        let title = ann.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let message = ann.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let path = ann.get("path").and_then(|v| v.as_str()).unwrap_or("");

        // Build test name from title or path
        let test_name = if !title.is_empty() {
            title.to_string()
        } else if !path.is_empty() {
            path.to_string()
        } else {
            continue;
        };

        if seen.insert(test_name.clone()) {
            let failure_info = if !message.is_empty() {
                // Truncate very long messages
                let msg = if message.len() > 500 {
                    // Find a valid UTF-8 char boundary at or before byte 500.
                    let mut end = 500;
                    while end > 0 && !message.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}...", &message[..end])
                } else {
                    message.to_string()
                };
                Some(msg)
            } else {
                None
            };
            failures.push(TestFailure {
                check_name: check_name.to_string(),
                test_name,
                failure_info,
            });
        }
    }

    failures
}

/// Get Azure DevOps PAT from environment or system keyring.
fn get_azure_pat() -> Option<String> {
    // 1. Environment variable
    if let Ok(pat) = std::env::var("AZURE_DEVOPS_PAT") {
        if !pat.is_empty() {
            return Some(pat);
        }
    }
    // 2. macOS Keychain
    if cfg!(target_os = "macos") {
        if let Ok(output) = Command::new("security")
            .args([
                "find-generic-password",
                "-s",
                "middle-manager",
                "-a",
                "azure-pat",
                "-w",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
        {
            if output.status.success() {
                let pat = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !pat.is_empty() {
                    return Some(pat);
                }
            }
        }
    }
    // 3. Linux keyring via secret-tool (GNOME Keyring / KDE Wallet)
    if !cfg!(target_os = "macos") {
        if let Ok(output) = Command::new("secret-tool")
            .args([
                "lookup",
                "service",
                "middle-manager",
                "account",
                "azure-pat",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
        {
            if output.status.success() {
                let pat = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !pat.is_empty() {
                    return Some(pat);
                }
            }
        }
    }
    None
}

/// Store Azure DevOps PAT in the system keyring.
/// Returns Ok(()) on success or Err with a message.
pub fn store_azure_pat(pat: &str) -> Result<(), String> {
    if cfg!(target_os = "macos") {
        // macOS `security add-generic-password` takes the PAT as argv (-w),
        // which is briefly visible to other local users via `ps`. The alternative
        // (interactive prompt) is unusable from a TUI. We use `-U` to upsert in
        // a single command, keeping the exposure window as short as possible.
        let status = Command::new("security")
            .args([
                "add-generic-password",
                "-U",
                "-s",
                "middle-manager",
                "-a",
                "azure-pat",
                "-l",
                "middle-manager azure PAT",
                "-w",
                pat,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| format!("Failed to run security: {}", e))?;
        if status.success() {
            Ok(())
        } else {
            Err("Failed to store PAT in macOS Keychain".to_string())
        }
    } else {
        // Linux: secret-tool store
        let mut child = Command::new("secret-tool")
            .args([
                "store",
                "--label",
                "middle-manager azure PAT",
                "service",
                "middle-manager",
                "account",
                "azure-pat",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to run secret-tool: {}", e))?;
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin
                .write_all(pat.as_bytes())
                .map_err(|e| format!("Failed to write PAT: {}", e))?;
        }
        let status = child
            .wait()
            .map_err(|e| format!("secret-tool failed: {}", e))?;
        if status.success() {
            Ok(())
        } else {
            Err("Failed to store PAT in keyring".to_string())
        }
    }
}

/// Build curl auth args for Azure DevOps (PAT or Bearer token).
fn azure_auth_args() -> Vec<String> {
    // 1. Try PAT (Basic auth)
    if let Some(pat) = get_azure_pat() {
        let encoded = crate::clipboard::base64_encode(format!(":{}", pat).as_bytes());
        return vec![
            "-H".to_string(),
            format!("Authorization: Basic {}", encoded),
        ];
    }
    // 2. Try Bearer token from device code flow
    if let Some(token) = crate::azure_auth::get_bearer_token() {
        return vec!["-H".to_string(), format!("Authorization: Bearer {}", token)];
    }
    vec![]
}

/// Try to get test failures directly from Azure DevOps test results API.
/// Returns None if the API is not accessible (auth required), or Some(vec) if it works.
fn query_azure_test_results(azure: &AzureInfo, check_name: &str) -> Option<Vec<TestFailure>> {
    // Step 1: Get test runs for this build
    let runs_url = format!(
        "https://dev.azure.com/{}/{}/_apis/test/runs?buildUri=vstfs:///Build/Build/{}&api-version=7.1",
        azure.org, azure.project, azure.build_id
    );

    let auth = azure_auth_args();
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-L", "--compressed", "--max-time", "10"]);
    for arg in &auth {
        cmd.arg(arg);
    }
    cmd.arg(&runs_url);
    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    // Check if we got HTML (auth redirect) instead of JSON
    if text.contains("<!DOCTYPE") || text.contains("<html") {
        crate::debug_log::log("Azure test results API: got HTML (auth required), skipping");
        return None;
    }

    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
    let runs = json.get("value")?.as_array()?;

    if runs.is_empty() {
        crate::debug_log::log("Azure test results API: no test runs found for this build");
        return None;
    }

    crate::debug_log::log(&format!(
        "Azure test results API: found {} test runs",
        runs.len()
    ));

    // Step 2: Get failed results from each run
    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for run in runs {
        let run_id = match run.get("id").and_then(|v| v.as_u64()) {
            Some(id) => id,
            None => continue,
        };
        let results_url = format!(
            "https://dev.azure.com/{}/{}/_apis/test/runs/{}/results?outcomes=Failed&$top=500&api-version=7.1",
            azure.org, azure.project, run_id
        );

        let mut cmd = Command::new("curl");
        cmd.args(["-s", "-L", "--compressed", "--max-time", "10"]);
        for arg in &auth {
            cmd.arg(arg);
        }
        cmd.arg(&results_url);
        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;

        if !output.status.success() {
            continue;
        }

        let text = String::from_utf8_lossy(&output.stdout);
        if text.contains("<!DOCTYPE") {
            continue;
        }

        let json: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue, // Skip unparseable runs, don't lose previous results
        };
        if let Some(results) = json.get("value").and_then(|v| v.as_array()) {
            for result in results {
                let test_name = result
                    .get("automatedTestName")
                    .or_else(|| result.get("testCaseTitle"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !test_name.is_empty() && seen.insert(test_name.to_string()) {
                    let error_msg = result
                        .get("errorMessage")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    failures.push(TestFailure {
                        check_name: check_name.to_string(),
                        test_name: test_name.to_string(),
                        failure_info: error_msg,
                    });
                }
            }
        }
    }

    Some(failures)
}

/// Write consolidated failures to a Markdown file.
pub fn write_failures_file(
    path: &Path,
    failures: &[TestFailure],
    repo: &str,
    pr_number: Option<u64>,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;

    let pr_str = pr_number.map(|n| format!(" PR #{}", n)).unwrap_or_default();
    writeln!(f, "# CI Failures — {}{}", repo, pr_str)?;
    writeln!(f)?;

    if failures.is_empty() {
        writeln!(f, "No test failures found in the logs.")?;
        writeln!(f)?;
        writeln!(
            f,
            "The failed checks may not contain recognizable test output."
        )?;
        return Ok(());
    }

    // Group by check name, preserving failure info
    let mut by_check: std::collections::BTreeMap<&str, Vec<&TestFailure>> =
        std::collections::BTreeMap::new();
    for fail in failures {
        by_check.entry(&fail.check_name).or_default().push(fail);
    }

    for (check, tests) in &by_check {
        writeln!(f, "## {}", check)?;
        for test in tests {
            writeln!(f, "- **{}**", test.test_name)?;
            if let Some(ref info) = test.failure_info {
                // Indent failure info as a code block under the test name
                writeln!(f, "  ```")?;
                for info_line in info.lines().take(10) {
                    writeln!(f, "  {}", info_line)?;
                }
                writeln!(f, "  ```")?;
            }
        }
        writeln!(f)?;
    }

    let unique: std::collections::HashSet<&str> =
        failures.iter().map(|f| f.test_name.as_str()).collect();
    writeln!(f, "---")?;
    writeln!(
        f,
        "{} unique failure(s) across {} check(s).",
        unique.len(),
        by_check.len()
    )?;

    // Append rate limit info if available
    if let Some((remaining, limit)) = check_github_rate_limit() {
        writeln!(
            f,
            "GitHub API rate limit: {}/{} remaining.",
            remaining, limit
        )?;
    }

    Ok(())
}

/// Detect the GitHub owner/repo from the current directory.
fn detect_repo(dir: &Path) -> Option<String> {
    let output = Command::new("gh")
        .args([
            "repo",
            "view",
            "--json",
            "nameWithOwner",
            "--jq",
            ".nameWithOwner",
        ])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

/// Query check runs for a branch's latest PR or commit.
fn query_checks(dir: &Path, _branch: &str) -> Result<(Option<u64>, Vec<CiCheck>), String> {
    let output = Command::new("gh")
        .args(["pr", "view", "--json", "number,statusCheckRollup"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to run gh: {}", e))?;

    if !output.status.success() {
        return Err("No PR found for this branch".to_string());
    }

    let text = String::from_utf8_lossy(&output.stdout);

    #[derive(Deserialize)]
    struct PrResponse {
        #[serde(default)]
        number: Option<u64>,
        #[serde(rename = "statusCheckRollup", default)]
        checks: Vec<GhCheck>,
    }

    #[derive(Deserialize)]
    struct GhCheck {
        #[serde(default)]
        name: String,
        #[serde(default)]
        status: String,
        #[serde(default)]
        conclusion: String,
        #[serde(rename = "workflowName", default)]
        workflow_name: String,
        #[serde(rename = "detailsUrl", default)]
        details_url: String,
    }

    let pr: PrResponse =
        serde_json::from_str(&text).map_err(|e| format!("Failed to parse response: {}", e))?;

    let checks = pr
        .checks
        .into_iter()
        .filter(|c| !c.name.is_empty() && !c.details_url.is_empty())
        .map(|c| {
            let status = match (c.status.as_str(), c.conclusion.as_str()) {
                (_, "SUCCESS") => CiStatus::Success,
                (_, "FAILURE") | (_, "ERROR") => CiStatus::Failure,
                (_, "CANCELLED") => CiStatus::Cancelled,
                (_, "SKIPPED") => CiStatus::Skipped,
                ("COMPLETED", _) => CiStatus::Success,
                _ => CiStatus::Pending,
            };
            let (run_id, job_id, azure_info) = parse_details_url(&c.details_url);
            CiCheck {
                workflow_name: c.workflow_name,
                job_name: c.name,
                status,
                details_url: c.details_url,
                run_id,
                job_id,
                azure_info,
            }
        })
        .collect();

    Ok((pr.number, checks))
}

/// Parse run_id and job_id from a GitHub Actions details URL.
fn parse_details_url(url: &str) -> (u64, u64, Option<AzureInfo>) {
    // GitHub: https://github.com/owner/repo/actions/runs/{run_id}/job/{job_id}
    if url.contains("github.com") {
        let mut run_id = 0u64;
        let mut job_id = 0u64;
        if let Some(runs_pos) = url.find("/runs/") {
            let after = &url[runs_pos + 6..];
            if let Some(slash) = after.find('/') {
                run_id = after[..slash].parse().unwrap_or(0);
                if let Some(job_pos) = after.find("/job/") {
                    job_id = after[job_pos + 5..].parse().unwrap_or(0);
                }
            }
        }
        return (run_id, job_id, None);
    }

    // Azure: https://dev.azure.com/{org}/{project}/_build/results?buildId={id}&view=logs&jobId={id}
    if url.contains("dev.azure.com") {
        let azure = parse_azure_url(url);
        return (0, 0, azure);
    }

    (0, 0, None)
}

fn parse_azure_url(url: &str) -> Option<AzureInfo> {
    // https://dev.azure.com/{org}/{project}/_build/results?buildId={id}&view=logs&jobId={id}
    let after_host = url.strip_prefix("https://dev.azure.com/")?;
    let parts: Vec<&str> = after_host.split('/').collect();
    if parts.len() < 2 {
        return None;
    }
    let org = parts[0].to_string();
    let project = parts[1].to_string();

    let build_id = url
        .find("buildId=")
        .map(|i| {
            let after = &url[i + 8..];
            after.split('&').next().unwrap_or("").to_string()
        })
        .unwrap_or_default();

    let job_id = url
        .find("jobId=")
        .map(|i| {
            let after = &url[i + 6..];
            after.split('&').next().unwrap_or("").to_string()
        })
        .unwrap_or_default();

    if build_id.is_empty() {
        return None;
    }

    Some(AzureInfo {
        org,
        project,
        build_id,
        job_id,
    })
}

/// Query steps for a specific job.
fn query_steps(repo: &str, job_id: u64) -> Result<Vec<CiStep>, String> {
    let output = Command::new("gh")
        .args([
            "api",
            &format!("repos/{}/actions/jobs/{}", repo, job_id),
            "--jq",
            ".steps[] | [.number, .name, .status, .conclusion] | @tsv",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to run gh: {}", e))?;

    if !output.status.success() {
        return Err("Failed to fetch job steps".to_string());
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let steps = text
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 4 {
                return None;
            }
            let number = parts[0].parse().unwrap_or(0);
            let name = parts[1].to_string();
            let status = match (parts[2], parts[3]) {
                (_, "success") => CiStatus::Success,
                (_, "failure") => CiStatus::Failure,
                (_, "cancelled") => CiStatus::Cancelled,
                (_, "skipped") => CiStatus::Skipped,
                ("completed", _) => CiStatus::Success,
                _ => CiStatus::Pending,
            };
            Some(CiStep {
                number,
                name,
                status,
                log_url: None,
            })
        })
        .collect();

    Ok(steps)
}

/// Query steps for an Azure DevOps job via the timeline API.
fn query_azure_steps(azure: &AzureInfo) -> Result<Vec<CiStep>, String> {
    let url = format!(
        "https://dev.azure.com/{}/{}/_apis/build/builds/{}/timeline?api-version=7.0",
        azure.org, azure.project, azure.build_id
    );

    let auth = azure_auth_args();
    let mut cmd = Command::new("curl");
    cmd.args([
        "-s",
        "-L",
        "--compressed",
        "--max-time",
        "15",
        "-w",
        "\n__HTTP_STATUS__:%{http_code}",
    ]);
    for arg in &auth {
        cmd.arg(arg);
    }
    cmd.arg(&url);
    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to fetch Azure timeline: {}", e))?;

    if !output.status.success() {
        return Err("Failed to fetch Azure timeline".to_string());
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    // Split body and status written by curl's -w flag.
    let (body, http_status) = match raw.rsplit_once("\n__HTTP_STATUS__:") {
        Some((b, s)) => (b, s.trim().parse::<u16>().unwrap_or(0)),
        None => (raw.as_ref(), 0),
    };

    // Check for HTML auth redirect or 401/403.
    let looks_html = body.contains("<!DOCTYPE") || body.contains("<html");
    if http_status == 401 || http_status == 403 || looks_html {
        crate::debug_log::log(&format!(
            "Azure timeline API: http_status={} body_len={} auth={}",
            http_status,
            body.len(),
            if auth.is_empty() {
                "none"
            } else {
                "bearer/basic"
            }
        ));
        if auth.is_empty() {
            return Err("Azure DevOps authentication required — open the auth dialog.".to_string());
        }
        return Err(format!(
            "Azure DevOps authentication failed (HTTP {}). \
             The stored token may be for the wrong tenant, expired, or lack \
             Azure DevOps scope. Try a different auth method.",
            http_status
        ));
    }

    // Azure returns 404 with a BuildNotFoundException when the build has been
    // deleted by retention policy or a user. Surface the server's message
    // (e.g. "The requested build N has been deleted.") instead of failing in
    // the Timeline JSON parser below.
    if http_status == 404 {
        let msg = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| {
                v.get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
            });
        return Err(msg.unwrap_or_else(|| {
            format!(
                "Azure build {} not found (it may have been deleted).",
                azure.build_id
            )
        }));
    }

    let text = body;

    #[derive(Deserialize)]
    struct Timeline {
        records: Vec<TimelineRecord>,
    }

    #[derive(Deserialize)]
    struct TimelineRecord {
        #[serde(default)]
        id: String,
        #[serde(rename = "parentId")]
        parent_id: Option<String>,
        #[serde(rename = "type", default)]
        record_type: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        result: Option<String>,
        #[serde(default)]
        state: Option<String>,
        #[serde(default)]
        log: Option<LogRef>,
        #[serde(default)]
        order: Option<u64>,
    }

    #[derive(Deserialize)]
    struct LogRef {
        id: u64,
    }

    let timeline: Timeline =
        serde_json::from_str(text).map_err(|e| format!("Failed to parse Azure timeline: {}", e))?;

    // Find tasks belonging to the target job
    let job_id = if azure.job_id.is_empty() {
        // No specific job — find the first failed job, or the first job
        timeline
            .records
            .iter()
            .find(|r| r.record_type == "Job" && r.result.as_deref() == Some("failed"))
            .or_else(|| timeline.records.iter().find(|r| r.record_type == "Job"))
            .map(|r| r.id.clone())
            .unwrap_or_default()
    } else {
        azure.job_id.clone()
    };

    let mut steps: Vec<CiStep> = timeline
        .records
        .iter()
        .filter(|r| r.record_type == "Task" && r.parent_id.as_deref() == Some(&job_id))
        .map(|r| {
            let status = match r.result.as_deref() {
                Some("succeeded") => CiStatus::Success,
                Some("failed") => CiStatus::Failure,
                Some("skipped") => CiStatus::Skipped,
                Some("canceled") | Some("cancelled") => CiStatus::Cancelled,
                _ => match r.state.as_deref() {
                    Some("completed") => CiStatus::Success,
                    _ => CiStatus::Pending,
                },
            };
            let log_url = r.log.as_ref().map(|l| {
                format!(
                    "https://dev.azure.com/{}/{}/_apis/build/builds/{}/logs/{}?api-version=7.0",
                    azure.org, azure.project, azure.build_id, l.id
                )
            });
            CiStep {
                number: r.order.unwrap_or(0),
                name: r.name.clone(),
                status,
                log_url,
            }
        })
        .collect();

    steps.sort_by_key(|s| s.number);
    Ok(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // parse_details_url: GitHub Actions
    // ---------------------------------------------------------------

    #[test]
    fn parse_details_url_github() {
        let url = "https://github.com/owner/repo/actions/runs/12345/job/67890";
        let (run_id, job_id, azure) = parse_details_url(url);
        assert_eq!(run_id, 12345);
        assert_eq!(job_id, 67890);
        assert!(azure.is_none());
    }

    #[test]
    fn parse_details_url_github_no_job() {
        let url = "https://github.com/owner/repo/actions/runs/12345/";
        let (run_id, job_id, azure) = parse_details_url(url);
        assert_eq!(run_id, 12345);
        assert_eq!(job_id, 0); // no /job/ segment
        assert!(azure.is_none());
    }

    // ---------------------------------------------------------------
    // parse_details_url / parse_azure_url: Azure DevOps
    // ---------------------------------------------------------------

    #[test]
    fn parse_details_url_azure() {
        let url = "https://dev.azure.com/myorg/myproject/_build/results?buildId=999&view=logs&jobId=abc-123";
        let (run_id, job_id, azure) = parse_details_url(url);
        assert_eq!(run_id, 0);
        assert_eq!(job_id, 0);
        let azure = azure.unwrap();
        assert_eq!(azure.org, "myorg");
        assert_eq!(azure.project, "myproject");
        assert_eq!(azure.build_id, "999");
        assert_eq!(azure.job_id, "abc-123");
    }

    #[test]
    fn parse_azure_url_no_build_id() {
        let url = "https://dev.azure.com/org/proj/_build/results?view=logs";
        let azure = parse_azure_url(url);
        assert!(azure.is_none()); // buildId is required
    }

    // ---------------------------------------------------------------
    // parse_details_url: unknown URL
    // ---------------------------------------------------------------

    #[test]
    fn parse_details_url_unknown() {
        let (run_id, job_id, azure) = parse_details_url("https://example.com/status/123");
        assert_eq!(run_id, 0);
        assert_eq!(job_id, 0);
        assert!(azure.is_none());
    }

    // ---------------------------------------------------------------
    // CiStatus helpers
    // ---------------------------------------------------------------

    #[test]
    fn ci_status_sort_priority_ordering() {
        // Failures should sort first (lowest priority number)
        assert!(CiStatus::Failure.sort_priority() < CiStatus::Pending.sort_priority());
        assert!(CiStatus::Pending.sort_priority() < CiStatus::Success.sort_priority());
        assert!(CiStatus::Success.sort_priority() < CiStatus::Skipped.sort_priority());
    }

    fn make_check(workflow: &str, job: &str) -> CiCheck {
        CiCheck {
            workflow_name: workflow.to_string(),
            job_name: job.to_string(),
            status: CiStatus::Success,
            details_url: format!("https://example.com/{}/{}", workflow, job),
            run_id: 0,
            job_id: 0,
            azure_info: None,
        }
    }

    #[test]
    fn compute_visible_empty_filter_returns_all() {
        let checks = vec![make_check("CI", "build"), make_check("CI", "lint")];
        let items: Vec<TreeItem> = checks
            .iter()
            .map(|c| TreeItem::Check {
                check: c.clone(),
                expanded: false,
                loading: false,
            })
            .collect();
        let visible = compute_visible("", &items, &checks);
        assert_eq!(visible, vec![0, 1]);
    }

    #[test]
    fn compute_visible_filters_check_by_name_case_insensitive() {
        let checks = vec![
            make_check("CI", "Build (linux)"),
            make_check("CI", "lint"),
            make_check("CI", "test (BUILD)"),
        ];
        let items: Vec<TreeItem> = checks
            .iter()
            .map(|c| TreeItem::Check {
                check: c.clone(),
                expanded: false,
                loading: false,
            })
            .collect();
        let visible = compute_visible("build", &items, &checks);
        assert_eq!(visible, vec![0, 2]); // both contain "build" (case-insensitive)
    }

    #[test]
    fn compute_visible_includes_steps_under_matching_check() {
        let checks = vec![make_check("CI", "build"), make_check("CI", "lint")];
        // items: c0 (build), s0a, s0b, c1 (lint), s1a
        let items = vec![
            TreeItem::Check {
                check: checks[0].clone(),
                expanded: true,
                loading: false,
            },
            TreeItem::Step {
                step: CiStep {
                    number: 1,
                    name: "step1".to_string(),
                    status: CiStatus::Success,
                    log_url: None,
                },
                check_idx: 0,
            },
            TreeItem::Step {
                step: CiStep {
                    number: 2,
                    name: "step2".to_string(),
                    status: CiStatus::Success,
                    log_url: None,
                },
                check_idx: 0,
            },
            TreeItem::Check {
                check: checks[1].clone(),
                expanded: true,
                loading: false,
            },
            TreeItem::Step {
                step: CiStep {
                    number: 1,
                    name: "step1".to_string(),
                    status: CiStatus::Success,
                    log_url: None,
                },
                check_idx: 1,
            },
        ];
        // Filter "build" matches c0 → c0 and its steps visible; c1 (lint) hidden.
        let visible = compute_visible("build", &items, &checks);
        assert_eq!(visible, vec![0, 1, 2]);
    }

    #[test]
    fn compute_visible_no_matches_returns_empty() {
        let checks = vec![make_check("CI", "build")];
        let items: Vec<TreeItem> = checks
            .iter()
            .map(|c| TreeItem::Check {
                check: c.clone(),
                expanded: false,
                loading: false,
            })
            .collect();
        let visible = compute_visible("xyz_nope", &items, &checks);
        assert!(visible.is_empty());
    }

    #[test]
    fn ci_check_display_name() {
        let check = CiCheck {
            workflow_name: "CI".to_string(),
            job_name: "build".to_string(),
            status: CiStatus::Success,
            details_url: String::new(),
            run_id: 0,
            job_id: 0,
            azure_info: None,
        };
        assert_eq!(check.display_name(), "CI / build");

        let check_no_workflow = CiCheck {
            workflow_name: String::new(),
            job_name: "lint".to_string(),
            status: CiStatus::Success,
            details_url: String::new(),
            run_id: 0,
            job_id: 0,
            azure_info: None,
        };
        assert_eq!(check_no_workflow.display_name(), "lint");
    }
}
