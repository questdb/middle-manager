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

const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// Active log download state.
pub struct LogDownload {
    pub step_name: String,
    pub output_path: PathBuf,
    rx: std::sync::mpsc::Receiver<Result<(), String>>,
}

impl LogDownload {
    /// Start an async download. Returns immediately.
    pub fn start(url: &str, output_path: PathBuf, step_name: String) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let url = url.to_string();
        let path = output_path.clone();

        std::thread::spawn(move || {
            let result = Command::new("curl")
                .args(["-s", "-L", "-o"])
                .arg(&path)
                .arg(&url)
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
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
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
        }
    }

    /// Poll for async results. Call on each tick.
    pub fn poll(&mut self) {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);
        self.poll_steps();

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
    }

    /// Poll for async step fetch results.
    fn poll_steps(&mut self) {
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
                        }
                        Err(_) => {
                            // Revert to collapsed
                            items[item_idx] = TreeItem::Check {
                                check,
                                expanded: false,
                                loading: false,
                            };
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
        match &mut self.view {
            CiView::Tree {
                checks,
                items,
                selected,
                ..
            } => {
                let sel = *selected;
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
                        checks.get(*check_idx).map(|check| (check.run_id, step.clone()))
                    }
                    None => None,
                }
            }
            _ => None,
        }
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
            } => match items.get(*selected)? {
                TreeItem::Check { check, .. } => Some(&check.details_url),
                TreeItem::Step { check_idx, .. } => checks.get(*check_idx).map(|c| c.details_url.as_str()),
            },
            _ => None,
        }
    }

    /// Left arrow: collapse if on expanded check, jump to parent if on step.
    pub fn collapse_or_parent(&mut self) {
        if let CiView::Tree {
            items,
            selected,
            scroll,
            ..
        } = &mut self.view
        {
            let sel = *selected;
            match &items[sel] {
                TreeItem::Check { expanded: true, .. } => {
                    Self::collapse_at(items, sel);
                }
                TreeItem::Step { .. } => {
                    // Jump to parent check
                    for i in (0..sel).rev() {
                        if matches!(items[i], TreeItem::Check { .. }) {
                            *selected = i;
                            if *selected < *scroll {
                                *scroll = *selected;
                            }
                            break;
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
        if let CiView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            if *selected + 1 < items.len() {
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
        if let CiView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            let max = items.len().saturating_sub(1);
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
        if let CiView::Tree {
            selected,
            scroll,
            items,
            ..
        } = &mut self.view
        {
            let max = items.len().saturating_sub(1);
            *selected = max;
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
        let failed_count = checks.iter().filter(|c| c.status == CiStatus::Failure).count();
        crate::debug_log::log(&format!(
            "FailureExtraction::start: {} total checks, {} failed",
            checks.len(), failed_count
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

        Self { rx, progress_rx, progress: "Starting extraction...".to_string() }
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

/// Check GitHub API rate limit. Returns (remaining, limit) or None if check fails.
fn check_github_rate_limit() -> Option<(u32, u32)> {
    let output = Command::new("gh")
        .args(["api", "rate_limit", "--jq", ".resources.core | \"\\(.remaining) \\(.limit)\""])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut parts = text.trim().split_whitespace();
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
            crate::debug_log::log(&format!("GitHub rate limit: {}/{} remaining", remaining, limit));
            let _ = progress.send(format!("GitHub API: {}/{} requests remaining", remaining, limit));
            if remaining < 10 {
                return Err(format!(
                    "GitHub API rate limit nearly exhausted ({}/{}). Try again later.",
                    remaining, limit
                ));
            }
        }
    }

    let mut all_failures = Vec::new();
    let total = checks.iter().filter(|c| c.status == CiStatus::Failure).count();
    let mut current = 0;

    for check in checks {
        if check.status != CiStatus::Failure {
            continue;
        }

        current += 1;
        let check_name = check.display_name();
        let _ = progress.send(format!("Downloading {}/{}: {}", current, total, check_name));

        // Download the job log
        let tmp_path = std::env::temp_dir().join(format!(
            "mm-ci-log-{}-{}.txt",
            check.job_id,
            std::process::id()
        ));

        crate::debug_log::log(&format!(
            "Extracting: {} (job_id={}, run_id={}, is_gh={}, has_azure={})",
            check_name, check.job_id, check.run_id,
            check.is_github_actions(),
            check.azure_info.is_some(),
        ));

        // Try Azure test results API first (fast path -- no log download needed)
        if let Some(ref azure) = check.azure_info {
            let _ = progress.send(format!("Check {}/{}: {} (querying test results)", current, total, check_name));
            if let Some(failures) = query_azure_test_results(azure, &check_name) {
                if !failures.is_empty() {
                    crate::debug_log::log(&format!("Got {} failures from test results API for {}", failures.len(), check_name));
                    all_failures.extend(failures);
                    let _ = std::fs::remove_file(&tmp_path);
                    continue;
                }
            }
        }

        let download_ok = if check.is_github_actions() {
            let _ = progress.send(format!("Check {}/{}: {} (GitHub job log)", current, total, check_name));
            download_github_log(repo, check.job_id, &tmp_path)
        } else if check.details_url.contains("github.com") && check.run_id > 0 {
            let _ = progress.send(format!("Check {}/{}: {} (GitHub run logs)", current, total, check_name));
            download_github_run_logs(repo, check.run_id, &tmp_path)
        } else if let Some(ref azure) = check.azure_info {
            let _ = progress.send(format!("Check {}/{}: {} (Azure logs fallback)", current, total, check_name));
            download_azure_log(azure, &tmp_path, progress, current, total, &check_name)
        } else {
            crate::debug_log::log(&format!("Skipping {}: no download method available", check_name));
            false
        };

        if !download_ok {
            crate::debug_log::log(&format!("Download failed for {}", check_name));
            continue;
        }

        crate::debug_log::log(&format!("Downloaded log for {}, parsing...", check_name));
        let _ = progress.send(format!("Parsing {}/{}: {}", current, total, check_name));

        // Parse only the tail of the log (failure summaries are at the end)
        if let Ok(content) = std::fs::read_to_string(&tmp_path) {
            let tail_size = 512 * 1024; // 512KB
            let content = if content.len() > tail_size {
                // Take from the last tail_size bytes, but start at a line boundary
                let start = content.len() - tail_size;
                match content[start..].find('\n') {
                    Some(pos) => &content[start + pos + 1..],
                    None => &content[start..],
                }
            } else {
                &content
            };
            crate::debug_log::log(&format!(
                "Log: {} bytes (parsing tail), first 200 chars: {:?}",
                content.len(),
                &content[..content.len().min(200)]
            ));
            let failures = parse_failures(content, &check_name);
            crate::debug_log::log(&format!("Found {} failures in {}", failures.len(), check_name));
            all_failures.extend(failures);
        }

        let _ = std::fs::remove_file(&tmp_path);
    }

    // Report final rate limit status
    if has_github_checks {
        if let Some((remaining, limit)) = check_github_rate_limit() {
            let _ = progress.send(format!(
                "Done. GitHub API: {}/{} requests remaining",
                remaining, limit
            ));
            crate::debug_log::log(&format!("GitHub rate limit after extraction: {}/{}", remaining, limit));
        }
    }

    Ok(all_failures)
}

/// Get Azure DevOps PAT from environment or system keyring.
fn get_azure_pat() -> Option<String> {
    // 1. Environment variable
    if let Ok(pat) = std::env::var("AZURE_DEVOPS_PAT") {
        if !pat.is_empty() {
            return Some(pat);
        }
    }
    // 2. System keyring via secret-tool (GNOME Keyring / KDE Wallet)
    if let Ok(output) = Command::new("secret-tool")
        .args(["lookup", "service", "middle-manager", "account", "azure-pat"])
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
    None
}

/// Build curl auth args for Azure DevOps (if PAT available).
fn azure_auth_args() -> Vec<String> {
    if let Some(pat) = get_azure_pat() {
        // Azure DevOps PAT uses Basic auth with empty username
        let encoded = crate::editor::base64_encode(format!(":{}", pat).as_bytes());
        vec!["-H".to_string(), format!("Authorization: Basic {}", encoded)]
    } else {
        vec![]
    }
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

    crate::debug_log::log(&format!("Azure test results API: found {} test runs", runs.len()));

    // Step 2: Get failed results from each run
    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for run in runs {
        let run_id = run.get("id")?.as_u64()?;
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
                    failures.push(TestFailure {
                        check_name: check_name.to_string(),
                        test_name: test_name.to_string(),
                    });
                }
            }
        }
    }

    Some(failures)
}

fn download_github_log(repo: &str, job_id: u64, output_path: &Path) -> bool {
    let url = format!("repos/{}/actions/jobs/{}/logs", repo, job_id);
    let out_file = match std::fs::File::create(output_path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    Command::new("gh")
        .args(["api", &url, "-H", "Accept: application/vnd.github+json"])
        .stdout(out_file)
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Fallback: download all logs for a GitHub Actions run via `gh run view --log-failed`.
fn download_github_run_logs(repo: &str, run_id: u64, output_path: &Path) -> bool {
    crate::debug_log::log(&format!("Fallback: gh run view --log-failed for run {}", run_id));
    let out_file = match std::fs::File::create(output_path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    // `gh run view <run_id> --log-failed` dumps all failed job logs to stdout
    Command::new("gh")
        .args(["run", "view", &run_id.to_string(), "--log-failed", "-R", repo])
        .stdout(out_file)
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn download_azure_log(
    azure: &AzureInfo,
    output_path: &Path,
    progress: &std::sync::mpsc::Sender<String>,
    check_num: usize,
    check_total: usize,
    check_name: &str,
) -> bool {
    // 1. Fetch timeline to find failed task log IDs
    let timeline_url = format!(
        "https://dev.azure.com/{}/{}/_apis/build/builds/{}/timeline?api-version=7.0",
        azure.org, azure.project, azure.build_id
    );
    crate::debug_log::log(&format!("Azure timeline: {}", timeline_url));

    let auth = azure_auth_args();
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-L", "--compressed"]);
    for arg in &auth {
        cmd.arg(arg);
    }
    cmd.arg(&timeline_url);
    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();

    let timeline_json = match output {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).into_owned();
            if s.is_empty() {
                crate::debug_log::log("Azure: timeline response is empty");
                return false;
            }
            s
        }
        Ok(o) => {
            crate::debug_log::log(&format!("Azure: timeline curl failed with status {}", o.status));
            return false;
        }
        Err(e) => {
            crate::debug_log::log(&format!("Azure: timeline curl error: {}", e));
            return false;
        }
    };

    crate::debug_log::log(&format!(
        "Azure timeline response: {} bytes, first 300: {:?}",
        timeline_json.len(),
        &timeline_json[..timeline_json.len().min(300)]
    ));

    // 2. Parse timeline to find failed records with log IDs
    let timeline: serde_json::Value = match serde_json::from_str(&timeline_json) {
        Ok(v) => v,
        Err(e) => {
            crate::debug_log::log(&format!("Azure timeline JSON parse error: {}", e));
            return false;
        }
    };

    let mut log_ids: Vec<u64> = Vec::new();
    if let Some(records) = timeline.get("records").and_then(|r| r.as_array()) {
        crate::debug_log::log(&format!("Azure: timeline has {} records", records.len()));
        for record in records {
            let name = record.get("name").and_then(|n| n.as_str()).unwrap_or("?");
            let result = record.get("result").and_then(|r| r.as_str()).unwrap_or("");
            let rec_type = record.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let _has_log = record.get("log").and_then(|l| l.get("id")).is_some();
            if result == "failed" || result == "partiallySucceeded" {
                if let Some(log_id) = record.get("log").and_then(|l| l.get("id")).and_then(|i| i.as_u64()) {
                    // Only download Task-level logs that contain test output
                    // Skip Phase/Stage/Job/Checkpoint records and "Upload crash logs" tasks
                    let is_test_task = rec_type == "Task"
                        && (name.contains("test") || name.contains("Test")
                            || name.contains("coverage") || name.contains("Coverage"))
                        && !name.contains("Upload");
                    if is_test_task {
                        crate::debug_log::log(&format!("Azure: will download '{}' log_id={}", name, log_id));
                        log_ids.push(log_id);
                    } else {
                        crate::debug_log::log(&format!("Azure: skipping '{}' type={} (not a test task)", name, rec_type));
                    }
                } else {
                    crate::debug_log::log(&format!("Azure: failed record '{}' type={} but no log ID", name, rec_type));
                }
            }
        }
    } else {
        crate::debug_log::log(&format!("Azure: no 'records' key in timeline. Keys: {:?}",
            timeline.as_object().map(|o| o.keys().collect::<Vec<_>>())));
    }

    if log_ids.is_empty() {
        crate::debug_log::log("Azure: no failed records with log IDs found in timeline");
        return false;
    }

    crate::debug_log::log(&format!("Azure: found {} failed step logs to download", log_ids.len()));

    // 3. Download the TAIL of each failed step's log (last 512KB).
    //    Test failure summaries are always near the end.
    //    Uses HTTP Range header; servers that don't support it return full content.
    let tail_bytes = 512 * 1024; // 512KB
    let range_header = format!("Range: bytes=-{}", tail_bytes);
    let mut combined = String::new();
    for (i, log_id) in log_ids.iter().enumerate() {
        let _ = progress.send(format!(
            "Check {}/{}: {} — fetching log {}/{}",
            check_num, check_total, check_name, i + 1, log_ids.len()
        ));
        crate::debug_log::log(&format!("Azure: downloading log tail {}/{} (id={})", i + 1, log_ids.len(), log_id));
        let log_url = format!(
            "https://dev.azure.com/{}/{}/_apis/build/builds/{}/logs/{}?api-version=7.0",
            azure.org, azure.project, azure.build_id, log_id
        );
        // Use --compressed for gzip transfer, -w for stats, auth if available
        let auth = azure_auth_args();
        let mut cmd = Command::new("curl");
        cmd.args(["-s", "-L", "--compressed", "-H", &range_header,
                   "-w", "\n__CURL_STATS__ %{size_download} %{speed_download} %{time_total}"]);
        for arg in &auth {
            cmd.arg(arg);
        }
        cmd.arg(&log_url);
        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output();
        if let Ok(o) = output {
            if o.status.success() {
                let full_output = String::from_utf8_lossy(&o.stdout);
                // Extract curl stats from the end
                let (text, stats) = if let Some(pos) = full_output.rfind("\n__CURL_STATS__ ") {
                    (&full_output[..pos], &full_output[pos..])
                } else {
                    (full_output.as_ref(), "")
                };
                // Parse stats for progress display
                if !stats.is_empty() {
                    let parts: Vec<&str> = stats.trim().strip_prefix("__CURL_STATS__ ").unwrap_or("").split_whitespace().collect();
                    if parts.len() >= 3 {
                        let size_bytes: f64 = parts[0].parse().unwrap_or(0.0);
                        let speed: f64 = parts[1].parse().unwrap_or(0.0);
                        let time: f64 = parts[2].parse().unwrap_or(0.0);
                        let size_kb = size_bytes / 1024.0;
                        let speed_kb = speed / 1024.0;
                        let _ = progress.send(format!(
                            "Check {}/{}: {} — log {}/{} ({:.0}KB in {:.1}s, {:.0}KB/s)",
                            check_num, check_total, check_name, i + 1, log_ids.len(),
                            size_kb, time, speed_kb
                        ));
                    }
                }
                crate::debug_log::log(&format!("Azure: log {} returned {} bytes", log_id, text.len()));
                combined.push_str(text);
                combined.push('\n');
            }
        }
    }

    if combined.is_empty() {
        return false;
    }

    std::fs::write(output_path, &combined).is_ok()
}

/// Parse a CI log for test failure patterns.
pub fn parse_failures(content: &str, check_name: &str) -> Vec<TestFailure> {
    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Rust: "test path::to::test ... FAILED"
        if trimmed.starts_with("test ") && trimmed.ends_with("FAILED") {
            if let Some(name) = trimmed
                .strip_prefix("test ")
                .and_then(|rest| rest.strip_suffix(" ... FAILED"))
            {
                let name = name.trim();
                if !name.is_empty() && seen.insert(name.to_string()) {
                    failures.push(TestFailure {
                        check_name: check_name.to_string(),
                        test_name: name.to_string(),
                    });
                }
            }
        }
        // Go: "--- FAIL: TestName (0.01s)"
        else if trimmed.starts_with("--- FAIL:") {
            let rest = trimmed.strip_prefix("--- FAIL:").unwrap().trim();
            let name = rest.split_whitespace().next().unwrap_or(rest);
            if !name.is_empty() && seen.insert(name.to_string()) {
                failures.push(TestFailure {
                    check_name: check_name.to_string(),
                    test_name: name.to_string(),
                });
            }
        }
        // Python/pytest: "FAILED path/to/test.py::TestClass::test_method"
        else if trimmed.starts_with("FAILED ") {
            let name = trimmed.strip_prefix("FAILED ").unwrap().trim();
            // Strip trailing info like " - AssertionError"
            let name = name.split(" - ").next().unwrap_or(name).trim();
            if !name.is_empty() && seen.insert(name.to_string()) {
                failures.push(TestFailure {
                    check_name: check_name.to_string(),
                    test_name: name.to_string(),
                });
            }
        }
        // Jest/vitest: "FAIL src/path/file.test.ts"
        else if trimmed.starts_with("FAIL ") && !trimmed.starts_with("FAILED") {
            let name = trimmed.strip_prefix("FAIL ").unwrap().trim();
            if !name.is_empty() && seen.insert(name.to_string()) {
                failures.push(TestFailure {
                    check_name: check_name.to_string(),
                    test_name: name.to_string(),
                });
            }
        }
        // Java/Maven/Gradle: "Tests run: X, Failures: Y" or specific test names
        // Surefire: "  testMethodName(com.example.TestClass)  Time elapsed: 0.1 s  <<< FAILURE!"
        else if trimmed.contains("<<< FAILURE!") || trimmed.contains("<<< ERROR!") {
            // Extract test name: "  testMethod(com.pkg.Class)"
            let name = trimmed.split("Time elapsed").next().unwrap_or(trimmed).trim();
            if !name.is_empty() && name.len() < 200 && seen.insert(name.to_string()) {
                failures.push(TestFailure {
                    check_name: check_name.to_string(),
                    test_name: name.to_string(),
                });
            }
        }
        // Java/JUnit: "  testName  FAILED" (Gradle output)
        else if trimmed.ends_with(" FAILED") && !trimmed.starts_with("test ") && !trimmed.contains("TASK") {
            let name = trimmed.strip_suffix(" FAILED").unwrap_or(trimmed).trim();
            // Looks like a test name if it starts with a letter and has no spaces (or is qualified)
            if !name.is_empty() && !name.contains(' ') && name.len() < 200 {
                if seen.insert(name.to_string()) {
                    failures.push(TestFailure {
                        check_name: check_name.to_string(),
                        test_name: name.to_string(),
                    });
                }
            }
        }
        // Maven Surefire summary: "Failed tests:"  followed by "  methodName(ClassName)"
        // We catch the individual lines after "Failed tests:" header
        else if trimmed.starts_with("Failed tests:") || trimmed.starts_with("Tests in error:") {
            // The header line itself -- next lines are the actual test names
            // Just skip the header, the indented lines below are caught by other patterns
        }
        // GitHub Actions error annotation: "##[error]..."
        else if trimmed.starts_with("##[error]") {
            let msg = trimmed.strip_prefix("##[error]").unwrap().trim();
            // Only capture if it looks like a test name (contains :: or / or .)
            if (msg.contains("::") || msg.contains('/') || msg.contains('.')) && msg.len() < 200 {
                if seen.insert(msg.to_string()) {
                    failures.push(TestFailure {
                        check_name: check_name.to_string(),
                        test_name: msg.to_string(),
                    });
                }
            }
        }
        // Azure DevOps: "##[error]  testName(className) <<< FAILURE!"
        // (caught by the <<< FAILURE! check above, but also match raw ##[error] lines)

        // Indented test name (common in Maven/Gradle failure summaries): "  methodName(com.pkg.Class)"
        else if trimmed.starts_with("  ") && trimmed.contains('(') && trimmed.contains(')') && trimmed.len() < 200 {
            let name = trimmed.trim();
            if !name.starts_with("at ") && !name.starts_with("//") && seen.insert(name.to_string()) {
                failures.push(TestFailure {
                    check_name: check_name.to_string(),
                    test_name: name.to_string(),
                });
            }
        }
    }

    failures
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

    let pr_str = pr_number
        .map(|n| format!(" PR #{}", n))
        .unwrap_or_default();
    writeln!(f, "# CI Failures — {}{}", repo, pr_str)?;
    writeln!(f)?;

    if failures.is_empty() {
        writeln!(f, "No test failures found in the logs.")?;
        writeln!(f)?;
        writeln!(f, "The failed checks may not contain recognizable test output.")?;
        return Ok(());
    }

    // Group by check name
    let mut by_check: std::collections::BTreeMap<&str, Vec<&str>> = std::collections::BTreeMap::new();
    for fail in failures {
        by_check
            .entry(&fail.check_name)
            .or_default()
            .push(&fail.test_name);
    }

    for (check, tests) in &by_check {
        writeln!(f, "## {}", check)?;
        for test in tests {
            writeln!(f, "- {}", test)?;
        }
        writeln!(f)?;
    }

    let unique: std::collections::HashSet<&str> = failures.iter().map(|f| f.test_name.as_str()).collect();
    writeln!(f, "---")?;
    writeln!(
        f,
        "{} unique failure(s) across {} check(s).",
        unique.len(),
        by_check.len()
    )?;

    // Append rate limit info if available
    if let Some((remaining, limit)) = check_github_rate_limit() {
        writeln!(f, "GitHub API rate limit: {}/{} remaining.", remaining, limit)?;
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

    let output = Command::new("curl")
        .args(["-s", &url])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to fetch Azure timeline: {}", e))?;

    if !output.status.success() {
        return Err("Failed to fetch Azure timeline".to_string());
    }

    let text = String::from_utf8_lossy(&output.stdout);

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

    let timeline: Timeline = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse Azure timeline: {}", e))?;

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
