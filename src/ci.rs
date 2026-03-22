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

/// CI panel state.
pub struct CiPanel {
    pub view: CiView,
    pub repo: String,
    pub branch: String,
    pub pr_number: Option<u64>,
    /// Receiver for async check fetches.
    pending_checks: Option<std::sync::mpsc::Receiver<Result<(Option<u64>, Vec<CiCheck>), String>>>,
    /// Spinner tick counter.
    pub spinner_tick: usize,
    /// Visible height (set by renderer).
    pub visible_height: usize,
    /// Active log download.
    pub download: Option<LogDownload>,
    /// Pending async step fetch: (item_index, check_idx, check, receiver)
    pending_steps: Option<(usize, usize, CiCheck, std::sync::mpsc::Receiver<Result<Vec<CiStep>, String>>)>,
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
                        let check = &checks[*check_idx];
                        Some((check.run_id, step.clone()))
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
            CiView::Tree { checks, items, selected, .. } => {
                match items.get(*selected)? {
                    TreeItem::Check { check, .. } => Some(&check.details_url),
                    TreeItem::Step { check_idx, .. } => Some(&checks[*check_idx].details_url),
                }
            }
            _ => None,
        }
    }

    /// Left arrow: collapse if on expanded check, jump to parent if on step.
    pub fn collapse_or_parent(&mut self) {
        if let CiView::Tree { items, selected, scroll, .. } = &mut self.view {
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
        if let CiView::Tree { selected, scroll, .. } = &mut self.view {
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
        if let CiView::Tree { selected, scroll, items, .. } = &mut self.view {
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
        if let CiView::Tree { selected, scroll, .. } = &mut self.view {
            *selected = selected.saturating_sub(vh);
            if *selected < *scroll {
                *scroll = *selected;
            }
        }
    }

    pub fn page_down(&mut self) {
        let vh = self.visible_height.max(1);
        if let CiView::Tree { selected, scroll, items, .. } = &mut self.view {
            let max = items.len().saturating_sub(1);
            *selected = (*selected + vh).min(max);
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }

    pub fn move_to_top(&mut self) {
        if let CiView::Tree { selected, scroll, .. } = &mut self.view {
            *selected = 0;
            *scroll = 0;
        }
    }

    pub fn move_to_bottom(&mut self) {
        let vh = self.visible_height;
        if let CiView::Tree { selected, scroll, items, .. } = &mut self.view {
            let max = items.len().saturating_sub(1);
            *selected = max;
            if vh > 0 && *selected >= *scroll + vh {
                *scroll = *selected - vh + 1;
            }
        }
    }
}

/// Detect the GitHub owner/repo from the current directory.
fn detect_repo(dir: &Path) -> Option<String> {
    let output = Command::new("gh")
        .args(["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"])
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
        .args([
            "pr", "view",
            "--json", "number,statusCheckRollup",
        ])
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

    let pr: PrResponse = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse response: {}", e))?;

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

