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

    pub fn ci_provider(&self) -> &'static str {
        if self.details_url.contains("dev.azure.com") {
            "Azure DevOps"
        } else if self.details_url.contains("github.com") {
            "GitHub Actions"
        } else {
            "External CI"
        }
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
        #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub total_bytes: Option<u64>,
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
            total_bytes: None,
            rx,
        }
    }

    /// Start download for a GitHub Actions log (zip, then extract specific step).
    pub fn start_github(
        repo: &str,
        run_id: u64,
        step_number: u64,
        step_name: &str,
        output_path: PathBuf,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let repo = repo.to_string();
        let step_name_clone = step_name.to_string();
        let path = output_path.clone();

        std::thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let tmp_dir = std::env::temp_dir().join(format!("mm_ci_logs_{}", run_id));
                let _ = std::fs::create_dir_all(&tmp_dir);
                let zip_path = tmp_dir.join("logs.zip");

                let zip_file = std::fs::File::create(&zip_path)
                    .map_err(|e| format!("Failed to create temp file: {}", e))?;

                let output = Command::new("gh")
                    .args([
                        "api",
                        &format!("repos/{}/actions/runs/{}/logs", repo, run_id),
                        "-H",
                        "Accept: application/vnd.github+json",
                    ])
                    .stdout(zip_file)
                    .stderr(Stdio::null())
                    .output()
                    .map_err(|e| format!("Failed to download: {}", e))?;

                if !output.status.success() {
                    return Err("Failed to download logs".to_string());
                }

                let _ = Command::new("unzip")
                    .args(["-o", "-q"])
                    .arg(&zip_path)
                    .arg("-d")
                    .arg(&tmp_dir)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();

                // GitHub Actions bundles all steps into one file per job.
                // Try step-level match first, then fall back to any log file.
                let log_file = find_step_log(&tmp_dir, step_number, &step_name_clone)
                    .or_else(|| find_any_log(&tmp_dir));

                if let Some(log_file) = log_file {
                    std::fs::copy(&log_file, &path)
                        .map_err(|e| format!("Failed to copy log: {}", e))?;
                    Ok(())
                } else {
                    Err("No log files found in archive".to_string())
                }
            })();
            let _ = tx.send(result);
        });

        Self {
            step_name: step_name.to_string(),
            output_path,
            total_bytes: None,
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
    /// Receiver for async check fetches.
    pending_checks: Option<std::sync::mpsc::Receiver<Result<Vec<CiCheck>, String>>>,
    /// Spinner tick counter.
    pub spinner_tick: usize,
    /// Visible height (set by renderer).
    pub visible_height: usize,
    /// Active log download.
    pub download: Option<LogDownload>,
}

impl CiPanel {
    /// Create the panel and start fetching checks asynchronously.
    pub fn for_branch(dir: &Path, branch: &str) -> Self {
        let repo = detect_repo(dir).unwrap_or_default();
        if repo.is_empty() {
            return Self {
                view: CiView::Error("Not a GitHub repository".to_string()),
                repo,
                pending_checks: None,
                spinner_tick: 0,
                visible_height: 0,
                download: None,
            };
        }

        // Spawn async fetch
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = dir.to_path_buf();
        let branch = branch.to_string();
        std::thread::spawn(move || {
            let result = query_checks(&dir, &branch);
            let _ = tx.send(result);
        });

        Self {
            view: CiView::Loading("Fetching checks...".to_string()),
            repo,
            pending_checks: Some(rx),
            spinner_tick: 0,
            visible_height: 0,
            download: None,
        }
    }

    /// Poll for async results. Call on each tick.
    pub fn poll(&mut self) {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);

        if let Some(ref rx) = self.pending_checks {
            if let Ok(result) = rx.try_recv() {
                self.pending_checks = None;
                match result {
                    Ok(mut checks) => {
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
                        // Expand: fetch steps and insert below
                        let check = check.clone();
                        let steps_result = if let Some(ref azure) = check.azure_info {
                            query_azure_steps(azure)
                        } else if check.is_github_actions() {
                            query_steps(&repo, check.job_id)
                        } else {
                            Err(format!("{} — press 'o' to open in browser", check.ci_provider()))
                        };

                        // Find which check index this is
                        let check_idx = checks.iter().position(|c| c.details_url == check.details_url).unwrap_or(0);

                        match steps_result {
                            Ok(steps) => {
                                // Mark as expanded
                                items[sel] = TreeItem::Check {
                                    check: check.clone(),
                                    expanded: true,
                                    loading: false,
                                };
                                // Insert step items after the check
                                let step_items: Vec<TreeItem> = steps
                                    .into_iter()
                                    .map(|step| TreeItem::Step { step, check_idx })
                                    .collect();
                                let insert_pos = sel + 1;
                                for (i, item) in step_items.into_iter().enumerate() {
                                    items.insert(insert_pos + i, item);
                                }
                            }
                            Err(_e) => {
                                // Can't expand — just open in browser hint is in the provider name
                            }
                        }
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
fn query_checks(dir: &Path, _branch: &str) -> Result<Vec<CiCheck>, String> {
    let output = Command::new("gh")
        .args([
            "pr", "view",
            "--json", "statusCheckRollup",
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

    Ok(checks)
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

#[allow(dead_code)]
/// Download logs for a run and return the path to the extracted log file.
pub fn download_run_logs(repo: &str, run_id: u64) -> Result<std::path::PathBuf, String> {
    let tmp_dir = std::env::temp_dir().join(format!("mm_ci_logs_{}", run_id));
    let _ = std::fs::create_dir_all(&tmp_dir);

    let zip_path = tmp_dir.join("logs.zip");

    // Download the log archive
    let output = Command::new("gh")
        .args([
            "api",
            &format!("repos/{}/actions/runs/{}/logs", repo, run_id),
            "-H", "Accept: application/vnd.github+json",
        ])
        .stdout(std::fs::File::create(&zip_path).map_err(|e| e.to_string())?)
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to download logs: {}", e))?;

    if !output.status.success() {
        return Err("Failed to download logs".to_string());
    }

    // Extract the zip
    let extract_status = Command::new("unzip")
        .args(["-o", "-q"])
        .arg(&zip_path)
        .arg("-d")
        .arg(&tmp_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| format!("Failed to extract logs: {}", e))?;

    if !extract_status.success() {
        return Err("Failed to extract log archive".to_string());
    }

    Ok(tmp_dir)
}

#[allow(dead_code)]
/// Download a log from a direct URL (Azure DevOps) and return the path.
pub fn download_log_url(url: &str, label: &str) -> Result<std::path::PathBuf, String> {
    let tmp_dir = std::env::temp_dir().join("mm_ci_logs");
    let _ = std::fs::create_dir_all(&tmp_dir);

    let safe_name: String = label
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let path = tmp_dir.join(format!("{}.txt", safe_name));

    let output = Command::new("curl")
        .args(["-s", "-o"])
        .arg(&path)
        .arg(url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to download log: {}", e))?;

    if !output.status.success() {
        return Err("Failed to download log".to_string());
    }

    Ok(path)
}

/// Find the largest log file in the directory (fallback for GitHub Actions).
fn find_any_log(logs_dir: &Path) -> Option<PathBuf> {
    let mut best: Option<(PathBuf, u64)> = None;

    fn scan_dir(dir: &Path, best: &mut Option<(PathBuf, u64)>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_dir(&path, best);
                } else if path.extension().and_then(|e| e.to_str()) == Some("txt") {
                    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    if best.as_ref().map(|(_, s)| size > *s).unwrap_or(true) {
                        *best = Some((path, size));
                    }
                }
            }
        }
    }

    scan_dir(logs_dir, &mut best);
    best.map(|(p, _)| p)
}

/// Find the log file for a specific step within the extracted logs directory.
pub fn find_step_log(logs_dir: &Path, step_number: u64, _step_name: &str) -> Option<std::path::PathBuf> {
    // GitHub Actions log files are named like: "0_stepname.txt" or "StepNumber_StepName.txt"
    if let Ok(entries) = std::fs::read_dir(logs_dir) {
        // First try: look in subdirectories (job-name/step-file)
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Ok(sub_entries) = std::fs::read_dir(&path) {
                    for sub_entry in sub_entries.flatten() {
                        let sub_path = sub_entry.path();
                        let name = sub_path.file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default();
                        if name.starts_with(&format!("{}_", step_number)) && name.ends_with(".txt") {
                            return Some(sub_path);
                        }
                    }
                }
            }
            // Also check top-level files
            let name = path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if name.starts_with(&format!("{}_", step_number)) && name.ends_with(".txt") {
                return Some(path);
            }
        }
    }

    None
}
