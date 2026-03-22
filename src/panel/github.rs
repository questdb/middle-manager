use serde::Deserialize;
use std::path::Path;
use std::process::{Command, Stdio};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PrInfo {
    pub number: u64,
    pub title: String,
    pub url: String,
    pub state: String,
    pub checks: PrCheckStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrCheckStatus {
    /// All checks passed.
    Pass,
    /// Some checks failed.
    Fail,
    /// Checks are still running.
    Pending,
    /// No checks configured.
    None,
}

impl PrCheckStatus {
    pub fn marker(&self) -> &'static str {
        match self {
            Self::Pass => "\u{2713}",    // ✓
            Self::Fail => "\u{2717}",    // ✗
            Self::Pending => "\u{25cb}", // ○
            Self::None => "",
        }
    }
}

#[derive(Deserialize)]
struct GhPrItem {
    number: u64,
    title: String,
    url: String,
    state: String,
    #[serde(rename = "statusCheckRollup", default)]
    status_check_rollup: Vec<GhCheckRun>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct GhCheckRun {
    #[serde(default)]
    status: String,
    #[serde(default)]
    conclusion: String,
    #[serde(default)]
    name: String,
}

/// Query GitHub for a PR associated with the given branch.
/// Returns None if `gh` is not installed, not authenticated, or no PR exists.
/// Skips default branches (master, main, develop) which don't have PRs.
pub fn query_pr_info(dir: &Path, branch: &str) -> Option<PrInfo> {
    if branch.is_empty() || branch == "HEAD" {
        return None;
    }

    // Default/trunk branches don't have PRs — skip the network call
    if matches!(branch, "master" | "main" | "develop" | "dev" | "trunk") {
        return None;
    }

    // Use `gh pr view` which finds the PR for this specific branch
    let output = Command::new("gh")
        .args([
            "pr", "view",
            "--json", "number,title,state,statusCheckRollup,url",
        ])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let item: GhPrItem = serde_json::from_str(&text).ok()?;

    let checks = if item.status_check_rollup.is_empty() {
        PrCheckStatus::None
    } else {
        let any_fail = item.status_check_rollup.iter().any(|c| {
            c.conclusion == "FAILURE" || c.conclusion == "ERROR" || c.conclusion == "CANCELLED"
        });
        let any_pending = item.status_check_rollup.iter().any(|c| {
            c.status != "COMPLETED"
        });
        if any_fail {
            PrCheckStatus::Fail
        } else if any_pending {
            PrCheckStatus::Pending
        } else {
            PrCheckStatus::Pass
        }
    };

    Some(PrInfo {
        number: item.number,
        title: item.title,
        url: item.url,
        state: item.state,
        checks,
    })
}

/// Open a URL in the default browser.
pub fn open_url(url: &str) {
    #[cfg(target_os = "macos")]
    let _ = Command::new("open").arg(url).spawn();
    #[cfg(target_os = "linux")]
    let _ = Command::new("xdg-open").arg(url).spawn();
}

/// Get the checks detail URL for a PR (the "Checks" tab).
#[allow(dead_code)]
pub fn checks_url(pr_url: &str) -> String {
    format!("{}/checks", pr_url)
}
