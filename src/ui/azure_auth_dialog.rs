use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::app::{AzCliStatus, AzureAuthDialogState, AzureAuthMode};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

const DIALOG_WIDTH: u16 = 70;
const DIALOG_HEIGHT: u16 = 12;

// Row layout (inside content area):
//   0: tab bar
//   1: separator
//   2: blank
//   3: labeled input / spinner / account info
//   4: credential status line (✓ stored / dim-not-stored)
//   5: blank
//   6: error (reserved slot)
//   7: separator
//   8: blank
//   9: buttons
const ROW_TABS: u16 = 0;
const ROW_SEP_TOP: u16 = 1;
const ROW_BODY: u16 = 3;
const ROW_ERROR: u16 = 6;
const ROW_SEP_BOTTOM: u16 = 7;
const ROW_BUTTONS: u16 = 9;

pub fn render(frame: &mut Frame, state: &AzureAuthDialogState) -> Rect {
    let t = theme();
    let dbg = t.dialog_bg;
    let layout = dh::render_dialog_frame(
        frame,
        " Azure DevOps Authentication ",
        DIALOG_WIDTH,
        DIALOG_HEIGHT,
    );
    let (normal, highlight, input_normal) = dh::dialog_styles();
    let error_style = Style::default().fg(t.error_fg).bg(dbg);
    let dim = Style::default().fg(t.dialog_hint_fg).bg(dbg);

    render_tabs(frame, &layout, state, normal, highlight, dim);

    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + ROW_SEP_TOP,
        t.dialog_border_style(),
    );

    render_body(frame, &layout, state, normal, highlight, input_normal, dim);

    if let Some(ref err) = state.error {
        dh::render_line(
            frame,
            layout.content,
            ROW_ERROR,
            Line::from(Span::styled(
                format!("{:<w$}", err, w = layout.cw),
                error_style,
            )),
        );
    }

    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + ROW_SEP_BOTTOM,
        t.dialog_border_style(),
    );

    let (ok_label, ok_visible) = match state.mode {
        AzureAuthMode::Pat => ("{ OK }", true),
        AzureAuthMode::Browser => ("{ Login }", state.browser_flow.is_none()),
        AzureAuthMode::AzCli => ("{ Fetch }", !state.az_fetching),
    };
    let mut buttons: Vec<(&str, bool)> = Vec::with_capacity(2);
    if ok_visible {
        buttons.push((ok_label, state.on_ok()));
    }
    buttons.push(("[ Cancel ]", state.on_cancel()));

    dh::render_buttons(
        frame,
        layout.content,
        ROW_BUTTONS,
        &buttons,
        Style::default().fg(t.dialog_text_fg).bg(dbg),
        highlight,
    );

    layout.outer
}

fn render_body(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &AzureAuthDialogState,
    normal: Style,
    highlight: Style,
    input_normal: Style,
    dim: Style,
) {
    let input_focused = state.on_input();
    let input_style = if input_focused {
        highlight
    } else {
        input_normal
    };
    let t = theme();
    let success = Style::default().fg(t.git_added_fg).bg(t.dialog_bg);

    match state.mode {
        AzureAuthMode::Pat => {
            dh::render_labeled_text_input(
                frame,
                layout.content,
                ROW_BODY,
                "PAT:      ",
                &state.pat_input,
                input_focused,
                normal,
                input_style,
                layout.cw,
            );
            // Status line right below the input
            let status = if pat_stored() {
                ("✓ PAT stored — entering a new one will replace it", success)
            } else {
                ("No PAT stored", dim)
            };
            dh::render_line(
                frame,
                layout.content,
                ROW_BODY + 1,
                Line::from(Span::styled(
                    format!("  {:<w$}", status.0, w = layout.cw.saturating_sub(2)),
                    status.1,
                )),
            );
        }
        AzureAuthMode::Browser => {
            if let Some(ref flow) = state.browser_flow {
                dh::render_line(
                    frame,
                    layout.content,
                    ROW_BODY,
                    Line::from(Span::styled(
                        format!("{:<w$}", flow.status, w = layout.cw),
                        normal,
                    )),
                );
            } else {
                dh::render_labeled_text_input(
                    frame,
                    layout.content,
                    ROW_BODY,
                    "Tenant:   ",
                    &state.tenant_input,
                    input_focused,
                    normal,
                    input_style,
                    layout.cw,
                );
                let status = if bearer_stored() {
                    ("✓ Bearer token stored", success)
                } else {
                    ("No browser token stored", dim)
                };
                dh::render_line(
                    frame,
                    layout.content,
                    ROW_BODY + 1,
                    Line::from(Span::styled(
                        format!("  {:<w$}", status.0, w = layout.cw.saturating_sub(2)),
                        status.1,
                    )),
                );
            }
        }
        AzureAuthMode::AzCli => {
            let text = if state.az_fetching {
                state.az_fetch_status()
            } else {
                match &state.az_status {
                    AzCliStatus::Unknown => "Checking az CLI...".to_string(),
                    AzCliStatus::Checking => "Checking az CLI...".to_string(),
                    AzCliStatus::LoggedIn { user, tenant } => {
                        format!("Logged in as {}  ·  tenant {}", user, tenant)
                    }
                    AzCliStatus::NotLoggedIn => "az found — run `az login` first".to_string(),
                    AzCliStatus::NotInstalled => "az CLI not found in PATH".to_string(),
                }
            };
            dh::render_line(
                frame,
                layout.content,
                ROW_BODY,
                Line::from(Span::styled(format!("{:<w$}", text, w = layout.cw), normal)),
            );
            let status = if bearer_stored() {
                ("✓ Bearer token stored (from a previous fetch)", success)
            } else {
                ("No token stored yet", dim)
            };
            dh::render_line(
                frame,
                layout.content,
                ROW_BODY + 1,
                Line::from(Span::styled(
                    format!("  {:<w$}", status.0, w = layout.cw.saturating_sub(2)),
                    status.1,
                )),
            );
        }
    }
}

/// True if a PAT is in the keychain.
fn pat_stored() -> bool {
    crate::ci::has_stored_pat()
}

/// True if a bearer token (from Browser or az CLI flow) is in the keychain.
fn bearer_stored() -> bool {
    crate::azure_auth::get_bearer_token().is_some()
}

fn render_tabs(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &AzureAuthDialogState,
    normal: Style,
    highlight: Style,
    dim: Style,
) {
    let bar_focused = state.on_bar();
    // When the tab bar has focus, use the highlight background across the
    // entire row so the user can see which zone is active.
    let line_bg = if bar_focused { highlight } else { normal };
    let sep_style = if bar_focused { highlight } else { dim };

    let pat_has = pat_stored();
    let bearer_has = bearer_stored();
    let tabs = [
        ("PAT", AzureAuthMode::Pat, pat_has),
        ("Browser", AzureAuthMode::Browser, bearer_has),
        ("az CLI", AzureAuthMode::AzCli, bearer_has),
    ];

    // Build the center strip: "[ ✓ PAT ] │  Browser  │  az CLI".
    let mut strip: Vec<Span> = Vec::with_capacity(tabs.len() * 2);
    let mut strip_cols = 0usize;
    for (i, (label, mode, has_cred)) in tabs.iter().enumerate() {
        if i > 0 {
            let sep = " │ ";
            strip.push(Span::styled(sep, sep_style));
            strip_cols += sep.chars().count();
        }
        let active = *mode == state.mode;
        let style = if active && bar_focused {
            highlight.add_modifier(Modifier::BOLD)
        } else if active {
            normal.add_modifier(Modifier::BOLD)
        } else if bar_focused {
            highlight
        } else {
            dim
        };
        let marker = if *has_cred { "✓ " } else { "  " };
        let text = if active {
            format!("[ {}{} ]", marker, label)
        } else {
            format!("  {}{}  ", marker, label)
        };
        strip_cols += text.chars().count();
        strip.push(Span::styled(text, style));
    }

    // Center horizontally within the content width.
    let left_pad = layout.cw.saturating_sub(strip_cols) / 2;
    let right_pad = layout.cw.saturating_sub(left_pad + strip_cols);

    let mut spans: Vec<Span> = Vec::with_capacity(strip.len() + 2);
    spans.push(Span::styled(" ".repeat(left_pad), line_bg));
    spans.extend(strip);
    spans.push(Span::styled(" ".repeat(right_pad), line_bg));

    dh::render_line(frame, layout.content, ROW_TABS, Line::from(spans));
}
