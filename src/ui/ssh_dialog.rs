use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{RemoteProtocol, SshDialogState};
use crate::ssh::HostSource;
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

pub fn render(frame: &mut Frame, state: &SshDialogState) {
    let t = theme();
    let dialog_height = 22u16.min(frame.area().height.saturating_sub(4));

    // Show saved connections view or protocol form
    if let Some(sel) = state.saved_selected {
        let layout = dh::render_dialog_frame(
            frame,
            " Connectivity: Saved Connections [experimental] ",
            62,
            dialog_height,
        );
        let (normal, _highlight, _) = dh::dialog_styles();

        dh::render_line(
            frame,
            layout.content,
            0,
            Line::from(Span::styled(
                format!("{:<width$}", "Saved connections:", width = layout.cw),
                normal,
            )),
        );

        let list_start = 2u16;
        let list_height = layout.content.height.saturating_sub(list_start + 2) as usize;
        let total = state.saved_connections.len();
        let scroll = if total <= list_height {
            0
        } else if sel >= list_height {
            (sel + 1).saturating_sub(list_height)
        } else {
            0
        };

        for (vi, conn) in state
            .saved_connections
            .iter()
            .skip(scroll)
            .take(list_height)
            .enumerate()
        {
            let is_selected = scroll + vi == sel;
            let label = conn.display_label();
            let cw = layout.cw;
            let padded = format!("{:<width$}", label, width = cw);
            let display = if padded.len() > cw {
                super::truncate_to_width(&padded, cw).to_string()
            } else {
                padded
            };
            let style = if is_selected {
                Style::default()
                    .fg(t.dialog_input_fg_focused)
                    .bg(t.dialog_input_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                normal
            };
            let row = list_start + vi as u16;
            if row < layout.content.height.saturating_sub(2) {
                let rect = Rect::new(
                    layout.content.x,
                    layout.content.y + row,
                    layout.content.width,
                    1,
                );
                frame.render_widget(
                    Paragraph::new(Line::from(Span::styled(display, style))),
                    rect,
                );
            }
        }

        // Footer hint
        let hint_row = layout.content.height.saturating_sub(1);
        let hint = "Enter: connect  Tab: new  Del: remove  Esc: close";
        let dim = Style::default()
            .fg(t.dialog_text_fg)
            .bg(t.dialog_bg)
            .add_modifier(Modifier::DIM);
        dh::render_line(
            frame,
            layout.content,
            hint_row,
            Line::from(Span::styled(
                format!("{:<width$}", hint, width = layout.cw),
                dim,
            )),
        );
        return;
    }

    let title = match state.protocol {
        RemoteProtocol::Ssh => " Connectivity: SSH [experimental] ",
        RemoteProtocol::Sftp => " Connectivity: SFTP [experimental] ",
        RemoteProtocol::Smb => " Connectivity: SMB [experimental] ",
        RemoteProtocol::WebDav => " Connectivity: WebDAV [experimental] ",
        RemoteProtocol::S3 => " Connectivity: S3 [experimental] ",
        RemoteProtocol::Gcs => " Connectivity: GCS [experimental] ",
        RemoteProtocol::AzureBlob => " Connectivity: Azure Blob [experimental] ",
        RemoteProtocol::Nfs => " Connectivity: NFS [experimental] ",
    };
    let layout = dh::render_dialog_frame(frame, title, 62, dialog_height);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // Row 0: Protocol selector tabs
    render_protocol_tabs(
        frame,
        layout.content,
        0,
        state.protocol,
        state.on_protocol_bar(),
        &t,
    );
    // Row 1: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 1,
        t.dialog_border_style(),
    );

    match state.protocol {
        RemoteProtocol::Ssh | RemoteProtocol::Sftp => {
            render_ssh_sftp(frame, &layout, state, normal, highlight, input_normal, &t);
        }
        RemoteProtocol::Smb => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("Host:", &state.input),
                    ("Share:", &state.smb_share),
                    ("Username:", &state.smb_user),
                    ("Password:", &state.smb_pass),
                ],
            );
        }
        RemoteProtocol::WebDav => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("URL:", &state.input),
                    ("Username:", &state.webdav_user),
                    ("Password:", &state.webdav_pass),
                ],
            );
        }
        RemoteProtocol::S3 => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("Bucket:", &state.s3_bucket),
                    ("Profile:", &state.s3_profile),
                    ("Endpoint URL:", &state.s3_endpoint),
                    ("Region:", &state.s3_region),
                ],
            );
        }
        RemoteProtocol::Gcs => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("Bucket:", &state.gcs_bucket),
                    ("Project:", &state.gcs_project),
                ],
            );
        }
        RemoteProtocol::AzureBlob => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("Account:", &state.azure_account),
                    ("Container (or browse):", &state.azure_container),
                    ("SAS Token:", &state.azure_sas),
                    ("Connection String:", &state.azure_conn_str),
                ],
            );
        }
        RemoteProtocol::Nfs => {
            render_fields(
                frame,
                &layout,
                state,
                normal,
                highlight,
                input_normal,
                &[
                    ("Host:", &state.nfs_host),
                    ("Export:", &state.nfs_export),
                    ("Options:", &state.nfs_options),
                ],
            );
        }
    }
}

fn render_protocol_tabs(
    frame: &mut Frame,
    content: Rect,
    y_off: u16,
    active: RemoteProtocol,
    bar_focused: bool,
    t: &crate::theme::Theme,
) {
    let protocols = [
        RemoteProtocol::Ssh,
        RemoteProtocol::Sftp,
        RemoteProtocol::Smb,
        RemoteProtocol::WebDav,
        RemoteProtocol::S3,
        RemoteProtocol::Gcs,
        RemoteProtocol::AzureBlob,
        RemoteProtocol::Nfs,
    ];

    // Focused: input highlight strip. Unfocused: border color accent so tabs are always visible.
    let (bar_bg, active_fg, inactive_fg, sep_fg) = if bar_focused {
        (
            t.dialog_input_bg,
            t.dialog_input_fg_focused,
            t.dialog_text_fg,
            t.dialog_text_fg,
        )
    } else {
        (
            t.dialog_bg,
            t.dialog_border_fg,
            t.dialog_hint_fg,
            t.dialog_hint_fg,
        )
    };

    let sep_style = Style::default().fg(sep_fg).bg(bar_bg);
    let active_style = Style::default()
        .fg(active_fg)
        .bg(bar_bg)
        .add_modifier(Modifier::BOLD);
    let inactive_style = Style::default().fg(inactive_fg).bg(bar_bg);

    let mut spans = Vec::new();
    for (i, proto) in protocols.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" | ", sep_style));
        }
        if *proto == active {
            spans.push(Span::styled(format!("[{}]", proto.label()), active_style));
        } else {
            spans.push(Span::styled(proto.label(), inactive_style));
        }
    }
    // Pad to content width
    let used: usize = spans.iter().map(|s| s.width()).sum();
    let cw = content.width as usize;
    if used < cw {
        spans.push(Span::styled(
            " ".repeat(cw - used),
            Style::default().bg(bar_bg),
        ));
    }

    let rect = Rect::new(content.x, content.y + y_off, content.width, 1);
    frame.render_widget(Paragraph::new(Line::from(spans)), rect);
}

fn render_ssh_sftp(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
    input_normal: Style,
    t: &crate::theme::Theme,
) {
    let cw = layout.cw;

    // Row 3: Input label
    let label = if state.protocol == RemoteProtocol::Sftp {
        "Browse (user@host[:port]):"
    } else {
        "Connect to (user@host[:port]):"
    };
    dh::render_line(
        frame,
        layout.content,
        3,
        Line::from(Span::styled(
            format!("{:<width$}", label, width = cw),
            normal,
        )),
    );

    // Row 4: Quick-connect input (focused when field_focus == 1)
    let input_focused = state.field_focus == 1;
    let input_style = if input_focused {
        highlight
    } else {
        input_normal
    };
    dh::render_text_input(
        frame,
        layout.content,
        4,
        &state.input,
        input_focused,
        input_style,
        cw,
    );

    // Row 6: Saved hosts
    let hosts_label = if state.hosts.is_empty() {
        "No saved hosts (type above to quick-connect)"
    } else {
        "Saved hosts:"
    };
    dh::render_line(
        frame,
        layout.content,
        6,
        Line::from(Span::styled(
            format!("{:<width$}", hosts_label, width = cw),
            normal,
        )),
    );

    // Host list
    let list_start = 7u16;
    let list_height = layout.content.height.saturating_sub(list_start) as usize;
    let total = state.filtered.len();
    let scroll = if total <= list_height {
        0
    } else if state.selected >= list_height {
        (state.selected + 1).saturating_sub(list_height)
    } else {
        0
    };

    for (vi, fi) in state
        .filtered
        .iter()
        .skip(scroll)
        .take(list_height)
        .enumerate()
    {
        let host = &state.hosts[*fi];
        let is_selected = scroll + vi == state.selected;

        let source_tag = if host.source == HostSource::SshConfig {
            " [config]"
        } else {
            ""
        };
        let dl = host.display_label();
        let mut buf = String::with_capacity(cw);
        buf.push_str(&dl);
        buf.push_str(source_tag);
        if host.name != dl {
            buf.push_str(" (");
            buf.push_str(&host.name);
            buf.push(')');
        }
        // Pad or truncate to content width
        let display = if buf.chars().count() > cw {
            super::truncate_to_width(&buf, cw).to_string()
        } else {
            while buf.len() < cw {
                buf.push(' ');
            }
            buf
        };

        let list_focused = state.field_focus == 2;
        let style = if is_selected && list_focused {
            Style::default()
                .fg(t.dialog_input_fg_focused)
                .bg(t.dialog_input_bg)
                .add_modifier(Modifier::BOLD)
        } else if is_selected {
            Style::default().fg(t.dialog_text_fg).bg(t.dialog_input_bg)
        } else {
            normal
        };

        let row = list_start + vi as u16;
        if row < layout.content.height {
            let rect = Rect::new(
                layout.content.x,
                layout.content.y + row,
                layout.content.width,
                1,
            );
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(display, style))),
                rect,
            );
        }
    }
}

/// Generic form renderer for protocols with labeled text fields.
fn render_fields(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
    input_normal: Style,
    fields: &[(&str, &crate::text_input::TextInput)],
) {
    let cw = layout.cw;

    let mut row = 3u16;
    for (i, (label, input)) in fields.iter().enumerate() {
        let focused = state.field_focus == i + 1;
        let style = if focused { highlight } else { input_normal };

        dh::render_line(
            frame,
            layout.content,
            row,
            Line::from(Span::styled(
                format!("{:<width$}", label, width = cw),
                normal,
            )),
        );
        row += 1;
        dh::render_text_input(frame, layout.content, row, input, focused, style, cw);
        row += 2;
    }
}
