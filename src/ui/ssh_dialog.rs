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
        let scroll = if total <= list_height { 0 }
            else if sel >= list_height { (sel + 1).saturating_sub(list_height) }
            else { 0 };

        for (vi, conn) in state.saved_connections.iter().skip(scroll).take(list_height).enumerate() {
            let is_selected = scroll + vi == sel;
            let label = conn.display_label();
            let cw = layout.cw;
            let padded = format!("{:<width$}", label, width = cw);
            let display = if padded.len() > cw { super::truncate_to_width(&padded, cw).to_string() } else { padded };
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
                let rect = Rect::new(layout.content.x, layout.content.y + row, layout.content.width, 1);
                frame.render_widget(Paragraph::new(Line::from(Span::styled(display, style))), rect);
            }
        }

        // Footer hint
        let hint_row = layout.content.height.saturating_sub(1);
        let hint = "Enter: connect  Tab: new  Del: remove  Esc: close";
        let dim = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg).add_modifier(Modifier::DIM);
        dh::render_line(
            frame,
            layout.content,
            hint_row,
            Line::from(Span::styled(format!("{:<width$}", hint, width = layout.cw), dim)),
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
    let (normal, highlight, _input_normal) = dh::dialog_styles();

    // Row 0: Protocol selector tabs
    render_protocol_tabs(frame, layout.content, 0, state.protocol, &t);

    match state.protocol {
        RemoteProtocol::Ssh | RemoteProtocol::Sftp => {
            render_ssh_sftp(frame, &layout, state, normal, highlight, &t);
        }
        RemoteProtocol::Smb => {
            render_smb(frame, &layout, state, normal, highlight);
        }
        RemoteProtocol::WebDav => {
            render_webdav(frame, &layout, state, normal, highlight);
        }
        RemoteProtocol::S3 => {
            render_fields(frame, &layout, state, normal, highlight,
                &[("Bucket:", &state.s3_bucket), ("Profile:", &state.s3_profile),
                  ("Endpoint URL:", &state.s3_endpoint), ("Region:", &state.s3_region)]);
        }
        RemoteProtocol::Gcs => {
            render_fields(frame, &layout, state, normal, highlight,
                &[("Bucket:", &state.gcs_bucket), ("Project:", &state.gcs_project)]);
        }
        RemoteProtocol::AzureBlob => {
            render_fields(frame, &layout, state, normal, highlight,
                &[("Account:", &state.azure_account), ("Container (or browse):", &state.azure_container),
                  ("SAS Token:", &state.azure_sas), ("Connection String:", &state.azure_conn_str)]);
        }
        RemoteProtocol::Nfs => {
            render_fields(frame, &layout, state, normal, highlight,
                &[("Host:", &state.nfs_host), ("Export:", &state.nfs_export),
                  ("Options:", &state.nfs_options)]);
        }
    }
}

fn render_protocol_tabs(
    frame: &mut Frame,
    content: Rect,
    y_off: u16,
    active: RemoteProtocol,
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

    let dim = Style::default()
        .fg(t.dialog_text_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::DIM);
    let active_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = Vec::new();
    for (i, proto) in protocols.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" | ", dim));
        }
        if *proto == active {
            spans.push(Span::styled(format!("[{}]", proto.label()), active_style));
        } else {
            spans.push(Span::styled(proto.label(), dim));
        }
    }
    // Pad to content width
    let used: usize = spans.iter().map(|s| s.width()).sum();
    let cw = content.width as usize;
    if used < cw {
        spans.push(Span::styled(" ".repeat(cw - used), dim));
    }

    let rect = Rect::new(content.x, content.y + y_off, content.width, 1);
    frame.render_widget(Paragraph::new(Line::from(spans)), rect);

    // Hint line
    let hint = "Alt+Left/Right: switch protocol  F2: save connection";
    dh::render_line(
        frame,
        content,
        1,
        Line::from(Span::styled(
            format!("{:<width$}", hint, width = cw),
            dim,
        )),
    );
}

fn render_ssh_sftp(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
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
        Line::from(Span::styled(format!("{:<width$}", label, width = cw), normal)),
    );

    // Row 4: Quick-connect input
    dh::render_text_input(frame, layout.content, 4, &state.input, true, highlight, cw);

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

    for (vi, fi) in state.filtered.iter().skip(scroll).take(list_height).enumerate() {
        let host = &state.hosts[*fi];
        let is_selected = scroll + vi == state.selected;

        let source_tag = if host.source == HostSource::SshConfig {
            " [config]"
        } else {
            ""
        };
        let label = format!("{}{}", host.display_label(), source_tag);
        let name_part = if host.name != host.display_label() {
            format!(" ({})", host.name)
        } else {
            String::new()
        };

        let line_text = format!("{}{}", label, name_part);
        let padded = format!("{:<width$}", line_text, width = cw);
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
        if row < layout.content.height {
            let rect = Rect::new(layout.content.x, layout.content.y + row, layout.content.width, 1);
            frame.render_widget(Paragraph::new(Line::from(Span::styled(display, style))), rect);
        }
    }
}

fn render_smb(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
) {
    let cw = layout.cw;

    let fields: &[(&str, &crate::text_input::TextInput)] = &[
        ("Host:", &state.input),
        ("Share:", &state.smb_share),
        ("Username:", &state.smb_user),
        ("Password:", &state.smb_pass),
    ];

    let mut row = 3u16;
    for (i, (label, input)) in fields.iter().enumerate() {
        let focused = state.field_focus == i;
        let style = if focused { highlight } else { normal };

        dh::render_line(
            frame,
            layout.content,
            row,
            Line::from(Span::styled(format!("{:<width$}", label, width = cw), normal)),
        );
        row += 1;
        dh::render_text_input(frame, layout.content, row, input, focused, style, cw);
        row += 2;
    }

    render_hint(frame, layout, row, normal);
}

fn render_webdav(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
) {
    let cw = layout.cw;

    let fields: &[(&str, &crate::text_input::TextInput)] = &[
        ("URL:", &state.input),
        ("Username:", &state.webdav_user),
        ("Password:", &state.webdav_pass),
    ];

    let mut row = 3u16;
    for (i, (label, input)) in fields.iter().enumerate() {
        let focused = state.field_focus == i;
        let style = if focused { highlight } else { normal };

        dh::render_line(
            frame,
            layout.content,
            row,
            Line::from(Span::styled(format!("{:<width$}", label, width = cw), normal)),
        );
        row += 1;
        dh::render_text_input(frame, layout.content, row, input, focused, style, cw);
        row += 2;
    }

    render_hint(frame, layout, row, normal);
}

fn render_hint(frame: &mut Frame, layout: &dh::DialogLayout, row: u16, normal: Style) {
    let cw = layout.cw;
    let hint = "Tab: switch field  Enter: connect  F2: save";
    let dim = Style::default()
        .fg(normal.fg.unwrap_or_default())
        .add_modifier(Modifier::DIM);
    if row < layout.content.height {
        dh::render_line(
            frame,
            layout.content,
            row,
            Line::from(Span::styled(
                format!("{:<width$}", hint, width = cw),
                dim,
            )),
        );
    }
}

/// Generic form renderer for protocols with labeled text fields.
fn render_fields(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &SshDialogState,
    normal: Style,
    highlight: Style,
    fields: &[(&str, &crate::text_input::TextInput)],
) {
    let cw = layout.cw;

    let mut row = 3u16;
    for (i, (label, input)) in fields.iter().enumerate() {
        let focused = state.field_focus == i;
        let style = if focused { highlight } else { normal };

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

    render_hint(frame, layout, row, normal);
}
