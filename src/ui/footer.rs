use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::ci::CiView;
use crate::theme::theme;

pub fn render(frame: &mut Frame, area: Rect) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = &[
        ("F1", "Help"),
        ("F2", "CI"),
        ("F3", "Size"),
        ("F4", "Edit"),
        ("F5", "Copy"),
        ("F6", "RnMov"),
        ("F7", "MkFld"),
        ("F8", "Del"),
        ("F9", "Sort"),
        ("F10", "Quit"),
        ("F12", "Claude"),
    ];

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(*label, label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_ci(frame: &mut Frame, area: Rect, view: &CiView) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = match view {
        CiView::Tree { .. } => &[
            ("Enter", "Expand/Log"),
            ("o", "Browser"),
            ("Tab", "Switch"),
            ("F2", "Close"),
        ],
        _ => &[("Tab", "Switch"), ("F2", "Close")],
    };

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(format!(": {}", label), label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_terminal(frame: &mut Frame, area: Rect) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = &[
        ("F1", "Switch"),
        ("F5", "Open"),
        ("F10", "Quit"),
        ("F12", "Close"),
        ("A-Up/Dn", "Resize"),
        ("A-Enter", "Max"),
    ];

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(format!(": {}", label), label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_shell(frame: &mut Frame, area: Rect) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = &[
        ("F1", "Switch"),
        ("C-o", "Close"),
        ("F10", "Quit"),
        ("A-Up/Dn", "Resize"),
        ("A-Enter", "Max"),
    ];

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(format!(": {}", label), label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_diff(frame: &mut Frame, area: Rect) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = &[
        ("Enter", "Diff"),
        ("F4", "Edit"),
        ("\u{2190}\u{2192}", "Fold"),
        ("Tab", "Switch"),
        ("C-d", "Close"),
    ];

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(format!(": {}", label), label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_search(frame: &mut Frame, area: Rect) {
    let t = theme();
    let fkey_style = Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg);
    let label_style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let sep_style = Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg);

    let items: &[(&str, &str)] = &[
        ("Enter", "Open"),
        ("Tab", "Switch"),
        ("Esc", "Close"),
        ("\u{2190}\u{2192}", "Fold"),
    ];

    let mut spans = Vec::with_capacity(items.len() * 3);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(format!(": {}", label), label_style));
    }

    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area.width {
        let padding = " ".repeat((area.width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Render a status message in the footer area (replaces key hints temporarily).
pub fn render_status(frame: &mut Frame, area: Rect, msg: &str) {
    let t = theme();
    let style = Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg);
    let width = area.width as usize;
    let text = format!(" {}", msg);
    let padded = if text.len() < width {
        format!("{}{}", text, " ".repeat(width - text.len()))
    } else {
        text[..width].to_string()
    };
    frame.render_widget(Paragraph::new(Span::styled(padded, style)), area);
}
