use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::ci::CiView;
use crate::theme::theme;

/// Common footer styles: (fkey_style, label_style, sep_style).
fn footer_styles() -> (Style, Style, Style) {
    let t = theme();
    (
        Style::default().fg(t.footer_fkey_fg).bg(t.footer_key_bg),
        Style::default().fg(t.footer_key_fg).bg(t.footer_key_bg),
        Style::default().fg(t.footer_sep_fg).bg(t.footer_sep_bg),
    )
}

/// Build footer spans from key/label pairs using colon-separated style (`: label`).
fn build_colon_spans<'a>(
    items: &[(&'a str, &'a str)],
    fkey_style: Style,
    label_style: Style,
    sep_style: Style,
    area_width: u16,
) -> Vec<Span<'a>> {
    let mut spans = Vec::with_capacity(items.len() * 4);
    for (i, (key, label)) in items.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", sep_style));
        }
        spans.push(Span::styled(*key, fkey_style));
        spans.push(Span::styled(": ", label_style));
        spans.push(Span::styled(*label, label_style));
    }
    let content_width: usize = spans.iter().map(|s| s.width()).sum();
    if (content_width as u16) < area_width {
        let padding = " ".repeat((area_width as usize) - content_width);
        spans.push(Span::styled(padding, sep_style));
    }
    spans
}

pub fn render(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

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
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = match view {
        CiView::Tree { .. } => &[
            ("Enter", "Expand/Log"),
            ("C-e", "Failures"),
            ("o", "Browser"),
            ("Tab", "Switch"),
            ("F2", "Close"),
        ],
        _ => &[("Tab", "Switch"), ("F2", "Close")],
    };

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_terminal(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = &[
        ("F1", "Switch"),
        ("F5", "Open"),
        ("F10", "Quit"),
        ("F12", "Close"),
        ("A-Up/Dn", "Resize"),
        ("A-Enter", "Max"),
    ];

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_shell(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = &[
        ("F1", "Switch"),
        ("C-o", "Close"),
        ("F10", "Quit"),
        ("A-Up/Dn", "Resize"),
        ("A-Enter", "Max"),
    ];

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_ssh(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = &[
        ("F1", "Switch"),
        ("C-t", "Close"),
        ("F10", "Quit"),
        ("A-Up/Dn", "Resize"),
        ("A-Enter", "Max"),
    ];

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_diff(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = &[
        ("Enter", "Diff"),
        ("F4", "Edit"),
        ("\u{2190}\u{2192}", "Fold"),
        ("Tab", "Switch"),
        ("C-d", "Close"),
    ];

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_search(frame: &mut Frame, area: Rect) {
    let (fkey_style, label_style, sep_style) = footer_styles();

    let items: &[(&str, &str)] = &[
        ("Enter", "Open"),
        ("Tab", "Switch"),
        ("Esc", "Close"),
        ("\u{2190}\u{2192}", "Fold"),
    ];

    let spans = build_colon_spans(items, fkey_style, label_style, sep_style, area.width);
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
