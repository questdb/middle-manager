use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::action::Action;
use crate::app::MenuState;
use crate::panel::sort::SortField;
use crate::theme::theme;

/// A single item in a dropdown menu.
pub(crate) struct MenuItem {
    pub label: &'static str,
    pub shortcut: &'static str,
    pub action: Option<Action>, // None = separator
}

/// A top-level menu category.
pub(crate) struct MenuCategory {
    pub title: &'static str,
    pub items: &'static [MenuItem],
}

const SEP: MenuItem = MenuItem {
    label: "",
    shortcut: "",
    action: None,
};

macro_rules! item {
    ($label:expr, $shortcut:expr, $action:expr) => {
        MenuItem {
            label: $label,
            shortcut: $shortcut,
            action: Some($action),
        }
    };
}

static LEFT_ITEMS: &[MenuItem] = &[
    item!("Sort by name", "", Action::SortByName(0)),
    item!("Sort by size", "", Action::SortBySize(0)),
    item!("Sort by date", "", Action::SortByDate(0)),
];

static FILE_ITEMS: &[MenuItem] = &[
    item!("View", "Shift+F3", Action::ViewFile),
    item!("Edit", "F4", Action::EditBuiltin),
    item!("Edit with $EDITOR", "Shift+F4", Action::EditFile),
    SEP,
    item!("Copy", "F5", Action::Copy),
    item!("Move", "F6", Action::Move),
    item!("Rename", "Shift+F6", Action::Rename),
    SEP,
    item!("Create directory", "F7", Action::CreateDir),
    item!("Create file", "Shift+F7", Action::CreateFile),
    item!("Archive", "Shift+F5", Action::Archive),
    SEP,
    item!("Delete", "F8", Action::Delete),
    item!("Calculate size", "F3", Action::CalcSize),
];

static COMMAND_ITEMS: &[MenuItem] = &[
    item!("Shell panel", "Ctrl+O", Action::ToggleShell),
    item!("Claude Code", "F12", Action::ToggleClaude),
    item!("CI panel", "F2", Action::ToggleCi),
    item!("PR diff panel", "Ctrl+D", Action::ToggleDiff),
    item!("Remote connect", "Ctrl+T", Action::ToggleSsh),
    item!("Sessions", "Ctrl+Y", Action::ToggleSessions),
    SEP,
    item!("File search", "Ctrl+S", Action::FileSearchPrompt),
    item!("Fuzzy search", "Ctrl+F", Action::FuzzySearchPrompt),
    item!("Go to path", "Ctrl+G", Action::GotoPathPrompt),
    SEP,
    item!("Open PR in browser", "F11", Action::OpenPr),
    item!("Copy filename", "Ctrl+C", Action::CopyName),
    item!("Copy full path", "Ctrl+P", Action::CopyPath),
];

static OPTIONS_ITEMS: &[MenuItem] = &[
    item!("Settings", "Shift+F1", Action::ToggleSettings),
    SEP,
    item!("Help", "F1", Action::ShowHelp),
];

static RIGHT_ITEMS: &[MenuItem] = &[
    item!("Sort by name", "", Action::SortByName(1)),
    item!("Sort by size", "", Action::SortBySize(1)),
    item!("Sort by date", "", Action::SortByDate(1)),
];

pub(crate) static MENUS: &[MenuCategory] = &[
    MenuCategory {
        title: "Left",
        items: LEFT_ITEMS,
    },
    MenuCategory {
        title: "File",
        items: FILE_ITEMS,
    },
    MenuCategory {
        title: "Command",
        items: COMMAND_ITEMS,
    },
    MenuCategory {
        title: "Options",
        items: OPTIONS_ITEMS,
    },
    MenuCategory {
        title: "Right",
        items: RIGHT_ITEMS,
    },
];

/// Number of selectable (non-separator) items in a menu.
pub(crate) fn selectable_count(menu_idx: usize) -> usize {
    MENUS[menu_idx]
        .items
        .iter()
        .filter(|i| i.action.is_some())
        .count()
}

/// Map a selectable index to the actual item index (skipping separators).
fn selectable_to_item_index(menu_idx: usize, sel: usize) -> usize {
    let mut count = 0;
    for (i, item) in MENUS[menu_idx].items.iter().enumerate() {
        if item.action.is_some() {
            if count == sel {
                return i;
            }
            count += 1;
        }
    }
    0
}

/// Get the action for the currently selected menu item.
pub(crate) fn selected_action(state: &MenuState) -> Option<Action> {
    let idx = selectable_to_item_index(state.active_menu, state.selected_item);
    MENUS[state.active_menu].items[idx].action.clone()
}

/// Compute the unclamped dropdown content width for a menu.
fn menu_content_width(menu: &MenuCategory) -> usize {
    let max_label: usize = menu.items.iter().map(|i| i.label.len()).max().unwrap_or(10);
    let max_shortcut: usize = menu
        .items
        .iter()
        .map(|i| i.shortcut.len())
        .max()
        .unwrap_or(0);
    max_label + 2 + max_shortcut + 2
}

/// Compute the clamped dropdown rect given screen bounds.
fn dropdown_rect(menu: &MenuCategory, dropdown_x: u16, dropdown_y: u16, screen: Rect) -> Rect {
    let inner_width = menu_content_width(menu);
    let raw_w = inner_width as u16 + 2; // +2 for border
    let raw_h = menu.items.len() as u16 + 2;

    let avail_w = screen
        .width
        .saturating_sub(dropdown_x.saturating_sub(screen.x));
    let avail_h = screen
        .height
        .saturating_sub(dropdown_y.saturating_sub(screen.y));
    let dw = raw_w.min(avail_w).max(10);
    let dh = raw_h.min(avail_h).max(3);

    Rect::new(dropdown_x, dropdown_y, dw, dh)
}

/// Given a click position and the menu title ranges, check if the click is inside
/// the dropdown area and return the selectable item index if so.
/// `bar_y` is the y of the menu bar row; the dropdown starts at bar_y + 1.
/// `screen` is the full terminal area (for clamping dimensions to match render).
pub(crate) fn dropdown_click(
    state: &MenuState,
    menu_title_ranges: &[(u16, u16)],
    bar_y: u16,
    screen: Rect,
    col: u16,
    row: u16,
) -> Option<usize> {
    let menu = &MENUS[state.active_menu];
    let dropdown_x = menu_title_ranges.get(state.active_menu)?.0;
    let dropdown_y = bar_y + 1;

    let rect = dropdown_rect(menu, dropdown_x, dropdown_y, screen);
    // Inner area (inside border)
    let inner_x = rect.x + 1;
    let inner_y = rect.y + 1;
    let inner_w = rect.width.saturating_sub(2);
    let inner_h = rect.height.saturating_sub(2);

    // Check bounds
    if col < inner_x || col >= inner_x + inner_w || row < inner_y || row >= inner_y + inner_h {
        return None;
    }

    let item_row = (row - inner_y) as usize;
    if item_row >= menu.items.len() {
        return None;
    }

    // Separator — not clickable
    menu.items[item_row].action.as_ref()?;

    // Convert item index to selectable index
    let mut sel_idx = 0;
    for (i, item) in menu.items.iter().enumerate() {
        if i == item_row {
            return Some(sel_idx);
        }
        if item.action.is_some() {
            sel_idx += 1;
        }
    }
    None
}

/// Check if a sort action matches the current sort field for a panel.
fn is_active_sort(action: &Option<Action>, sort_fields: [SortField; 2]) -> bool {
    match action {
        Some(Action::SortByName(side)) => sort_fields.get(*side) == Some(&SortField::Name),
        Some(Action::SortBySize(side)) => sort_fields.get(*side) == Some(&SortField::Size),
        Some(Action::SortByDate(side)) => sort_fields.get(*side) == Some(&SortField::Date),
        _ => false,
    }
}

/// Render the menu bar and dropdown overlay.
/// Returns the x-ranges for each menu title (for click detection).
/// `sort_fields` contains [left_panel_sort, right_panel_sort] for showing active sort.
pub(crate) fn render(
    frame: &mut Frame,
    state: &MenuState,
    bar_area: Rect,
    sort_fields: [SortField; 2],
) -> Vec<(u16, u16)> {
    let t = theme();

    let bar_bg = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let bar_active = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);

    // Render the menu bar (single row)
    let mut spans: Vec<Span> = Vec::new();
    spans.push(Span::styled(" ", bar_bg));

    let mut menu_ranges: Vec<(u16, u16)> = Vec::new();
    let mut x_pos: u16 = bar_area.x + 1;

    for (i, menu) in MENUS.iter().enumerate() {
        let label = format!(" {} ", menu.title);
        let start = x_pos;
        let style = if i == state.active_menu {
            bar_active
        } else {
            bar_bg
        };
        let w = label.len() as u16;
        spans.push(Span::styled(label, style));
        menu_ranges.push((start, start + w));
        x_pos += w;
    }

    // Fill remaining width
    let used: usize = spans.iter().map(|s| s.width()).sum();
    if (used as u16) < bar_area.width {
        let pad = " ".repeat(bar_area.width as usize - used);
        spans.push(Span::styled(pad, bar_bg));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), bar_area);

    // Render the dropdown below the active menu title
    let menu = &MENUS[state.active_menu];
    let dropdown_x = menu_ranges[state.active_menu].0;
    let dropdown_y = bar_area.y + 1;

    let drect = dropdown_rect(menu, dropdown_x, dropdown_y, frame.area());
    frame.render_widget(Clear, drect);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(t.dialog_border_fg).bg(t.dialog_bg))
        .style(Style::default().bg(t.dialog_bg));

    let inner = block.inner(drect);
    frame.render_widget(block, drect);

    // Render items
    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let highlight = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);
    let shortcut_style = Style::default().fg(t.dialog_border_fg).bg(t.dialog_bg);
    let shortcut_hl = Style::default()
        .fg(t.dialog_border_fg)
        .bg(t.dialog_input_bg);
    let sep_style = Style::default().fg(t.dialog_border_fg).bg(t.dialog_bg);

    let iw = inner.width as usize;
    let mut sel_count = 0;

    for (i, item) in menu.items.iter().enumerate() {
        if i as u16 >= inner.height {
            break;
        }
        let row = Rect::new(inner.x, inner.y + i as u16, inner.width, 1);

        if item.action.is_none() {
            // Separator
            let sep = "─".repeat(iw);
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(sep, sep_style))),
                row,
            );
        } else {
            let is_selected = sel_count == state.selected_item;
            let st = if is_selected { highlight } else { normal };
            let sc = if is_selected {
                shortcut_hl
            } else {
                shortcut_style
            };

            let marker = if is_active_sort(&item.action, sort_fields) {
                "\u{2022}" // bullet •
            } else {
                " "
            };
            let label_width = iw.saturating_sub(item.shortcut.len() + 1);
            let mut spans = vec![Span::styled(
                format!(
                    "{}{:<width$}",
                    marker,
                    item.label,
                    width = label_width.saturating_sub(1)
                ),
                st,
            )];
            if !item.shortcut.is_empty() {
                spans.push(Span::styled(
                    format!("{} ", item.shortcut),
                    if is_selected { st } else { sc },
                ));
            } else {
                // Pad the rest
                let used: usize = spans.iter().map(|s| s.width()).sum();
                if used < iw {
                    spans.push(Span::styled(" ".repeat(iw - used), st));
                }
            }

            frame.render_widget(Paragraph::new(Line::from(spans)), row);
            sel_count += 1;
        }
    }

    menu_ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_menus_have_items() {
        for menu in MENUS {
            assert!(!menu.items.is_empty(), "menu '{}' has no items", menu.title);
        }
    }

    #[test]
    fn selectable_count_skips_separators() {
        for (i, menu) in MENUS.iter().enumerate() {
            let expected = menu.items.iter().filter(|it| it.action.is_some()).count();
            assert_eq!(selectable_count(i), expected);
        }
    }

    #[test]
    fn selectable_to_item_roundtrip() {
        for (mi, menu) in MENUS.iter().enumerate() {
            let count = selectable_count(mi);
            for sel in 0..count {
                let idx = selectable_to_item_index(mi, sel);
                assert!(
                    menu.items[idx].action.is_some(),
                    "menu {}, sel {} mapped to separator at {}",
                    mi,
                    sel,
                    idx
                );
            }
        }
    }

    #[test]
    fn selected_action_returns_action() {
        let state = MenuState {
            active_menu: 1,
            selected_item: 0,
        };
        let action = selected_action(&state);
        assert!(action.is_some());
    }

    // ── is_active_sort ──────────────────────────────────────────────

    #[test]
    fn is_active_sort_matches_name() {
        let sorts = [SortField::Name, SortField::Size];
        assert!(is_active_sort(&Some(Action::SortByName(0)), sorts));
        assert!(!is_active_sort(&Some(Action::SortByName(1)), sorts));
    }

    #[test]
    fn is_active_sort_matches_size() {
        let sorts = [SortField::Name, SortField::Size];
        assert!(!is_active_sort(&Some(Action::SortBySize(0)), sorts));
        assert!(is_active_sort(&Some(Action::SortBySize(1)), sorts));
    }

    #[test]
    fn is_active_sort_matches_date() {
        let sorts = [SortField::Date, SortField::Date];
        assert!(is_active_sort(&Some(Action::SortByDate(0)), sorts));
        assert!(is_active_sort(&Some(Action::SortByDate(1)), sorts));
    }

    #[test]
    fn is_active_sort_non_sort_action() {
        let sorts = [SortField::Name, SortField::Name];
        assert!(!is_active_sort(&Some(Action::Copy), sorts));
        assert!(!is_active_sort(&None, sorts));
    }

    #[test]
    fn is_active_sort_out_of_bounds_returns_false() {
        let sorts = [SortField::Name, SortField::Name];
        assert!(!is_active_sort(&Some(Action::SortByName(2)), sorts));
    }

    // ── dropdown_click ──────────────────────────────────────────────

    fn make_screen() -> Rect {
        Rect::new(0, 0, 120, 40)
    }

    /// Compute the expected inner top-left of the dropdown for a given menu.
    fn dropdown_inner_origin(menu_idx: usize, ranges: &[(u16, u16)], bar_y: u16) -> (u16, u16) {
        let x = ranges[menu_idx].0 + 1; // border
        let y = bar_y + 1 + 1; // below bar + border
        (x, y)
    }

    fn sample_ranges() -> Vec<(u16, u16)> {
        // Mimics the positions from render: " Left  File  Command  Options  Right "
        vec![(1, 7), (7, 13), (13, 22), (22, 31), (31, 38)]
    }

    #[test]
    fn dropdown_click_on_first_item() {
        let state = MenuState {
            active_menu: 0,
            selected_item: 0,
        };
        let ranges = sample_ranges();
        let (ix, iy) = dropdown_inner_origin(0, &ranges, 0);
        let result = dropdown_click(&state, &ranges, 0, make_screen(), ix, iy);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn dropdown_click_on_second_item() {
        let state = MenuState {
            active_menu: 0,
            selected_item: 0,
        };
        let ranges = sample_ranges();
        let (ix, iy) = dropdown_inner_origin(0, &ranges, 0);
        let result = dropdown_click(&state, &ranges, 0, make_screen(), ix, iy + 1);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn dropdown_click_on_separator_returns_none() {
        // FILE_ITEMS index 3 is a separator
        let state = MenuState {
            active_menu: 1,
            selected_item: 0,
        };
        let ranges = sample_ranges();
        let (ix, iy) = dropdown_inner_origin(1, &ranges, 0);
        // Row 3 in FILE_ITEMS is SEP
        let result = dropdown_click(&state, &ranges, 0, make_screen(), ix, iy + 3);
        assert_eq!(result, None);
    }

    #[test]
    fn dropdown_click_outside_returns_none() {
        let state = MenuState {
            active_menu: 0,
            selected_item: 0,
        };
        let ranges = sample_ranges();
        // Click way off to the right
        let result = dropdown_click(&state, &ranges, 0, make_screen(), 100, 5);
        assert_eq!(result, None);
    }

    #[test]
    fn dropdown_click_on_border_returns_none() {
        let state = MenuState {
            active_menu: 0,
            selected_item: 0,
        };
        let ranges = sample_ranges();
        let dropdown_x = ranges[0].0;
        // Click on the border itself (dropdown_y = bar_y+1, border is at dropdown_y)
        let result = dropdown_click(&state, &ranges, 0, make_screen(), dropdown_x, 1);
        assert_eq!(result, None);
    }
}
