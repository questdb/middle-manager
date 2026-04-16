use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{FileSearchDialogState, FileSearchField};
use crate::theme::theme;

use super::dialog_helpers::{self as dh};

const LABEL_WIDTH: usize = 18;
const COMPLETION_MAX_VISIBLE: usize = 6;

pub fn render(frame: &mut Frame, state: &FileSearchDialogState) -> Rect {
    let t = theme();
    let layout = dh::render_dialog_frame(frame, " Search in Files (rg) \u{2500} F1 help ", 66, 37);
    let (normal, highlight, input_normal) = dh::dialog_styles();

    // --- Input fields (y=1..6) ---

    render_input(
        frame,
        &layout,
        1,
        "-e pattern:",
        &state.term,
        state.focused == FileSearchField::Term,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        2,
        "-r replace:",
        &state.replace,
        state.focused == FileSearchField::Replace,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        3,
        "   path:",
        &state.path,
        state.focused == FileSearchField::Path,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        4,
        "-g glob:",
        &state.filter,
        state.focused == FileSearchField::Filter,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        5,
        "-t type:",
        &state.file_type,
        state.focused == FileSearchField::FileType,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        6,
        "-T type-not:",
        &state.type_exclude,
        state.focused == FileSearchField::TypeExclude,
        normal,
        highlight,
        input_normal,
    );

    // y=7: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 7,
        t.dialog_border_style(),
    );

    // --- Search options (y=8..16) ---

    dh::render_checkbox(
        frame,
        layout.content,
        8,
        "    Regex (default)",
        state.is_regex,
        state.focused == FileSearchField::Regex,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        9,
        "-i  Case insensitive",
        state.case_insensitive,
        state.focused == FileSearchField::CaseInsensitive,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        10,
        "-S  Smart case",
        state.smart_case,
        state.focused == FileSearchField::SmartCase,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        11,
        "-w  Word regexp",
        state.whole_word,
        state.focused == FileSearchField::WholeWord,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        12,
        "-x  Line regexp",
        state.whole_line_match,
        state.focused == FileSearchField::WholeLineMatch,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        13,
        "-v  Invert match",
        state.invert_match,
        state.focused == FileSearchField::InvertMatch,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        14,
        "-U  Multiline",
        state.multiline,
        state.focused == FileSearchField::Multiline,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        15,
        "    Multiline dot-all",
        state.multiline_dotall,
        state.focused == FileSearchField::MultilineDotAll,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        16,
        "    CRLF",
        state.crlf,
        state.focused == FileSearchField::Crlf,
        normal,
        highlight,
    );

    // y=17: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 17,
        t.dialog_border_style(),
    );

    // --- Filter options (y=18..25) ---

    dh::render_checkbox(
        frame,
        layout.content,
        18,
        "-.  Hidden files",
        state.hidden,
        state.focused == FileSearchField::Hidden,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        19,
        "-L  Follow symlinks",
        state.follow_symlinks,
        state.focused == FileSearchField::FollowSymlinks,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        20,
        "    No ignore",
        state.no_gitignore,
        state.focused == FileSearchField::NoGitignore,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        21,
        "-a  Text (binary as text)",
        state.binary,
        state.focused == FileSearchField::Binary,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        22,
        "-z  Search zip",
        state.search_zip,
        state.focused == FileSearchField::SearchZip,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        23,
        "    Glob case insensitive",
        state.glob_case_insensitive,
        state.focused == FileSearchField::GlobCaseInsensitive,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        24,
        "    One file system",
        state.one_file_system,
        state.focused == FileSearchField::OneFileSystem,
        normal,
        highlight,
    );
    dh::render_checkbox(
        frame,
        layout.content,
        25,
        "    Trim whitespace",
        state.trim_whitespace,
        state.focused == FileSearchField::TrimWhitespace,
        normal,
        highlight,
    );

    // y=26: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 26,
        t.dialog_border_style(),
    );

    // --- Limits / output inputs (y=27..32) ---

    render_input(
        frame,
        &layout,
        27,
        "-B before:",
        &state.before_context,
        state.focused == FileSearchField::BeforeContext,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        28,
        "-A after:",
        &state.after_context,
        state.focused == FileSearchField::AfterContext,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        29,
        "-d max-depth:",
        &state.max_depth,
        state.focused == FileSearchField::MaxDepth,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        30,
        "-m max-count:",
        &state.max_count,
        state.focused == FileSearchField::MaxCount,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        31,
        "   max-filesize:",
        &state.max_filesize,
        state.focused == FileSearchField::MaxFileSize,
        normal,
        highlight,
        input_normal,
    );

    render_input(
        frame,
        &layout,
        32,
        "-E encoding:",
        &state.encoding,
        state.focused == FileSearchField::Encoding,
        normal,
        highlight,
        input_normal,
    );

    // y=33: separator
    dh::render_separator(
        frame,
        layout.area,
        layout.inner.y + 33,
        t.dialog_border_style(),
    );

    // y=34: buttons
    dh::render_buttons(
        frame,
        layout.content,
        34,
        &[
            ("{ Search }", state.focused == FileSearchField::ButtonSearch),
            ("[ Cancel ]", state.focused == FileSearchField::ButtonCancel),
        ],
        normal,
        highlight,
    );

    // --- Auto-complete popup (rendered on top of dialog content) ---
    if state.has_completions() {
        let input_y_off: u16 = match state.focused {
            FileSearchField::FileType => 5,
            FileSearchField::TypeExclude => 6,
            _ => 5,
        };
        render_completion_popup(frame, &layout, state, input_y_off);
    }

    layout.outer
}

/// Render the auto-complete dropdown below the focused type input field.
fn render_completion_popup(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    state: &FileSearchDialogState,
    input_y_off: u16,
) {
    let t = theme();
    let types = crate::file_search::rg_file_types();
    let matches = &state.completion_matches;
    let selected = state.completion_selected;

    let visible_count = matches.len().min(COMPLETION_MAX_VISIBLE);
    if visible_count == 0 {
        return;
    }

    // Scroll the view to keep selected visible
    let scroll = if selected >= COMPLETION_MAX_VISIBLE {
        selected - COMPLETION_MAX_VISIBLE + 1
    } else {
        0
    };

    // Position popup below the input field, offset by label width
    let popup_x = layout.content.x + LABEL_WIDTH as u16;
    let popup_y = layout.content.y + input_y_off + 1;
    let popup_w = layout.content.width.saturating_sub(LABEL_WIDTH as u16);
    let popup_h = visible_count as u16;

    // Clamp to frame bounds
    let frame_area = frame.area();
    if popup_y + popup_h > frame_area.bottom() || popup_x >= frame_area.right() {
        return;
    }

    let popup_rect = Rect::new(popup_x, popup_y, popup_w, popup_h);

    // Clear background
    frame.render_widget(Clear, popup_rect);

    let normal_style = Style::default().fg(t.dialog_text_fg).bg(t.dialog_input_bg);
    let selected_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_input_bg)
        .add_modifier(Modifier::BOLD);

    for (vi, &mi) in matches.iter().skip(scroll).take(visible_count).enumerate() {
        let ft = &types[mi];
        let is_selected = scroll + vi == selected;
        let style = if is_selected {
            selected_style
        } else {
            normal_style
        };

        let w = popup_w as usize;
        let name_globs = format!("{}: {}", ft.name, ft.globs);
        let display = if name_globs.chars().count() > w {
            let truncated: String = name_globs.chars().take(w.saturating_sub(1)).collect();
            format!("{}\u{2026}", truncated)
        } else {
            format!("{:<width$}", name_globs, width = w)
        };

        let row_rect = Rect::new(popup_x, popup_y + vi as u16, popup_w, 1);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(display, style))),
            row_rect,
        );
    }
}

/// Render the F1 file type list dialog (replaces the help dialog when triggered from type fields).
pub fn render_type_list(frame: &mut Frame, scroll: usize, filter: &str) -> Rect {
    let t = theme();
    let area = frame.area();

    let width = area.width.saturating_sub(8).min(70);
    let height = area.height.saturating_sub(4);
    // Offset from center so it doesn't sit directly on top of the search dialog
    let x = (area.x + (area.width.saturating_sub(width)) / 2 + 3)
        .min(area.right().saturating_sub(width));
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let rect = Rect::new(x, y, width, height);

    frame.render_widget(Clear, rect);

    let title = if filter.is_empty() {
        " File Types — Esc close, type to filter ".to_string()
    } else {
        format!(" File Types — filter: {} ", filter)
    };

    let block = Block::default()
        .title(Span::styled(title, t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let types = crate::file_search::rg_file_types();
    let iw = inner.width as usize;
    let filter_lower = filter.to_lowercase();

    let name_col = 16; // fixed width for name column
    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let name_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);
    let match_style = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::UNDERLINED);

    let lines: Vec<Line> = types
        .iter()
        .filter(|ft| {
            filter.is_empty()
                || ft.name.to_lowercase().contains(&filter_lower)
                || ft.globs.to_lowercase().contains(&filter_lower)
        })
        .map(|ft| {
            let name_padded = format!("{:<width$}", ft.name, width = name_col);
            let globs_w = iw.saturating_sub(name_col);
            let globs = if ft.globs.chars().count() > globs_w {
                let truncated: String = ft.globs.chars().take(globs_w.saturating_sub(1)).collect();
                format!("{}\u{2026}", truncated)
            } else {
                ft.globs.clone()
            };
            if !filter.is_empty() && ft.name.to_lowercase().contains(&filter_lower) {
                Line::from(vec![
                    Span::styled(name_padded, match_style),
                    Span::styled(globs, normal),
                ])
            } else {
                Line::from(vec![
                    Span::styled(name_padded, name_style),
                    Span::styled(globs, normal),
                ])
            }
        })
        .collect();

    let total = lines.len();
    let visible = inner.height as usize;
    let max_scroll = total.saturating_sub(visible);
    let scroll = scroll.min(max_scroll);
    let visible_lines: Vec<Line> = lines.into_iter().skip(scroll).take(visible).collect();
    frame.render_widget(Paragraph::new(visible_lines), inner);

    rect
}

/// Render the F1 glob help dialog — static reference content, no filter needed.
pub fn render_glob_help(frame: &mut Frame, scroll: usize) -> Rect {
    let t = theme();
    let area = frame.area();

    let width = area.width.saturating_sub(8).min(62);
    let height = area.height.saturating_sub(4).min(30);
    // Offset from center so it doesn't sit directly on top of the search dialog
    let x = (area.x + (area.width.saturating_sub(width)) / 2 + 3)
        .min(area.right().saturating_sub(width));
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let rect = Rect::new(x, y, width, height);

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(
            " Glob Patterns (-g) — Esc close, ↑↓ scroll ",
            t.dialog_title_style(),
        ))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let bold = Style::default()
        .fg(t.dialog_input_fg_focused)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);
    let example = Style::default()
        .fg(t.dialog_input_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);

    let lines: Vec<Line> = vec![
        Line::from(Span::styled(
            "Glob patterns filter which files rg searches.",
            normal,
        )),
        Line::from(Span::styled(
            "Multiple -g flags are combined (AND for includes,",
            normal,
        )),
        Line::from(Span::styled(
            "OR within includes, OR within excludes).",
            normal,
        )),
        Line::from(""),
        Line::from(Span::styled("WILDCARDS", bold)),
        Line::from(vec![
            Span::styled("  *        ", example),
            Span::styled("any characters (not /)", normal),
        ]),
        Line::from(vec![
            Span::styled("  **       ", example),
            Span::styled("any characters (including /)", normal),
        ]),
        Line::from(vec![
            Span::styled("  ?        ", example),
            Span::styled("any single character", normal),
        ]),
        Line::from(vec![
            Span::styled("  [abc]    ", example),
            Span::styled("character class", normal),
        ]),
        Line::from(vec![
            Span::styled("  [a-z]    ", example),
            Span::styled("character range", normal),
        ]),
        Line::from(vec![
            Span::styled("  {a,b}    ", example),
            Span::styled("alternation", normal),
        ]),
        Line::from(""),
        Line::from(Span::styled("INCLUDE PATTERNS", bold)),
        Line::from(vec![
            Span::styled("  *.rs     ", example),
            Span::styled("only Rust files", normal),
        ]),
        Line::from(vec![
            Span::styled("  *.{js,ts}", example),
            Span::styled("JS and TS files", normal),
        ]),
        Line::from(vec![
            Span::styled("  src/**   ", example),
            Span::styled("files under src/", normal),
        ]),
        Line::from(vec![
            Span::styled("  test_*   ", example),
            Span::styled("files starting with test_", normal),
        ]),
        Line::from(""),
        Line::from(Span::styled("EXCLUDE PATTERNS (prefix with !)", bold)),
        Line::from(vec![
            Span::styled("  !*.log   ", example),
            Span::styled("skip log files", normal),
        ]),
        Line::from(vec![
            Span::styled("  !vendor/*", example),
            Span::styled("skip vendor directory", normal),
        ]),
        Line::from(vec![
            Span::styled("  !*.min.js", example),
            Span::styled("skip minified JS", normal),
        ]),
        Line::from(""),
        Line::from(Span::styled("COMBINING PATTERNS", bold)),
        Line::from(Span::styled(
            "  Separate multiple patterns with commas:",
            normal,
        )),
        Line::from(vec![
            Span::styled("  *.rs, *.toml    ", example),
            Span::styled("Rust + TOML files", normal),
        ]),
        Line::from(vec![
            Span::styled("  src/**, !test_* ", example),
            Span::styled("src/ but not tests", normal),
        ]),
    ];

    let total = lines.len();
    let visible = inner.height as usize;
    let max_scroll = total.saturating_sub(visible);
    let scroll = scroll.min(max_scroll);
    let visible_lines: Vec<Line> = lines.into_iter().skip(scroll).take(visible).collect();
    frame.render_widget(Paragraph::new(visible_lines), inner);

    rect
}

/// Return (title, lines) help text for a given field.
fn field_help_text(field: FileSearchField) -> (&'static str, &'static [&'static str]) {
    match field {
        FileSearchField::Term => (
            " Pattern (-e) ",
            &[
                "The search pattern. By default treated as a regex.",
                "Uncheck 'Regex' to search for literal text.",
                "",
                "Examples:",
                "  fn\\s+main         function definition",
                "  TODO|FIXME        either keyword",
                "  err(or)?          'err' or 'error'",
                "  \\bword\\b          exact word boundary",
            ],
        ),
        FileSearchField::Replace => (
            " Replace (-r) ",
            &[
                "Replacement text shown in output for each match.",
                "Supports capture group references.",
                "",
                "Examples:",
                "  $0                entire match",
                "  $1                first capture group",
                "  ${name}           named capture group",
                "  fixed text        literal replacement",
            ],
        ),
        FileSearchField::Path => (
            " Search Path ",
            &[
                "The directory (or file) to search in.",
                "Defaults to the current panel directory.",
                "",
                "Accepts an absolute or relative path.",
                "Searching starts recursively from this location.",
            ],
        ),
        // Filter and FileType/TypeExclude have their own rich help dialogs
        FileSearchField::Filter | FileSearchField::FileType | FileSearchField::TypeExclude => {
            ("", &[])
        }
        FileSearchField::Regex => (
            " Regex (default) ",
            &[
                "When checked, the pattern is a regular expression.",
                "When unchecked, equivalent to rg --fixed-strings (-F):",
                "all regex metacharacters are treated as literals.",
                "",
                "Tip: uncheck this when searching for strings that",
                "contain special characters like . * + ? ( ) [ ] etc.",
            ],
        ),
        FileSearchField::CaseInsensitive => (
            " Case Insensitive (-i) ",
            &[
                "Match letters regardless of case.",
                "",
                "  rg -i hello       matches Hello, HELLO, hello",
                "",
                "Note: mutually exclusive with Smart case.",
                "If both are set, -i takes precedence.",
            ],
        ),
        FileSearchField::SmartCase => (
            " Smart Case (-S) ",
            &[
                "Case insensitive if the pattern is all lowercase.",
                "Case sensitive if the pattern has any uppercase.",
                "",
                "  rg -S hello       matches Hello, HELLO, hello",
                "  rg -S Hello       matches only Hello",
                "",
                "This is a convenient middle ground between -i and",
                "the default case-sensitive search.",
            ],
        ),
        FileSearchField::WholeWord => (
            " Word Regexp (-w) ",
            &[
                "Only match when the pattern is surrounded by word",
                "boundaries. Equivalent to wrapping the pattern in \\b.",
                "",
                "  rg -w err         matches 'err' but not 'error'",
                "  rg -w log         matches 'log' but not 'logging'",
            ],
        ),
        FileSearchField::WholeLineMatch => (
            " Line Regexp (-x) ",
            &[
                "Only match when the entire line matches the pattern.",
                "Equivalent to wrapping the pattern in ^...$ anchors.",
                "",
                "  rg -x 'use std;'  matches lines that are exactly",
                "                    'use std;' and nothing else",
            ],
        ),
        FileSearchField::InvertMatch => (
            " Invert Match (-v) ",
            &[
                "Show lines that do NOT match the pattern.",
                "",
                "  rg -v TODO        all lines without 'TODO'",
                "  rg -v '^$'        all non-empty lines",
            ],
        ),
        FileSearchField::Multiline => (
            " Multiline (-U) ",
            &[
                "Allow the pattern to match across multiple lines.",
                "Without this, each line is matched independently.",
                "",
                "  rg -U 'struct.*\\{[\\s\\S]*?\\}'",
                "                    match entire struct blocks",
                "",
                "Consider also enabling 'Multiline dot-all' so that",
                "'.' matches newline characters too.",
            ],
        ),
        FileSearchField::MultilineDotAll => (
            " Multiline Dot-All (--multiline-dotall) ",
            &[
                "Make '.' match line terminators (\\n) in multiline",
                "mode. Without this, '.' stops at newlines even in",
                "multiline mode.",
                "",
                "Only meaningful when Multiline (-U) is also enabled.",
                "",
                "  rg -U --multiline-dotall 'start.*end'",
                "                    matches across lines",
            ],
        ),
        FileSearchField::Crlf => (
            " CRLF (--crlf) ",
            &[
                "Use \\r\\n as the line terminator instead of \\n.",
                "Useful for Windows-style text files.",
                "",
                "When enabled, anchors like $ match before \\r\\n,",
                "and '.' will not match \\r.",
            ],
        ),
        FileSearchField::Hidden => (
            " Hidden Files (-.) ",
            &[
                "Search hidden files and directories (dotfiles).",
                "By default, rg skips entries starting with '.'",
                "",
                "  .env, .gitconfig, .config/, .cache/",
                "",
                "Enable this to include them in search results.",
            ],
        ),
        FileSearchField::FollowSymlinks => (
            " Follow Symlinks (-L) ",
            &[
                "Follow symbolic links when traversing directories.",
                "By default, rg does not follow symlinks.",
                "",
                "Warning: this can cause infinite loops if symlinks",
                "create cycles, though rg has some cycle detection.",
            ],
        ),
        FileSearchField::NoGitignore => (
            " No Ignore (--no-ignore) ",
            &[
                "Don't respect ignore files:",
                "  .gitignore, .ignore, .rgignore",
                "",
                "By default, rg honors these files and skips entries",
                "they list. Enable this to search everything.",
                "",
                "See also: Hidden files (-.) for dotfiles,",
                "and -a for binary files.",
            ],
        ),
        FileSearchField::Binary => (
            " Text / Binary as Text (-a) ",
            &[
                "Search binary files as if they were text.",
                "By default, rg detects binary files (files with",
                "NUL bytes) and skips them.",
                "",
                "With this enabled, binary file contents are",
                "searched and matching lines are printed.",
                "May produce garbled output for true binary files.",
            ],
        ),
        FileSearchField::SearchZip => (
            " Search Zip (-z) ",
            &[
                "Search inside compressed files.",
                "Supports: gzip, bzip2, xz, LZ4, zstd.",
                "",
                "  rg -z pattern archive.gz",
                "",
                "The file is decompressed on the fly.",
            ],
        ),
        FileSearchField::GlobCaseInsensitive => (
            " Glob Case Insensitive (--glob-case-insensitive) ",
            &[
                "Process all -g glob patterns case insensitively.",
                "",
                "  With this enabled:",
                "  -g '*.RS' will also match *.rs and *.Rs",
            ],
        ),
        FileSearchField::OneFileSystem => (
            " One File System (--one-file-system) ",
            &[
                "Do not descend into directories on different file",
                "systems than the search root.",
                "",
                "Useful when searching / or a mount point to avoid",
                "traversing into network mounts or other volumes.",
            ],
        ),
        FileSearchField::TrimWhitespace => (
            " Trim Whitespace (--trim) ",
            &[
                "Trim leading ASCII whitespace from each matching",
                "line before displaying it.",
                "",
                "Does not affect the search itself, only the output.",
            ],
        ),
        FileSearchField::BeforeContext => (
            " Before Context (-B) ",
            &[
                "Number of lines to show before each match.",
                "",
                "  rg -B 3 pattern   3 lines of context before",
                "",
                "Leave empty for no context lines.",
            ],
        ),
        FileSearchField::AfterContext => (
            " After Context (-A) ",
            &[
                "Number of lines to show after each match.",
                "",
                "  rg -A 3 pattern   3 lines of context after",
                "",
                "Leave empty for no context lines.",
            ],
        ),
        FileSearchField::MaxDepth => (
            " Max Depth (-d) ",
            &[
                "Maximum directory depth to descend.",
                "",
                "  0 = only the search path itself (no recursion)",
                "  1 = immediate children",
                "  2 = children and grandchildren",
                "",
                "Leave empty for unlimited depth.",
            ],
        ),
        FileSearchField::MaxCount => (
            " Max Count (-m) ",
            &[
                "Maximum number of matching lines per file.",
                "After this many matches in a file, rg moves on.",
                "",
                "  rg -m 1 pattern   first match per file only",
                "",
                "Leave empty for no limit.",
            ],
        ),
        FileSearchField::MaxFileSize => (
            " Max File Size (--max-filesize) ",
            &[
                "Skip files larger than this size.",
                "Accepts suffixes: K, M, G (base 1024).",
                "",
                "  100K, 1M, 500M",
                "",
                "Leave empty for no limit.",
            ],
        ),
        FileSearchField::Encoding => (
            " Encoding (-E) ",
            &[
                "Force a specific text encoding for searched files.",
                "By default, rg auto-detects encoding.",
                "",
                "Common values:",
                "  utf-8, utf-16le, utf-16be",
                "  ascii, latin1, euc-jp, shift_jis, gbk",
                "",
                "Leave empty for auto-detection.",
            ],
        ),
        FileSearchField::ButtonSearch | FileSearchField::ButtonCancel => ("", &[]),
    }
}

/// Render a compact F1 help popup for a single field.
pub fn render_field_help(frame: &mut Frame, field: FileSearchField) -> Rect {
    let (title, lines_data) = field_help_text(field);
    if title.is_empty() {
        return Rect::default();
    }

    let t = theme();
    let area = frame.area();

    let width = area.width.saturating_sub(8).min(54);
    let content_h = lines_data.len() as u16 + 2; // +2 for top/bottom padding inside border
    let height = (content_h + 2).min(area.height.saturating_sub(4)); // +2 for border
                                                                     // Offset from center
    let x = (area.x + (area.width.saturating_sub(width)) / 2 + 3)
        .min(area.right().saturating_sub(width));
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let rect = Rect::new(x, y, width, height);

    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(Span::styled(title, t.dialog_title_style()))
        .borders(Borders::ALL)
        .border_style(t.dialog_border_style())
        .style(t.dialog_bg_style());

    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let normal = Style::default().fg(t.dialog_text_fg).bg(t.dialog_bg);
    let example_style = Style::default()
        .fg(t.dialog_input_fg)
        .bg(t.dialog_bg)
        .add_modifier(Modifier::BOLD);

    let lines: Vec<Line> = lines_data
        .iter()
        .map(|&s| {
            if s.starts_with("  ") && (s.contains("rg ") || s.contains("  ")) {
                // Lines starting with "  " and containing examples get example styling
                let trimmed = s.trim_start();
                if let Some(desc_start) = trimmed.find("  ") {
                    // "  rg -i hello       matches Hello" -> split at double-space
                    let cmd_end = s.len() - trimmed.len() + desc_start;
                    Line::from(vec![
                        Span::styled(&s[..cmd_end], example_style),
                        Span::styled(&s[cmd_end..], normal),
                    ])
                } else {
                    Line::from(Span::styled(s, example_style))
                }
            } else {
                Line::from(Span::styled(s, normal))
            }
        })
        .collect();

    let visible = inner.height as usize;
    let visible_lines: Vec<Line> = lines.into_iter().take(visible).collect();
    frame.render_widget(Paragraph::new(visible_lines), inner);

    rect
}

#[allow(clippy::too_many_arguments)]
fn render_input(
    frame: &mut Frame,
    layout: &dh::DialogLayout,
    y_off: u16,
    label: &str,
    input: &crate::text_input::TextInput,
    focused: bool,
    normal: ratatui::style::Style,
    highlight: ratatui::style::Style,
    input_normal: ratatui::style::Style,
) {
    let style = if focused { highlight } else { input_normal };
    dh::render_labeled_text_input(
        frame,
        layout.content,
        y_off,
        &format!("{:<width$}", label, width = LABEL_WIDTH),
        input,
        focused,
        normal,
        style,
        layout.cw,
    );
}
