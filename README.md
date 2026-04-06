# Middle Manager TUI

A dual-panel file manager for the terminal, inspired by [Far Manager](https://www.farmanager.com/) and Norton Commander. Built in Rust with [ratatui](https://ratatui.rs/).

Designed to handle **very large files** — the viewer, hex viewer, and editor all use sliding buffers and lazy scanning, so opening a 10 GB log file is instant.

<img width="2051" height="1252" alt="image" src="https://github.com/user-attachments/assets/948abfb3-d7b2-49bc-9b4d-fa90397f7b7f" />


## Features

**File Manager**
- Dual-panel layout with directory listings (name, size, date, permissions)
- File operations: copy (F5), move (F6), rename (Shift+F6), mkdir (F7), delete (F8), archive (Shift+F5)
- Multi-file selection with Shift+Up/Down (toggle) and Insert key for batch operations
- Far Manager-style dialogs with keyboard navigation (copy, mkdir, delete, rename)
- Quick search — just start typing to jump to a file, Enter to open
- Go-to-path (Ctrl+G) — type a path to navigate instantly, with zsh-style tab completion (case-insensitive, overlay dropdown)
- Fuzzy file search (Ctrl+F) — recursively finds files by partial/misspelled names, ranked by match quality, opens in editor
- Search in files (Ctrl+S) — ripgrep-powered content search with file filter, regex support, tree view results, Enter opens editor at match line with search term highlighted
- Clipboard copy — Ctrl+C copies filename, Ctrl+P copies full path (OSC 52)
- All dialog inputs support text selection (Shift+arrows), Ctrl+A select all, Ctrl+C copy, Ctrl+X cut, Ctrl+Z undo, Ctrl+Shift+Z redo
- Sort by name, size, or date (F9), persisted across restarts
- Mouse support — click to select, double-click to open, scroll wheel to navigate
- Filesystem watcher — panels auto-refresh on external changes (kqueue/inotify, zero-cost idle)
- Persistent state — panel paths, sort preferences, search queries, open panels, and split sizes survive restarts

**Git Integration**
- Git status markers per file: `●` modified, `+` added, `-` deleted, `→` renamed, `?` untracked, `!` conflict
- Branch name with `⎇` glyph in panel title
- Ahead/behind remote counts (`↑N ↓M`)
- Repo-wide status summary in panel title (`● 6 + 3 ? 7`)
- GitHub PR status with CI check indicators (`PR #42 ✓` / `✗` / `○`), merged (`●` magenta) and closed (`✘` red) states
- Shared git cache across panels, fully async git status + PR queries (instant startup, never blocks UI)
- Uses `--no-optional-locks` to avoid index.lock conflicts with other tools
- File name coloring based on git status

**CI Panel (F2)**
- Tree view of CI checks with expand/collapse
- GitHub Actions and Azure DevOps support
- Expand a check to see its steps with status markers (✓ ✗ ○ –)
- Download step logs and open in the built-in editor
- Async fetching with animated spinners — never blocks the UI
- Per-panel CI (left and right panels can each have their own CI view)
- Tab/Shift+Tab cycles focus forward/backward through all panels
- PageUp/PageDown/Home/End for fast scrolling through long check lists
- Alt+Up/Down to resize split, Alt+Enter to maximize/restore
- Mouse click support for selecting items in the tree
- Failed checks sorted to top for quick access
- PR number displayed in panel title

**Shell Panel (Ctrl+O)**
- Spawns your default `$SHELL` at the bottom of the active panel
- Full PTY emulation — colors, cursor, scrollback all work
- Independent per-side (left and right panels can each have their own shell)
- Alt+Up/Down to resize split, Alt+Enter to maximize/restore
- F1 switches focus back to file panel, Ctrl+O closes
- Auto-closes when the shell exits
- Restored on restart

**Archive (Shift+F5)**
- Create tar.zst, tar.gz, tar.xz, or zip archives from selected files
- Smart auto-naming: common prefix detection, dominant extension, parent dir fallback
- Format picker: Space to cycle through formats
- Background compression with progress bar in status bar
- Symlink-safe traversal, collision resolution, UTF-8 safe naming

**Parquet Viewer**
- Auto-detected when opening `.parquet` files (F3 / Enter)
- Tree view: file metadata, key-value metadata (JSON pretty-printed), schema, row groups with column details
- Column statistics: null count, distinct count, min/max values formatted with logical types
- Tabular alignment for schema fields, column info, and metadata keys
- Table view (Tab / F4): scrollable data grid with row group lazy loading
- Encoding names displayed as `Plain`, `RleDictionary`, `DeltaBinaryPacked`, etc.
- Handles pre-epoch timestamps correctly

**Claude Code Panel (F12)**
- Spawns `claude` maximized on the opposite panel, using the active panel's directory
- Full PTY emulation via custom VT terminal emulator — colors, cursor, alternate screen all work
- All keystrokes (including Tab) forwarded to Claude Code
- 10,000-line scrollback buffer with trackpad/mouse scroll (like Ghostty/iTerm2)
- F5 opens file:line references from terminal output in the built-in editor
- F1 switches focus back to file panel, F12 closes
- Restored on restart with `claude -c` (continues last session)
- Auto-closes when Claude exits
- Coalescing wakeup mechanism — terminal output renders immediately without flooding the event loop
- Zero-allocation render loop with direct buffer writes

**Text Viewer (F3 / Enter)**
- Sliding buffer: only ~10K lines in memory at a time
- Sparse line index for instant seeking to any position
- Opens multi-GB files instantly, scrolls smoothly
- Go-to-line with `g` — supports `line` or `line:col` format
- Tab expansion and control character sanitization

**Hex Viewer**
- Auto-detects binary files (null byte check)
- VS Code-style layout: offset | hex bytes | ASCII decode
- 256 KB sliding buffer for arbitrarily large binaries
- Toggle between text and hex with Tab or F4

**Built-in Editor (F4)**
- Line-level piece table — only edited lines live in memory
- Opens and navigates multi-GB files with no delay
- Tree-sitter syntax highlighting for Rust, Java, Python, JavaScript/TypeScript, JSON, Go, C, Bash, TOML
- Hybrid highlighting: files under 10 MB get a cached full parse (always accurate); larger files use a context-window approach
- Search (F7) with forward/backward, case-sensitive toggle, wrap-around
- **Undo/redo** (Ctrl+Z / Ctrl+Shift+Z): operation-based with minimal deltas, word-level grouping, 10K entry cap
- Streaming byte-level search — seeks directly to cursor position, no full-file scan
- Search results highlighted; Shift+F7 repeats last search
- Text selection with Shift+arrow keys
- Word navigation with Ctrl+Left/Right (Linux) or Option+Left/Right (Mac)
- Mouse click to position cursor, mouse scroll
- Ctrl+C to copy to system clipboard (OSC 52)
- Ctrl+A to select all
- Ctrl+K / Ctrl+Y to delete line
- Ctrl+Up/Down or Alt+PageUp/PageDown to jump to top/bottom of file
- F2 / Ctrl+S to save — uses byte-range copying for unmodified segments
- Ctrl+G for go-to-line
- Unsaved changes dialog on all exit paths (Esc, F10, Ctrl+Q)
- Dynamic line number width (scales with file size)
- Configurable cursor shape in theme
- Shift+F4 opens `$EDITOR` instead

**UI**
- Far Manager classic blue color scheme
- Centralized theme system — all colors in one file (`src/theme.rs`), including syntax highlighting and git status colors
- Consistent dialog styling with shared helpers (padding, separators, buttons, checkboxes)
- Contextual footer — shows relevant key hints for the active panel/mode
- Panel border titles with path (shortened with `~`), git info, and CI status
- Active/inactive panel focus with visual border and title color changes
- Dialog drop shadows
- Quit confirmation dialog (F10)
- Panic hook that restores the terminal before printing errors
- Cursor shape restored to terminal default on exit

## Install

### macOS

```
brew install questdb/middle-manager/mm
```

### Linux

```
curl -fsSL https://mm.questdb.io | sh
```

Or install from a package:

```bash
# Debian / Ubuntu — download .deb from the latest release
sudo dpkg -i middle-manager_*.deb

# Fedora / RHEL — download .rpm from the latest release
sudo rpm -i middle-manager-*.rpm
```

Packages and tarballs for all platforms are available on the [releases page](https://github.com/questdb/middle-manager/releases/latest).

### Build from source

```
git clone https://github.com/questdb/middle-manager.git
cd middle-manager
cargo build --release
./target/release/middle-manager
```

## Key Bindings

### Panels

| Key | Action |
|-----|--------|
| Up / Down | Navigate |
| Shift+Up / Shift+Down | Toggle selection and move |
| Insert | Toggle selection on current item |
| Left / Home | Jump to top |
| Right / End | Jump to bottom |
| Enter | Open directory / view file |
| Backspace | Go to parent directory |
| Tab | Switch panel forward (cycles through CI/terminal panels) |
| Shift+Tab | Switch panel backward |
| Ctrl+F | Fuzzy file search (opens in editor) |
| Ctrl+G | Go to path (with tab completion) |
| Ctrl+S | Search in files (ripgrep-powered) |
| Ctrl+C | Copy filename to clipboard |
| Ctrl+O | Toggle shell panel |
| Ctrl+P | Copy full path to clipboard |
| F2 | Toggle CI panel |
| F3 | View file |
| F4 | Edit file (built-in) |
| Shift+F4 | Edit file ($EDITOR) |
| F5 | Copy (operates on selection if active) |
| Shift+F5 | Create archive (tar.zst/gz/xz/zip) |
| F6 | Move (operates on selection if active) |
| Shift+F6 | Rename |
| F7 | Create directory |
| F8 | Delete (operates on selection if active) |
| F9 | Cycle sort |
| F10 | Quit (with confirmation) |
| F11 | Open PR in browser |
| F12 | Toggle Claude Code panel (maximized, opposite side) |
| Type chars | Quick search |

### CI Panel

| Key | Action |
|-----|--------|
| Up / Down | Navigate tree |
| PageUp / PageDown | Page through tree |
| Home / End | Jump to top / bottom |
| Right | Expand check (load steps) |
| Left | Collapse check / jump to parent |
| Enter | Expand/collapse check, or download step log |
| o | Open check in browser |
| Alt+Up / Alt+Down | Resize panel split |
| Alt+Enter | Maximize / restore panel |
| Tab / Shift+Tab | Switch panel forward / backward |
| F2 | Close CI panel |
| Mouse click | Select item and focus panel |

### Shell Panel

| Key | Action |
|-----|--------|
| All keys (incl. Tab) | Forwarded to the shell |
| Scroll / Trackpad | Scroll through scrollback buffer |
| Alt+Up / Alt+Down | Resize panel split |
| Alt+Enter | Maximize / restore panel |
| F1 | Switch focus to file panel |
| Ctrl+O | Close shell panel |
| F10 | Quit (with confirmation) |

### Claude Code Panel

| Key | Action |
|-----|--------|
| All keys (incl. Tab) | Forwarded to Claude Code |
| Scroll / Trackpad | Scroll through scrollback buffer |
| F5 | Open file:line reference in editor |
| F1 | Switch focus to file panel |
| F12 | Close Claude Code panel |
| F10 | Quit (with confirmation) |

### Search Results Panel

| Key | Action |
|-----|--------|
| Up / Down | Navigate results |
| PageUp / PageDown | Page through results |
| Home / End | Jump to top / bottom |
| Enter | Open file in editor at match line |
| Right | Expand file matches |
| Left | Collapse file / jump to parent from match |
| Tab / Shift+Tab | Switch panel |
| Esc | Close search results |
| Scroll / Trackpad | Scroll results |
| Mouse click | Select result and focus panel |
| F10 | Quit (with confirmation) |

### Dialog Inputs (all dialogs)

| Key | Action |
|-----|--------|
| Shift+Left/Right | Select text |
| Shift+Home/End | Select to start/end |
| Ctrl+A | Select all |
| Ctrl+C | Copy selection to clipboard |
| Ctrl+X | Cut selection |
| Ctrl+Z | Undo |
| Ctrl+Shift+Z | Redo |
| Delete | Delete forward |
| Mouse click | Focus input field |

### Viewer / Hex Viewer

| Key | Action |
|-----|--------|
| Up / Down | Scroll |
| PgUp / PgDn | Scroll by page |
| Home / End | Top / bottom |
| g | Go to line |
| Tab / F4 | Toggle text / hex |
| q / Esc | Close |

### Parquet Viewer

| Key | Action |
|-----|--------|
| Up / Down | Navigate tree / scroll table |
| Right / Enter | Expand node |
| Left | Collapse node / jump to parent |
| PgUp / PgDn | Page through tree or table |
| Home / End | Jump to top / bottom |
| Tab / F4 | Toggle tree / table view |
| g | Go to row |
| q / Esc | Close |

### Editor

| Key | Action |
|-----|--------|
| Arrow keys | Move cursor |
| Ctrl+Left / Right | Word skip (Linux) |
| Option+Left / Right | Word skip (Mac) |
| Home / End | Line start / end |
| Ctrl+Up / Ctrl+Down | File start / end |
| Alt+PageUp / Alt+PageDown | File start / end (Mac: Fn+Opt+Up/Down) |
| PgUp / PgDn | Page up / down |
| Shift+arrows | Select text |
| Ctrl+A | Select all |
| Ctrl+C | Copy selection to clipboard |
| Ctrl+Z | Undo |
| Ctrl+Shift+Z | Redo |
| Ctrl+K / Ctrl+Y | Delete line |
| Ctrl+G | Go to line:col |
| Ctrl+F / F7 | Search |
| Shift+F7 | Find next (repeat last search) |
| F2 / Ctrl+S | Save |
| Esc | Close editor (prompts if unsaved) |
| Mouse click | Position cursor |
| Mouse scroll | Scroll |

## Architecture

```
src/
  main.rs           Terminal setup, event loop, panic hook
  app.rs            App state machine, action dispatch, all modes
  action.rs         Action enum (every possible user intent)
  event.rs          Background thread event polling, coalescing wakeup mechanism
  terminal.rs       Embedded terminal: PTY lifecycle, key encoding (shell + Claude)
  parquet_viewer.rs Parquet file viewer: metadata tree, column stats, table data decoding
  file_search.rs    File content search: ripgrep engine (ignore + grep-searcher), streaming results
  text_input.rs     Reusable text input: selection, undo/redo, cut/copy, horizontal scroll
  ci.rs             CI panel: check/step fetching, log download, tree state
  state.rs          Persistent state (JSON, ~/.config/middle-manager/)
  syntax.rs         Tree-sitter syntax highlighting with hybrid caching
  theme.rs          Centralized color scheme (panel, editor, git, syntax, dialog)
  editor.rs         Built-in editor with line-level piece table and streaming search
  viewer.rs         Text viewer with sliding buffer
  hex_viewer.rs     Hex viewer with sliding buffer
  watcher.rs        Filesystem watcher (kqueue/inotify via notify crate)
  panel/
    mod.rs          Panel state, directory reading, navigation, multi-file selection
    entry.rs        File entry metadata and formatting
    sort.rs         Sort by name/size/date
    git.rs          Git status cache, branch/ahead-behind, async PR queries
    github.rs       GitHub PR info via gh CLI
  vt/
    mod.rs          Custom VT terminal emulator (replaces vt100 crate)
    parser.rs       ANSI/VT escape sequence parser (CSI, OSC, DEC private modes)
    screen.rs       Terminal screen state: cursor, scrollback, resize
    grid.rs         Cell grid with O(1) scroll via ring buffer
    cell.rs         Cell storage with attributes
    attrs.rs        SGR text attributes and color handling
    color.rs        Color types (16, 256, RGB)
  fs_ops/
    mod.rs          Copy, move, delete, mkdir, rename (with nested path support)
    archive.rs      Archive creation (tar.zst, tar.gz, tar.xz, zip) with progress
  ui/
    mod.rs          Top-level layout, mode routing, split CI panels
    panel_view.rs   Panel table rendering with git status column and tree title
    ci_view.rs      CI panel tree rendering with expand/collapse
    header.rs       Header margin
    terminal_view.rs  Terminal panel rendering (VT screen to ratatui spans)
    file_search_dialog.rs  Search-in-files dialog (path, term, filter, regex)
    search_results_view.rs  Search results tree view (files + matching lines)
    footer.rs       Contextual function key hints (normal / CI / terminal mode)
    dialog.rs       Simple dialogs (delete, rename) with cursor navigation
    dialog_helpers.rs  Shared dialog rendering (frame, buttons, checkboxes, separators)
    mkdir_dialog.rs Far Manager-style mkdir dialog with "process multiple names"
    copy_dialog.rs  Far Manager-style copy/move dialog
    search_dialog.rs  Editor search dialog (query, direction, case sensitivity)
    editor_view.rs  Editor rendering with syntax highlighting and selection
    viewer_view.rs  Text viewer rendering with tab expansion
    hex_view.rs     Hex viewer rendering
    parquet_view.rs Parquet viewer rendering (tree + table modes)
    archive_dialog.rs  Archive format picker dialog
    shadow.rs       Transparent dialog drop shadows
```

The editor uses a **line-level piece table**: the file stays on disk, split into segments. Unmodified segments reference the original file by byte offset. Edited lines are stored in memory. On save, unmodified segments are copied as byte ranges (fast), and only edited segments are written from memory. This makes editing a 10 GB file with a few changes practical.

The search engine **streams raw file bytes** in 4 MB chunks, using the sparse line index to seek directly to the cursor position. Forward search reads sequentially; backward search reads in reverse chunks. No per-line file I/O, no full-file scan.

The CI panel fetches data **asynchronously** — check lists, step details, and log downloads all run in background threads. The UI shows animated spinners and never blocks. GitHub Actions and Azure DevOps are both supported, with per-job log downloads (no full-run zip).

## Roadmap

This project is in early development. Things we're considering:

- [ ] Search in viewer (F3)
- [ ] Syntax highlighting in viewer
- [x] Undo/redo in editor
- [ ] File permissions dialog
- [ ] Configurable key bindings
- [ ] Multiple color schemes
- [ ] Archive browsing (tar, zip)
- [x] Built-in terminal panel
- [ ] FTP/SFTP support
- [ ] Plugin system

**We'd love to hear what you want.** Open an [issue](https://github.com/questdb/middle-manager/issues) to request features, report bugs, or share ideas.

## Contributing

Contributions are welcome! Please open an issue to discuss before sending large PRs.

## License

Licensed under the [Apache License 2.0](LICENSE).
