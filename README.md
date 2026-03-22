# Middle Manager

A dual-panel file manager for the terminal, inspired by [Far Manager](https://www.farmanager.com/) and Norton Commander. Built in Rust with [ratatui](https://ratatui.rs/).

Designed to handle **very large files** — the viewer, hex viewer, and editor all use sliding buffers and lazy scanning, so opening a 10 GB log file is instant.

![Middle Manager screenshot](https://img.shields.io/badge/status-early%20development-orange)

## Features

**File Manager**
- Dual-panel layout with directory listings (name, size, date, permissions)
- File operations: copy (F5), move (F6), rename (Shift+F6), mkdir (F7), delete (F8)
- Multi-file selection with Shift+Up/Down and Insert key for batch operations
- Far Manager-style dialogs with keyboard navigation (copy, mkdir, delete, rename)
- Quick search — just start typing to jump to a file, Enter to open
- Sort by name, size, or date (F9), persisted across restarts
- Mouse support — click to select, double-click to open, scroll wheel to navigate
- Filesystem watcher — panels auto-refresh on external changes (kqueue/inotify, zero-cost idle)
- Persistent state — panel paths, sort preferences, and search queries survive restarts

**Git Integration**
- Git status markers per file: `●` modified, `+` added, `-` deleted, `→` renamed, `?` untracked, `!` conflict
- Branch name with `⎇` glyph in panel title
- Ahead/behind remote counts (`↑N ↓M`)
- Repo-wide status summary in panel title (`● 6 + 3 ? 7`)
- GitHub PR status with CI check indicators (`PR #42 ✓` / `✗` / `○`)
- Shared git cache across panels, async PR queries (never blocks UI)
- File name coloring based on git status

**CI Panel (F2)**
- Tree view of CI checks with expand/collapse
- GitHub Actions and Azure DevOps support
- Expand a check to see its steps with status markers (✓ ✗ ○ –)
- Download step logs and open in the built-in editor
- Async fetching with animated spinners — never blocks the UI
- Per-panel CI (left and right panels can each have their own CI view)
- Tab cycles focus: file panel → CI panel → other file panel
- Mouse click support for selecting items in the tree
- Failed checks sorted to top for quick access
- PR number displayed in panel title

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

```
cargo install --path .
```

Or build from source:

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
| Shift+Up / Shift+Down | Extend selection |
| Insert | Toggle selection on current item |
| Left / Home | Jump to top |
| Right / End | Jump to bottom |
| Enter | Open directory / view file |
| Backspace | Go to parent directory |
| Tab | Switch panel (cycles through CI panels too) |
| F2 | Toggle CI panel |
| F3 | View file |
| F4 | Edit file (built-in) |
| Shift+F4 | Edit file ($EDITOR) |
| F5 | Copy (operates on selection if active) |
| F6 | Move (operates on selection if active) |
| Shift+F6 | Rename |
| F7 | Create directory |
| F8 | Delete (operates on selection if active) |
| F9 | Cycle sort |
| F10 | Quit (with confirmation) |
| F11 | Open PR in browser |
| Type chars | Quick search |

### CI Panel

| Key | Action |
|-----|--------|
| Up / Down | Navigate tree |
| Right | Expand check (load steps) |
| Left | Collapse check / jump to parent |
| Enter | Expand/collapse check, or download step log |
| o | Open check in browser |
| Tab | Switch panel |
| F2 | Close CI panel |
| Mouse click | Select item and focus panel |

### Viewer / Hex Viewer

| Key | Action |
|-----|--------|
| Up / Down | Scroll |
| PgUp / PgDn | Scroll by page |
| Home / End | Top / bottom |
| g | Go to line |
| Tab / F4 | Toggle text / hex |
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
  event.rs          Background thread event polling with graceful shutdown
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
  fs_ops/
    mod.rs          Copy, move, delete, mkdir, rename (with nested path support)
  ui/
    mod.rs          Top-level layout, mode routing, split CI panels
    panel_view.rs   Panel table rendering with git status column and tree title
    ci_view.rs      CI panel tree rendering with expand/collapse
    header.rs       Header margin
    footer.rs       Contextual function key hints (normal / CI mode)
    dialog.rs       Simple dialogs (delete, rename) with cursor navigation
    dialog_helpers.rs  Shared dialog rendering (frame, buttons, checkboxes, separators)
    mkdir_dialog.rs Far Manager-style mkdir dialog with "process multiple names"
    copy_dialog.rs  Far Manager-style copy/move dialog
    search_dialog.rs  Editor search dialog (query, direction, case sensitivity)
    editor_view.rs  Editor rendering with syntax highlighting and selection
    viewer_view.rs  Text viewer rendering with tab expansion
    hex_view.rs     Hex viewer rendering
    shadow.rs       Transparent dialog drop shadows
```

The editor uses a **line-level piece table**: the file stays on disk, split into segments. Unmodified segments reference the original file by byte offset. Edited lines are stored in memory. On save, unmodified segments are copied as byte ranges (fast), and only edited segments are written from memory. This makes editing a 10 GB file with a few changes practical.

The search engine **streams raw file bytes** in 4 MB chunks, using the sparse line index to seek directly to the cursor position. Forward search reads sequentially; backward search reads in reverse chunks. No per-line file I/O, no full-file scan.

The CI panel fetches data **asynchronously** — check lists, step details, and log downloads all run in background threads. The UI shows animated spinners and never blocks. GitHub Actions and Azure DevOps are both supported, with per-job log downloads (no full-run zip).

## Roadmap

This project is in early development. Things we're considering:

- [ ] Search in viewer (F3)
- [ ] Syntax highlighting in viewer
- [ ] Undo/redo in editor
- [ ] File permissions dialog
- [ ] Configurable key bindings
- [ ] Multiple color schemes
- [ ] Archive browsing (tar, zip)
- [ ] Built-in terminal panel
- [ ] FTP/SFTP support
- [ ] Plugin system

**We'd love to hear what you want.** Open an [issue](https://github.com/questdb/middle-manager/issues) to request features, report bugs, or share ideas.

## Contributing

Contributions are welcome! Please open an issue to discuss before sending large PRs.

## License

Licensed under the [Apache License 2.0](LICENSE).
