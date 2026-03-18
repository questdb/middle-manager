# Middle Manager

A dual-panel file manager for the terminal, inspired by [Far Manager](https://www.farmanager.com/) and Norton Commander. Built in Rust with [ratatui](https://ratatui.rs/).

Designed to handle **very large files** — the viewer, hex viewer, and editor all use sliding buffers and lazy scanning, so opening a 10 GB log file is instant.

![Middle Manager screenshot](https://img.shields.io/badge/status-early%20development-orange)

## Features

**File Manager**
- Dual-panel layout with directory listings (name, size, date, permissions)
- File operations: copy (F5), move (F6), rename (Shift+F6), mkdir (F7), delete (F8)
- Far Manager-style copy dialog with overwrite modes, symlink handling, and more
- Quick search — just start typing to jump to a file, Enter to open
- Sort by name, size, or date (F9)
- Mouse support — click to select, double-click to open, scroll wheel to navigate

**Text Viewer (F3 / Enter)**
- Sliding buffer: only ~10K lines in memory at a time
- Sparse line index for instant seeking to any position
- Opens multi-GB files instantly, scrolls smoothly
- Go-to-line with `g` — supports `line` or `line:col` format

**Hex Viewer**
- Auto-detects binary files (null byte check)
- VS Code-style layout: offset | hex bytes | ASCII decode
- 256 KB sliding buffer for arbitrarily large binaries
- Toggle between text and hex with Tab or F4

**Built-in Editor (F4)**
- Line-level piece table — only edited lines live in memory
- Opens and navigates multi-GB files with no delay
- Tab expansion, control character sanitization
- Text selection with Shift+arrow keys
- Ctrl+C to copy to system clipboard (OSC 52)
- Ctrl+A to select all
- Ctrl+K to delete line
- F2 / Ctrl+S to save — uses byte-range copying for unmodified segments
- Ctrl+G for go-to-line
- Shift+F4 opens `$EDITOR` instead

**UI**
- Far Manager classic blue color scheme
- Centralized theme system — change all colors in one file (`src/theme.rs`)
- Dialog drop shadows
- Panic hook that restores the terminal before printing errors

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
| Left / Home | Jump to top |
| Right / End | Jump to bottom |
| Enter | Open directory / view file |
| Backspace | Go to parent directory |
| Tab | Switch panel |
| F3 | View file |
| F4 | Edit file (built-in) |
| Shift+F4 | Edit file ($EDITOR) |
| F5 | Copy |
| F6 | Move |
| Shift+F6 | Rename |
| F7 | Create directory |
| F8 | Delete |
| F9 | Cycle sort |
| F10 / Esc | Quit |
| Type chars | Quick search |

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
| Home / End | Line start / end |
| Ctrl+Home / Ctrl+End | File start / end |
| PgUp / PgDn | Page up / down |
| Shift+arrows | Select text |
| Ctrl+A | Select all |
| Ctrl+C | Copy selection to clipboard |
| Ctrl+K | Delete line |
| Ctrl+G | Go to line:col |
| F2 / Ctrl+S | Save |
| Esc | Close editor |

## Architecture

```
src/
  main.rs           Terminal setup, event loop, panic hook
  app.rs            App state machine, action dispatch, all modes
  action.rs         Action enum (every possible user intent)
  event.rs          Background thread event polling
  theme.rs          Centralized color scheme
  editor.rs         Built-in editor with line-level piece table
  viewer.rs         Text viewer with sliding buffer
  hex_viewer.rs     Hex viewer with sliding buffer
  panel/
    mod.rs          Panel state, directory reading, navigation
    entry.rs        File entry metadata and formatting
    sort.rs         Sort by name/size/date
  fs_ops/
    mod.rs          Copy, move, delete, mkdir, rename
  ui/
    mod.rs          Top-level layout, mode routing
    panel_view.rs   Panel table rendering
    header.rs       Path display bar
    footer.rs       Function key hints
    dialog.rs       Simple dialogs (delete, mkdir, rename)
    copy_dialog.rs  Far Manager-style copy/move dialog
    editor_view.rs  Editor rendering with selection highlight
    viewer_view.rs  Text viewer rendering
    hex_view.rs     Hex viewer rendering
    shadow.rs       Transparent dialog drop shadows
```

The editor uses a **line-level piece table**: the file stays on disk, split into segments. Unmodified segments reference the original file by byte offset. Edited lines are stored in memory. On save, unmodified segments are copied as byte ranges (fast), and only edited segments are written from memory. This makes editing a 10 GB file with a few changes practical.

## Roadmap

This project is in early development. Things we're considering:

- [ ] Search / find in viewer and editor
- [ ] Undo/redo in editor
- [ ] File permissions dialog
- [ ] Multi-file selection and batch operations
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
