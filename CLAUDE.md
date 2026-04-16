# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Middle Manager is a dual-panel TUI file manager written in Rust, inspired by Far Manager/Norton Commander. Built on ratatui + crossterm. Designed for large files — viewer, hex viewer, and editor use sliding buffers and lazy scanning (10 GB files open instantly).

Binary is installed as `mm`.

## Build & Test Commands

```bash
cargo build                    # dev build
cargo build --release          # release build
cargo test                     # all tests
cargo test test_name           # single test
cargo test test_name -- --nocapture  # with stdout
cargo fmt --check              # format check
cargo fmt                      # format fix
cargo clippy -- -D warnings    # lint (CI enforces -D warnings)
```

CI runs: fmt check, clippy with `-D warnings`, tests on both Ubuntu and macOS.

## Fixing issues

Fix every issue you encounter, even if it's pre-existing and unrelated to your current task — do not leave broken clippy lints, failing tests, fmt drift, or compiler warnings for a later PR. If CI is red because of a problem that exists on master, fix it here rather than skipping it. Scope creep from a few mechanical fixes is cheaper than a persistently red main branch.

`cargo clippy -- -D warnings` is not good enough — it only checks the default (bin) target. Always verify with `cargo clippy --all-targets -- -D warnings` so test-code lints are not missed, and fix anything it surfaces.

## Architecture

**Single crate, no workspace.** ~50 source files, ~400+ inline unit tests.

### Action-Dispatch Pattern
Key presses → `Action` enum (action.rs, 120+ variants) → `App` handlers (app.rs). This decouples input handling from business logic. All user intents go through this enum.

### App State Machine (app.rs)
Central struct holding: two `Panel`s (left/right), active panel index, `PanelFocus` (file panel, CI, shell, Claude, diff, search), `AppMode` (Normal, Viewer, Editor, etc.), 20+ overlay states (dialogs, prompts). This is the largest file in the codebase.

### Large-File Handling
- **ViewerState** (viewer.rs): Sliding buffer (~10K lines in memory), sparse line index (every 1000 lines) for O(1) seeking
- **EditorState** (editor.rs): Line-level piece table — unmodified segments reference disk by byte offset, only edited lines in memory. Save copies unmodified byte ranges directly.
- **HexViewerState** (hex_viewer.rs): 256 KB sliding buffer
- **Search**: Streams 4 MB chunks, seeks directly to cursor position via sparse index. No full-file scan.

### Custom VT Terminal Emulator (vt/)
Replaces vt100 crate for full control. Ring buffer grid for O(1) scrolling, 10K-line scrollback. Handles SGR, OSC, DEC private modes. Used by Shell Panel (Ctrl+O) and Claude Code Panel (F12).

### Event System (event.rs)
Background thread polls keyboard/mouse/resize. Coalescing wakeup mechanism: multiple PTY output signals collapse to one event, preventing event loop flooding. `WakeupSender` passed to background threads.

### Module Organization
- `panel/` — Panel state, directory reading, entry metadata, sorting, git status cache, GitHub PR queries (via `gh` CLI)
- `vt/` — VT parser, screen state, ring buffer grid, cell/attribute storage, color types
- `fs_ops/` — Copy/move/delete/mkdir/rename, archive creation (tar.zst/gz/xz, zip)
- `ui/` — All rendering: one `*_view.rs` per mode, dialog helpers, footer, header, shadows. Rendering is strictly separated from logic.
- Top-level modules: `ci.rs` (CI panel), `terminal.rs` (PTY lifecycle), `syntax.rs` (tree-sitter highlighting), `theme.rs` (centralized Far Manager blue color scheme), `state.rs` (persistent JSON state at `~/.config/middle-manager/state.json`), `text_input.rs` (reusable input widget with selection/undo/redo), `file_search.rs` (ripgrep-powered search)

### Key Patterns
- **Syntax highlighting** (syntax.rs): Tree-sitter with hybrid caching — files < 10 MB get full parse cached; ≥ 10 MB use context-window (200 lines before viewport)
- **Git integration** (panel/git.rs): `GitCache` shared across both panels, async queries, 30-second refresh, `--no-optional-locks` to avoid index.lock conflicts
- **Persistent state** (state.rs): Panel paths, sort prefs, search queries, open panels, split sizes survive restarts
- **Background threading**: Git status, PR queries, CI fetches, file search, archive compression — all non-blocking with `WakeupSender` for UI updates

### Key Dependencies
- `ratatui` 0.30 + `crossterm` 0.28 — TUI framework
- `tree-sitter` + language grammars — syntax highlighting
- `portable-pty` — PTY spawning for shell/Claude panels
- `ignore` + `grep-regex`/`grep-searcher` — ripgrep's search engine
- `parquet2` (QuestDB fork from git) — Parquet file reading
- `notify` — filesystem watcher (kqueue/inotify)
- `tar`/`flate2`/`xz2`/`zstd`/`zip` — archive compression
