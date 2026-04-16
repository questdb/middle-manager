# Review PR

Review a pull request for the Middle Manager TUI project (Rust / ratatui).

## Review tone

Be critical and ruthless. This is not a courtesy review — it is a gate. The goal is to catch every issue before it enters the codebase, not to make the author feel good.

- **Do not hedge.** Write "This is wrong because..." not "You might want to consider...". Write "This will panic" not "This could potentially cause issues".
- **Do not approve to be nice.** If there are warnings, the verdict is COMMENT. If there are blockers, the verdict is REQUEST_CHANGES. APPROVE means you found zero substantive issues — not that the issues are "minor enough".
- **Assume nothing is correct.** Verify every arithmetic expression, every index, every unwrap, every boundary. The author may have tested the happy path; your job is to find what they missed.
- **Don't stop at the first issue.** Find all of them. A review that catches 1 of 5 problems wastes everyone's time in re-review cycles.
- **Every finding must be actionable.** State what's wrong, where (file:line), why it matters, and what the fix is. Vague complaints are worthless.

## Input

$ARGUMENTS — a PR number, branch name, or empty (defaults to current branch vs master).

## Steps

### 1. Identify the PR

- If a PR number is given (e.g. `17`), fetch it with `gh pr view <number>`.
- If a branch name is given, fetch `gh pr view <branch>`.
- If empty, use the current branch; fail if on `master`.
- Print the PR title, author, and URL.

### 2. Gather the diff

Run `gh pr diff <number> --patch` to get the full diff. Also run `gh pr view <number> --json commits,files,additions,deletions` to get stats. Note the list of changed files and the nature of each change (added/modified/deleted) — this drives what you read in step 3.

### 3. Read changed files

For every file in the diff, you need the current version to understand context. But context is finite — prioritize:

- **Files under 500 lines:** Read the full file.
- **Files 500–2000 lines:** Read the changed regions plus 150 lines of context above and below each hunk. Also read the file's imports and struct/enum definitions so you understand the types in play.
- **Files over 2000 lines (e.g. `app.rs` at ~8000 lines):** Read only the changed hunks with 150 lines of context. If a finding requires understanding distant code, read that specific region on demand during review — don't pre-load the entire file.

**Also review deletions.** The diff shows removed code that no longer exists in the current file. For every deletion, check:
- Was the deleted code the only call site for a function/method? If so, that function is now dead code.
- Was the deleted code handling an edge case that still needs handling elsewhere?
- Did the deletion remove error handling or safety checks that are still needed?

### 4. Review checklist

Go through each of the following categories. For each, note findings as **pass**, **info** (neutral observation), **nit** (trivial suggestion), **warn** (should fix before merge), or **block** (must fix before merge).

#### 4a. Correctness & Logic
- Does the code do what the PR description says?
- Are there off-by-one errors, especially in index/offset arithmetic (common in editor, hex viewer, panel layout)?
- Are `match` arms exhaustive? Are new enum variants handled everywhere they need to be?
- Are boundary conditions handled (empty panels, zero-length files, terminal resize to 1x1, etc.)?
- Does the dirty flag get set on every state mutation that should trigger a re-render?

#### 4b. No panics (important)
This is a TUI app that takes over the terminal. A panic leaves the terminal in a broken state. Treat any potential panic path in non-test code as a **block**.

Scan all changed lines for:
- `.unwrap()` — must be replaced with `?`, `.unwrap_or()`, `.unwrap_or_default()`, `.unwrap_or_else()`, or a match/if-let. The only exception is when the value is **provably** `Some`/`Ok` by construction (e.g. a `const` regex, or an index that was just bounds-checked on the previous line) — and even then, prefer `.expect("reason")` with an explanation.
- `.expect()` without a clear invariant comment — if used, the message must explain *why* this can't fail.
- Direct indexing `foo[i]` on slices/vecs — prefer `.get(i)` with a fallback, especially when `i` comes from arithmetic or user input. Indexing is acceptable only when the bounds are trivially guaranteed (e.g. iterating `0..vec.len()`).
- Integer underflow: `usize` subtraction (`a - b`) where `b` could exceed `a` — use `.saturating_sub()` or check before subtracting. This is especially common in scroll offsets, cursor positions, and layout calculations.
- Integer overflow: `usize` addition/multiplication that could wrap — use `.saturating_add()` / `.checked_mul()` where the inputs aren't bounded.
- Slice operations: `.split_at()`, `&s[start..end]` — verify `start <= end` and both are within bounds, or use `.get(start..end)`.

#### 4c. No unnecessary allocations (important)
This app re-renders on every state change (dirty flag pattern) and handles files up to 10 GB. Unnecessary allocations in render and keystroke-handler paths cause visible jank — every allocation in these paths runs on every user interaction.

Scan for:
- `format!()` used only to pass to another `format!()` or to a function that accepts `&str` — pass the parts directly or use `write!()`.
- `.to_string()` / `.to_owned()` when borrowing would suffice — especially in match arms, function arguments, and struct construction.
- `.clone()` on types that implement `Copy`, or on values that could be borrowed instead.
- `String::new()` + `.push_str()` in a loop — prefer `String::with_capacity()` or `write!()`.
- `Vec::new()` or `vec![]` inside a loop body — hoist the allocation outside and `.clear()` each iteration.
- `collect::<Vec<_>>()` followed immediately by `.iter()` — iterate directly without materializing.
- Allocating `String`/`Vec` in `ui::render()` functions — these run on every state change and should reuse buffers or use stack-allocated alternatives (`ArrayString`, `SmallVec`, or `Cow<str>`).
- Building a `String` just to measure its `.len()` — compute the length arithmetically.

Flag unnecessary allocations in render paths or per-keystroke handlers as **warn**. In cold paths (startup, one-shot file ops), they are **nit**.

#### 4d. Rust idioms
- Atomic ordering: `SeqCst` where correctness matters across threads, `Relaxed` only where documented safe.
- Lifetime and ownership: no needless `Arc`/`Rc` when a borrow suffices.
- Prefer `if let` / `let else` over `.is_some()` followed by `.unwrap()`.

#### 4e. Safety & Security
- No `unsafe` blocks without a `// SAFETY:` comment explaining the invariant.
- File operations: paths must be validated against traversal (e.g. `..` in user-provided names).
- Shell commands built from user input must be properly escaped.
- PTY/terminal operations: verify file descriptors are closed on error paths.

#### 4f. Concurrency
- Shared state between threads uses proper synchronization.
- Background threads (git cache, CI fetching, archive ops) check cancellation / don't outlive the app.
- No potential deadlocks from lock ordering.

#### 4g. Algorithmic performance (important)
This app handles directories with 100k+ entries, files up to 10 GB, and re-renders on every state change. Mediocre algorithms are not acceptable — always look for the optimal approach.

For every loop, search, or data structure in the diff, ask:
- **Is this the best complexity class?** O(n^2) where O(n) or O(n log n) exists is a **block**. O(n) where O(log n) or O(1) is achievable (e.g. binary search on sorted data, hash lookup instead of linear scan) is a **warn**.
- **Linear scans over sorted data:** If the data is sorted or could be sorted, use binary search (`.binary_search()`, `.partition_point()`), not `.iter().find()` or `.iter().position()`.
- **Repeated lookups:** If the same collection is searched multiple times, should it be a `HashMap`/`HashSet`/`BTreeMap` instead of a `Vec`?
- **Redundant passes:** Multiple `.iter()` chains over the same collection that could be fused into a single pass.
- **Rendering path:** `ui::render()` must only read state and produce spans. Any sorting, filtering, searching, or diffing belongs in state update, not in render. Render runs on every state change — it must be as close to a dumb data-to-spans projection as possible.
- **File I/O:** Large reads must use `BufReader`. Sequential small reads to the same file should be batched.
- **Pre-computation:** Can work be done once at state-change time and cached, rather than recomputed per-render or per-keystroke?

If you see an algorithm that works but isn't optimal, don't just flag it — suggest the better algorithm with enough detail to implement it.

#### 4h. UI/UX
- Keyboard mappings: do new keybindings conflict with existing ones? Check `map_key_to_action` in app.rs.
- Layout: do new UI elements handle small terminal sizes gracefully (< 80x24)?
- Footer hints: are new actions reflected in `footer.rs`?

**Help dialog sync (important):**
Every keyboard shortcut introduced or changed in a PR **must** have a corresponding update in `src/ui/help_dialog.rs`. The help is defined as a static `HELP_SECTIONS` const — an array of `(section_title, &[(key, description)])` tuples.

To verify:
  1. Identify all key-handling changes in the PR: look for new/modified arms in `map_key_to_action()` in `app.rs`, or `KeyCode::`/`KeyEvent::` matches in any changed file.
  2. For each new or changed binding, confirm there is a matching entry in the appropriate section of `HELP_SECTIONS` in `src/ui/help_dialog.rs`.
  3. If a shortcut was removed, confirm its help entry was also removed.
  4. If a new mode or panel was added, confirm a new section was added to `HELP_SECTIONS`.

Flag missing help updates as a **block** — users have no other way to discover keybindings.

**Theme enforcement (important):**
All colors in this project must go through the centralized theme system in `src/theme.rs`. The theme is accessed via the global `theme()` function which returns `&'static Theme`.

To verify, grep for `Color::` in all changed files outside of `src/theme.rs`. Any match is a violation unless it is:
  - Inside `theme.rs` itself (where the palette is defined)
  - A test or non-UI module

The correct patterns are:
  - `theme().some_field` for a raw `Color` value (e.g. `theme().error_fg`)
  - `theme().some_style_method()` for a complete `Style` (e.g. `theme().highlight_style()`)

If the PR introduces a new color need, the fix is:
  1. Add a new field to the `Theme` struct
  2. Set its value in `Theme::far_manager()`
  3. Optionally add a convenience style builder method
  4. Use `theme().new_field` in UI code

Flag any hardcoded `Color::` in UI code as a **block** — this breaks theming consistency and makes future theme-switching impossible.

**Dialog consistency (important):**
All dialogs must use the shared dialog system in `src/ui/dialog_helpers.rs`. No ad-hoc dialog rendering.

To verify new or modified dialogs follow the established pattern:
  1. **Frame:** Must use `dh::render_dialog_frame()` for centering, border, background, title. No manual `Rect` centering or `Block` rendering.
  2. **Styles:** Must call `dh::dialog_styles()` to get the `(normal, highlight, input_normal)` triple. No custom one-off styles for dialog elements.
  3. **Components — use the shared primitives, no reimplementations:**
     - Text inputs: `dh::render_text_input()` (wraps `TextInput` from `src/text_input.rs` — supports selection, undo/redo, horizontal scroll)
     - Buttons: `dh::render_buttons()` (centered, highlight when focused)
     - Checkboxes: `dh::render_checkbox()`
     - Separators: `dh::render_separator()`
     - Text lines: `dh::render_line()`
  4. **Focus / tab order:** Each dialog must define a `FooDialogField` enum in `app.rs` that lists all focusable elements. Tab/Shift+Tab must cycle through them in visual top-to-bottom order. Focused fields use `highlight` style, unfocused use `input_normal`.
  5. **Text inputs must use `TextInput`** from `src/text_input.rs`, not raw string buffers. This ensures undo/redo, selection, clipboard, and cursor behavior are consistent across all dialogs.

Flag any dialog that rolls its own frame, styles, input handling, or tab order instead of using the shared components as a **block**.

#### 4i. Cross-platform
This project builds for 4 targets: `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`. Code must work on all of them.

- `#[cfg(target_os = "macos")]` / `#[cfg(target_os = "linux")]` — any platform-specific code must have both branches or a clear fallback. A `#[cfg]` for one platform without the other is a **block**.
- libc types: `c_ulong` vs platform-specific ioctl types differ between macOS and Linux (see PR #15 for precedent). Use `libc::` types, not `c_ulong` directly.
- File paths: no assumptions about case sensitivity (macOS HFS+ is case-insensitive by default, Linux ext4 is not). No hardcoded path separators.
- Filesystem behavior: symlink resolution, xattr support, and sparse file handling differ across platforms. Test both code paths mentally.
- Hex editor: if touching binary data representation, consider endianness — though both current targets are little-endian, don't introduce assumptions.
- Terminal escape sequences: some sequences behave differently across terminal emulators (especially iTerm2 vs Linux console vs tmux). If the PR adds new escape sequence handling in `src/vt/`, verify it follows the ANSI/VT standard, not a single emulator's quirk.
- musl linking: musl targets are statically linked. New dependencies that rely on dynamic linking or system libraries (e.g. openssl, libgit2) will break musl builds — flag as **block**.

#### 4j. Code quality (important)

**No dead code:**
The codebase must not accumulate unused code. Scan the diff for:
- Functions, methods, structs, enums, or enum variants that are added but never called/used — or that were made unreachable by other changes in the PR.
- `#[allow(dead_code)]` added to silence the compiler — this is almost always wrong. If the code isn't used, delete it. The only exception is code that is used conditionally via `#[cfg()]`.
- Commented-out code — delete it. Git history preserves old code; comments are not version control.
- Imports (`use`) that are no longer needed after the PR's changes.
- `pub` visibility on items that are only used within the same module — should be `pub(crate)` or private.

Flag dead code as **warn**. Flag `#[allow(dead_code)]` on new code as **block**.

**No code duplication:**
Scan for:
- Copy-pasted logic across the PR — if two blocks of code do the same thing with minor variations, they should be extracted into a shared function or parameterized.
- New code that duplicates logic already existing elsewhere in the codebase. Read the surrounding modules to check — don't just look at the diff in isolation.
- Repeated match arms with identical bodies — collapse with `|` patterns.
- Duplicated constants or magic numbers that should be a single `const`.

Flag duplication as **warn** for 2 copies, **block** for 3+.

**Code smell:**
- Functions over 100 lines — should they be broken up into smaller, well-named functions?
- Deeply nested code (3+ levels of `if`/`match`/`for`) — can it be flattened with early returns, `let else`, or extraction?
- New public API: is it the minimal surface area needed? Don't expose more than necessary.
- Magic numbers: should constants be named?
- Boolean parameters — should they be an enum for clarity at the call site?
- Long parameter lists (5+) — should they be grouped into a struct?

#### 4k. Test coverage (important)
This is a TUI project — rendering code is hard to unit test and that's accepted. But everything underneath the UI layer is testable and **must** be tested. The codebase already has test modules in `editor.rs`, `hex_viewer.rs`, `diff_viewer.rs`, `text_input.rs`, `pr_diff.rs`, `panel/mod.rs`, `fs_ops/mod.rs`, `vt/*`, `clipboard.rs`, and some UI utility modules. New code in these areas must maintain or improve coverage.

**What must be tested (flag missing tests as block):**
- Parsers and data transformations: diff parsing (`pr_diff.rs`), VT escape sequence parsing (`vt/parser.rs`), any new format handling.
- Algorithms and data structures: piece table operations (`editor.rs`), sliding buffer logic (`hex_viewer.rs`), ring buffer (`vt/grid.rs`), sort implementations, search algorithms.
- Arithmetic and offset calculations: scroll positions, cursor movement, byte offset conversions, line/column mapping — these are the #1 source of off-by-one bugs.
- State machines and transitions: mode switching, dialog field focus cycling, selection logic.
- File operations: copy/move/delete edge cases (`fs_ops/`), path manipulation, archive creation.
- Text input: cursor movement, selection, undo/redo, clipboard (`text_input.rs`).

**What doesn't need unit tests:**
- `ui::render()` functions that produce ratatui widgets — these are visual and tested by using the app.
- `main.rs` terminal setup/teardown.
- Thin wrappers around external tools (`gh`, `git` CLI calls).

**Test quality — if the PR adds tests, verify:**
- Do they test edge cases and failure modes, not just the happy path? A test suite that only checks "normal input produces normal output" is nearly worthless. Test: empty input, single element, maximum size, malformed data, boundary values (0, 1, max-1, max).
- Are assertions specific? `assert!(result.is_ok())` is weak — assert on the actual value. `assert_eq!` with expected output, not just `assert!` with a boolean.
- Do tests cover the boundaries that the code actually guards? If the code has a `saturating_sub`, there should be a test where the subtraction would underflow.
- Are test names descriptive? `test_it_works` is not a test name. `test_cursor_at_line_end_wraps_to_next_line` is.

**Existing test breakage:**
- If the PR changes behavior that existing tests cover, those tests must be updated. A PR that changes logic but doesn't touch existing tests is a **block** — either the tests are stale or the behavioral change wasn't intentional.

**Missing coverage in changed modules:**
- If the PR modifies a module that already has a `#[cfg(test)]` section, new logic added to that module should have corresponding test cases. Flag as **block** — the testing infrastructure is already there, there is no excuse.

#### 4l. CI compatibility
- Will `cargo fmt --check` pass? (formatting)
- Will `cargo clippy -- -D warnings` pass? (lints)
- Will `cargo test` pass? (if tests exist for the area)
- Any new dependencies that might break cross-compilation (musl targets)?

### 5. Verify all findings (important — do not skip)

Every finding from step 4 must be double-checked before it appears in the final review. Unverified findings are worse than no findings — they waste the author's time and erode trust in the review process.

**Why this matters:** LLM reviewers are prone to:
- **Hallucinated line numbers** — citing a line that doesn't contain what was claimed.
- **Phantom bugs** — describing a bug that doesn't exist because the code was misread or context was lost.
- **Context poisoning** — after finding one real issue, pattern-matching similar-looking code as also broken when it's actually fine.
- **Stale context** — confusing the current file state with an earlier version seen in the diff.

**Verification process:**

For each **block** and **warn** finding, spawn an independent agent to verify it. Nits are low-stakes — spot-check a few if time permits, but don't verify all of them. The agent must:
  1. **Re-read the exact file and line** cited in the finding — confirm the code actually says what the finding claims.
  2. **Evaluate the claim independently** — does the alleged bug/issue actually exist? Trace the logic. Don't just confirm because the original review said so.
  3. **Check for false positives** — is there surrounding context (a bounds check earlier, a type guarantee, a cfg gate) that makes the finding invalid?
  4. **Verdict:** Confirm, reject, or amend the finding.

**Parallelism:** Launch verification agents in parallel — one per finding or one per group of related findings in the same file. Each agent gets only the finding text and the file path, NOT the full review — this prevents context poisoning where the verifier is biased by the reviewer's narrative.

**Agent prompt template:**
> Verify this code review finding. Read the file yourself and evaluate independently whether the issue is real.
> 
> **File:** `{path}`
> **Line(s):** `{line range}`
> **Claimed issue:** `{finding text}`
> 
> Read the file at the specified location. Is this finding correct? Check for surrounding context that might invalidate it. Report: CONFIRMED, REJECTED (with reason), or AMENDED (with correction).

**After verification:**
- Drop any finding that was REJECTED.
- Update any finding that was AMENDED.
- Keep CONFIRMED findings as-is.
- If more than 30% of findings were rejected, re-run the review on the sections that produced false positives — your initial read of that code was likely wrong.

### 6. Summarize

Output a structured review with:

```
## PR Review: <title> (#<number>)

**Verdict:** APPROVE | REQUEST_CHANGES | COMMENT

### Summary
<2-3 sentence overview of what the PR does and overall quality>

### Findings

#### Blockers
<list or "None">

#### Warnings  
<list or "None">

#### Nits
<list or "None">

#### Positive observations
<things done well worth calling out>
```

For each finding, reference the specific file and line, explain the issue, and suggest a fix when possible. Use the format `path/to/file.rs:123` for locations.

### 7. Post to GitHub (only if asked)

If the user says "post" or "submit", use `gh pr review <number>` to post the review. Otherwise just print it locally. When posting:
- Use `--approve` for APPROVE verdict
- Use `--request-changes` for REQUEST_CHANGES
- Use `--comment` for COMMENT
- Pass the body via `--body`
