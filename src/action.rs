/// All possible actions the application can perform.
/// Key presses are mapped to Actions; the App dispatches them.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    // Navigation
    MoveUp,
    MoveDown,
    MoveToTop,
    MoveToBottom,
    PageUp,
    PageDown,
    Enter,
    GoUp,
    SwitchPanel,
    SwitchPanelReverse,
    CopyName,
    CopyPath,
    GotoPathPrompt,
    FuzzySearchPrompt,

    // Panel multi-file selection
    ToggleSelect,
    SelectMoveUp,
    SelectMoveDown,

    // File operations
    Copy,
    Move,
    Rename,
    CreateDir,
    Delete,
    ViewFile,
    EditFile,

    // Sorting
    CycleSort,

    // Quick search
    QuickSearch(char),
    QuickSearchClear,

    // Dialog
    DialogConfirm,
    DialogCancel,
    DialogInput(char),
    DialogBackspace,

    // Toggle checkbox / cycle dropdown in copy dialog
    Toggle,

    // Go to line (all viewer/editor modes)
    GotoLinePrompt,

    // Selection
    SelectUp,
    SelectDown,
    SelectLeft,
    SelectRight,
    SelectLineStart,
    SelectLineEnd,
    SelectPageUp,
    SelectPageDown,
    SelectAll,
    CopySelection,

    // Search (editor)
    SearchPrompt,
    FindNext,

    // Word navigation (editor)
    WordLeft,
    WordRight,

    // Editor undo/redo
    EditorUndo,
    EditorRedo,

    // Editor
    EditBuiltin,
    CursorLeft,
    CursorRight,
    CursorLineStart,
    CursorLineEnd,
    EditorSave,
    EditorNewline,
    EditorDeleteForward,
    EditorDeleteLine,

    // Mouse
    MouseClick(u16, u16), // (column, row)
    MouseDoubleClick(u16, u16),
    MouseScrollUp(u16, u16),
    MouseScrollDown(u16, u16),

    // GitHub / CI
    OpenPr,
    ToggleCi,

    // Terminal
    ToggleTerminal,
    TerminalInput(Vec<u8>),
    TerminalOpenFile,

    // Help
    ShowHelp,

    // Application
    Quit,
    Resize(u16, u16),
    Tick,
    None,
}
