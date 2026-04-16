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
    EditFile,
    Archive,
    CalcSize,
    ViewFile,

    CreateFile,

    // Sorting
    CycleSort,
    SortByName(usize), // panel index
    SortBySize(usize),
    SortByDate(usize),

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
    ToggleReverse,

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
    FindPrev,

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
    MouseClick(u16, u16),       // (column, row)
    MouseShiftClick(u16, u16),  // shift+click: extend selection
    MouseDoubleClick(u16, u16), // select word
    MouseTripleClick(u16, u16), // select line
    MouseDrag(u16, u16),        // click-and-drag: extend selection
    MouseScrollUp(u16, u16),
    MouseScrollDown(u16, u16),

    // GitHub / CI
    OpenPr,
    ToggleCi,
    ExtractCiFailures,
    OpenAzureAuth,
    ToggleDiff,

    // Terminal / Shell / SSH / Sessions
    ToggleClaude,
    ToggleShell,
    ToggleSsh,
    ToggleSessions,
    TerminalInput(Vec<u8>),
    TerminalOpenFile,
    BottomResizeUp,
    BottomResizeDown,
    BottomMaximize,

    // File content search
    FileSearchPrompt,

    // Help
    ShowHelp,

    // Menu
    OpenMenu,

    // Settings
    ToggleSettings,

    // Application
    Quit,
    Resize(u16, u16),
    Tick,
    None,
}
