/// Reusable text input with cursor, selection, clipboard, undo/redo, and cut support.
#[derive(Clone)]
pub struct TextInput {
    pub text: String,
    pub cursor: usize,
    /// Selection anchor. When Some, selection spans from anchor to cursor.
    pub anchor: Option<usize>,
    /// Undo stack: (text, cursor) snapshots before each edit.
    undo_stack: Vec<(String, usize)>,
    /// Redo stack: (text, cursor) snapshots.
    redo_stack: Vec<(String, usize)>,
}

impl TextInput {
    pub fn new(text: String) -> Self {
        let cursor = text.len();
        Self {
            text,
            cursor,
            anchor: None,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self::new(String::new())
    }

    /// Save current state to undo stack before an edit.
    fn save_undo(&mut self) {
        self.undo_stack.push((self.text.clone(), self.cursor));
        self.redo_stack.clear();
        // Cap at 100 entries
        if self.undo_stack.len() > 100 {
            self.undo_stack.remove(0);
        }
    }

    /// Undo the last edit (Ctrl+Z).
    pub fn undo(&mut self) {
        if let Some((text, cursor)) = self.undo_stack.pop() {
            self.redo_stack.push((self.text.clone(), self.cursor));
            self.text = text;
            self.cursor = cursor;
            self.anchor = None;
        }
    }

    /// Redo the last undone edit (Ctrl+Shift+Z).
    pub fn redo(&mut self) {
        if let Some((text, cursor)) = self.redo_stack.pop() {
            self.undo_stack.push((self.text.clone(), self.cursor));
            self.text = text;
            self.cursor = cursor;
            self.anchor = None;
        }
    }

    /// Cut selection to clipboard (Ctrl+X). Returns true if something was cut.
    pub fn cut_selection(&mut self) -> bool {
        if let Some(text) = self.selected_text() {
            if !text.is_empty() {
                crate::clipboard::copy(text);
                self.save_undo();
                self.delete_selection();
                return true;
            }
        }
        false
    }

    /// Clear any active selection.
    pub fn clear_selection(&mut self) {
        self.anchor = None;
    }

    /// Get the selected range as (start, end) byte offsets, ordered.
    pub fn selection_range(&self) -> Option<(usize, usize)> {
        self.anchor.map(|a| {
            if a <= self.cursor {
                (a, self.cursor)
            } else {
                (self.cursor, a)
            }
        })
    }

    /// Get the selected text, if any.
    pub fn selected_text(&self) -> Option<&str> {
        self.selection_range().map(|(s, e)| &self.text[s..e])
    }

    /// Delete the selected text, leaving cursor at the start of the selection.
    pub fn delete_selection(&mut self) -> bool {
        if let Some((start, end)) = self.selection_range() {
            if start != end {
                self.text.drain(start..end);
                self.cursor = start;
                self.anchor = None;
                return true;
            }
        }
        self.anchor = None;
        false
    }

    /// Insert a character, replacing selection if active.
    pub fn insert_char(&mut self, c: char) {
        self.save_undo();
        self.delete_selection();
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
    }

    /// Backspace: delete selection or char before cursor.
    pub fn backspace(&mut self) {
        self.save_undo();
        if self.delete_selection() {
            return;
        }
        if self.cursor > 0 {
            let prev = self.text[..self.cursor]
                .char_indices()
                .last()
                .map(|(i, _)| i)
                .unwrap_or(0);
            self.text.remove(prev);
            self.cursor = prev;
        }
    }

    /// Delete forward: delete selection or char at cursor.
    pub fn delete_forward(&mut self) {
        self.save_undo();
        if self.delete_selection() {
            return;
        }
        if self.cursor < self.text.len() {
            self.text.remove(self.cursor);
        }
    }

    /// Move cursor left, clearing selection.
    pub fn move_left(&mut self) {
        self.anchor = None;
        if self.cursor > 0 {
            self.cursor = self.text[..self.cursor]
                .char_indices()
                .last()
                .map(|(i, _)| i)
                .unwrap_or(0);
        }
    }

    /// Move cursor right, clearing selection.
    pub fn move_right(&mut self) {
        self.anchor = None;
        if self.cursor < self.text.len() {
            self.cursor += self.text[self.cursor..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(0);
        }
    }

    /// Move to start, clearing selection.
    pub fn move_home(&mut self) {
        self.anchor = None;
        self.cursor = 0;
    }

    /// Move to end, clearing selection.
    pub fn move_end(&mut self) {
        self.anchor = None;
        self.cursor = self.text.len();
    }

    /// Extend selection left (Shift+Left).
    pub fn select_left(&mut self) {
        if self.anchor.is_none() {
            self.anchor = Some(self.cursor);
        }
        if self.cursor > 0 {
            self.cursor = self.text[..self.cursor]
                .char_indices()
                .last()
                .map(|(i, _)| i)
                .unwrap_or(0);
        }
    }

    /// Extend selection right (Shift+Right).
    pub fn select_right(&mut self) {
        if self.anchor.is_none() {
            self.anchor = Some(self.cursor);
        }
        if self.cursor < self.text.len() {
            self.cursor += self.text[self.cursor..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(0);
        }
    }

    /// Select to start (Shift+Home).
    pub fn select_home(&mut self) {
        if self.anchor.is_none() {
            self.anchor = Some(self.cursor);
        }
        self.cursor = 0;
    }

    /// Select to end (Shift+End).
    pub fn select_end(&mut self) {
        if self.anchor.is_none() {
            self.anchor = Some(self.cursor);
        }
        self.cursor = self.text.len();
    }

    /// Select all (Ctrl+A).
    pub fn select_all(&mut self) {
        self.anchor = Some(0);
        self.cursor = self.text.len();
    }

    /// Copy selection to clipboard (OSC 52).
    pub fn copy_selection(&self) {
        if let Some(text) = self.selected_text() {
            if !text.is_empty() {
                crate::clipboard::copy(text);
            }
        }
    }

    /// Handle a text-input-related action. Returns true if handled.
    /// Call this from dialog action handlers to avoid duplicating match arms.
    pub fn handle_action(&mut self, action: &crate::action::Action) -> bool {
        use crate::action::Action;
        match action {
            Action::DialogInput(c) => self.insert_char(*c),
            Action::DialogBackspace => self.backspace(),
            Action::EditorDeleteForward => self.delete_forward(),
            Action::CursorLeft => self.move_left(),
            Action::CursorRight => self.move_right(),
            Action::CursorLineStart => self.move_home(),
            Action::CursorLineEnd => self.move_end(),
            Action::SelectLeft => self.select_left(),
            Action::SelectRight => self.select_right(),
            Action::SelectLineStart => self.select_home(),
            Action::SelectLineEnd => self.select_end(),
            Action::SelectAll => self.select_all(),
            Action::CopySelection => {
                self.copy_selection();
                return true;
            }
            Action::EditorUndo => self.undo(),
            Action::EditorRedo => self.redo(),
            Action::EditorDeleteLine => {
                self.cut_selection();
            }
            _ => return false,
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_cursor() {
        let mut input = TextInput::empty();
        input.insert_char('a');
        input.insert_char('b');
        assert_eq!(input.text, "ab");
        assert_eq!(input.cursor, 2);
    }

    #[test]
    fn backspace() {
        let mut input = TextInput::new("abc".into());
        input.backspace();
        assert_eq!(input.text, "ab");
        assert_eq!(input.cursor, 2);
    }

    #[test]
    fn selection_replace() {
        let mut input = TextInput::new("hello".into());
        input.cursor = 5;
        input.anchor = Some(0); // select all
        input.insert_char('x');
        assert_eq!(input.text, "x");
        assert_eq!(input.cursor, 1);
    }

    #[test]
    fn selection_delete() {
        let mut input = TextInput::new("hello".into());
        input.cursor = 3;
        input.anchor = Some(1);
        input.delete_selection();
        assert_eq!(input.text, "hlo");
        assert_eq!(input.cursor, 1);
    }

    #[test]
    fn select_all_and_backspace() {
        let mut input = TextInput::new("test".into());
        input.select_all();
        input.backspace();
        assert_eq!(input.text, "");
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn shift_arrows() {
        let mut input = TextInput::new("abcd".into());
        input.cursor = 2;
        input.select_left();
        assert_eq!(input.anchor, Some(2));
        assert_eq!(input.cursor, 1);
        input.select_left();
        assert_eq!(input.cursor, 0);
        assert_eq!(input.selected_text(), Some("ab"));
    }

    #[test]
    fn move_clears_selection() {
        let mut input = TextInput::new("abc".into());
        input.select_all();
        assert!(input.anchor.is_some());
        input.move_left();
        assert!(input.anchor.is_none());
    }
}
