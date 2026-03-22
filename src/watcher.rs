use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// Filesystem watcher for panel directories.
/// Uses kqueue (macOS) or inotify (Linux) for zero-cost idle monitoring.
pub struct DirWatcher {
    _watcher: RecommendedWatcher,
    rx: mpsc::Receiver<()>,
    watched_paths: [Option<PathBuf>; 2],
}

impl DirWatcher {
    pub fn new() -> Option<Self> {
        let (tx, rx) = mpsc::channel();

        let watcher = notify::recommended_watcher(move |res: Result<Event, _>| {
            if let Ok(_event) = res {
                // Send a simple notification — the tick handler will figure out which panel
                let _ = tx.send(());
            }
        })
        .ok()?;

        Some(Self {
            _watcher: watcher,
            rx,
            watched_paths: [None, None],
        })
    }

    /// Update the watched directories. Only re-watches if a path changed.
    pub fn watch_dirs(&mut self, dirs: [&Path; 2]) {
        for (i, dir) in dirs.iter().enumerate() {
            let dir = dir.to_path_buf();
            let needs_update = self.watched_paths[i]
                .as_ref()
                .map(|old| *old != dir)
                .unwrap_or(true);

            if needs_update {
                // Unwatch old path
                if let Some(ref old) = self.watched_paths[i] {
                    let _ = self._watcher.unwatch(old);
                }
                // Watch new path (non-recursive — only direct children)
                let _ = self._watcher.watch(&dir, RecursiveMode::NonRecursive);
                self.watched_paths[i] = Some(dir);
            }
        }
    }

    /// Check if any filesystem changes were detected. Non-blocking.
    /// Drains all pending notifications and returns true if any exist.
    pub fn has_changes(&self) -> bool {
        let mut changed = false;
        while self.rx.try_recv().is_ok() {
            changed = true;
        }
        changed
    }
}
