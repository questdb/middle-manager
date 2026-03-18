use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyEvent, MouseEvent};

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize(u16, u16),
    Tick,
}

pub struct EventHandler {
    rx: mpsc::Receiver<AppEvent>,
    _thread: thread::JoinHandle<()>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::channel();

        let thread = thread::spawn(move || loop {
            if event::poll(tick_rate).unwrap_or(false) {
                match event::read() {
                    Ok(Event::Key(key)) => {
                        if tx.send(AppEvent::Key(key)).is_err() {
                            return;
                        }
                    }
                    Ok(Event::Mouse(mouse)) => {
                        if tx.send(AppEvent::Mouse(mouse)).is_err() {
                            return;
                        }
                    }
                    Ok(Event::Resize(w, h)) => {
                        if tx.send(AppEvent::Resize(w, h)).is_err() {
                            return;
                        }
                    }
                    _ => {}
                }
            } else if tx.send(AppEvent::Tick).is_err() {
                return;
            }
        });

        Self {
            rx,
            _thread: thread,
        }
    }

    pub fn next(&self) -> Result<AppEvent> {
        Ok(self.rx.recv()?)
    }

    /// Drain any queued events (useful after suspending for an editor).
    pub fn drain(&self) {
        while self.rx.try_recv().is_ok() {}
    }
}
