use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyEvent, MouseEvent};

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize(u16, u16),
    Tick,
    /// Wakeup signal — PTY has output ready, re-render immediately.
    Wakeup,
}

/// A coalescing wakeup sender. Multiple sends between polls collapse into one Wakeup event.
#[derive(Clone)]
pub struct WakeupSender {
    tx: mpsc::Sender<AppEvent>,
    pending: Arc<AtomicBool>,
}

impl WakeupSender {
    /// Signal the event loop. Multiple calls before the next poll are coalesced into one event.
    pub fn wake(&self) {
        // Only send if not already pending — avoids flooding the channel.
        if !self.pending.swap(true, Ordering::AcqRel) {
            let _ = self.tx.send(AppEvent::Wakeup);
        }
    }
}

pub struct EventHandler {
    rx: mpsc::Receiver<AppEvent>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    wakeup_pending: Arc<AtomicBool>,
    wakeup_tx: mpsc::Sender<AppEvent>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let wakeup_pending = Arc::new(AtomicBool::new(false));
        let wakeup_tx = tx.clone();

        let thread = thread::spawn(move || loop {
            if stop_flag.load(Ordering::Relaxed) {
                return;
            }
            if event::poll(tick_rate).unwrap_or(false) {
                let app_event = match event::read() {
                    Ok(Event::Key(key)) => Some(AppEvent::Key(key)),
                    Ok(Event::Mouse(mouse)) => Some(AppEvent::Mouse(mouse)),
                    Ok(Event::Resize(w, h)) => Some(AppEvent::Resize(w, h)),
                    _ => None,
                };
                if let Some(ev) = app_event {
                    if tx.send(ev).is_err() {
                        return;
                    }
                }
            } else if tx.send(AppEvent::Tick).is_err() {
                return;
            }
        });

        Self {
            rx,
            stop,
            thread: Some(thread),
            wakeup_pending,
            wakeup_tx,
        }
    }

    pub fn next(&self) -> Result<AppEvent> {
        Ok(self.rx.recv()?)
    }

    /// Get a coalescing wakeup sender for background threads.
    pub fn wakeup_sender(&self) -> WakeupSender {
        WakeupSender {
            tx: self.wakeup_tx.clone(),
            pending: self.wakeup_pending.clone(),
        }
    }

    /// Clear the wakeup pending flag after handling a Wakeup event.
    /// This allows the next wake() call to send a new Wakeup.
    pub fn ack_wakeup(&self) {
        self.wakeup_pending.store(false, Ordering::Release);
    }

    /// Drain any queued events (useful after suspending for an editor).
    pub fn drain(&self) {
        while self.rx.try_recv().is_ok() {}
    }

    /// Signal the event thread to stop and wait for it to finish.
    /// Must be called before disabling raw mode to avoid stdout garbage.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for EventHandler {
    fn drop(&mut self) {
        self.stop();
    }
}
