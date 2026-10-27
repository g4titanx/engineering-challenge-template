//! Human-readable diagnostics. The dashboard consumes them without blocking a feed worker.

use std::sync::{
    Mutex, OnceLock,
    mpsc::{SyncSender, TrySendError},
};

use crate::tui::Message;

static SINK: OnceLock<Mutex<Option<SyncSender<Message>>>> = OnceLock::new();

/// Sends later diagnostics to the dashboard when it is active.
pub fn install(sender: SyncSender<Message>) {
    *SINK.get_or_init(|| Mutex::new(None)).lock().unwrap() = Some(sender);
}

/// Returns diagnostics to stderr after the dashboard stops.
pub fn uninstall() {
    if let Some(sink) = SINK.get() {
        *sink.lock().unwrap() = None;
    }
}

/// Logs without waiting for a full dashboard queue.
pub fn emit(message: String) {
    if let Some(sender) = SINK.get().and_then(|sink| sink.lock().unwrap().clone()) {
        match sender.try_send(Message::Event(message)) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(Message::Event(message))) => eprintln!("{message}"),
            Err(TrySendError::Disconnected(Message::Status(_))) => unreachable!(),
        }
    } else {
        eprintln!("{message}");
    }
}
