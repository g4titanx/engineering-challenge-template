//! Interactive dashboard on stderr; feed workers never wait for rendering.

use std::{
    collections::VecDeque,
    io::{IsTerminal, stderr, stdin},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use crossterm::{
    cursor::{Hide, Show},
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::diagnostics;

const REFRESH: Duration = Duration::from_millis(250);
const INPUT_POLL: Duration = Duration::from_millis(50);
const EVENT_HISTORY: usize = 256;

/// Panel selected by the keyboard.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Panel {
    #[default]
    Coverage,
    Connections,
    Throughput,
    Events,
}

impl Panel {
    fn next(self) -> Self {
        match self {
            Self::Coverage => Self::Connections,
            Self::Connections => Self::Throughput,
            Self::Throughput => Self::Events,
            Self::Events => Self::Coverage,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Coverage => Self::Events,
            Self::Connections => Self::Coverage,
            Self::Throughput => Self::Connections,
            Self::Events => Self::Throughput,
        }
    }
}

/// Keyboard focus and scroll position for recent events.
#[derive(Default)]
struct UiState {
    focus: Panel,
    event_scroll: usize,
}

impl UiState {
    /// Updates focus or event scroll; returns true when the user requests exit.
    fn on_key(&mut self, key: KeyEvent, event_count: usize, page_size: usize) -> bool {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return false;
        }
        let max_scroll = event_count.saturating_sub(page_size);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('\u{3}') => return true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
            KeyCode::Up if self.focus == Panel::Events => {
                self.event_scroll = self.event_scroll.saturating_add(1).min(max_scroll);
            }
            KeyCode::Down if self.focus == Panel::Events => {
                self.event_scroll = self.event_scroll.saturating_sub(1);
            }
            KeyCode::Up | KeyCode::Left | KeyCode::BackTab => self.focus = self.focus.previous(),
            KeyCode::Down | KeyCode::Right | KeyCode::Tab => self.focus = self.focus.next(),
            KeyCode::PageUp => {
                self.focus = Panel::Events;
                self.event_scroll = self.event_scroll.saturating_add(page_size).min(max_scroll);
            }
            KeyCode::PageDown => {
                self.focus = Panel::Events;
                self.event_scroll = self.event_scroll.saturating_sub(page_size);
            }
            KeyCode::Home => {
                self.focus = Panel::Events;
                self.event_scroll = max_scroll;
            }
            KeyCode::End => {
                self.focus = Panel::Events;
                self.event_scroll = 0;
            }
            _ => {}
        }
        false
    }
}

/// A periodic summary; rendering never receives individual feed events.
#[derive(Clone, Default)]
pub struct Status {
    pub exchange: String,
    pub tracked: usize,
    pub ready: usize,
    pub overdue: usize,
    pub ready_both: usize,
    pub sockets: usize,
    pub allocated_sockets: usize,
    pub assigned_copies: usize,
    pub resets: u64,
    pub switches: u64,
    pub paired: u64,
    pub deltas_per_sec: u64,
    pub bytes_per_sec: u64,
    pub bytes_total: u64,
    pub minute_snapshots: usize,
    pub scan_complete: bool,
    pub capture: String,
}

/// Bounded messages sent to the dashboard thread.
pub enum Message {
    Status(Status),
    Event(String),
}

/// Sends nonblocking status updates and owns the renderer thread.
pub struct Dashboard {
    sender: Option<SyncSender<Message>>,
    thread: Option<JoinHandle<()>>,
    quit: Arc<AtomicBool>,
}

impl Dashboard {
    /// Starts the dashboard only when input and stderr are terminals.
    pub fn start() -> anyhow::Result<Self> {
        ensure!(
            stdin().is_terminal() && stderr().is_terminal(),
            "--tui requires stdin and stderr to be terminals"
        );
        let (sender, receiver) = mpsc::sync_channel(256);
        let (started_tx, started_rx) = mpsc::channel();
        let quit = Arc::new(AtomicBool::new(false));
        let render_quit = quit.clone();
        let thread = thread::Builder::new()
            .name("dashboard".into())
            .spawn(move || {
                let result = render_loop(receiver, started_tx, render_quit);
                if let Err(error) = result {
                    eprintln!("dashboard: {error:#}");
                }
            })
            .context("start dashboard thread")?;
        started_rx.recv().context("dashboard startup")??;
        diagnostics::install(sender.clone());
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            quit,
        })
    }

    /// Drops a display update if the renderer has fallen behind.
    pub fn publish(&self, status: Status) {
        if let Some(sender) = &self.sender {
            let _ = sender.try_send(Message::Status(status));
        }
    }

    /// Reports whether the keyboard requested a clean shutdown.
    pub fn quit_requested(&self) -> bool {
        self.quit.load(Ordering::Relaxed)
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        diagnostics::uninstall();
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Restores raw mode, cursor, and screen on every normal exit path.
struct TerminalSession {
    alternate_screen: bool,
}

impl TerminalSession {
    /// Takes control of stderr without redirecting binary stdout.
    fn enter() -> std::io::Result<Self> {
        enable_raw_mode()?;
        let mut session = Self {
            alternate_screen: false,
        };
        execute!(stderr(), EnterAlternateScreen)?;
        session.alternate_screen = true;
        execute!(stderr(), Hide)?;
        Ok(session)
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        if self.alternate_screen {
            let _ = execute!(stderr(), Show, LeaveAlternateScreen);
        }
    }
}

/// Receives summaries, handles keys, and redraws at a bounded rate.
fn render_loop(
    receiver: Receiver<Message>,
    started: mpsc::Sender<anyhow::Result<()>>,
    quit: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let guard = match TerminalSession::enter() {
        Ok(guard) => guard,
        Err(error) => {
            let _ = started.send(Err(error.into()));
            return Ok(());
        }
    };
    let mut terminal = match Terminal::new(CrosstermBackend::new(stderr())) {
        Ok(terminal) => terminal,
        Err(error) => {
            let _ = started.send(Err(error.into()));
            return Ok(());
        }
    };
    terminal.clear()?;
    started.send(Ok(()))?;
    let mut status = Status::default();
    let mut events = VecDeque::<String>::new();
    let mut ui = UiState::default();
    let mut last_draw = Instant::now() - REFRESH;
    loop {
        match receiver.recv_timeout(INPUT_POLL) {
            Ok(Message::Status(next)) => status = next,
            Ok(Message::Event(event)) => {
                events.push_back(event);
                if events.len() > EVENT_HISTORY {
                    events.pop_front();
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        for _ in 0..256 {
            let Ok(message) = receiver.try_recv() else {
                break;
            };
            match message {
                Message::Status(next) => status = next,
                Message::Event(event) => {
                    events.push_back(event);
                    if events.len() > EVENT_HISTORY {
                        events.pop_front();
                    }
                }
            }
        }
        let mut key_changed = false;
        for _ in 0..64 {
            if !event::poll(Duration::ZERO)? {
                break;
            }
            match event::read()? {
                Event::Key(key) => {
                    let page_size = terminal.size()?.height.saturating_sub(18).max(1) as usize;
                    if ui.on_key(key, events.len(), page_size) {
                        quit.store(true, Ordering::Relaxed);
                    }
                    key_changed = true;
                }
                Event::Resize(_, _) => key_changed = true,
                _ => {}
            }
        }
        if key_changed || last_draw.elapsed() >= REFRESH {
            terminal.draw(|frame| draw(frame, &status, &events, &ui))?;
            last_draw = Instant::now();
        }
    }
    drop(terminal);
    drop(guard);
    Ok(())
}

/// Marks the focused dashboard panel.
fn panel_block(title: &str, selected: bool) -> Block<'static> {
    Block::default()
        .title(if selected {
            format!("> {title} ")
        } else {
            format!(" {title} ")
        })
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if selected { Color::Yellow } else { Color::Gray }))
}

/// Draws coverage, connection, throughput, and event panels.
fn draw(frame: &mut Frame<'_>, status: &Status, events: &VecDeque<String>, ui: &UiState) {
    let [header, coverage, middle, event_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Min(5),
    ])
    .areas(frame.area());
    frame.render_widget(
        Paragraph::new(format!(
            "{}  |  {}\nSBE: {}\nArrows: panels/Events scroll  PgUp/PgDn: page  q/Ctrl-C: exit",
            status.exchange,
            if status.scan_complete {
                "scan complete"
            } else {
                "scanning"
            },
            status.capture
        ))
        .style(Style::default().fg(Color::Cyan)),
        header,
    );
    let pending = status.tracked.saturating_sub(status.ready);
    let missing_backup = status.ready.saturating_sub(status.ready_both);
    frame.render_widget(
        Paragraph::new(format!(
            "Discovered: {}   Ready: {}   Pending: {}   Overdue (60s): {}\nReady on both lanes: {}   Missing ready backup: {}\nCoverage: {:.1}%",
            status.tracked,
            status.ready,
            pending,
            status.overdue,
            status.ready_both,
            missing_backup,
            if status.tracked == 0 { 0.0 } else { 100.0 * status.ready as f64 / status.tracked as f64 }
        ))
        .block(panel_block("Coverage", ui.focus == Panel::Coverage)),
        coverage,
    );
    let [connections, throughput] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(middle);
    frame.render_widget(
        Paragraph::new(format!(
            "Connected / allocated sockets: {} / {}\nAssigned market copies: {}\nSocket resets: {}\nSource switches: {}   Paired deltas: {}",
            status.sockets, status.allocated_sockets, status.assigned_copies,
            status.resets, status.switches, status.paired
        ))
        .block(panel_block("Connections", ui.focus == Panel::Connections)),
        connections,
    );
    frame.render_widget(
        Paragraph::new(format!(
            "Emitted deltas/s: {}\nSBE bytes/s: {}\nSBE bytes total: {}\nLast minute snapshot: {} books",
            status.deltas_per_sec, status.bytes_per_sec, status.bytes_total,
            status.minute_snapshots
        ))
        .block(panel_block("Throughput", ui.focus == Panel::Throughput)),
        throughput,
    );
    let page_size = event_area.height.saturating_sub(2).max(1) as usize;
    let offset = ui.event_scroll.min(events.len().saturating_sub(page_size));
    let start = events.len().saturating_sub(page_size + offset);
    let visible = events
        .iter()
        .skip(start)
        .take(page_size)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(visible)
            .block(panel_block(
                &format!("Events (+{offset} older)"),
                ui.focus == Panel::Events,
            ))
            .wrap(Wrap { trim: true }),
        event_area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn dashboard_renders_coverage_and_throughput() {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let status = Status {
            exchange: "Kalshi".into(),
            tracked: 100,
            ready: 97,
            ready_both: 90,
            deltas_per_sec: 123,
            ..Status::default()
        };
        terminal
            .draw(|frame| draw(frame, &status, &VecDeque::new(), &UiState::default()))
            .unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("Pending: 3"));
        assert!(screen.contains("Missing ready backup: 7"));
        assert!(screen.contains("Emitted deltas/s: 123"));
    }
}
