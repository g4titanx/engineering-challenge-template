//! Shared websocket lifecycle, subscription reconciliation, and receive timing.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
    tungstenite::{Error as WebSocketError, Message, http::Request},
};

use super::{ControlEvent, Decoded, ExchangeAdapter, FeedEvent, WorkerCommand};

/// Shared handshake schedule and backoff after HTTP 429.
struct ConnectState {
    next: tokio::time::Instant,
    cooldown: Duration,
    last_rate_limit: Option<tokio::time::Instant>,
}

/// Spaces handshakes across both redundant lanes. A 429 pauses every worker,
/// preventing hundreds of independent reconnect loops from retrying together.
pub struct ConnectGate {
    state: Mutex<ConnectState>,
    spacing: Duration,
}

impl ConnectGate {
    /// Sets the minimum gap between new WebSocket handshakes.
    pub fn new(spacing: Duration) -> Self {
        Self {
            state: Mutex::new(ConnectState {
                next: tokio::time::Instant::now(),
                cooldown: Duration::from_secs(2),
                last_rate_limit: None,
            }),
            spacing,
        }
    }

    /// Waits for a handshake slot or returns false during shutdown.
    async fn wait_turn(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        loop {
            if *shutdown.borrow() {
                return false;
            }
            let next = {
                let mut state = self.state.lock().await;
                let now = tokio::time::Instant::now();
                if now >= state.next {
                    state.next = now + self.spacing;
                    return true;
                }
                state.next
            };
            tokio::select! {
                _ = tokio::time::sleep_until(next) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return false; }
                }
            }
        }
    }

    /// Extends the shared handshake pause after a 429 response.
    async fn rate_limited(&self) {
        let mut state = self.state.lock().await;
        let now = tokio::time::Instant::now();
        if state
            .last_rate_limit
            .is_some_and(|last| now.duration_since(last) >= Duration::from_secs(60))
        {
            state.cooldown = Duration::from_secs(2);
        }
        let announce = state
            .last_rate_limit
            .is_none_or(|last| now.duration_since(last) >= Duration::from_secs(5));
        state.last_rate_limit = Some(now);
        state.next = state.next.max(now + state.cooldown);
        state.cooldown = (state.cooldown * 2).min(Duration::from_secs(60));
        if announce {
            crate::diagnostics::emit(
                "WebSocket HTTP 429: pausing new handshakes; retry delay grows to 60s if throttling continues".to_owned(),
            );
        }
    }
}

/// Recognizes a WebSocket handshake rejected with HTTP 429.
fn is_rate_limited(error: &anyhow::Error) -> bool {
    error.downcast_ref::<WebSocketError>().is_some_and(
        |error| matches!(error, WebSocketError::Http(response) if response.status() == 429),
    )
}

/// Takes a stable, bounded batch of markets not yet subscribed.
fn next_batch(wanted: &HashSet<String>, subscribed: &HashSet<String>, size: usize) -> Vec<String> {
    let mut batch: Vec<_> = wanted.difference(subscribed).cloned().collect();
    batch.sort_unstable();
    batch.truncate(size);
    batch
}

/// One task owns one socket and one bounded market shard. All messages from a
/// shard retain socket order when sent to the central book/writer task.
pub struct SocketEndpoint {
    pub number: usize,
    pub url: String,
    pub source_port_base: Option<u16>,
    pub connect_gate: Arc<ConnectGate>,
    pub connected_sockets: Arc<AtomicUsize>,
}

/// Keeps the connected-socket count correct across every exit path.
struct ConnectedGuard(Arc<AtomicUsize>);

impl ConnectedGuard {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(count)
    }
}

impl Drop for ConnectedGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A closed collector ends the worker; socket failures reconnect the shard.
enum SessionExit {
    Reconnect,
    Stop,
}

/// State that belongs to one connection and is discarded on reconnect.
struct SocketSession<'a, A> {
    number: usize,
    socket: Socket,
    adapter: &'a A,
    events: &'a mpsc::Sender<FeedEvent>,
    sid: Option<i64>,
    request_id: i64,
    subscribed: HashSet<String>,
    ready: HashSet<String>,
    pending_snapshots: HashSet<String>,
    reinitializing: HashSet<String>,
    snapshot_requests: HashMap<String, Instant>,
    snapshot_probes: HashMap<String, Instant>,
    market_clock: HashMap<String, (i64, u32)>,
    last_seq: Option<i64>,
    last_batch: Instant,
    last_refresh: Instant,
    probe_requests: u64,
    probe_responses: u64,
    probe_deltas: u64,
    probe_timeouts: u64,
    last_probe_warning: Instant,
    connected_at: Instant,
    last_frame_at: Instant,
    peer: Option<std::net::SocketAddr>,
    pings_seen: u64,
    blocked_sends: u64,
    max_send_wait: Duration,
}

impl<'a, A: ExchangeAdapter> SocketSession<'a, A> {
    fn new(
        number: usize,
        socket: Socket,
        adapter: &'a A,
        events: &'a mpsc::Sender<FeedEvent>,
        initial: &[String],
        connected_at: Instant,
    ) -> Self {
        let subscribed: HashSet<_> = initial.iter().cloned().collect();
        let peer = socket.get_ref().get_ref().peer_addr().ok();
        Self {
            number,
            socket,
            adapter,
            events,
            sid: adapter.initial_subscription_id(),
            request_id: 1,
            pending_snapshots: subscribed.clone(),
            subscribed,
            ready: HashSet::new(),
            reinitializing: HashSet::new(),
            snapshot_requests: HashMap::new(),
            snapshot_probes: HashMap::new(),
            market_clock: HashMap::new(),
            last_seq: None,
            last_batch: Instant::now(),
            last_refresh: Instant::now(),
            probe_requests: 0,
            probe_responses: 0,
            probe_deltas: 0,
            probe_timeouts: 0,
            last_probe_warning: Instant::now(),
            connected_at,
            last_frame_at: connected_at,
            peer,
            pings_seen: 0,
            blocked_sends: 0,
            max_send_wait: Duration::ZERO,
        }
    }

    /// Sends one add, delete, or snapshot request when symbols are present.
    async fn update_subscription(
        &mut self,
        sid: i64,
        action: &str,
        symbols: &[String],
    ) -> anyhow::Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }
        self.request_id += 1;
        self.socket
            .send(Message::Text(
                self.adapter
                    .update(sid, self.request_id, action, symbols)
                    .to_string()
                    .into(),
            ))
            .await?;
        Ok(())
    }

    fn remove_market(&mut self, symbol: &str) {
        self.ready.remove(symbol);
        self.reinitializing.remove(symbol);
        self.market_clock.remove(symbol);
        self.pending_snapshots.remove(symbol);
        self.snapshot_requests.remove(symbol);
        self.snapshot_probes.remove(symbol);
        self.subscribed.remove(symbol);
    }

    async fn reconcile_removed_markets(
        &mut self,
        wanted: &HashSet<String>,
    ) -> Result<(), SessionExit> {
        let Some(sid) = self.sid else {
            return Ok(());
        };
        let mut removed: Vec<_> = self.subscribed.difference(wanted).cloned().collect();
        removed.sort_unstable();
        if let Err(error) = self
            .update_subscription(sid, "delete_markets", &removed)
            .await
        {
            crate::diagnostics::emit(format!("socket {} remove markets: {error:#}", self.number));
            return Err(SessionExit::Reconnect);
        }
        if !removed.is_empty() {
            self.events
                .send(FeedEvent::Retire {
                    source: self.number,
                    symbols: removed.clone(),
                })
                .await
                .map_err(|_| SessionExit::Stop)?;
        }
        for symbol in &removed {
            self.remove_market(symbol);
        }
        Ok(())
    }

    async fn maintain_subscriptions(
        &mut self,
        desired_rx: &watch::Receiver<HashSet<String>>,
        batch_size: usize,
    ) -> Result<(), SessionExit> {
        let Some(sid) = self.sid else {
            if self.last_batch.elapsed() >= Duration::from_secs(5) {
                crate::diagnostics::emit(format!(
                    "socket {} subscribe acknowledgement timed out",
                    self.number
                ));
                return Err(SessionExit::Reconnect);
            }
            return Ok(());
        };
        self.expire_snapshot_probe();
        if !self.pending_snapshots.is_empty() && self.last_batch.elapsed() < Duration::from_secs(2)
        {
            return Ok(());
        }
        let added = {
            let wanted = desired_rx.borrow();
            if self.subscribed.len() < wanted.len() {
                next_batch(&wanted, &self.subscribed, batch_size)
            } else {
                Vec::new()
            }
        };
        if !added.is_empty() {
            if let Err(error) = self.update_subscription(sid, "add_markets", &added).await {
                crate::diagnostics::emit(format!("socket {} add markets: {error:#}", self.number));
                return Err(SessionExit::Reconnect);
            }
            self.pending_snapshots.extend(added.iter().cloned());
            self.subscribed.extend(added);
            self.last_batch = Instant::now();
        } else if let Some(action) = A::SNAPSHOT_REFRESH_ACTION
            && self.ready.len() < self.subscribed.len()
            && self.last_refresh.elapsed() >= Duration::from_secs(5)
        {
            let now = Instant::now();
            let mut missing: Vec<_> = self
                .subscribed
                .difference(&self.ready)
                .filter(|symbol| {
                    self.snapshot_requests
                        .get(*symbol)
                        .is_none_or(|last| now.duration_since(*last) >= Duration::from_secs(30))
                })
                .cloned()
                .collect();
            missing.sort_unstable();
            missing.truncate(batch_size);
            self.last_refresh = now;
            if !missing.is_empty() {
                if let Err(error) = self.update_subscription(sid, action, &missing).await {
                    crate::diagnostics::emit(format!(
                        "socket {} refresh snapshots: {error:#}",
                        self.number
                    ));
                    return Err(SessionExit::Reconnect);
                }
                for symbol in missing {
                    self.snapshot_requests.insert(symbol, now);
                }
            }
        }
        Ok(())
    }

    fn expire_snapshot_probe(&mut self) {
        let expired = self
            .snapshot_probes
            .iter()
            .find(|(_, requested)| requested.elapsed() >= Duration::from_secs(15))
            .map(|(symbol, _)| symbol.clone());
        let Some(symbol) = expired else {
            return;
        };
        self.snapshot_probes.remove(&symbol);
        if self.subscribed.contains(&symbol) && self.ready.contains(&symbol) {
            self.probe_timeouts += 1;
            if self.probe_timeouts == 1
                || self.last_probe_warning.elapsed() >= Duration::from_secs(60)
            {
                crate::diagnostics::emit(format!(
                    "socket {}: snapshot probe timed out for {symbol}; keeping subscription (timeouts={}, responses={}, delta-confirmed={}, last frame {:.1}s ago)",
                    self.number,
                    self.probe_timeouts,
                    self.probe_responses,
                    self.probe_deltas,
                    self.last_frame_at.elapsed().as_secs_f64()
                ));
                self.last_probe_warning = Instant::now();
            }
        }
    }

    async fn probe_market(&mut self, symbol: String) -> Result<(), SessionExit> {
        if !self.subscribed.contains(&symbol) || !self.ready.contains(&symbol) {
            return Ok(());
        }
        let Some(sid) = self.sid else {
            return Ok(());
        };
        let symbols = [symbol.clone()];
        if let Some(action) = A::SNAPSHOT_REFRESH_ACTION {
            if self.snapshot_probes.contains_key(&symbol) {
                return Ok(());
            }
            self.update_subscription(sid, action, &symbols)
                .await
                .map_err(|_| SessionExit::Reconnect)?;
            self.snapshot_probes.insert(symbol, Instant::now());
            self.probe_requests += 1;
            return Ok(());
        }
        self.update_subscription(sid, "delete_markets", &symbols)
            .await
            .map_err(|_| SessionExit::Reconnect)?;
        self.ready.remove(&symbol);
        self.reinitializing.insert(symbol.clone());
        self.market_clock.remove(&symbol);
        self.pending_snapshots.insert(symbol);
        self.last_batch = Instant::now();
        self.events
            .send(FeedEvent::Reset {
                source: self.number,
                symbols: symbols.to_vec(),
            })
            .await
            .map_err(|_| SessionExit::Stop)?;
        self.update_subscription(sid, "add_markets", &symbols)
            .await
            .map_err(|_| SessionExit::Reconnect)
    }

    async fn handle_message(
        &mut self,
        message: Message,
        arrival: Instant,
        received_at: DateTime<Utc>,
        desired_rx: &watch::Receiver<HashSet<String>>,
        batch_size: &mut usize,
    ) -> Result<(), SessionExit> {
        let parsed: serde_json::Result<Value> = match message {
            Message::Text(text) => {
                if self.adapter.is_heartbeat_reply(text.as_str()) {
                    return Ok(());
                }
                serde_json::from_str(text.as_str())
            }
            Message::Binary(bytes) => serde_json::from_slice(&bytes),
            Message::Ping(_) => {
                self.pings_seen += 1;
                return Ok(());
            }
            Message::Close(_) => return Err(SessionExit::Reconnect),
            _ => return Ok(()),
        };
        let frame = parsed.map_err(|error| {
            crate::diagnostics::emit(format!("socket {} JSON: {error}", self.number));
            SessionExit::Reconnect
        })?;
        let frames = match frame.as_array() {
            Some(frames) => frames.as_slice(),
            None => std::slice::from_ref(&frame),
        };
        for frame in frames {
            match self.adapter.control(frame) {
                ControlEvent::Error(error) => {
                    if error.contains("Subscription buffer overflow") {
                        *batch_size = (*batch_size / 2).max(1);
                        crate::diagnostics::emit(format!(
                            "socket {} subscription overflow: retrying with {batch_size} markets per batch",
                            self.number
                        ));
                    } else {
                        crate::diagnostics::emit(format!(
                            "socket {} exchange error: {error}",
                            self.number
                        ));
                    }
                    return Err(SessionExit::Reconnect);
                }
                ControlEvent::Subscribed(sid) => {
                    self.sid = Some(sid);
                    // Reconcile changes received before the initial acknowledgement.
                    let wanted = desired_rx.borrow().clone();
                    self.reconcile_removed_markets(&wanted).await?;
                    continue;
                }
                ControlEvent::Other => {}
            }
            if let (Some(expected_sid), Some((message_sid, seq))) =
                (self.sid, self.adapter.subscription_sequence(frame))
                && message_sid == expected_sid
            {
                if let Some(previous) = self.last_seq
                    && seq != previous + 1
                {
                    crate::diagnostics::emit(format!(
                        "socket {} sequence gap: {previous} -> {seq}",
                        self.number
                    ));
                    return Err(SessionExit::Reconnect);
                }
                self.last_seq = Some(seq);
            }
            let decoded = self
                .adapter
                .decode(frame, self.number, arrival, received_at)
                .map_err(|error| {
                    crate::diagnostics::emit(format!("socket {} payload: {error:#}", self.number));
                    SessionExit::Reconnect
                })?;
            let (first, rest) = match decoded {
                Decoded::None => continue,
                Decoded::One(event) => (Some(event), Vec::new()),
                Decoded::Many(events) => (None, events),
            };
            for mut event in first.into_iter().chain(rest) {
                if !self.prepare_event(&mut event)? {
                    continue;
                }
                let blocked_at = (self.events.capacity() == 0).then(Instant::now);
                self.events
                    .send(event)
                    .await
                    .map_err(|_| SessionExit::Stop)?;
                if let Some(started) = blocked_at {
                    self.blocked_sends += 1;
                    self.max_send_wait = self.max_send_wait.max(started.elapsed());
                }
            }
        }
        Ok(())
    }

    /// Updates readiness and ordering; false means the event must be ignored.
    fn prepare_event(&mut self, event: &mut FeedEvent) -> Result<bool, SessionExit> {
        let symbol = match &*event {
            FeedEvent::Snapshot { symbol, .. } | FeedEvent::Delta { symbol, .. } => symbol,
            _ => unreachable!(),
        };
        if !self.subscribed.contains(symbol) {
            return Ok(false);
        }
        if let FeedEvent::Snapshot { clock, .. } = &*event {
            self.ready.insert(symbol.clone());
            self.pending_snapshots.remove(symbol);
            self.snapshot_requests.remove(symbol);
            if self.snapshot_probes.remove(symbol).is_some() {
                self.probe_responses += 1;
            }
            self.reinitializing.remove(symbol);
            self.market_clock.insert(symbol.clone(), (*clock, 0));
        } else if !self.ready.contains(symbol) {
            if self.reinitializing.contains(symbol) {
                return Ok(false);
            }
            crate::diagnostics::emit(format!(
                "socket {}: delta before snapshot for {symbol}",
                self.number
            ));
            return Err(SessionExit::Reconnect);
        }
        if let FeedEvent::Delta {
            symbol,
            clock,
            ordinal,
            ..
        } = event
        {
            if !self.snapshot_probes.is_empty() && self.snapshot_probes.remove(symbol).is_some() {
                self.probe_deltas += 1;
            }
            let next = match self.market_clock.get(symbol) {
                Some(&(previous, _))
                    if *clock != i64::MIN && previous != i64::MIN && *clock < previous =>
                {
                    crate::diagnostics::emit(format!(
                        "socket {}: clock moved backwards for {symbol}",
                        self.number
                    ));
                    return Err(SessionExit::Reconnect);
                }
                Some(&(previous, last_ordinal)) if *clock == previous => {
                    last_ordinal.saturating_add(1)
                }
                _ => 0,
            };
            *ordinal = next;
            self.market_clock.insert(symbol.clone(), (*clock, next));
        }
        Ok(true)
    }

    fn log_read_error(&self, error: &WebSocketError) {
        crate::diagnostics::emit(format!(
            "socket {} read: {error}; peer={:?}, uptime={:.1}s, last_frame={:.1}s ago, subscribed={}, ready={}, pending_snapshots={}, ping_frames={}, probes={}/{} replies/{} deltas/{} timeouts, blocked_sends={}, max_send_wait={:.1}ms",
            self.number,
            self.peer,
            self.connected_at.elapsed().as_secs_f64(),
            self.last_frame_at.elapsed().as_secs_f64(),
            self.subscribed.len(),
            self.ready.len(),
            self.pending_snapshots.len(),
            self.pings_seen,
            self.probe_requests,
            self.probe_responses,
            self.probe_deltas,
            self.probe_timeouts,
            self.blocked_sends,
            self.max_send_wait.as_secs_f64() * 1_000.0
        ));
    }
}

/// Reconnects one shard, stamps frames after reads, and forwards ordered events.
pub async fn run_socket<A: ExchangeAdapter>(
    endpoint: SocketEndpoint,
    auth: Arc<A>,
    mut desired_rx: watch::Receiver<HashSet<String>>,
    mut commands: mpsc::Receiver<WorkerCommand>,
    events: mpsc::Sender<FeedEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let SocketEndpoint {
        number,
        url,
        source_port_base,
        connect_gate,
        connected_sockets,
    } = endpoint;
    let debug_sockets = std::env::var("DEBUG_SOCKETS").as_deref() == Ok("1");
    let mut backoff = Duration::from_secs(1);
    let mut batch_size = A::SUBSCRIPTION_BATCH_SIZE;
    let mut connection_attempt = 0usize;
    loop {
        if *shutdown.borrow() {
            return;
        }
        if desired_rx.borrow().is_empty() {
            tokio::select! {
                _ = desired_rx.changed() => {},
                _ = shutdown.changed() => {},
            }
            continue;
        }
        if !connect_gate.wait_turn(&mut shutdown).await {
            return;
        }
        let wanted = desired_rx.borrow().clone();
        if wanted.is_empty() {
            continue;
        }
        let source_port = source_port_base.and_then(|base| {
            number
                .checked_mul(16)
                .and_then(|offset| offset.checked_add(connection_attempt % 16))
                .and_then(|offset| u16::try_from(offset).ok())
                .and_then(|offset| base.checked_add(offset))
        });
        connection_attempt = connection_attempt.wrapping_add(1);
        let connection = match auth.signed_request(&url) {
            Ok(request) => tokio::select! {
                result = connect_with_source(request, source_port) => result,
                _ = shutdown.changed() => return,
            },
            Err(error) => {
                crate::diagnostics::emit(format!("socket {number} authentication: {error:#}"));
                return;
            }
        };
        let (mut socket, _) = match connection {
            Ok(pair) => pair,
            Err(error) => {
                if *shutdown.borrow() {
                    return;
                }
                if is_rate_limited(&error) {
                    connect_gate.rate_limited().await;
                } else {
                    crate::diagnostics::emit(format!("socket {number} connect: {error}"));
                }
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = shutdown.changed() => {},
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        if *shutdown.borrow() {
            let _ = tokio::time::timeout(Duration::from_millis(500), socket.close(None)).await;
            return;
        }
        let connected = ConnectedGuard::new(connected_sockets.clone());
        let connected_at = Instant::now();
        if debug_sockets && let Ok(local) = socket.get_ref().get_ref().local_addr() {
            crate::diagnostics::emit(format!(
                "socket {number} connected via {url}, local={local}"
            ));
        }
        backoff = Duration::from_secs(1);
        let initial = next_batch(&wanted, &HashSet::new(), batch_size);
        let command = auth.subscribe(&initial);
        if let Err(error) = socket.send(Message::Text(command.to_string().into())).await {
            crate::diagnostics::emit(format!("socket {number} subscribe: {error}"));
            continue;
        }
        let mut session = SocketSession::new(
            number,
            socket,
            auth.as_ref(),
            &events,
            &initial,
            connected_at,
        );
        let heartbeat_period = auth
            .heartbeat_interval()
            .unwrap_or(Duration::from_secs(3600));
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + heartbeat_period,
            heartbeat_period,
        );
        let mut subscription_tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            let outcome = tokio::select! {
                _ = shutdown.changed() => break,
                _ = subscription_tick.tick() => {
                    session.maintain_subscriptions(&desired_rx, batch_size).await
                }
                _ = heartbeat.tick(), if auth.heartbeat_text().is_some() => {
                    let text = auth.heartbeat_text().unwrap();
                    session.socket
                        .send(Message::Text(text.into()))
                        .await
                        .map_err(|_| SessionExit::Reconnect)
                }
                command = commands.recv() => {
                    match command {
                        Some(WorkerCommand::Probe(symbol)) => session.probe_market(symbol).await,
                        Some(WorkerCommand::Restart) | None => break,
                    }
                }
                changed = desired_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let wanted = desired_rx.borrow().clone();
                    if wanted.is_empty() {
                        break;
                    }
                    session.reconcile_removed_markets(&wanted).await
                }
                next = session.socket.next() => {
                    let frame = match next {
                        Some(Ok(frame)) => frame,
                        Some(Err(error)) => {
                            session.log_read_error(&error);
                            break;
                        }
                        None => break,
                    };
                    let arrival = Instant::now();
                    session.last_frame_at = arrival;
                    let received_at = Utc::now();
                    session
                        .handle_message(frame, arrival, received_at, &desired_rx, &mut batch_size)
                        .await
                }
            };
            match outcome {
                Ok(()) => {}
                Err(SessionExit::Reconnect) => break,
                Err(SessionExit::Stop) => return,
            }
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), session.socket.close(None)).await;
        drop(connected);
        if events
            .send(FeedEvent::Reset {
                source: number,
                symbols: session.subscribed.into_iter().collect(),
            })
            .await
            .is_err()
        {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {},
            _ = shutdown.changed() => {},
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Connects normally or binds a local TCP port for a route experiment.
async fn connect_with_source(
    request: Request<()>,
    source_port: Option<u16>,
) -> anyhow::Result<(
    Socket,
    tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
)> {
    let Some(source_port) = source_port else {
        return Ok(connect_async(request).await?);
    };
    let host = request
        .uri()
        .host()
        .context("websocket URL has no host")?
        .to_owned();
    let port = request.uri().port_u16().unwrap_or(443);
    let mut last_error = None;
    for address in tokio::net::lookup_host((host.as_str(), port)).await? {
        let attempt = async {
            let socket = if address.is_ipv4() {
                tokio::net::TcpSocket::new_v4()?
            } else {
                tokio::net::TcpSocket::new_v6()?
            };
            let bind = if address.is_ipv4() {
                std::net::SocketAddr::from(([0, 0, 0, 0], source_port))
            } else {
                std::net::SocketAddr::from(([0u16; 8], source_port))
            };
            socket.bind(bind)?;
            let stream = socket.connect(address).await?;
            stream.set_nodelay(true)?;
            let connected =
                client_async_tls_with_config(request.clone(), stream, None, None).await?;
            Ok::<_, anyhow::Error>(connected)
        }
        .await;
        match attempt {
            Ok(connected) => return Ok(connected),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no websocket addresses resolved")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::DiscoveryUpdate;
    use serde_json::json;
    use tokio::{net::TcpListener, time::timeout};
    use tokio_tungstenite::{accept_async, tungstenite::client::IntoClientRequest};

    struct MockExchange;

    impl ExchangeAdapter for MockExchange {
        const SUBSCRIPTION_BATCH_SIZE: usize = 3;
        const SNAPSHOT_REFRESH_ACTION: Option<&'static str> = Some("get_snapshot");

        fn name(&self) -> &'static str {
            "Mock"
        }
        async fn discover(
            &self,
            _client: &reqwest::Client,
            _progress: &mpsc::Sender<DiscoveryUpdate>,
        ) -> anyhow::Result<HashSet<String>> {
            Ok(HashSet::new())
        }
        fn default_websocket_url(&self) -> &str {
            "ws://127.0.0.1"
        }
        fn signed_request(&self, url: &str) -> anyhow::Result<Request<()>> {
            Ok(url.into_client_request()?)
        }
        fn subscribe(&self, symbols: &[String]) -> Value {
            json!({"cmd":"subscribe","symbols":symbols})
        }
        fn update(&self, _sid: i64, _id: i64, action: &str, symbols: &[String]) -> Value {
            json!({"cmd":action,"symbols":symbols})
        }
        fn control(&self, frame: &Value) -> ControlEvent {
            if frame["type"] == "subscribed" {
                ControlEvent::Subscribed(1)
            } else if frame["type"] == "error" {
                ControlEvent::Error(frame["msg"].as_str().unwrap().to_owned())
            } else {
                ControlEvent::Other
            }
        }
        fn subscription_sequence(&self, frame: &Value) -> Option<(i64, i64)> {
            Some((frame["sid"].as_i64()?, frame["seq"].as_i64()?))
        }
        fn decode(
            &self,
            frame: &Value,
            source: usize,
            arrival: Instant,
            received_at: chrono::DateTime<Utc>,
        ) -> anyhow::Result<Decoded> {
            if let Some(frames) = frame.get("events").and_then(Value::as_array) {
                let mut events = Vec::new();
                for frame in frames {
                    if let Decoded::One(event) = self.decode(frame, source, arrival, received_at)? {
                        events.push(event);
                    }
                }
                return Ok(Decoded::Many(events));
            }
            let Some(symbol) = frame.get("symbol").and_then(Value::as_str) else {
                return Ok(Decoded::None);
            };
            let clock = frame["clock"].as_i64().unwrap_or(0);
            if frame["type"] == "delta" {
                return Ok(Decoded::One(FeedEvent::Delta {
                    source,
                    symbol: symbol.into(),
                    clock,
                    ordinal: 0,
                    event_id: None,
                    side: engineering_challenge::Side::Buy,
                    price: 400_000,
                    change: crate::exchanges::LevelChange::Delta(1_000_000),
                    arrival,
                    received_at,
                    exchange_timestamp: received_at,
                }));
            }
            Ok(Decoded::One(FeedEvent::Snapshot {
                source,
                symbol: symbol.into(),
                clock,
                bids: vec![],
                asks: vec![],
                arrival,
                received_at,
                exchange_timestamp: received_at,
            }))
        }
    }

    #[tokio::test]
    async fn market_removal_is_reconciled_before_and_after_acknowledgement() {
        for before_ack in [true, false] {
            timeout(Duration::from_secs(5), async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (desired_tx, desired_rx) =
                    watch::channel(HashSet::from(["A".to_owned(), "B".to_owned()]));
                let (_commands_tx, commands_rx) = mpsc::channel(4);
                let (events_tx, mut events_rx) = mpsc::channel(8);
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                let worker = tokio::spawn(run_socket(
                    SocketEndpoint {
                        number: 0,
                        url: format!("ws://{address}"),
                        source_port_base: None,
                        connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                        connected_sockets: Arc::new(AtomicUsize::new(0)),
                    },
                    Arc::new(MockExchange),
                    desired_rx,
                    commands_rx,
                    events_tx,
                    shutdown_rx,
                ));
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap();
                let subscription: Value =
                    serde_json::from_str(subscription.to_text().unwrap()).unwrap();
                assert_eq!(subscription["symbols"], json!(["A", "B"]));
                if before_ack {
                    desired_tx.send_replace(HashSet::from(["B".to_owned()]));
                }
                socket
                    .send(Message::Text(
                        json!({"type":"subscribed"}).to_string().into(),
                    ))
                    .await
                    .unwrap();
                if !before_ack {
                    socket
                        .send(Message::Text(
                            json!([{"symbol":"A"}, {"symbol":"B"}]).to_string().into(),
                        ))
                        .await
                        .unwrap();
                    for _ in 0..2 {
                        assert!(matches!(
                            events_rx.recv().await,
                            Some(FeedEvent::Snapshot { .. })
                        ));
                    }
                    desired_tx.send_replace(HashSet::from(["B".to_owned()]));
                }
                let deletion = socket.next().await.unwrap().unwrap();
                let deletion: Value = serde_json::from_str(deletion.to_text().unwrap()).unwrap();
                assert_eq!(deletion["cmd"], "delete_markets");
                assert_eq!(deletion["symbols"], json!(["A"]));
                assert!(matches!(events_rx.recv().await,
                    Some(FeedEvent::Retire { symbols, .. }) if symbols == ["A"]));

                // A late snapshot for the retired market must not revive it.
                socket
                    .send(Message::Text(
                        json!([{"symbol":"A"}, {"symbol":"B"}]).to_string().into(),
                    ))
                    .await
                    .unwrap();
                assert!(matches!(events_rx.recv().await,
                    Some(FeedEvent::Snapshot { symbol, .. }) if symbol == "B"));
                shutdown_tx.send_replace(true);
                worker.await.unwrap();
                assert!(matches!(events_rx.recv().await,
                    Some(FeedEvent::Reset { symbols, .. }) if symbols == ["B"]));
                assert!(events_rx.recv().await.is_none());
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn batched_events_keep_order_and_receive_time_under_backpressure() {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_desired_tx, desired_rx) = watch::channel(HashSet::from(["M".to_owned()]));
            let (_commands_tx, commands_rx) = mpsc::channel(4);
            let (events_tx, mut events_rx) = mpsc::channel(1);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let worker = tokio::spawn(run_socket(
                SocketEndpoint {
                    number: 0,
                    url: format!("ws://{address}"),
                    source_port_base: None,
                    connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                    connected_sockets: Arc::new(AtomicUsize::new(0)),
                },
                Arc::new(MockExchange),
                desired_rx,
                commands_rx,
                events_tx,
                shutdown_rx,
            ));
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            let before = (Instant::now(), Utc::now());
            socket
                .send(Message::Text(
                    json!([
                        {"type":"subscribed"},
                        {"symbol":"M", "clock":100, "sid":1, "seq":1},
                        {"sid":1, "seq":2, "events":[
                            {"type":"delta", "symbol":"M", "clock":100},
                            {"type":"delta", "symbol":"M", "clock":100}
                        ]},
                        {"type":"delta", "symbol":"M", "clock":101, "sid":1, "seq":3}
                    ])
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let Some(FeedEvent::Snapshot {
                arrival,
                received_at,
                ..
            }) = events_rx.recv().await
            else {
                panic!("expected the initial snapshot");
            };
            assert!(arrival >= before.0 && arrival <= Instant::now());
            assert!(received_at >= before.1 && received_at <= Utc::now());
            // Leave the one-slot channel full before consuming the remaining events.
            tokio::task::yield_now().await;
            for (expected_clock, expected_ordinal) in [(100, 1), (100, 2), (101, 0)] {
                let Some(FeedEvent::Delta {
                    clock,
                    ordinal,
                    arrival: delta_arrival,
                    received_at: delta_received_at,
                    ..
                }) = events_rx.recv().await
                else {
                    panic!("expected an ordered delta");
                };
                assert_eq!((clock, ordinal), (expected_clock, expected_ordinal));
                assert_eq!(delta_arrival, arrival);
                assert_eq!(delta_received_at, received_at);
            }
            socket
                .send(Message::Binary(
                    json!({"type":"delta", "symbol":"M", "clock":101, "sid":1, "seq":4})
                        .to_string()
                        .into_bytes()
                        .into(),
                ))
                .await
                .unwrap();
            assert!(matches!(
                events_rx.recv().await,
                Some(FeedEvent::Delta {
                    clock: 101,
                    ordinal: 1,
                    ..
                })
            ));
            // A closed collector stops the worker without reconnecting.
            drop(events_rx);
            socket
                .send(Message::Text(
                    json!({"type":"delta", "symbol":"M", "clock":101, "sid":1, "seq":5})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            worker.await.unwrap();
            assert!(!*shutdown_tx.borrow());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn invalid_updates_reset_the_source_before_later_events_are_forwarded() {
        let cases = [
            (false, json!({"type":"delta", "symbol":"M", "clock":100})),
            (true, json!({"type":"delta", "symbol":"M", "clock":99})),
            (
                true,
                json!({"type":"delta", "symbol":"M", "clock":100, "sid":1, "seq":3}),
            ),
        ];
        for (with_snapshot, invalid) in cases {
            timeout(Duration::from_secs(5), async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (_desired_tx, desired_rx) = watch::channel(HashSet::from(["M".to_owned()]));
                let (_commands_tx, commands_rx) = mpsc::channel(4);
                let (events_tx, mut events_rx) = mpsc::channel(4);
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                let worker = tokio::spawn(run_socket(
                    SocketEndpoint {
                        number: 0,
                        url: format!("ws://{address}"),
                        source_port_base: None,
                        connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                        connected_sockets: Arc::new(AtomicUsize::new(0)),
                    },
                    Arc::new(MockExchange),
                    desired_rx,
                    commands_rx,
                    events_tx,
                    shutdown_rx,
                ));
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                socket.next().await.unwrap().unwrap();
                socket
                    .send(Message::Text(
                        json!({"type":"subscribed"}).to_string().into(),
                    ))
                    .await
                    .unwrap();
                if with_snapshot {
                    socket
                        .send(Message::Text(
                            json!({"symbol":"M", "clock":100, "sid":1, "seq":1})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    assert!(matches!(
                        events_rx.recv().await,
                        Some(FeedEvent::Snapshot { .. })
                    ));
                }
                socket
                    .send(Message::Text(
                        json!([invalid, {"symbol":"M", "clock":102}])
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                assert!(matches!(events_rx.recv().await,
                    Some(FeedEvent::Reset { symbols, .. }) if symbols == ["M"]));
                shutdown_tx.send_replace(true);
                worker.await.unwrap();
                assert!(events_rx.recv().await.is_none());
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn snapshots_release_bounded_subscription_batches() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let mut seen = HashSet::new();
            let mut batches = 0;
            while seen.len() < 7 {
                let message = timeout(Duration::from_secs(3), socket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                let symbols: Vec<String> =
                    serde_json::from_value(frame["symbols"].clone()).unwrap();
                assert!(symbols.len() <= 3, "subscription batch exceeded limit");
                assert_eq!(
                    frame["cmd"],
                    if batches == 0 {
                        "subscribe"
                    } else {
                        "add_markets"
                    }
                );
                if batches == 0 {
                    socket
                        .send(Message::Text(
                            json!({"type":"subscribed"}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                    assert!(
                        timeout(Duration::from_millis(150), socket.next())
                            .await
                            .is_err(),
                        "sent another batch before receiving snapshots"
                    );
                }
                for symbol in symbols {
                    assert!(seen.insert(symbol.clone()), "market subscribed twice");
                    socket
                        .send(Message::Text(
                            json!({"type":"snapshot","symbol":symbol})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
                batches += 1;
            }
            assert_eq!(batches, 3);
            seen
        });
        let desired: HashSet<_> = (0..7).map(|id| format!("M{id}")).collect();
        let (_desired_tx, desired_rx) = watch::channel(desired.clone());
        let (_commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(run_socket(
            SocketEndpoint {
                number: 0,
                url: format!("ws://{address}"),
                source_port_base: None,
                connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                connected_sockets: Arc::new(AtomicUsize::new(0)),
            },
            Arc::new(MockExchange),
            desired_rx,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        let mut received = HashSet::new();
        timeout(Duration::from_secs(6), async {
            while received.len() < desired.len() {
                if let FeedEvent::Snapshot { symbol, .. } = events_rx.recv().await.unwrap() {
                    received.insert(symbol);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received, desired);
        assert_eq!(server.await.unwrap(), desired);
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn missing_snapshot_is_requested_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let first = socket.next().await.unwrap().unwrap();
            let frame: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
            let initial: Vec<String> = serde_json::from_value(frame["symbols"].clone()).unwrap();
            assert_eq!(initial.len(), 3);
            socket
                .send(Message::Text(
                    json!({"type":"subscribed"}).to_string().into(),
                ))
                .await
                .unwrap();
            for symbol in initial.iter().skip(1) {
                socket
                    .send(Message::Text(
                        json!({"type":"snapshot","symbol":symbol})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
            }
            let second = timeout(Duration::from_secs(4), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: Value = serde_json::from_str(second.to_text().unwrap()).unwrap();
            assert_eq!(frame["cmd"], "add_markets");
            let added: Vec<String> = serde_json::from_value(frame["symbols"].clone()).unwrap();
            assert_eq!(added.len(), 1);
            socket
                .send(Message::Text(
                    json!({"type":"snapshot","symbol":added[0]})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let refresh = timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: Value = serde_json::from_str(refresh.to_text().unwrap()).unwrap();
            assert_eq!(frame["cmd"], "get_snapshot");
            let missing: Vec<String> = serde_json::from_value(frame["symbols"].clone()).unwrap();
            assert_eq!(missing, vec![initial[0].clone()]);
            socket
                .send(Message::Text(
                    json!({"type":"snapshot","symbol":missing[0]})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        });
        let desired: HashSet<_> = (0..4).map(|id| format!("M{id}")).collect();
        let (_desired_tx, desired_rx) = watch::channel(desired.clone());
        let (_commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(run_socket(
            SocketEndpoint {
                number: 0,
                url: format!("ws://{address}"),
                source_port_base: None,
                connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                connected_sockets: Arc::new(AtomicUsize::new(0)),
            },
            Arc::new(MockExchange),
            desired_rx,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        let mut received = HashSet::new();
        timeout(Duration::from_secs(6), async {
            while received.len() < desired.len() {
                if let FeedEvent::Snapshot { symbol, .. } = events_rx.recv().await.unwrap() {
                    received.insert(symbol);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received, desired);
        server.await.unwrap();
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn subscription_overflow_retries_with_smaller_batches() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let first = socket.next().await.unwrap().unwrap();
            let frame: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
            assert_eq!(frame["symbols"].as_array().unwrap().len(), 3);
            socket
                .send(Message::Text(
                    json!({"type":"error","msg":"Subscription buffer overflow"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            drop(socket);

            let (stream, _) = timeout(Duration::from_secs(4), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let mut seen = HashSet::new();
            while seen.len() < 4 {
                let message = timeout(Duration::from_secs(3), socket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                let symbols: Vec<String> =
                    serde_json::from_value(frame["symbols"].clone()).unwrap();
                assert_eq!(symbols.len(), 1, "overflow retry did not reduce batch size");
                if seen.is_empty() {
                    socket
                        .send(Message::Text(
                            json!({"type":"subscribed"}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                }
                for symbol in symbols {
                    assert!(seen.insert(symbol.clone()));
                    socket
                        .send(Message::Text(
                            json!({"type":"snapshot","symbol":symbol})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
            }
            seen
        });
        let desired: HashSet<_> = (0..4).map(|id| format!("M{id}")).collect();
        let (_desired_tx, desired_rx) = watch::channel(desired.clone());
        let (_commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(run_socket(
            SocketEndpoint {
                number: 0,
                url: format!("ws://{address}"),
                source_port_base: None,
                connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                connected_sockets: Arc::new(AtomicUsize::new(0)),
            },
            Arc::new(MockExchange),
            desired_rx,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        let mut received = HashSet::new();
        timeout(Duration::from_secs(8), async {
            while received.len() < desired.len() {
                if let FeedEvent::Snapshot { symbol, .. } = events_rx.recv().await.unwrap() {
                    received.insert(symbol);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received, desired);
        assert_eq!(server.await.unwrap(), desired);
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn quiet_market_probe_preserves_the_live_book() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"subscribed"}).to_string().into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"snapshot","symbol":"M"}).to_string().into(),
                ))
                .await
                .unwrap();
            let probe = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: Value = serde_json::from_str(probe.to_text().unwrap()).unwrap();
            assert_eq!(frame["cmd"], "get_snapshot");
            assert_eq!(frame["symbols"], json!(["M"]));
            socket
                .send(Message::Text(
                    json!({"type":"snapshot","symbol":"M"}).to_string().into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await;
        });
        let (_desired_tx, desired_rx) = watch::channel(HashSet::from(["M".to_owned()]));
        let (commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let connected_sockets = Arc::new(AtomicUsize::new(0));
        let worker = tokio::spawn(run_socket(
            SocketEndpoint {
                number: 0,
                url: format!("ws://{address}"),
                source_port_base: None,
                connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                connected_sockets: connected_sockets.clone(),
            },
            Arc::new(MockExchange),
            desired_rx,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        assert!(matches!(
            timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(FeedEvent::Snapshot { .. })
        ));
        assert_eq!(connected_sockets.load(Ordering::Relaxed), 1);
        commands_tx
            .send(WorkerCommand::Probe("M".into()))
            .await
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(FeedEvent::Snapshot { .. })
        ));
        assert!(events_rx.try_recv().is_err(), "probe reset the live source");
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(connected_sockets.load(Ordering::Relaxed), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unanswered_snapshot_probe_does_not_reset_a_ready_book() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"subscribed"}).to_string().into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"snapshot","symbol":"M"}).to_string().into(),
                ))
                .await
                .unwrap();
            let probe = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: Value = serde_json::from_str(probe.to_text().unwrap()).unwrap();
            assert_eq!(frame["cmd"], "get_snapshot");
            tokio::time::sleep(Duration::from_secs(17)).await;
        });
        let (_desired_tx, desired_rx) = watch::channel(HashSet::from(["M".to_owned()]));
        let (commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(run_socket(
            SocketEndpoint {
                number: 0,
                url: format!("ws://{address}"),
                source_port_base: None,
                connect_gate: Arc::new(ConnectGate::new(Duration::ZERO)),
                connected_sockets: Arc::new(AtomicUsize::new(0)),
            },
            Arc::new(MockExchange),
            desired_rx,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(FeedEvent::Snapshot { .. })
        ));
        commands_tx
            .send(WorkerCommand::Probe("M".into()))
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_secs(16), events_rx.recv())
                .await
                .is_err(),
            "a missing probe response must not reset a ready source"
        );
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rate_limit_pauses_all_new_connections() {
        let gate = ConnectGate::new(Duration::from_millis(10));
        gate.rate_limited().await;
        let state = gate.state.lock().await;
        assert!(state.next > tokio::time::Instant::now() + Duration::from_secs(1));
        assert_eq!(state.cooldown, Duration::from_secs(4));
    }

    #[test]
    fn recognizes_websocket_http_429() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .body(None)
            .unwrap();
        let error = anyhow::Error::new(WebSocketError::Http(Box::new(response)));
        assert!(is_rate_limited(&error));
    }
}
