//! Starts discovery, socket workers, the book collector, and optional dashboard.

mod book;
mod collector;
mod diagnostics;
mod exchanges;
mod tui;

use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::ensure;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use collector::{Collector, Output};
use exchanges::kalshi::Auth;
use exchanges::polymarket::Polymarket;
use exchanges::socket::{ConnectGate, SocketEndpoint, run_socket};
use exchanges::{DiscoveryUpdate, ExchangeAdapter, FeedEvent, WorkerCommand};

/// One assigned market shard and its running socket task.
struct Worker {
    lane: usize,
    symbols: HashSet<String>,
    sender: watch::Sender<HashSet<String>>,
    commands: mpsc::Sender<WorkerCommand>,
    handle: JoinHandle<()>,
    last_restart: Instant,
}

/// Limits and connection settings shared by one redundant lane.
struct LaneConfig<'a> {
    lane: usize,
    url: &'a str,
    source_port_base: Option<u16>,
    capacity: usize,
    max_sockets: Option<usize>,
    connect_gate: &'a Arc<ConnectGate>,
    connected_sockets: &'a Arc<AtomicUsize>,
}

/// Assigns every desired market to one socket on this lane.
fn assign_lane<A: ExchangeAdapter>(
    desired: &HashSet<String>,
    config: LaneConfig<'_>,
    workers: &mut Vec<Worker>,
    auth: &Arc<A>,
    events: &mpsc::Sender<FeedEvent>,
    shutdown: &watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let LaneConfig {
        lane,
        url,
        source_port_base,
        capacity,
        max_sockets,
        connect_gate,
        connected_sockets,
    } = config;
    for worker in workers.iter_mut().filter(|w| w.lane == lane) {
        worker.symbols.retain(|symbol| desired.contains(symbol));
    }
    let assigned: HashSet<_> = workers
        .iter()
        .filter(|w| w.lane == lane)
        .flat_map(|worker| worker.symbols.iter().cloned())
        .collect();
    let mut new: Vec<_> = desired.difference(&assigned).cloned().collect();
    new.sort_unstable();
    let mut available: VecDeque<usize> = workers
        .iter()
        .enumerate()
        .filter(|(_, worker)| worker.lane == lane && worker.symbols.len() < capacity)
        .map(|(index, _)| index)
        .collect();
    for symbol in new {
        match available.front().copied() {
            Some(index) => {
                workers[index].symbols.insert(symbol);
                if workers[index].symbols.len() == capacity {
                    available.pop_front();
                }
            }
            None => {
                if max_sockets.is_some_and(|limit| workers.len() >= limit) {
                    anyhow::bail!(
                        "discovered {} markets needs more than {} WebSockets at {} markets per socket on two lanes; raise MARKETS_PER_SOCKET or MAX_WS_CONNECTIONS only if the exchange accepts it",
                        desired.len(),
                        max_sockets.unwrap(),
                        capacity
                    );
                }
                let symbols = HashSet::from([symbol]);
                let (sender, receiver) = watch::channel(symbols.clone());
                let (commands, command_rx) = mpsc::channel(128);
                let number = workers.len();
                if let Some(base) = source_port_base {
                    let offset = number
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(15))
                        .and_then(|n| u16::try_from(n).ok())
                        .ok_or_else(|| anyhow::anyhow!("source port range exhausted"))?;
                    ensure!(
                        base.checked_add(offset).is_some(),
                        "source port range exhausted"
                    );
                }
                let handle = tokio::spawn(run_socket(
                    SocketEndpoint {
                        number,
                        url: url.to_owned(),
                        source_port_base,
                        connect_gate: connect_gate.clone(),
                        connected_sockets: connected_sockets.clone(),
                    },
                    auth.clone(),
                    receiver,
                    command_rx,
                    events.clone(),
                    shutdown.clone(),
                ));
                workers.push(Worker {
                    lane,
                    symbols,
                    sender,
                    commands,
                    handle,
                    last_restart: Instant::now(),
                });
                if capacity > 1 {
                    available.push_back(number);
                }
            }
        }
    }
    for worker in workers.iter().filter(|w| w.lane == lane) {
        if *worker.sender.borrow() != worker.symbols {
            worker.sender.send_replace(worker.symbols.clone());
        }
    }
    Ok(())
}

/// Reads a positive integer setting, using its default when unset.
fn setting(name: &str, default: usize) -> anyhow::Result<usize> {
    let value = std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?;
    let value = value.unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(value)
}

/// Keeps alternate WebSocket routes on the exchange's expected path.
fn route(name: &str, default: &str) -> anyhow::Result<String> {
    let url = std::env::var(name).unwrap_or_else(|_| default.to_owned());
    let parsed = url.parse::<tokio_tungstenite::tungstenite::http::Uri>()?;
    let default_path = default
        .parse::<tokio_tungstenite::tungstenite::http::Uri>()?
        .path()
        .to_owned();
    ensure!(
        parsed.scheme_str() == Some("wss") && parsed.path() == default_path,
        "{name} must be a wss URL with path {default_path}"
    );
    Ok(url)
}

/// Reads an optional local port range for route experiments.
fn source_port_base(name: &str) -> anyhow::Result<Option<u16>> {
    std::env::var(name)
        .ok()
        .map(|value| {
            let port: u16 = value.parse()?;
            ensure!(port > 0, "{name} must be a nonzero port");
            Ok(port)
        })
        .transpose()
}

/// The exchange selected for this process.
#[derive(Debug, PartialEq, Eq)]
enum ExchangeSelection {
    Kalshi,
    Polymarket,
}

/// Command-line choices; the dashboard is off unless requested.
#[derive(Debug, PartialEq, Eq)]
struct Options {
    exchange: ExchangeSelection,
    tui: bool,
}

/// Parses arguments, with `EXCHANGE` as the default exchange.
fn parse_args(args: &[String], configured: Option<&str>) -> anyhow::Result<Options> {
    let mut name = configured.unwrap_or("kalshi");
    let mut tui = false;
    let mut seen_exchange = false;
    let mut seen_display = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--exchange" if !seen_exchange => {
                index += 1;
                name = args
                    .get(index)
                    .ok_or_else(|| anyhow::anyhow!("--exchange needs kalshi or polymarket"))?;
                seen_exchange = true;
            }
            "--tui" if !seen_display => {
                tui = true;
                seen_display = true;
            }
            "--headless" if !seen_display => {
                tui = false;
                seen_display = true;
            }
            _ => anyhow::bail!("usage: [--exchange kalshi|polymarket] [--tui|--headless]"),
        }
        index += 1;
    }
    let exchange = match name {
        "kalshi" => ExchangeSelection::Kalshi,
        "polymarket" => ExchangeSelection::Polymarket,
        _ => anyhow::bail!("exchange must be kalshi or polymarket, got {name}"),
    };
    Ok(Options { exchange, tui })
}

/// Runs the collector on Tokio's multithreaded runtime.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_selected_exchange().await
}

/// Loads `.env` and starts the requested exchange adapter.
async fn run_selected_exchange() -> anyhow::Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let configured = std::env::var("EXCHANGE").ok();
    let options = parse_args(&args, configured.as_deref())?;
    match options.exchange {
        ExchangeSelection::Kalshi => run(Auth::from_env()?, options.tui).await,
        ExchangeSelection::Polymarket => run(Polymarket, options.tui).await,
    }
}

/// Runs discovery, feed processing, snapshots, health checks, and clean shutdown.
async fn run<A: ExchangeAdapter>(adapter: A, tui: bool) -> anyhow::Result<()> {
    let auth = Arc::new(adapter);
    let dashboard = tui.then(tui::Dashboard::start).transpose()?;
    diagnostics::emit(format!("exchange: {}", auth.name()));
    let routes = [
        route("WS_URL_A", auth.default_websocket_url())?,
        route("WS_URL_B", auth.default_websocket_url())?,
    ];
    let source_ports = [
        source_port_base("SOURCE_PORT_BASE_A")?,
        source_port_base("SOURCE_PORT_BASE_B")?,
    ];
    let capacity = setting("MARKETS_PER_SOCKET", A::MARKETS_PER_SOCKET)?;
    let max_sockets = std::env::var("MAX_WS_CONNECTIONS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .or(A::SOCKET_LIMIT);
    ensure!(
        max_sockets.is_none_or(|limit| limit > 0),
        "MAX_WS_CONNECTIONS must be positive"
    );
    let connect_gate = Arc::new(ConnectGate::new(Duration::from_millis(setting(
        "CONNECT_SPACING_MS",
        500,
    )? as u64)));
    let connected_sockets = Arc::new(AtomicUsize::new(0));
    let poll_seconds = setting("MARKET_POLL_SECONDS", 60)?;
    let stats_seconds = setting("STAT_SECONDS", 60)?;
    let quiet = Duration::from_secs(setting("QUIET_SECONDS", 600)? as u64);
    let recycle = Duration::from_secs(setting("SOCKET_RECYCLE_SECONDS", 3600)? as u64);
    let explicit_symbols = std::env::var("SYMBOLS").ok().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|symbol| !symbol.is_empty())
            .map(str::to_owned)
            .collect::<HashSet<String>>()
    });
    ensure!(
        explicit_symbols
            .as_ref()
            .is_none_or(|symbols| !symbols.is_empty()),
        "SYMBOLS is empty"
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let (market_tx, mut market_rx) = mpsc::channel(16);
    let (event_tx, mut event_rx) = mpsc::channel(16_384);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let discovery_adapter = auth.clone();
    let discovery_shutdown = shutdown_rx.clone();
    let discovery = tokio::spawn(async move {
        let mut shutdown = discovery_shutdown;
        if let Some(symbols) = explicit_symbols {
            let _ = market_tx.send(DiscoveryUpdate::Complete(symbols)).await;
            let _ = shutdown.changed().await;
            return;
        }
        loop {
            diagnostics::emit(format!(
                "scanning {} open markets...",
                discovery_adapter.name()
            ));
            match discovery_adapter.discover(&client, &market_tx).await {
                Ok(markets) => {
                    diagnostics::emit(format!(
                        "{} market scan complete: {} symbols",
                        discovery_adapter.name(),
                        markets.len()
                    ));
                    if market_tx
                        .send(DiscoveryUpdate::Complete(markets))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => diagnostics::emit(format!("market discovery: {error:#}")),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(poll_seconds as u64)) => {},
                _ = shutdown.changed() => return,
            }
        }
    });

    let mut workers = Vec::<Worker>::new();
    let mut desired = HashSet::<String>::new();
    let mut last_tracking_log = 0usize;
    let mut collector = Collector::new(Output::new(auth.name())?);
    let mut scan_complete = false;
    let mut minute_snapshots = 0usize;
    let mut last_ui_at = Instant::now();
    let mut last_ui_deltas = 0u64;
    let mut last_ui_bytes = 0u64;
    let mut snapshot_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    snapshot_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut flush_tick = tokio::time::interval(Duration::from_millis(100));
    let mut health_tick = tokio::time::interval(Duration::from_secs(1));
    let mut lag_tick = tokio::time::interval(Duration::from_millis(10));
    let mut statistics_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(stats_seconds as u64),
        Duration::from_secs(stats_seconds as u64),
    );
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut quit_tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                diagnostics::emit("SIGINT: closing sockets and output".to_owned());
                break;
            }
            _ = quit_tick.tick(), if dashboard.is_some() => {
                if dashboard.as_ref().is_some_and(tui::Dashboard::quit_requested) {
                    diagnostics::emit("keyboard exit: closing sockets and output".to_owned());
                    break;
                }
            }
            Some(event) = event_rx.recv() => collector.on_event(event)?,
            Some(update) = market_rx.recv() => {
                let complete = matches!(update, DiscoveryUpdate::Complete(_));
                if complete { scan_complete = true; }
                let changed = match update {
                    DiscoveryUpdate::Add(additions) => {
                        let before = desired.len();
                        desired.extend(additions);
                        desired.len() != before
                    }
                    DiscoveryUpdate::Complete(markets) => {
                        let changed = desired != markets;
                        desired = markets;
                        changed
                    }
                };
                if !changed && !complete { continue; }
                collector.set_markets(desired.clone())?;
                for lane in 0..2 {
                    assign_lane(&desired, LaneConfig { lane, url: &routes[lane],
                        source_port_base: source_ports[lane], capacity, max_sockets,
                        connect_gate: &connect_gate, connected_sockets: &connected_sockets }, &mut workers,
                        &auth, &event_tx, &shutdown_rx)?;
                }
                for (source, worker) in workers.iter().enumerate() { collector.register_source(source, worker.lane); }
                if complete || last_tracking_log == 0 || desired.len().abs_diff(last_tracking_log) >= 1_000 {
                    diagnostics::emit(format!("tracking {} markets on {} sockets{}", desired.len(),
                        workers.iter().filter(|w| !w.symbols.is_empty()).count(),
                        if complete { " (scan complete)" } else { " (scan in progress)" }));
                    last_tracking_log = desired.len();
                }
            }
            _ = snapshot_tick.tick() => {
                // Consume queued deltas before constructing each minute's full books.
                while let Ok(event) = event_rx.try_recv() { collector.on_event(event)?; }
                minute_snapshots = collector.snapshot_all()?;
            }
            _ = flush_tick.tick() => collector.flush()?,
            _ = lag_tick.tick() => collector.resolve_lag()?,
            _ = health_tick.tick() => {
                for (source, symbol) in collector.health_actions(quiet, 10, A::SNAPSHOT_REFRESH_ACTION.is_none())? {
                    if let Some(worker) = workers.get(source)
                        && worker.commands.try_send(WorkerCommand::Probe(symbol)).is_err()
                    {
                        diagnostics::emit(format!("socket {source}: probe queue full"));
                    }
                }
                for (source, worker) in workers.iter_mut().enumerate() {
                    if !worker.symbols.is_empty() && worker.last_restart.elapsed() >= recycle
                        && collector.has_backup_for(source, &worker.symbols)
                        && worker.commands.try_send(WorkerCommand::Restart).is_ok()
                    {
                        worker.last_restart = Instant::now();
                        break;
                    }
                }
                if let Some(dashboard) = &dashboard {
                    let (ready, ready_both) = collector.coverage_counts();
                    let elapsed = last_ui_at.elapsed().as_secs_f64().max(0.001);
                    let bytes = collector.bytes_written();
                    let deltas = collector.emitted_deltas;
                    dashboard.publish(tui::Status {
                        exchange: auth.name().to_owned(),
                        tracked: desired.len(),
                        ready,
                        overdue: collector.overdue_count(),
                        ready_both,
                        sockets: connected_sockets.load(Ordering::Relaxed),
                        allocated_sockets: workers.len(),
                        assigned_copies: desired.len() * 2,
                        resets: collector.resets,
                        switches: collector.failovers,
                        paired: collector.paired_events,
                        deltas_per_sec: ((deltas - last_ui_deltas) as f64 / elapsed) as u64,
                        bytes_per_sec: ((bytes - last_ui_bytes) as f64 / elapsed) as u64,
                        bytes_total: bytes,
                        minute_snapshots,
                        scan_complete,
                        capture: collector.destination().to_owned(),
                    });
                    last_ui_at = Instant::now();
                    last_ui_deltas = deltas;
                    last_ui_bytes = bytes;
                }
            }
            _ = statistics_tick.tick() => {
                diagnostics::emit(format!("health: {} ready of {} tracked markets, {} source switches, {} paired deltas",
                    collector.market_count(), desired.len(), collector.failovers, collector.paired_events));
                if let Some(report) = collector.route_report() { diagnostics::emit(report); }
            }
        }
    }
    shutdown_tx.send_replace(true);
    while workers.iter().any(|worker| !worker.handle.is_finished()) {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                if matches!(&event, FeedEvent::Snapshot { .. } | FeedEvent::Delta { .. }) {
                    collector.on_event(event)?;
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }
    }
    while let Ok(event) = event_rx.try_recv() {
        if matches!(&event, FeedEvent::Snapshot { .. } | FeedEvent::Delta { .. }) {
            collector.on_event(event)?;
        }
    }
    for worker in workers {
        worker.handle.await?;
    }
    discovery.abort();
    collector.close()?;
    drop(dashboard);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_exchange_overrides_environment() {
        assert_eq!(
            parse_args(&["--exchange".into(), "polymarket".into()], Some("kalshi"))
                .unwrap()
                .exchange,
            ExchangeSelection::Polymarket,
        );
        assert_eq!(
            parse_args(&["--exchange".into(), "kalshi".into()], Some("polymarket"))
                .unwrap()
                .exchange,
            ExchangeSelection::Kalshi,
        );
        assert_eq!(
            parse_args(&[], None).unwrap().exchange,
            ExchangeSelection::Kalshi,
        );
    }

    #[test]
    fn tui_and_headless_flags_parse_in_either_order() {
        assert!(
            parse_args(
                &["--tui".into(), "--exchange".into(), "kalshi".into()],
                None
            )
            .unwrap()
            .tui
        );
        assert!(
            !parse_args(
                &[
                    "--exchange".into(),
                    "polymarket".into(),
                    "--headless".into()
                ],
                None
            )
            .unwrap()
            .tui
        );
        assert!(parse_args(&["--tui".into(), "--headless".into()], None).is_err());
        assert!(parse_args(&["--exchange".into()], None).is_err());
    }
}
