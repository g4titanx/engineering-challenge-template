//! Exchange-independent book state, source selection, and SBE output.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::OpenOptions,
    io::{BufWriter, IsTerminal, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use chrono::Utc;

use engineering_challenge::util::{AlignedSbeWriter, dtos::SbeEncode};

use crate::{
    book::Book,
    exchanges::{FeedEvent, LevelChange},
};

const FILE_CHUNK: usize = 64 << 20;
const PAIR_HISTORY: usize = 64;
const PAIR_CACHE_LIMIT: usize = 512;
const PAIR_MAX_DELAY: Duration = Duration::from_secs(2);
const LAG_GRACE: Duration = Duration::from_millis(20);
const LAG_SWITCH_COOLDOWN: Duration = Duration::from_secs(1);
const SWITCH_COOLDOWN: Duration = Duration::from_secs(300);
// A snapshot contains every delta through its exchange clock.
const SNAPSHOT_ORDINAL: u32 = u32::MAX;

/// Encodes SBE once and writes it to the capture and optional aligned file.
pub struct Output<W: Write> {
    stdout: BufWriter<W>,
    file: Option<AlignedSbeWriter>,
    buffer: Vec<u8>,
    bytes_written: u64,
    destination: String,
}

impl Output<Box<dyn Write>> {
    /// Uses stdout when piped, or creates a named capture at a terminal.
    pub fn new(exchange: &str) -> anyhow::Result<Self> {
        let file = std::env::var("SBE_OUTPUT_FILE")
            .ok()
            .map(|path| AlignedSbeWriter::create(path, FILE_CHUNK))
            .transpose()?;
        let (writer, destination): (Box<dyn Write>, String) = if std::io::stdout().is_terminal() {
            let stem = format!(
                "{}-orderbooks-{}",
                exchange.to_ascii_lowercase(),
                Utc::now().format("%Y-%m-%d_%H-%M-%S-UTC")
            );
            let mut attempt = 0;
            let (path, capture) = loop {
                let path = if attempt == 0 {
                    format!("{stem}.sbe")
                } else {
                    format!("{stem}-{attempt}.sbe")
                };
                match OpenOptions::new().write(true).create_new(true).open(&path) {
                    Ok(capture) => break (path, capture),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        attempt += 1;
                    }
                    Err(error) => {
                        return Err(error).with_context(|| format!("create SBE capture {path}"));
                    }
                }
            };
            crate::diagnostics::emit(format!("SBE capture: {path}"));
            (Box::new(capture), path)
        } else {
            (Box::new(std::io::stdout()), "stdout".to_owned())
        };
        let mut output = Self::with_writer(writer, file);
        output.destination = destination;
        Ok(output)
    }
}

impl<W: Write> Output<W> {
    /// Wraps a writer; tests use an in-memory writer.
    pub fn with_writer(writer: W, file: Option<AlignedSbeWriter>) -> Self {
        Self {
            stdout: BufWriter::with_capacity(256 << 10, writer),
            file,
            buffer: Vec::new(),
            bytes_written: 0,
            destination: "writer".to_owned(),
        }
    }

    /// Writes one complete SBE message, including its header.
    pub fn emit<T: SbeEncode>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        self.buffer.resize(len, 0);
        ensure!(
            message.encode(&mut self.buffer)? == len,
            "SBE length mismatch"
        );
        self.stdout.write_all(&self.buffer)?;
        self.bytes_written += len as u64;
        if let Some(file) = &mut self.file {
            file.write_msg(message)?;
        }
        Ok(())
    }

    /// Flushes buffered raw SBE output.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.stdout.flush().map_err(Into::into)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Flushes both output formats before releasing them.
    pub fn close(mut self) -> anyhow::Result<()> {
        self.flush()?;
        if let Some(file) = &mut self.file {
            file.flush()?;
        }
        Ok(())
    }
}

/// One socket's independent state for a market.
struct SourceBook {
    book: Book,
    clock: i64,
    ordinal: u32,
    last_arrival: Instant,
    last_probe: Instant,
}

impl SourceBook {
    /// Compares only books with exchange clocks; a snapshot wins its full clock.
    fn ahead_of(&self, other: &Self) -> bool {
        self.clock != i64::MIN
            && other.clock != i64::MIN
            && (self.clock, self.ordinal) > (other.clock, other.ordinal)
    }
}

/// First observed copy of an exchange update.
struct PairArrival {
    source: usize,
    arrival: Instant,
}

/// Matches the same update across two sockets.
#[derive(Clone, Hash, PartialEq, Eq)]
enum PairKey {
    Numeric(i64, u32),
    Hash(String),
}

/// The source books and short route-comparison history for one market.
struct MarketState {
    sources: HashMap<usize, SourceBook>,
    active: Option<usize>,
    last_switch: Instant,
    last_market_probe: Instant,
    pair_cache: HashMap<PairKey, PairArrival>,
    pair_order: VecDeque<(PairKey, Instant)>,
    recent_winners: VecDeque<(usize, u64)>,
}

impl MarketState {
    fn new(now: Instant) -> Self {
        Self {
            sources: HashMap::new(),
            active: None,
            last_switch: now,
            last_market_probe: now,
            pair_cache: HashMap::new(),
            pair_order: VecDeque::new(),
            recent_winners: VecDeque::new(),
        }
    }

    /// Chooses the most recently updated source after the active source fails.
    fn best_ready(&self) -> Option<usize> {
        self.sources
            .iter()
            .max_by_key(|(_, source)| source.last_arrival)
            .map(|(&id, _)| id)
    }

    /// Pairs two copies of an update using local monotonic arrival times.
    fn observe_pair(
        &mut self,
        source: usize,
        clock: i64,
        ordinal: u32,
        event_id: Option<&str>,
        arrival: Instant,
    ) -> Option<(usize, usize, usize, u64)> {
        let key = match event_id {
            Some(hash) => PairKey::Hash(hash.to_owned()),
            None => PairKey::Numeric(clock, ordinal),
        };
        if let Some(first) = self.pair_cache.remove(&key) {
            let difference = if first.arrival <= arrival {
                arrival.duration_since(first.arrival)
            } else {
                first.arrival.duration_since(arrival)
            };
            if first.source != source && difference <= PAIR_MAX_DELAY {
                let winner = if first.arrival <= arrival {
                    first.source
                } else {
                    source
                };
                let difference_us = difference.as_micros().min(u64::MAX as u128) as u64;
                self.recent_winners.push_back((winner, difference_us));
                if self.recent_winners.len() > PAIR_HISTORY {
                    self.recent_winners.pop_front();
                }
                return Some((first.source, source, winner, difference_us));
            }
        }
        self.pair_cache
            .insert(key.clone(), PairArrival { source, arrival });
        self.pair_order.push_back((key, arrival));
        while self.pair_order.len() > PAIR_CACHE_LIMIT {
            if let Some((old, old_arrival)) = self.pair_order.pop_front()
                && self
                    .pair_cache
                    .get(&old)
                    .is_some_and(|current| current.arrival == old_arrival)
            {
                self.pair_cache.remove(&old);
            }
        }
        None
    }

    /// Finds a backup with a sustained measured lead over the active source.
    fn favored_backup(&self) -> Option<usize> {
        if self.recent_winners.len() < PAIR_HISTORY {
            return None;
        }
        let active = self.active?;
        let mut wins = HashMap::<usize, Vec<u64>>::new();
        for &(winner, advantage_us) in &self.recent_winners {
            if winner != active {
                wins.entry(winner).or_default().push(advantage_us);
            }
        }
        wins.into_iter()
            .filter_map(|(source, mut advantages)| {
                if advantages.len() < 48 || !self.sources.contains_key(&source) {
                    return None;
                }
                advantages.sort_unstable();
                (advantages[advantages.len() / 2] >= 2_000).then_some(source)
            })
            .next()
    }
}

/// A market has two independent source books. Only the active source emits
/// deltas. A source change always emits its full book before further deltas.
pub struct Collector<W: Write> {
    markets: HashMap<String, MarketState>,
    desired: HashSet<String>,
    unready_since: HashMap<String, Instant>,
    output: Output<W>,
    source_lanes: HashMap<usize, usize>,
    route_differences_us: VecDeque<i64>,
    lagging: HashMap<String, Instant>,
    debug_switches: bool,
    pub failovers: u64,
    pub paired_events: u64,
    pub emitted_deltas: u64,
    pub resets: u64,
}

impl<W: Write> Collector<W> {
    /// Starts with no markets or ready source books.
    pub fn new(output: Output<W>) -> Self {
        Self {
            markets: HashMap::new(),
            desired: HashSet::new(),
            unready_since: HashMap::new(),
            output,
            source_lanes: HashMap::new(),
            route_differences_us: VecDeque::new(),
            lagging: HashMap::new(),
            debug_switches: std::env::var("DEBUG_SOCKETS").as_deref() == Ok("1"),
            failovers: 0,
            paired_events: 0,
            emitted_deltas: 0,
            resets: 0,
        }
    }

    /// Records which route a socket belongs to for paired timing.
    pub fn register_source(&mut self, source: usize, lane: usize) {
        self.source_lanes.insert(source, lane);
    }

    /// Adds discovered markets and retires markets from a completed scan.
    pub fn set_markets(&mut self, desired: HashSet<String>) -> anyhow::Result<()> {
        let removed: Vec<_> = self.desired.difference(&desired).cloned().collect();
        let discovered = Instant::now();
        for symbol in desired.difference(&self.desired) {
            self.unready_since.insert(symbol.clone(), discovered);
        }
        let now = Utc::now();
        for symbol in removed {
            self.lagging.remove(&symbol);
            self.unready_since.remove(&symbol);
            if self.markets.remove(&symbol).is_some() {
                self.output.emit(&Book::default().snapshot(&symbol, now))?;
            }
        }
        self.desired = desired;
        Ok(())
    }

    /// Emits a replacement snapshot before accepting deltas from a new source.
    fn select(&mut self, symbol: &str, source: usize, reason: &str) -> anyhow::Result<()> {
        let Some(state) = self.markets.get_mut(symbol) else {
            return Ok(());
        };
        if state.active == Some(source) {
            return Ok(());
        }
        let book = state
            .sources
            .get(&source)
            .context("selected source is not ready")?;
        self.output.emit(&book.book.snapshot(symbol, Utc::now()))?;
        if state.active.is_some() {
            self.failovers += 1;
        }
        if self.debug_switches {
            crate::diagnostics::emit(format!(
                "source switch {symbol}: {:?} -> {source} ({reason})",
                state.active
            ));
        }
        state.active = Some(source);
        state.last_switch = Instant::now();
        self.unready_since.remove(symbol);
        Ok(())
    }

    /// Fails over each affected market or marks it unready.
    fn remove_source(
        &mut self,
        source: usize,
        symbols: Vec<String>,
        reason: &str,
    ) -> anyhow::Result<()> {
        let mut switched = 0usize;
        let mut unavailable = 0usize;
        for symbol in symbols {
            self.lagging.remove(&symbol);
            let replacement = if let Some(state) = self.markets.get_mut(&symbol) {
                state.sources.remove(&source);
                if state.active == Some(source) {
                    state.best_ready()
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(new_source) = replacement {
                self.select(&symbol, new_source, reason)?;
                switched += 1;
            } else if let Some(state) = self.markets.get_mut(&symbol)
                && state.active == Some(source)
            {
                state.active = None;
                unavailable += 1;
                self.unready_since
                    .entry(symbol)
                    .or_insert_with(Instant::now);
            }
        }
        if switched > 0 || unavailable > 0 {
            crate::diagnostics::emit(format!(
                "socket {source} {reason}: {switched} markets switched, {unavailable} awaiting a fresh snapshot"
            ));
        }
        Ok(())
    }

    /// Applies a feed event to its source book and emits active-source data.
    pub fn on_event(&mut self, event: FeedEvent) -> anyhow::Result<()> {
        match event {
            FeedEvent::Snapshot {
                source,
                symbol,
                clock,
                bids,
                asks,
                arrival,
                received_at,
                exchange_timestamp,
            } => {
                if !self.desired.contains(&symbol) {
                    return Ok(());
                }
                let book =
                    Book::from_snapshot(&bids, &asks, exchange_timestamp).with_context(|| {
                        format!("invalid snapshot for {symbol} from socket {source}")
                    })?;
                let state = self
                    .markets
                    .entry(symbol.clone())
                    .or_insert_with(|| MarketState::new(arrival));
                if state.sources.contains_key(&source) {
                    state.pair_cache.clear();
                    state.pair_order.clear();
                    state.recent_winners.clear();
                }
                state.sources.insert(
                    source,
                    SourceBook {
                        book,
                        clock,
                        ordinal: if clock == i64::MIN {
                            0
                        } else {
                            SNAPSHOT_ORDINAL
                        },
                        last_arrival: arrival,
                        last_probe: arrival,
                    },
                );
                if state.active.is_none() {
                    self.output
                        .emit(&state.sources[&source].book.snapshot(&symbol, received_at))?;
                    state.active = Some(source);
                    state.last_switch = arrival;
                    self.unready_since.remove(&symbol);
                } else if state.active == Some(source) {
                    self.output
                        .emit(&state.sources[&source].book.snapshot(&symbol, received_at))?;
                }
                if state.active != Some(source)
                    && state.active.is_some_and(|active| {
                        state.sources[&source].ahead_of(&state.sources[&active])
                    })
                {
                    self.lagging.entry(symbol).or_insert(arrival);
                }
            }
            FeedEvent::Delta {
                source,
                symbol,
                clock,
                ordinal,
                event_id,
                side,
                price,
                change,
                arrival,
                received_at,
                exchange_timestamp,
            } => {
                if !self.desired.contains(&symbol) {
                    return Ok(());
                }
                let state = self
                    .markets
                    .get_mut(&symbol)
                    .with_context(|| format!("delta before snapshot for {symbol}"))?;
                let source_book = state.sources.get_mut(&source).with_context(|| {
                    format!("delta before source snapshot for {symbol} on socket {source}")
                })?;
                let update = match change {
                    LevelChange::Delta(delta) => source_book.book.apply_delta(
                        side,
                        price,
                        delta,
                        &symbol,
                        received_at,
                        exchange_timestamp,
                    )?,
                    LevelChange::Absolute(quantity) => source_book.book.set_absolute(
                        side,
                        price,
                        quantity,
                        &symbol,
                        received_at,
                        exchange_timestamp,
                    )?,
                };
                source_book.clock = clock;
                source_book.ordinal = ordinal;
                source_book.last_arrival = arrival;
                if let Some((first, second, winner, difference_us)) =
                    state.observe_pair(source, clock, ordinal, event_id.as_deref(), arrival)
                {
                    self.paired_events += 1;
                    if let (Some(first_lane), Some(second_lane)) = (
                        self.source_lanes.get(&first),
                        self.source_lanes.get(&second),
                    ) && first_lane != second_lane
                    {
                        let signed = if *first_lane == 0 {
                            if winner == first {
                                difference_us as i64
                            } else {
                                -(difference_us as i64)
                            }
                        } else if winner == second {
                            difference_us as i64
                        } else {
                            -(difference_us as i64)
                        };
                        self.route_differences_us.push_back(signed);
                        if self.route_differences_us.len() > 100_000 {
                            self.route_differences_us.pop_front();
                        }
                    }
                }
                if state.active == Some(source) {
                    self.output.emit(&update)?;
                    self.emitted_deltas += 1;
                }
                if let Some(active) = state.active {
                    let current = &state.sources[&active];
                    let backup_ahead = state
                        .sources
                        .iter()
                        .any(|(&id, backup)| id != active && backup.ahead_of(current));
                    if backup_ahead {
                        self.lagging.entry(symbol).or_insert(arrival);
                    } else {
                        self.lagging.remove(&symbol);
                    }
                }
            }
            FeedEvent::Reset { source, symbols } => {
                self.resets += 1;
                self.remove_source(source, symbols, "reset")?;
            }
            FeedEvent::Retire { source, symbols } => {
                self.remove_source(source, symbols, "unsubscribe")?
            }
        }
        Ok(())
    }

    /// Emits the latest full book for every ready market.
    pub fn snapshot_all(&mut self) -> anyhow::Result<usize> {
        let now = Utc::now();
        let mut count = 0;
        for (symbol, state) in &self.markets {
            if let Some(source) = state.active {
                self.output
                    .emit(&state.sources[&source].book.snapshot(symbol, now))?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Returns at most `budget` per-symbol probes. A destructive probe first
    /// moves the active book to a ready backup; Kalshi's get_snapshot does not.
    pub fn health_actions(
        &mut self,
        quiet: Duration,
        budget: usize,
        switch_before_probe: bool,
    ) -> anyhow::Result<Vec<(usize, String)>> {
        let now = Instant::now();
        let mut actions = Vec::new();
        let symbols: Vec<_> = self.markets.keys().cloned().collect();
        for symbol in symbols {
            let Some(state) = self.markets.get(&symbol) else {
                continue;
            };
            if let Some(active) = state.active
                && state.sources.contains_key(&active)
                && now.duration_since(state.last_switch) >= SWITCH_COOLDOWN
                && let Some(backup) = state.favored_backup()
            {
                self.select(&symbol, backup, "lower measured latency")?;
            }
            if actions.len() >= budget {
                continue;
            }
            let Some(state) = self.markets.get_mut(&symbol) else {
                continue;
            };
            if now.duration_since(state.last_market_probe) < Duration::from_secs(30) {
                continue;
            }
            let stale = state
                .sources
                .iter()
                .filter(|(_, source)| {
                    now.duration_since(source.last_arrival) >= quiet
                        && now.duration_since(source.last_probe) >= quiet
                })
                .min_by_key(|(id, _)| (**id == state.active.unwrap_or(usize::MAX), **id))
                .map(|(&id, _)| id);
            if let Some(source) = stale {
                // If this is the active source, move to a ready backup before recycling it.
                if switch_before_probe
                    && state.active == Some(source)
                    && let Some(backup) = state.sources.keys().copied().find(|id| *id != source)
                {
                    self.select(&symbol, backup, "quiet source probe")?;
                }
                if let Some(state) = self.markets.get_mut(&symbol) {
                    state.sources.get_mut(&source).unwrap().last_probe = now;
                    state.last_market_probe = now;
                }
                actions.push((source, symbol));
            }
        }
        Ok(actions)
    }

    /// Called every 10 ms. Only markets with a backup ahead are inspected.
    /// Switches to a backup whose exchange clock stayed ahead for 20 ms.
    pub fn resolve_lag(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        let due: Vec<_> = self
            .lagging
            .iter()
            .filter(|(_, since)| now.duration_since(**since) >= LAG_GRACE)
            .map(|(symbol, _)| symbol.clone())
            .collect();
        for symbol in due {
            if self
                .markets
                .get(&symbol)
                .is_some_and(|state| now.duration_since(state.last_switch) < LAG_SWITCH_COOLDOWN)
            {
                continue;
            }
            self.lagging.remove(&symbol);
            let backup = self.markets.get(&symbol).and_then(|state| {
                let active = state.active?;
                let current = state.sources.get(&active)?;
                state
                    .sources
                    .iter()
                    .filter(|(id, source)| **id != active && source.ahead_of(current))
                    .max_by_key(|(_, source)| (source.clock, source.ordinal))
                    .map(|(&id, _)| id)
            });
            if let Some(backup) = backup {
                self.select(&symbol, backup, "backup ahead by 20ms")?;
            }
        }
        Ok(())
    }

    /// Checks that assigned markets have another initialized source book.
    pub fn has_backup_for(&self, source: usize, symbols: &HashSet<String>) -> bool {
        symbols.iter().all(|symbol| {
            self.markets
                .get(symbol)
                .is_some_and(|state| state.sources.keys().any(|id| *id != source))
        })
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.output.flush()
    }
    pub fn close(self) -> anyhow::Result<()> {
        self.output.close()
    }
    pub fn market_count(&self) -> usize {
        self.markets
            .values()
            .filter(|state| state.active.is_some())
            .count()
    }

    /// Counts ready markets and those ready on both lanes.
    pub fn coverage_counts(&self) -> (usize, usize) {
        let mut ready = 0;
        let mut ready_both = 0;
        for state in self.markets.values() {
            if state.active.is_none() {
                continue;
            }
            ready += 1;
            if state
                .sources
                .keys()
                .any(|id| self.source_lanes.get(id) == Some(&0))
                && state
                    .sources
                    .keys()
                    .any(|id| self.source_lanes.get(id) == Some(&1))
            {
                ready_both += 1;
            }
        }
        (ready, ready_both)
    }

    /// Counts markets without a ready book for at least one minute.
    pub fn overdue_count(&self) -> usize {
        self.unready_since
            .values()
            .filter(|since| since.elapsed() >= Duration::from_secs(60))
            .count()
    }

    pub fn bytes_written(&self) -> u64 {
        self.output.bytes_written()
    }

    pub fn destination(&self) -> &str {
        self.output.destination()
    }

    /// Positive values mean route A delivered the matched delta first.
    /// Summarizes matched A/B arrivals using signed microsecond differences.
    pub fn route_report(&self) -> Option<String> {
        if self.route_differences_us.is_empty() {
            return None;
        }
        let mut samples: Vec<_> = self.route_differences_us.iter().copied().collect();
        samples.sort_unstable();
        let index = |pct: usize| (samples.len() - 1) * pct / 100;
        let a_wins = samples.iter().filter(|&&us| us > 0).count();
        Some(format!(
            "route comparison: {} matched deltas, A first {:.1}%, B-A receive difference p05={}us p50={}us p95={}us",
            samples.len(),
            100.0 * a_wins as f64 / samples.len() as f64,
            samples[index(5)],
            samples[index(50)],
            samples[index(95)]
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engineering_challenge::Side;
    use engineering_challenge::util::dtos::challenge_orderbook::AnyMessage;

    #[test]
    fn old_duplicate_event_ids_are_not_route_matches() {
        let now = Instant::now();
        let mut state = MarketState::new(now);
        assert!(state.observe_pair(0, 0, 0, Some("same"), now).is_none());
        assert!(
            state
                .observe_pair(1, 0, 0, Some("same"), now + Duration::from_secs(3))
                .is_none()
        );
        assert!(state.recent_winners.is_empty());
        assert!(
            state
                .observe_pair(
                    0,
                    0,
                    0,
                    Some("same"),
                    now + Duration::from_secs(3) + Duration::from_millis(1)
                )
                .is_some()
        );
    }

    fn snapshot(source: usize, clock: i64, arrival: Instant) -> FeedEvent {
        FeedEvent::Snapshot {
            source,
            symbol: "T".into(),
            clock,
            bids: vec![["0.4000".into(), "2.00".into()]],
            asks: vec![["0.6000".into(), "3.00".into()]],
            arrival,
            received_at: Utc::now(),
            exchange_timestamp: Utc::now(),
        }
    }

    fn snapshot_with_bid(source: usize, clock: i64, quantity: &str, arrival: Instant) -> FeedEvent {
        let mut event = snapshot(source, clock, arrival);
        if let FeedEvent::Snapshot { bids, .. } = &mut event {
            bids[0][1] = quantity.to_owned();
        }
        event
    }

    fn delta(source: usize, clock: i64, arrival: Instant) -> FeedEvent {
        FeedEvent::Delta {
            source,
            symbol: "T".into(),
            clock,
            ordinal: 0,
            event_id: None,
            side: Side::Buy,
            price: 400_000,
            change: LevelChange::Delta(1_000_000),
            arrival,
            received_at: Utc::now(),
            exchange_timestamp: Utc::now(),
        }
    }

    #[test]
    fn backup_is_replayed_only_after_source_switch() {
        let mut collector = Collector::new(Output::with_writer(Vec::<u8>::new(), None));
        collector.set_markets(HashSet::from(["T".into()])).unwrap();
        collector.register_source(0, 0);
        collector.register_source(1, 1);
        let now = Instant::now();
        collector.on_event(snapshot(0, 10, now)).unwrap();
        collector.on_event(snapshot(1, 10, now)).unwrap();
        collector.flush().unwrap();
        let initial_len = collector.output.stdout.get_ref().len();
        collector
            .on_event(delta(1, 11, now + Duration::from_millis(1)))
            .unwrap();
        collector.flush().unwrap();
        assert_eq!(collector.output.stdout.get_ref().len(), initial_len);
        collector
            .on_event(delta(0, 11, now + Duration::from_millis(3)))
            .unwrap();
        assert_eq!(collector.paired_events, 1);
        collector
            .on_event(FeedEvent::Reset {
                source: 0,
                symbols: vec!["T".into()],
            })
            .unwrap();
        assert_eq!(collector.markets["T"].active, Some(1));
        assert_eq!(collector.failovers, 1);
        collector
            .on_event(delta(1, 12, now + Duration::from_millis(4)))
            .unwrap();
        collector.flush().unwrap();
        let bytes = collector.output.stdout.get_ref();
        let mut offset = 0;
        let mut types = Vec::new();
        while offset < bytes.len() {
            let message = AnyMessage::try_decode(bytes, offset).unwrap();
            types.push(match &message {
                AnyMessage::Snapshot(_) => "snapshot",
                AnyMessage::PriceLevelUpdate(_) => "update",
                _ => "other",
            });
            offset += message.encoded_length_with_header().unwrap();
        }
        assert_eq!(types, ["snapshot", "update", "snapshot", "update"]);
    }

    #[test]
    fn complete_snapshot_stays_ahead_of_same_clock_partial_delta() {
        let mut collector = Collector::new(Output::with_writer(Vec::<u8>::new(), None));
        collector.set_markets(HashSet::from(["T".into()])).unwrap();
        let old = Instant::now() - Duration::from_secs(2);
        collector
            .on_event(snapshot_with_bid(0, 100, "5.00", old))
            .unwrap();
        collector
            .on_event(snapshot_with_bid(1, 99, "3.00", old))
            .unwrap();
        let mut partial = delta(1, 100, old);
        if let FeedEvent::Delta { ordinal, .. } = &mut partial {
            *ordinal = 1;
        }
        collector.on_event(partial).unwrap();

        let state = &collector.markets["T"];
        assert_eq!(
            state.sources[&0].book.snapshot("T", Utc::now()).bids[0].quantity,
            5_000_000
        );
        assert_eq!(
            state.sources[&1].book.snapshot("T", Utc::now()).bids[0].quantity,
            4_000_000
        );
        assert!(!collector.lagging.contains_key("T"));
        collector.resolve_lag().unwrap();
        assert_eq!(collector.markets["T"].active, Some(0));
    }

    #[test]
    fn same_clock_complete_snapshot_outranks_partial_delta() {
        let mut collector = Collector::new(Output::with_writer(Vec::<u8>::new(), None));
        collector.set_markets(HashSet::from(["T".into()])).unwrap();
        let old = Instant::now() - Duration::from_secs(2);
        collector
            .on_event(snapshot_with_bid(0, 99, "3.00", old))
            .unwrap();
        let mut partial = delta(0, 100, old);
        if let FeedEvent::Delta { ordinal, .. } = &mut partial {
            *ordinal = 1;
        }
        collector.on_event(partial).unwrap();
        collector
            .on_event(snapshot_with_bid(1, 100, "5.00", old))
            .unwrap();

        assert!(collector.lagging.contains_key("T"));
        collector.resolve_lag().unwrap();
        assert_eq!(collector.markets["T"].active, Some(1));
    }

    #[test]
    fn non_destructive_probe_keeps_the_active_source() {
        let mut collector = Collector::new(Output::with_writer(Vec::<u8>::new(), None));
        collector.set_markets(HashSet::from(["T".into()])).unwrap();
        collector.register_source(0, 0);
        collector.register_source(1, 1);
        let old = Instant::now() - Duration::from_secs(601);
        collector.on_event(snapshot(0, 10, old)).unwrap();
        collector.on_event(snapshot(1, 10, Instant::now())).unwrap();
        let actions = collector
            .health_actions(Duration::from_secs(600), 1, false)
            .unwrap();
        assert_eq!(actions, [(0, "T".into())]);
        assert_eq!(collector.markets["T"].active, Some(0));
        assert_eq!(collector.failovers, 0);
    }
}
