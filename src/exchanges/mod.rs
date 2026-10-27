//! The collector depends on normalized snapshots and signed, absolute-level
//! deltas. A new exchange implements this boundary without changing book,
//! source selection, health checks, or SBE output.

pub mod kalshi;
pub mod polymarket;
pub mod socket;

use std::{collections::HashSet, future::Future, time::Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::http::Request;

use engineering_challenge::Side;

/// Exchange messages after parsing, with both local and exchange times.
pub enum FeedEvent {
    Snapshot {
        source: usize,
        symbol: String,
        clock: i64,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
        arrival: Instant,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
    Delta {
        source: usize,
        symbol: String,
        clock: i64,
        ordinal: u32,
        event_id: Option<String>,
        side: Side,
        price: i64,
        change: LevelChange,
        arrival: Instant,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
    Reset {
        source: usize,
        symbols: Vec<String>,
    },
    Retire {
        source: usize,
        symbols: Vec<String>,
    },
}

/// Subscription acknowledgements and exchange-level errors.
pub enum ControlEvent {
    Subscribed(i64),
    Error(String),
    Other,
}

/// Requests sent from the collector to a socket worker.
pub enum WorkerCommand {
    Probe(String),
    Restart,
}

/// Pages add markets early; a complete scan can also retire them.
pub enum DiscoveryUpdate {
    Add(HashSet<String>),
    Complete(HashSet<String>),
}

/// Kalshi sends changes; Polymarket sends the resulting level size.
pub enum LevelChange {
    Delta(i64),
    Absolute(i64),
}

/// One frame can carry zero, one, or several book events.
pub enum Decoded {
    None,
    One(FeedEvent),
    Many(Vec<FeedEvent>),
}

/// Keeps exchange-specific wire formats out of the shared collector.
pub trait ExchangeAdapter: Send + Sync + 'static {
    const SUBSCRIPTION_BATCH_SIZE: usize = 500;
    const MARKETS_PER_SOCKET: usize = 500;
    const SOCKET_LIMIT: Option<usize> = None;
    const SNAPSHOT_REFRESH_ACTION: Option<&'static str> = None;

    fn name(&self) -> &'static str;
    fn discover(
        &self,
        client: &reqwest::Client,
        progress: &mpsc::Sender<DiscoveryUpdate>,
    ) -> impl Future<Output = anyhow::Result<HashSet<String>>> + Send;
    fn default_websocket_url(&self) -> &str;
    fn signed_request(&self, url: &str) -> anyhow::Result<Request<()>>;
    fn subscribe(&self, symbols: &[String]) -> Value;
    fn update(&self, sid: i64, id: i64, action: &str, symbols: &[String]) -> Value;
    fn control(&self, frame: &Value) -> ControlEvent;
    /// Returns a per-subscription sequence when the exchange supplies one.
    fn subscription_sequence(&self, _frame: &Value) -> Option<(i64, i64)> {
        None
    }
    /// Supplies an ID when the exchange sends no subscription acknowledgement.
    fn initial_subscription_id(&self) -> Option<i64> {
        None
    }
    /// Enables an exchange-specific text heartbeat when required.
    fn heartbeat_interval(&self) -> Option<std::time::Duration> {
        None
    }
    fn heartbeat_text(&self) -> Option<&'static str> {
        None
    }
    fn is_heartbeat_reply(&self, _text: &str) -> bool {
        false
    }
    fn decode(
        &self,
        frame: &Value,
        source: usize,
        arrival: Instant,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Decoded>;
}
