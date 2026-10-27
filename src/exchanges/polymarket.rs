//! Public Polymarket CLOB market stream adapter. A symbol is the YES outcome's
//! asset ID; its book already has bid and ask ladders in that outcome's price.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::Request};

use engineering_challenge::Side;

use super::{ControlEvent, Decoded, DiscoveryUpdate, ExchangeAdapter, FeedEvent, LevelChange};
use crate::book::fixed;

const GAMMA_MARKETS: &str = "https://gamma-api.polymarket.com/markets/keyset";
const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub struct Polymarket;

/// A cursor page from Gamma's market list.
#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    next_cursor: Option<String>,
}

/// Fields needed to find an active market's YES token.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    active: Option<bool>,
    accepting_orders: Option<bool>,
    enable_order_book: Option<bool>,
    clob_token_ids: Option<Value>,
    position_ids: Option<Vec<String>>,
}

/// Accepts token IDs encoded as either JSON or a JSON string.
fn outcome_ids(value: &Value) -> anyhow::Result<Vec<String>> {
    match value {
        Value::Array(_) => Ok(serde_json::from_value(value.clone())?),
        Value::String(text) => Ok(serde_json::from_str(text)?),
        _ => anyhow::bail!("invalid outcome IDs"),
    }
}

/// Parses Polymarket's timestamp forms, falling back to local receipt time.
fn milliseconds(value: Option<&Value>, fallback: DateTime<Utc>) -> anyhow::Result<i64> {
    let Some(value) = value else {
        return Ok(fallback.timestamp_millis());
    };
    match value {
        Value::String(text) => text
            .parse::<i64>()
            .or_else(|_| DateTime::parse_from_rfc3339(text).map(|time| time.timestamp_millis()))
            .context("invalid Polymarket timestamp"),
        Value::Number(number) => number.as_i64().context("invalid Polymarket timestamp"),
        Value::Null => Ok(fallback.timestamp_millis()),
        _ => anyhow::bail!("invalid Polymarket timestamp"),
    }
}

/// Converts object-shaped price levels into the shared book input.
fn levels(value: Option<&Value>) -> anyhow::Result<Vec<[String; 2]>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(levels) = value.as_array() else {
        anyhow::bail!("orderbook levels must be an array")
    };
    levels
        .iter()
        .map(|level| {
            Ok([
                level
                    .get("price")
                    .and_then(Value::as_str)
                    .context("missing level price")?
                    .to_owned(),
                level
                    .get("size")
                    .and_then(Value::as_str)
                    .context("missing level size")?
                    .to_owned(),
            ])
        })
        .collect()
}

/// Maps Polymarket's public market feed into shared book events.
impl ExchangeAdapter for Polymarket {
    fn name(&self) -> &'static str {
        "Polymarket"
    }

    async fn discover(
        &self,
        client: &reqwest::Client,
        progress: &mpsc::Sender<DiscoveryUpdate>,
    ) -> anyhow::Result<HashSet<String>> {
        let mut symbols = HashSet::new();
        // Gamma defaults to v1 when `version` is omitted. Scan both protocols.
        for version in ["v1", "v2"] {
            let mut cursor = String::new();
            loop {
                let response = client
                    .get(GAMMA_MARKETS)
                    .query(&[
                        ("closed", "false"),
                        ("limit", "500"),
                        ("version", version),
                        ("after_cursor", cursor.as_str()),
                    ])
                    .send()
                    .await?
                    .error_for_status()?;
                let page: MarketsPage = response.json().await?;
                let mut additions = HashSet::new();
                for market in page.markets {
                    if market.active != Some(true)
                        || market.accepting_orders != Some(true)
                        || market.enable_order_book != Some(true)
                    {
                        continue;
                    }
                    let ids = if version == "v2" {
                        market.position_ids
                    } else {
                        market
                            .clob_token_ids
                            .as_ref()
                            .map(outcome_ids)
                            .transpose()?
                    };
                    if let Some(ids) = ids
                        && let Some(yes) = ids.first()
                        && symbols.insert(yes.clone())
                    {
                        additions.insert(yes.clone());
                    }
                }
                if !additions.is_empty() {
                    progress.send(DiscoveryUpdate::Add(additions)).await?;
                }
                let next = page.next_cursor.unwrap_or_default();
                if next.is_empty() {
                    break;
                }
                ensure!(next != cursor, "Polymarket cursor did not advance");
                cursor = next;
            }
        }
        Ok(symbols)
    }

    fn default_websocket_url(&self) -> &str {
        WS_URL
    }

    fn signed_request(&self, url: &str) -> anyhow::Result<Request<()>> {
        Ok(url.into_client_request()?)
    }

    fn subscribe(&self, symbols: &[String]) -> Value {
        json!({"assets_ids": symbols, "type": "market"})
    }

    fn update(&self, _sid: i64, _id: i64, action: &str, symbols: &[String]) -> Value {
        let operation = if action == "delete_markets" {
            "unsubscribe"
        } else {
            "subscribe"
        };
        json!({"assets_ids": symbols, "operation": operation})
    }

    fn control(&self, frame: &Value) -> ControlEvent {
        if frame.get("event_type").and_then(Value::as_str) == Some("error")
            || frame.get("error").is_some()
        {
            ControlEvent::Error(frame.to_string())
        } else {
            ControlEvent::Other
        }
    }

    fn initial_subscription_id(&self) -> Option<i64> {
        Some(1)
    }
    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(10))
    }
    fn heartbeat_text(&self) -> Option<&'static str> {
        Some("PING")
    }
    fn is_heartbeat_reply(&self, text: &str) -> bool {
        text == "PONG"
    }

    fn decode(
        &self,
        frame: &Value,
        source: usize,
        arrival: Instant,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Decoded> {
        let kind = frame
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match kind {
            "book" => {
                let symbol = frame
                    .get("asset_id")
                    .and_then(Value::as_str)
                    .context("book missing asset_id")?
                    .to_owned();
                let event_time = milliseconds(frame.get("timestamp"), received_at)?;
                let exchange_timestamp = DateTime::from_timestamp_millis(event_time)
                    .context("timestamp out of range")?;
                Ok(Decoded::One(FeedEvent::Snapshot {
                    source,
                    symbol,
                    // Polymarket timestamps are not a documented sequence.
                    clock: i64::MIN,
                    bids: levels(frame.get("bids"))?,
                    asks: levels(frame.get("asks"))?,
                    arrival,
                    received_at,
                    exchange_timestamp,
                }))
            }
            "price_change" => {
                let event_time = milliseconds(frame.get("timestamp"), received_at)?;
                let exchange_timestamp = DateTime::from_timestamp_millis(event_time)
                    .context("timestamp out of range")?;
                let changes = frame
                    .get("price_changes")
                    .and_then(Value::as_array)
                    .context("price_change missing price_changes")?;
                let mut events = Vec::with_capacity(changes.len());
                for change in changes {
                    let symbol = change
                        .get("asset_id")
                        .and_then(Value::as_str)
                        .context("price_change missing asset_id")?
                        .to_owned();
                    let side = match change.get("side").and_then(Value::as_str) {
                        Some("BUY") => Side::Buy,
                        Some("SELL") => Side::Sell,
                        _ => anyhow::bail!("unknown Polymarket side"),
                    };
                    let price = fixed(
                        change
                            .get("price")
                            .and_then(Value::as_str)
                            .context("missing price")?,
                    )?;
                    let quantity = fixed(
                        change
                            .get("size")
                            .and_then(Value::as_str)
                            .context("missing size")?,
                    )?;
                    events.push(FeedEvent::Delta {
                        source,
                        symbol,
                        clock: i64::MIN,
                        ordinal: 0,
                        event_id: Some(match change.get("hash").and_then(Value::as_str) {
                            Some(hash) => format!("{hash}:{side}:{price}:{quantity}"),
                            None => format!("{event_time}:{side}:{price}:{quantity}"),
                        }),
                        side,
                        price,
                        change: LevelChange::Absolute(quantity),
                        arrival,
                        received_at,
                        exchange_timestamp,
                    });
                }
                match events.len() {
                    0 => Ok(Decoded::None),
                    1 => Ok(Decoded::One(events.pop().unwrap())),
                    _ => Ok(Decoded::Many(events)),
                }
            }
            _ => Ok(Decoded::None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_book_and_absolute_level_update() {
        let adapter = Polymarket;
        let now = Utc::now();
        let book = json!({"event_type":"book","asset_id":"YES","timestamp":"1700000000000",
            "bids":[{"price":"0.4","size":"2"}],"asks":[{"price":"0.6","size":"3"}]});
        match adapter.decode(&book, 0, Instant::now(), now).unwrap() {
            Decoded::One(FeedEvent::Snapshot {
                symbol,
                clock,
                bids,
                asks,
                ..
            }) => {
                assert_eq!(symbol, "YES");
                assert_eq!(clock, i64::MIN);
                assert_eq!(bids[0], ["0.4", "2"]);
                assert_eq!(asks[0], ["0.6", "3"]);
            }
            _ => panic!("expected book"),
        }
        let change = json!({"event_type":"price_change","timestamp":"1700000000001",
            "price_changes":[{"asset_id":"YES","price":"0.4","size":"1.5","side":"BUY"}]});
        match adapter.decode(&change, 0, Instant::now(), now).unwrap() {
            Decoded::One(FeedEvent::Delta {
                clock,
                event_id,
                change: LevelChange::Absolute(quantity),
                ..
            }) => {
                assert_eq!(clock, i64::MIN);
                assert!(event_id.unwrap().starts_with("1700000000001:"));
                assert_eq!(quantity, 1_500_000);
            }
            _ => panic!("expected absolute update"),
        }
    }

    #[test]
    fn decreasing_timestamps_do_not_become_sequence_regressions() {
        let adapter = Polymarket;
        let now = Utc::now();
        for timestamp in ["1700000000002", "1700000000001"] {
            let frame = json!({"event_type":"price_change","timestamp":timestamp,
                "price_changes":[{"asset_id":"YES","price":"0.4","size":"1.5","side":"BUY","hash":"book-state"}]});
            match adapter.decode(&frame, 0, Instant::now(), now).unwrap() {
                Decoded::One(FeedEvent::Delta { clock, .. }) => assert_eq!(clock, i64::MIN),
                _ => panic!("expected delta"),
            }
        }
    }
}
