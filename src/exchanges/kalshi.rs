//! Kalshi discovery, signed WebSocket requests, and YES-side book decoding.

use std::{
    collections::HashSet,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use openssl::{
    hash::MessageDigest,
    pkey::{Id, PKey, Private},
    rsa::Padding,
    sign::{RsaPssSaltlen, Signer},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, Request},
};

use engineering_challenge::Side;

use super::{ControlEvent, Decoded, DiscoveryUpdate, ExchangeAdapter, FeedEvent, LevelChange};
use crate::book::fixed;

const WS_PATH: &str = "/trade-api/ws/v2";
const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2/markets";

/// Key material used to sign each Kalshi WebSocket handshake.
pub struct Auth {
    key_id: String,
    key: PKey<Private>,
}

impl Auth {
    /// Loads a Kalshi key ID and PEM private key from `.env` or the process.
    pub fn from_env() -> anyhow::Result<Self> {
        let key_id = std::env::var("KALSHI_KEY_ID").context("set KALSHI_KEY_ID")?;
        let path =
            std::env::var("KALSHI_PRIVATE_KEY_PATH").context("set KALSHI_PRIVATE_KEY_PATH")?;
        let bytes = std::fs::read(Path::new(&path))
            .with_context(|| format!("read private key at {path}"))?;
        let key = PKey::private_key_from_pem(&bytes).context("parse Kalshi private key PEM")?;
        ensure!(
            matches!(key.id(), Id::RSA | Id::ED25519),
            "Kalshi key must be RSA or Ed25519"
        );
        Ok(Self { key_id, key })
    }

    /// Signs the handshake path and preserves WebSocket upgrade headers.
    fn build_signed_request(&self, url: &str) -> anyhow::Result<Request<()>> {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();
        let signed = format!("{milliseconds}GET{WS_PATH}");
        let signature = if self.key.id() == Id::ED25519 {
            Signer::new_without_digest(&self.key)?.sign_oneshot_to_vec(signed.as_bytes())?
        } else {
            let mut signer = Signer::new(MessageDigest::sha256(), &self.key)?;
            signer.set_rsa_padding(Padding::PKCS1_PSS)?;
            signer.set_rsa_mgf1_md(MessageDigest::sha256())?;
            signer.set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH)?;
            signer.sign_oneshot_to_vec(signed.as_bytes())?
        };
        let mut request = url.into_client_request()?;
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&self.key_id)?);
        headers.insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&STANDARD.encode(signature))?,
        );
        headers.insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&milliseconds)?,
        );
        Ok(request)
    }
}

/// One page of open regular markets from the REST API.
#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    cursor: String,
}

/// Only the ticker is needed to subscribe to a market.
#[derive(Deserialize)]
struct Market {
    ticker: String,
}

/// Publish additions as pages arrive; only the completed set can retire markets.
async fn discover_markets(
    client: &reqwest::Client,
    progress: &mpsc::Sender<DiscoveryUpdate>,
) -> anyhow::Result<HashSet<String>> {
    let mut cursor = String::new();
    let mut markets = HashSet::new();
    let scan_started = Instant::now();
    let mut pages = 0usize;
    let mut rest_time = std::time::Duration::ZERO;
    let mut enqueue_time = std::time::Duration::ZERO;
    loop {
        // A small first page makes the first subscriptions available quickly;
        // larger later pages keep the total REST request count low.
        let limit = if cursor.is_empty() { "100" } else { "1000" };
        let mut retries = 0;
        let page = loop {
            let page_started = Instant::now();
            let result = async {
                let response = client
                    .get(REST_URL)
                    .query(&[
                        ("status", "open"),
                        ("mve_filter", "exclude"),
                        ("limit", limit),
                        ("cursor", cursor.as_str()),
                    ])
                    .send()
                    .await?
                    .error_for_status()?;
                response.json::<MarketsPage>().await
            }
            .await;
            rest_time += page_started.elapsed();
            match result {
                Ok(page) => break page,
                Err(error)
                    if retries < 5
                        && (error.is_timeout()
                            || error.is_connect()
                            || error.is_body()
                            || error.status().is_some_and(|status| {
                                status == reqwest::StatusCode::TOO_MANY_REQUESTS
                                    || status.is_server_error()
                            })) =>
                {
                    let delay = Duration::from_secs(1 << retries);
                    retries += 1;
                    crate::diagnostics::emit(format!(
                        "Kalshi scan page {}: {error}; retry {retries}/5 in {}s",
                        pages + 1,
                        delay.as_secs()
                    ));
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let additions: HashSet<_> = page.markets.into_iter().map(|m| m.ticker).collect();
        markets.extend(additions.iter().cloned());
        let enqueue_started = Instant::now();
        if !additions.is_empty() {
            progress.send(DiscoveryUpdate::Add(additions)).await?;
        }
        enqueue_time += enqueue_started.elapsed();
        pages += 1;
        if pages.is_multiple_of(10) || page.cursor.is_empty() {
            crate::diagnostics::emit(format!(
                "Kalshi scan: {pages} pages, {} unique markets; REST {:.1}s, queue {:.1}s, elapsed {:.1}s",
                markets.len(),
                rest_time.as_secs_f64(),
                enqueue_time.as_secs_f64(),
                scan_started.elapsed().as_secs_f64()
            ));
        }
        if page.cursor.is_empty() {
            break;
        }
        ensure!(
            page.cursor != cursor,
            "market pagination cursor did not advance"
        );
        cursor = page.cursor;
    }
    Ok(markets)
}

/// Maps Kalshi's signed protocol into the shared socket lifecycle.
impl ExchangeAdapter for Auth {
    const SUBSCRIPTION_BATCH_SIZE: usize = 25;
    const MARKETS_PER_SOCKET: usize = 1_500;
    const SOCKET_LIMIT: Option<usize> = Some(180);
    const SNAPSHOT_REFRESH_ACTION: Option<&'static str> = Some("get_snapshot");

    fn name(&self) -> &'static str {
        "Kalshi"
    }

    async fn discover(
        &self,
        client: &reqwest::Client,
        progress: &mpsc::Sender<DiscoveryUpdate>,
    ) -> anyhow::Result<HashSet<String>> {
        discover_markets(client, progress).await
    }

    fn default_websocket_url(&self) -> &str {
        WS_URL
    }

    fn signed_request(&self, url: &str) -> anyhow::Result<Request<()>> {
        self.build_signed_request(url)
    }

    fn subscribe(&self, symbols: &[String]) -> Value {
        json!({"id": 1, "cmd": "subscribe", "params": {
            "channels": ["orderbook_delta"], "market_tickers": symbols
        }})
    }

    fn update(&self, sid: i64, id: i64, action: &str, symbols: &[String]) -> Value {
        json!({"id": id, "cmd": "update_subscription", "params": {
            "sid": sid, "market_tickers": symbols, "action": action
        }})
    }

    fn control(&self, frame: &Value) -> ControlEvent {
        match frame.get("type").and_then(Value::as_str) {
            Some("subscribed") => frame
                .get("msg")
                .and_then(|v| v.get("sid"))
                .and_then(Value::as_i64)
                .map(ControlEvent::Subscribed)
                .unwrap_or_else(|| ControlEvent::Error("missing subscription ID".into())),
            Some("error") => {
                ControlEvent::Error(frame.get("msg").unwrap_or(&Value::Null).to_string())
            }
            _ => ControlEvent::Other,
        }
    }

    fn subscription_sequence(&self, frame: &Value) -> Option<(i64, i64)> {
        Some((frame.get("sid")?.as_i64()?, frame.get("seq")?.as_i64()?))
    }

    fn decode(
        &self,
        frame: &Value,
        source: usize,
        arrival: Instant,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Decoded> {
        parse_book_event(frame, source, arrival, received_at)
    }
}

/// Reads an exchange timestamp expressed in Unix milliseconds.
fn timestamp_ms(value: &Value, key: &str) -> anyhow::Result<DateTime<Utc>> {
    let millis = value
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing {key}"))?;
    DateTime::from_timestamp_millis(millis).with_context(|| format!("invalid {key}"))
}

/// Treats an absent bid ladder as empty, as seen in live snapshots.
fn parse_levels(msg: &Value, key: &str) -> anyhow::Result<Vec<[String; 2]>> {
    match msg.get(key) {
        Some(Value::Null) | None => Ok(Vec::new()),
        Some(value) => Ok(serde_json::from_value(value.clone())?),
    }
}

/// Converts a NO bid price to its YES ask price.
fn no_to_yes(price: i64) -> anyhow::Result<i64> {
    ensure!((0..=1_000_000).contains(&price), "NO price outside [0, 1]");
    Ok(1_000_000 - price)
}

/// Converts a Kalshi snapshot or delta to the collector's YES-side events.
pub fn parse_book_event(
    frame: &Value,
    source: usize,
    arrival: Instant,
    received_at: DateTime<Utc>,
) -> anyhow::Result<Decoded> {
    let kind = frame
        .get("type")
        .and_then(Value::as_str)
        .context("missing message type")?;
    if kind != "orderbook_snapshot" && kind != "orderbook_delta" {
        return Ok(Decoded::None);
    }
    let msg = frame.get("msg").context("missing orderbook payload")?;
    let symbol = msg
        .get("market_ticker")
        .and_then(Value::as_str)
        .context("missing market_ticker")?
        .to_owned();
    if kind == "orderbook_snapshot" {
        // Production snapshots can omit clock even though the API example has it.
        // A missing clock must not be compared to another source's delta clock.
        let clock = msg.get("clock").and_then(Value::as_i64).unwrap_or(i64::MIN);
        Ok(Decoded::One(FeedEvent::Snapshot {
            source,
            symbol,
            clock,
            bids: parse_levels(msg, "yes_dollars_fp")?,
            asks: parse_levels(msg, "no_dollars_fp")?
                .into_iter()
                .map(|[price, quantity]| {
                    let yes_price = no_to_yes(fixed(&price)?)?;
                    Ok([
                        format!("{}.{:06}", yes_price / 1_000_000, yes_price % 1_000_000),
                        quantity,
                    ])
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            arrival,
            received_at,
            exchange_timestamp: timestamp_ms(frame, "sending_ts_ms")?,
        }))
    } else {
        let side = match msg
            .get("side")
            .and_then(Value::as_str)
            .context("missing side")?
        {
            "yes" => Side::Buy,
            "no" => Side::Sell,
            side => anyhow::bail!("unknown side: {side}"),
        };
        let raw_price = fixed(
            msg.get("price_dollars")
                .and_then(Value::as_str)
                .context("missing price_dollars")?,
        )?;
        let price = if side == Side::Sell {
            no_to_yes(raw_price)?
        } else {
            raw_price
        };
        let delta = fixed(
            msg.get("delta_fp")
                .and_then(Value::as_str)
                .context("missing delta_fp")?,
        )?;
        let exchange_timestamp =
            timestamp_ms(msg, "ts_ms").or_else(|_| timestamp_ms(frame, "sending_ts_ms"))?;
        // Some production deltas omit the documented clock. Event time is not
        // a substitute for the exchange clock; the subscription seq still
        // detects loss on this connection.
        let exchange_clock = msg.get("clock").and_then(Value::as_i64);
        let clock = exchange_clock.unwrap_or(i64::MIN);
        let event_id = exchange_clock.is_none().then(|| {
            format!(
                "{}:{side}:{price}:{delta}",
                exchange_timestamp.timestamp_millis()
            )
        });
        Ok(Decoded::One(FeedEvent::Delta {
            source,
            symbol,
            clock,
            ordinal: 0,
            event_id,
            side,
            price,
            change: LevelChange::Delta(delta),
            arrival,
            received_at,
            exchange_timestamp,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kalshi_delta_in_yes_coordinates() {
        let frame = json!({"type":"orderbook_delta","sending_ts_ms":1700000000100_i64,
            "msg":{"market_ticker":"T","side":"no","price_dollars":"0.6400",
            "delta_fp":"-2.50","clock":42,"ts_ms":1700000000000_i64}});
        match parse_book_event(&frame, 0, Instant::now(), Utc::now()).unwrap() {
            Decoded::One(FeedEvent::Delta {
                side,
                price,
                change: LevelChange::Delta(delta),
                ..
            }) => {
                assert_eq!(side, Side::Sell);
                assert_eq!(price, 360_000);
                assert_eq!(delta, -2_500_000);
            }
            _ => panic!("expected delta"),
        }
    }

    #[test]
    fn no_snapshot_bids_become_yes_asks() {
        let frame = json!({"type":"orderbook_snapshot","sending_ts_ms":1700000000100_i64,
            "msg":{"market_ticker":"T","clock":42,"yes_dollars_fp":[["0.4000","1.00"]],
            "no_dollars_fp":[["0.6400","2.00"]]}});
        match parse_book_event(&frame, 0, Instant::now(), Utc::now()).unwrap() {
            Decoded::One(FeedEvent::Snapshot { asks, .. }) => {
                assert_eq!(asks, vec![["0.360000".to_owned(), "2.00".to_owned()]]);
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn production_snapshot_can_omit_clock_and_empty_yes_ladder() {
        let frame = json!({"type":"orderbook_snapshot","sending_ts_ms":1700000000100_i64,
            "msg":{"market_ticker":"T","no_dollars_fp":[["0.6400","2.00"]]}});
        match parse_book_event(&frame, 0, Instant::now(), Utc::now()).unwrap() {
            Decoded::One(FeedEvent::Snapshot {
                clock, bids, asks, ..
            }) => {
                assert_eq!(clock, i64::MIN);
                assert!(bids.is_empty());
                assert_eq!(asks, vec![["0.360000".to_owned(), "2.00".to_owned()]]);
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn production_delta_can_fall_back_to_event_time() {
        let frame = json!({"type":"orderbook_delta","sending_ts_ms":1700000000100_i64,
            "msg":{"market_ticker":"T","side":"yes","price_dollars":"0.4000",
            "delta_fp":"1.00","ts_ms":1700000000000_i64}});
        match parse_book_event(&frame, 0, Instant::now(), Utc::now()).unwrap() {
            Decoded::One(FeedEvent::Delta { clock, price, .. }) => {
                assert_eq!(clock, i64::MIN);
                assert_eq!(price, 400_000);
            }
            _ => panic!("expected delta"),
        }
    }

    #[test]
    fn signed_websocket_request_keeps_upgrade_headers() {
        let auth = Auth {
            key_id: "test-key".into(),
            key: PKey::generate_ed25519().unwrap(),
        };
        let request = auth.build_signed_request(WS_URL).unwrap();
        assert!(request.headers().contains_key("sec-websocket-key"));
        assert!(request.headers().contains_key("sec-websocket-version"));
        assert!(request.headers().contains_key("KALSHI-ACCESS-SIGNATURE"));
        assert_eq!(request.uri().path(), WS_PATH);
    }
}
