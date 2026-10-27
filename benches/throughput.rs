#![allow(dead_code, unused_imports)]
//! Synthetic CPU costs for parsing, one level update, and SBE encoding.

#[path = "../src/book.rs"]
mod book;
#[path = "../src/exchanges/mod.rs"]
mod exchanges;
mod diagnostics {
    pub fn emit(_: String) {}
}
use exchanges::kalshi;

use std::{hint::black_box, time::Instant};

use chrono::Utc;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use serde_json::Value;

const WARMUP: usize = 10_000;

/// Timed result for one isolated processing stage.
struct Measurement {
    stage: &'static str,
    work: &'static str,
    messages_per_second: f64,
    ns_per_message: f64,
}

/// Warms up a stage, then reports its average throughput.
fn measure(
    mut run: impl FnMut(),
    iterations: usize,
    stage: &'static str,
    work: &'static str,
) -> Measurement {
    for _ in 0..WARMUP {
        run();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        run();
    }
    let elapsed = start.elapsed().as_secs_f64();
    Measurement {
        stage,
        work,
        messages_per_second: iterations as f64 / elapsed,
        ns_per_message: elapsed * 1e9 / iterations as f64,
    }
}

/// Runs the same fixed Kalshi delta through each benchmark stage.
fn main() {
    let iterations = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000_000);
    let raw = r#"{"type":"orderbook_delta","sid":1,"seq":3,"sending_ts_ms":1700000000100,"msg":{"market_ticker":"BENCH","price_dollars":"0.4000","delta_fp":"1.00","side":"yes","ts_ms":1700000000000}}"#;
    let wall = Utc::now();
    let arrival = Instant::now();
    let decode = measure(
        || {
            let frame: Value = serde_json::from_str(black_box(raw)).unwrap();
            black_box(kalshi::parse_book_event(&frame, 0, arrival, wall).unwrap());
        },
        iterations,
        "Parse + normalize",
        "JSON bytes -> Kalshi delta",
    );

    let mut book =
        book::Book::from_snapshot(&[["0.4000".into(), "2.00".into()]], &[], wall).unwrap();
    let book_update = measure(
        || {
            black_box(
                book.apply_delta(Side::Buy, 400_000, 1_000_000, "BENCH", wall, wall)
                    .unwrap(),
            );
        },
        iterations,
        "Book update",
        "Apply delta to one hot level",
    );

    let update = PriceLevelUpdateDomain {
        backtest_timestamp: wall,
        exchange_timestamp: wall,
        price_level: 400_000,
        quantity: 2_000_000,
        side: Side::Buy,
        symbol: "BENCH".into(),
    };
    let mut bytes = vec![0; update.encoded_length_with_header().unwrap()];
    let encode = measure(
        || {
            black_box(update.encode(black_box(&mut bytes)).unwrap());
        },
        iterations,
        "SBE encode",
        "Update struct -> binary SBE",
    );

    let mut book =
        book::Book::from_snapshot(&[["0.4000".into(), "2.00".into()]], &[], wall).unwrap();
    let combined = measure(
        || {
            let frame: Value = serde_json::from_str(black_box(raw)).unwrap();
            if let exchanges::Decoded::One(exchanges::FeedEvent::Delta {
                side,
                price,
                change: exchanges::LevelChange::Delta(delta),
                ..
            }) = kalshi::parse_book_event(&frame, 0, arrival, wall).unwrap()
            {
                let update = book
                    .apply_delta(side, price, delta, "BENCH", wall, wall)
                    .unwrap();
                bytes.resize(update.encoded_length_with_header().unwrap(), 0);
                black_box(update.encode(&mut bytes).unwrap());
            }
        },
        iterations,
        "Combined CPU path",
        "Parse + book + SBE encode",
    );

    println!("Kalshi order book delta CPU benchmark");
    println!(
        "Input: one synthetic production-shaped delta, repeated {iterations} times after {WARMUP} warmup iterations per stage."
    );
    println!("Higher M updates/s and lower ns/update are better. Each stage runs on one thread.");
    println!();
    println!(
        "{:<21} {:<30} {:>13} {:>13}",
        "Stage", "Work", "M updates/s", "ns/update"
    );
    for result in [&decode, &book_update, &encode, &combined] {
        println!(
            "{:<21} {:<30} {:>13.2} {:>13.0}",
            result.stage,
            result.work,
            result.messages_per_second / 1_000_000.0,
            result.ns_per_message
        );
    }
    println!();
    println!(
        "Combined: {:.2} million updates/s, or {:.0} ns per update on this synthetic input.",
        combined.messages_per_second / 1_000_000.0,
        combined.ns_per_message
    );
    println!("Excludes network, TLS, scheduling, channels, snapshots, and writing output.");
}
