//! A YES-denominated level-2 book. Exchange adapters normalize prices before
//! modifying these levels.

use std::collections::BTreeMap;

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

const SCALE: i64 = 1_000_000;

/// Parses an exchange decimal string into the template's six-decimal fixed point.
/// Rejecting excess precision is preferable to silently changing a price level.
pub fn fixed(value: &str) -> anyhow::Result<i64> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal: {value}"
    );
    ensure!(
        fraction.len() <= 6 && fraction.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal precision: {value}"
    );
    let whole: i64 = whole
        .parse()
        .with_context(|| format!("invalid decimal: {value}"))?;
    let fraction_len = fraction.len();
    let fraction: i64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse()?
    };
    let multiplier = 10_i64.pow((6 - fraction_len) as u32);
    let result = whole
        .checked_mul(SCALE)
        .and_then(|n| n.checked_add(fraction * multiplier))
        .context("fixed point overflow")?;
    Ok(if negative {
        result.checked_neg().context("fixed point overflow")?
    } else {
        result
    })
}

/// Sparse YES-side price levels; quantities are absolute after each update.
#[derive(Default)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
}

impl Book {
    /// Replaces a book with the exchange's full level snapshot.
    pub fn from_snapshot(
        bids: &[[String; 2]],
        asks: &[[String; 2]],
        exchange_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Self> {
        let mut book = Self {
            exchange_timestamp: Some(exchange_timestamp),
            ..Self::default()
        };
        for (levels, side) in [(bids, Side::Buy), (asks, Side::Sell)] {
            for [price, quantity] in levels {
                let price = fixed(price)?;
                let quantity = fixed(quantity)?;
                ensure!((0..=SCALE).contains(&price), "price outside [0, 1]");
                ensure!(quantity > 0, "snapshot quantity must be positive");
                ensure!(
                    book.levels_mut(side).insert(price, quantity).is_none(),
                    "duplicate snapshot level"
                );
            }
        }
        Ok(book)
    }

    /// Returns the bid or ask ladder for an update.
    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Applies a quantity change and emits the resulting absolute level.
    pub fn apply_delta(
        &mut self,
        side: Side,
        price: i64,
        delta: i64,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<PriceLevelUpdateDomain> {
        ensure!((0..=SCALE).contains(&price), "price outside [0, 1]");
        let levels = self.levels_mut(side);
        let quantity = levels
            .get(&price)
            .copied()
            .unwrap_or(0)
            .checked_add(delta)
            .context("quantity overflow")?;
        ensure!(
            quantity >= 0,
            "negative quantity at {symbol} {side} {price}"
        );
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        self.exchange_timestamp = Some(exchange_timestamp);
        Ok(PriceLevelUpdateDomain {
            backtest_timestamp: received_at,
            exchange_timestamp,
            price_level: price,
            quantity,
            side,
            symbol: symbol.to_owned(),
        })
    }

    /// Sets an absolute level, as used by Polymarket updates.
    pub fn set_absolute(
        &mut self,
        side: Side,
        price: i64,
        quantity: i64,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<PriceLevelUpdateDomain> {
        ensure!((0..=SCALE).contains(&price), "price outside [0, 1]");
        ensure!(quantity >= 0, "negative absolute quantity");
        let levels = self.levels_mut(side);
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        self.exchange_timestamp = Some(exchange_timestamp);
        Ok(PriceLevelUpdateDomain {
            backtest_timestamp: received_at,
            exchange_timestamp,
            price_level: price,
            quantity,
            side,
            symbol: symbol.to_owned(),
        })
    }

    /// Builds an SBE snapshot with sorted levels and the latest exchange time.
    pub fn snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp: self.exchange_timestamp.unwrap_or(received_at),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_is_exact_and_checked() {
        assert_eq!(fixed("0.1234").unwrap(), 123_400);
        assert_eq!(fixed("12.000001").unwrap(), 12_000_001);
        assert_eq!(fixed("-1.25").unwrap(), -1_250_000);
        assert!(fixed("0.1234567").is_err());
        assert!(fixed("1e-2").is_err());
    }

    #[test]
    fn no_bids_become_asks_and_delta_is_absolute() {
        let now = Utc::now();
        let mut book = Book::from_snapshot(
            &[["0.4000".into(), "2.00".into()]],
            &[["0.6000".into(), "3.00".into()]],
            now,
        )
        .unwrap();
        let update = book
            .apply_delta(Side::Sell, 600_000, -1_000_000, "T", now, now)
            .unwrap();
        assert_eq!(update.quantity, 2_000_000);
        assert_eq!(book.snapshot("T", now).asks[0].price_level, 600_000);
        assert!(
            book.apply_delta(Side::Sell, 600_000, -3_000_000, "T", now, now)
                .is_err()
        );
        assert_eq!(book.snapshot("T", now).asks[0].quantity, 2_000_000);
    }
}
