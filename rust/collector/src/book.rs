use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookState {
    AwaitingSnapshot,
    Synchronized,
    Desynchronized { reason: String },
}

/// YES and NO are both bids. Asks are complements of the opposite bid.
#[derive(Debug, Clone)]
pub struct OrderBook {
    state: BookState,
    yes: BTreeMap<Decimal, Decimal>,
    no: BTreeMap<Decimal, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quote {
    pub yes_bid: Option<Decimal>,
    pub no_bid: Option<Decimal>,
    pub yes_ask: Option<Decimal>,
    pub no_ask: Option<Decimal>,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self { state: BookState::AwaitingSnapshot, yes: BTreeMap::new(), no: BTreeMap::new() }
    }
}

fn number(value: &Value) -> Result<Decimal> {
    let text = value.as_str().map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    Decimal::from_str(&text).with_context(|| format!("invalid fixed-point value {text}"))
}

fn side(book: &Value, name: &str) -> Result<(Vec<(Decimal, Decimal)>, bool)> {
    let options = [
        format!("{name}_dollars_fp"), format!("{name}_dollars"), name.to_owned()
    ];
    for (i, field) in options.iter().enumerate() {
        if let Some(value) = book.get(field) {
            let rows = value.as_array().ok_or_else(|| anyhow!("{field} not an array"))?;
            let mut parsed = Vec::with_capacity(rows.len());
            for row in rows {
                let pair = row.as_array().ok_or_else(|| anyhow!("non-array book level"))?;
                if pair.len() < 2 { bail!("short book level") }
                let mut price = number(&pair[0])?;
                let quantity = number(&pair[1])?;
                let legacy_cents = i == 2;
                if legacy_cents { price /= Decimal::from(100); }
                if price <= Decimal::ZERO || price >= Decimal::ONE || quantity < Decimal::ZERO {
                    bail!("book level out of bounds")
                }
                parsed.push((price, quantity));
            }
            return Ok((parsed, true));
        }
    }
    Ok((Vec::new(), false))
}

impl OrderBook {
    pub fn state(&self) -> &BookState { &self.state }
    pub fn synchronized(&self) -> bool { self.state == BookState::Synchronized }

    pub fn invalidate(&mut self, reason: impl Into<String>) {
        self.state = BookState::Desynchronized {reason: reason.into()};
        self.yes.clear();
        self.no.clear();
    }

    pub fn snapshot(&mut self, raw: &Value) -> Result<()> {
        match self.try_snapshot(raw) {
            Ok((yes, no)) => {
                self.yes = yes;
                self.no = no;
                self.state = BookState::Synchronized;
                Ok(())
            }
            Err(error) => {
                self.invalidate(format!("invalid snapshot: {error}"));
                Err(error)
            }
        }
    }

    fn try_snapshot(&self, raw: &Value) -> Result<(BTreeMap<Decimal, Decimal>, BTreeMap<Decimal, Decimal>)> {
        let book = raw.get("orderbook_fp")
            .or_else(|| raw.get("orderbook"))
            .unwrap_or(raw);
        if !book.is_object() { bail!("snapshot is not an object") }
        let (yes_rows, has_yes) = side(book, "yes")?;
        let (no_rows, has_no) = side(book, "no")?;
        if !(has_yes || has_no) { bail!("snapshot has no recognized bid side") }
        let mut yes = BTreeMap::new();
        let mut no = BTreeMap::new();
        for (price, quantity) in yes_rows {
            if quantity > Decimal::ZERO { yes.insert(price, quantity); }
        }
        for (price, quantity) in no_rows {
            if quantity > Decimal::ZERO { no.insert(price, quantity); }
        }
        Ok((yes, no))
    }

    pub fn delta(&mut self, msg: &Value) -> Result<()> {
        if !self.synchronized() { bail!("delta refused without synchronized snapshot") }
        let side_name = msg.get("side").and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing delta side"))?;
        let uses_dollars = msg.get("price_dollars").is_some();
        let raw_price = msg.get("price_dollars").or_else(|| msg.get("price"))
            .ok_or_else(|| anyhow!("missing delta price"))?;
        let mut price = number(raw_price)?;
        if !uses_dollars { price /= Decimal::from(100); }
        let change = number(msg.get("delta_fp").or_else(|| msg.get("delta"))
            .ok_or_else(|| anyhow!("missing delta size"))?)?;
        if price <= Decimal::ZERO || price >= Decimal::ONE { bail!("delta price out of range") }
        let levels = match side_name {
            "yes" => &mut self.yes, "no" => &mut self.no, _ => bail!("unknown delta side"),
        };
        let updated = levels.get(&price).copied().unwrap_or(Decimal::ZERO) + change;
        if updated < Decimal::ZERO {
            self.invalidate("negative orderbook quantity");
            bail!("negative quantity invalidates book")
        }
        if updated == Decimal::ZERO { levels.remove(&price); } else { levels.insert(price, updated); }
        Ok(())
    }

    pub fn quote(&self) -> Option<Quote> {
        if !self.synchronized() { return None }
        let yes_bid = self.yes.last_key_value().map(|(p, _)| *p);
        let no_bid = self.no.last_key_value().map(|(p, _)| *p);
        Some(Quote {
            yes_bid, no_bid,
            yes_ask: no_bid.map(|p| Decimal::ONE - p),
            no_ask: yes_bid.map(|p| Decimal::ONE - p),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_snapshot_is_valid_but_malformed_is_not() {
        let mut b = OrderBook::default();
        assert!(b.snapshot(&json!({"yes_dollars_fp":[],"no_dollars_fp":[]})).is_ok());
        assert_eq!(b.quote().unwrap().yes_bid, None);
        assert!(b.snapshot(&json!({})).is_err());
        assert!(!b.synchronized());
        assert!(b.delta(&json!({"side":"yes","price_dollars":"0.4","delta_fp":"1"})).is_err());
    }
    #[test]
    fn legacy_cents_and_dollars_match() {
        let mut a = OrderBook::default();
        let mut b = OrderBook::default();
        a.snapshot(&json!({"yes":[[55,2]],"no":[[35,3]]})).unwrap();
        b.snapshot(&json!({"yes_dollars_fp":[["0.55","2"]],"no_dollars_fp":[["0.35","3"]]})).unwrap();
        assert_eq!(a.quote(), b.quote());
        a.delta(&json!({"side":"yes","price":55,"delta":-2})).unwrap();
        assert_eq!(a.quote().unwrap().yes_bid, None);
    }
    #[test]
    fn missing_delta_causes_error_and_negative_levels_desynchronize() {
        let mut b = OrderBook::default();
        b.snapshot(&json!({"yes_dollars_fp":[["0.4","1"]]})).unwrap();
        assert!(b.delta(&json!({"side":"yes","price_dollars":"0.4","delta_fp":"-2"})).is_err());
        assert!(!b.synchronized());
    }
}
