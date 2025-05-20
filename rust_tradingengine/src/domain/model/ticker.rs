use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use serde_json::Value;

/// Ticker data structure to represent market data across the application
/// This is the single, consolidated Ticker model for the entire application
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub instrument_name: String,
    pub mark_price: f64,
    pub mark_timestamp: f64,
    pub best_bid_price: f64,
    pub best_bid_amount: f64,
    pub best_ask_price: f64,
    pub best_ask_amount: f64,
    pub last_price: f64,
    pub delta: f64,
    pub volume_24h: f64,
    pub value_24h: f64,
    pub low_price_24h: f64,
    pub high_price_24h: f64,
    pub change_24h: f64,
    pub index_price: f64,
    pub forward: f64, 
    pub funding_mark: f64,
    pub funding_rate: f64,
    pub collar_low: f64,
    pub collar_high: f64,
    pub realised_funding_24h: f64,
    pub average_funding_rate_24h: f64,
    pub open_interest: f64,
    pub processing_timestamp: Option<f64>,
}

impl Ticker {
    /// Create a new Ticker with default values
    pub fn new(instrument_name: String) -> Self {
        Self {
            instrument_name,
            mark_price: 0.0,
            mark_timestamp: 0.0,
            best_bid_price: 0.0,
            best_bid_amount: 0.0,
            best_ask_price: 0.0,
            best_ask_amount: 0.0,
            last_price: 0.0,
            delta: 0.0,
            volume_24h: 0.0,
            value_24h: 0.0,
            low_price_24h: 0.0,
            high_price_24h: 0.0,
            change_24h: 0.0,
            index_price: 0.0,
            forward: 0.0,
            funding_mark: 0.0,
            funding_rate: 0.0,
            collar_low: 0.0,
            collar_high: 0.0,
            realised_funding_24h: 0.0,
            average_funding_rate_24h: 0.0,
            open_interest: 0.0,
            processing_timestamp: None,
        }
    }

    /// Create a Ticker from JSON notification data
    pub fn from_json(data: &Value, instrument_name: String) -> Result<Self> {
        // Get current time as processing timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        Ok(Self {
            instrument_name,
            mark_price: data["mark_price"].as_f64().ok_or_else(|| anyhow!("Missing mark_price"))?,
            mark_timestamp: data["mark_timestamp"].as_f64().ok_or_else(|| anyhow!("Missing mark_timestamp"))?,
            best_bid_price: data.get("best_bid_price").and_then(|v| v.as_f64()).unwrap_or(0.0),
            best_bid_amount: data.get("best_bid_amount").and_then(|v| v.as_f64()).unwrap_or(0.0),
            best_ask_price: data.get("best_ask_price").and_then(|v| v.as_f64()).unwrap_or(0.0),
            best_ask_amount: data.get("best_ask_amount").and_then(|v| v.as_f64()).unwrap_or(0.0),
            last_price: data.get("last_price").and_then(|v| v.as_f64()).unwrap_or(0.0),
            delta: data.get("delta").and_then(|v| v.as_f64()).unwrap_or(0.0),
            volume_24h: data.get("volume_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            value_24h: data.get("value_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            low_price_24h: data.get("low_price_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            high_price_24h: data.get("high_price_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            change_24h: data.get("change_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            index_price: data["index"].as_f64().ok_or_else(|| anyhow!("Missing index"))?,
            forward: data.get("forward").and_then(|v| v.as_f64()).unwrap_or(0.0),
            funding_mark: data.get("funding_mark").and_then(|v| v.as_f64()).unwrap_or(0.0),
            funding_rate: data["funding_rate"].as_f64().ok_or_else(|| anyhow!("Missing funding_rate"))?,
            collar_low: data.get("collar_low").and_then(|v| v.as_f64()).unwrap_or(0.0),
            collar_high: data.get("collar_high").and_then(|v| v.as_f64()).unwrap_or(0.0),
            realised_funding_24h: data.get("realised_funding_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            average_funding_rate_24h: data.get("average_funding_rate_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
            open_interest: data.get("open_interest").and_then(|v| v.as_f64()).unwrap_or(0.0),
            processing_timestamp: Some(now),
        })
    }

    /// Get best bid price, returning None if not available
    pub fn best_bid(&self) -> Option<f64> {
        if self.best_bid_price > 0.0 {
            Some(self.best_bid_price)
        } else {
            None
        }
    }

    /// Get best ask price, returning None if not available
    pub fn best_ask(&self) -> Option<f64> {
        if self.best_ask_price > 0.0 {
            Some(self.best_ask_price)
        } else {
            None
        }
    }
}
