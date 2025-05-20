use std::fmt;
use serde::{Serialize, Deserialize};

/// Represents a trade (fill) execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trade {
    /// Unique trade identifier
    pub trade_id: String,
    
    /// Exchange order ID
    pub order_id: String,
    
    /// Client-assigned order ID
    pub client_order_id: Option<u64>,
    
    /// Name of the instrument
    pub instrument_name: String,
    
    /// Price at which the trade was executed
    pub price: f64,
    
    /// Amount of the instrument that was traded
    pub amount: f64,
    
    /// Whether the trade was as a maker or taker
    pub maker_taker: String,
    
    /// Timestamp of the trade (seconds since epoch with decimal precision)
    pub time: f64,
    
    /// Timestamp when record was processed by Rust system (seconds since epoch)
    pub processing_timestamp: Option<f64>,
}

impl fmt::Display for Trade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Trade {{ id: {}, order_id: {}, price: {}, amount: {} }}", 
            self.trade_id, self.order_id, self.price, self.amount)
    }
}

impl Trade {
    /// Convert the timestamp to a human-readable date string
    pub fn format_time(&self) -> String {
        let seconds = self.time.trunc() as i64;
        let nanos = ((self.time - seconds as f64) * 1_000_000_000.0) as u32;
        
        match chrono::DateTime::from_timestamp(seconds, nanos) {
            Some(dt) => dt.naive_local().format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            None => format!("Invalid timestamp: {}", self.time),
        }
    }
    
    /// Create a new Trade with current processing_timestamp
    pub fn with_processing_timestamp(mut self) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        self.processing_timestamp = Some(now);
        self
    }
}
