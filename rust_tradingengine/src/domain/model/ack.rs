use serde::{Serialize, Deserialize};
use crate::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};

/// Represents an order acknowledgment from the exchange
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ack {
    /// Exchange order ID
    pub order_id: String,
    
    /// Client-assigned order ID
    pub client_order_id: Option<u64>,
    
    /// Name of the instrument
    pub instrument_name: String,
    
    /// Order direction (buy/sell)
    pub direction: OrderSide,
    
    /// Order price (if limit order)
    pub price: Option<f64>,
    
    /// Total order amount
    pub amount: f64,
    
    /// Amount that has been filled
    pub filled_amount: f64,
    
    /// Amount still in the order book
    pub remaining_amount: f64,
    
    /// Current order status
    pub status: OrderStatus,
    
    /// Order type (limit, market)
    pub order_type: OrderType,
    
    /// Time in force setting
    pub time_in_force: TimeInForce,
    
    /// Reason for order status change
    pub change_reason: String,
    
    /// Reason for order deletion (if applicable)
    pub delete_reason: Option<String>,
    
    /// Reason for order insertion (if applicable)
    pub insert_reason: Option<String>,
    
    /// Order creation timestamp (seconds since epoch)
    pub create_time: f64,
    
    /// Whether order is persistent
    pub persistent: bool,
    
    /// Timestamp when record was processed by Rust system (seconds since epoch)
    pub processing_timestamp: Option<f64>,
}

impl Ack {
    /// Add processing timestamp to the Ack
    pub fn with_processing_timestamp(mut self) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        self.processing_timestamp = Some(now);
        self
    }
}
