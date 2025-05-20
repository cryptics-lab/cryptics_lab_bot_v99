// Domain model for orders
use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::domain::enums::OrderStatus;
use crate::domain::enums::OrderSide;

/// Represents an order with ID, price, amount, and status
#[derive(Clone, Debug)]
pub struct Order {
    pub id: u64,
    pub price: f64,
    pub amount: f64,
    pub filled_amount: Option<f64>,
    pub status: Option<OrderStatus>,
}

impl Order {
    pub fn new(id: u64, price: f64, amount: f64, status: Option<OrderStatus>) -> Self {
        Self { id, price, amount, filled_amount: None, status }
    }

    pub fn is_open(&self) -> bool {
        matches!(self.status, Some(OrderStatus::Open) | Some(OrderStatus::PartiallyFilled))
    }
}

/// Helper function to convert order data from JSON
pub fn order_from_data(data: &Value) -> Result<Order> {
    let client_order_id = data["client_order_id"].as_u64()
        .ok_or_else(|| anyhow!("Missing client_order_id"))?;
    
    let price = data["price"].as_f64()
        .ok_or_else(|| anyhow!("Missing price"))?;
    
    let amount = data["remaining_amount"].as_f64()
        .ok_or_else(|| anyhow!("Missing remaining_amount"))?;
    
    let filled_amount = data["filled_amount"].as_f64();
    
    let status_str = data["status"].as_str()
        .ok_or_else(|| anyhow!("Missing status"))?;
    
    let status = match status_str {
        "open" => Some(OrderStatus::Open),
        "partially_filled" => Some(OrderStatus::PartiallyFilled),
        "cancelled" => Some(OrderStatus::Cancelled),
        "cancelled_partially_filled" => Some(OrderStatus::CancelledPartiallyFilled),
        "filled" => Some(OrderStatus::Filled),
        _ => None,
    };

    let mut order = Order::new(client_order_id, price, amount, status);
    order.filled_amount = filled_amount;
    Ok(order)
}

/// Helper function to convert OrderSide to string
pub fn side_to_string(side: &OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}


#[derive(Debug, Clone)]
pub struct OrderPlaced {
    pub client_order_id: u64,
    pub instrument_name: String,
    pub direction: OrderSide,
    pub price: Option<f64>,
    pub amount: f64,
    pub order_type: String,
    pub time_in_force: String,
    pub label: Option<String>,
    pub reduce_only: Option<bool>,
    pub persistent: bool,
    pub insert_reason: String,
    pub timestamp: String, // ISO 8601
}