use serde::Serialize;
use crate::domain::model::order::{OrderAck, OrderFill};

/// Represents different types of order events
#[derive(Debug, Clone, Serialize)]
pub enum OrderEvent {
    OrderAck(OrderAck),
    OrderFill(OrderFill),
    CancelOrder(CancelOrderEvent),
}

/// Represents a cancel order event
#[derive(Debug, Clone, Serialize)]
pub struct CancelOrderEvent {
    pub order_id: String,
    pub client_order_id: Option<u64>,
    pub instrument_name: String,
    pub reason: String,
    pub timestamp: String,
}
