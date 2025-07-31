use crate::domain::enums::*;
use serde::Deserialize;


// OrderRequest is a send-side intent
// Therefore separate from Order
#[derive(Clone, Debug)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub client_order_id: Option<u64>,
    pub time_in_force: Option<TimeInForce>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Instrument {
    pub instrument_name: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub underlying: String,
    pub tick_size: f64,
}
