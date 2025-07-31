use anyhow::Result;
use apache_avro::types::Value as AvroValue;
use log::debug;

use crate::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use crate::domain::model::ack::Ack;
use crate::domain::model::ticker::Ticker;
use crate::domain::model::trade::Trade;

/// Converter for domain models to Avro format
pub struct AvroConverter;

impl AvroConverter {
    /// Convert OrderSide enum to Avro value
    pub fn order_side_to_avro(side: &OrderSide) -> AvroValue {
        match side {
            OrderSide::Buy => AvroValue::Enum(0, "buy".to_string()),
            OrderSide::Sell => AvroValue::Enum(1, "sell".to_string()),
        }
    }

    /// Convert OrderType enum to Avro value
    pub fn order_type_to_avro(order_type: &OrderType) -> AvroValue {
        match order_type {
            OrderType::Limit => AvroValue::Enum(0, "limit".to_string()),
            OrderType::Market => AvroValue::Enum(1, "market".to_string()),
        }
    }

    /// Convert TimeInForce enum to Avro value
    pub fn time_in_force_to_avro(tif: &TimeInForce) -> AvroValue {
        match tif {
            TimeInForce::GTC => AvroValue::Enum(0, "good_till_cancelled".to_string()),
            TimeInForce::IOC => AvroValue::Enum(1, "immediate_or_cancel".to_string()),
        }
    }

    /// Convert OrderStatus enum to Avro value
    pub fn order_status_to_avro(status: &OrderStatus) -> AvroValue {
        match status {
            OrderStatus::Open => AvroValue::Enum(0, "open".to_string()),
            OrderStatus::PartiallyFilled => AvroValue::Enum(1, "partially_filled".to_string()),
            OrderStatus::Cancelled => AvroValue::Enum(2, "cancelled".to_string()),
            OrderStatus::CancelledPartiallyFilled => AvroValue::Enum(3, "cancelled_partially_filled".to_string()),
            OrderStatus::Filled => AvroValue::Enum(4, "filled".to_string()),
        }
    }

    /// Convert an Ack domain model to Avro Value
    pub fn ack_to_avro_value(ack: &Ack) -> Vec<(String, AvroValue)> {
        debug!("Converting Ack to Avro: {:?}", ack);
        let mut fields = Vec::with_capacity(17);  // Pre-allocate for all fields
        
        // Add all fields in the correct order according to the schema
        fields.push(("order_id".to_string(), AvroValue::String(ack.order_id.clone())));
        
        // Handle Option<u64> for client_order_id
        let client_order_id_value = match ack.client_order_id {
            Some(id) => AvroValue::Union(1, Box::new(AvroValue::Long(id as i64))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("client_order_id".to_string(), client_order_id_value));
        
        fields.push(("instrument_name".to_string(), AvroValue::String(ack.instrument_name.clone())));
        fields.push(("direction".to_string(), Self::order_side_to_avro(&ack.direction)));
        
        // Handle Option<f64> for price
        let price_value = match ack.price {
            Some(p) => AvroValue::Union(1, Box::new(AvroValue::Double(p))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("price".to_string(), price_value));
        
        fields.push(("amount".to_string(), AvroValue::Double(ack.amount)));
        fields.push(("filled_amount".to_string(), AvroValue::Double(ack.filled_amount)));
        fields.push(("remaining_amount".to_string(), AvroValue::Double(ack.remaining_amount)));
        fields.push(("status".to_string(), Self::order_status_to_avro(&ack.status)));
        fields.push(("order_type".to_string(), Self::order_type_to_avro(&ack.order_type)));
        fields.push(("time_in_force".to_string(), Self::time_in_force_to_avro(&ack.time_in_force)));
        fields.push(("change_reason".to_string(), AvroValue::String(ack.change_reason.clone())));
        
        // Handle Option<String> for delete_reason
        let delete_reason_value = match &ack.delete_reason {
            Some(reason) => AvroValue::Union(1, Box::new(AvroValue::String(reason.clone()))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("delete_reason".to_string(), delete_reason_value));
        
        // Handle Option<String> for insert_reason
        let insert_reason_value = match &ack.insert_reason {
            Some(reason) => AvroValue::Union(1, Box::new(AvroValue::String(reason.clone()))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("insert_reason".to_string(), insert_reason_value));
        
        fields.push(("create_time".to_string(), AvroValue::Double(ack.create_time)));
        fields.push(("persistent".to_string(), AvroValue::Boolean(ack.persistent)));
        
        // Handle processing_timestamp field (optional)
        let processing_timestamp_value = match ack.processing_timestamp {
            Some(ts) => AvroValue::Union(1, Box::new(AvroValue::Double(ts))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("processing_timestamp".to_string(), processing_timestamp_value));
        
        fields
    }

    /// Convert a Trade domain model to Avro Value
    pub fn trade_to_avro_value(trade: &Trade) -> Result<Vec<(String, AvroValue)>> {
        let mut fields = Vec::with_capacity(9);  // Pre-allocate for all fields including processing_timestamp
        
        // Add fields in the same order as the schema
        fields.push(("trade_id".to_string(), AvroValue::String(trade.trade_id.clone())));
        fields.push(("order_id".to_string(), AvroValue::String(trade.order_id.clone())));
        
        // Handle Option<u64> for client_order_id (avsc expects long)
        let client_order_id_value = match trade.client_order_id {
            Some(id) => AvroValue::Union(1, Box::new(AvroValue::Long(id as i64))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("client_order_id".to_string(), client_order_id_value));
        
        fields.push(("instrument_name".to_string(), AvroValue::String(trade.instrument_name.clone())));
        fields.push(("price".to_string(), AvroValue::Double(trade.price)));
        fields.push(("amount".to_string(), AvroValue::Double(trade.amount)));
        fields.push(("maker_taker".to_string(), AvroValue::String(trade.maker_taker.clone())));
        fields.push(("time".to_string(), AvroValue::Double(trade.time)));
        
        // Handle processing_timestamp field (optional)
        let processing_timestamp_value = match trade.processing_timestamp {
            Some(ts) => AvroValue::Union(1, Box::new(AvroValue::Double(ts))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("processing_timestamp".to_string(), processing_timestamp_value));
        
        Ok(fields)
    }

    /// Convert a Ticker domain model to Avro Value
    /// Instead of returning an AvroValue::Record, it returns the field vector directly
    /// to be used with the Confluent encoder
    pub fn ticker_to_avro_value(ticker: &Ticker) -> Result<Vec<(String, AvroValue)>> {
        let mut fields = Vec::with_capacity(24); // Pre-allocate for all fields including processing_timestamp
        
        // Add all fields in the correct order according to the schema
        fields.push(("instrument_name".to_string(), AvroValue::String(ticker.instrument_name.clone())));
        fields.push(("mark_price".to_string(), AvroValue::Double(ticker.mark_price)));
        fields.push(("mark_timestamp".to_string(), AvroValue::Double(ticker.mark_timestamp)));
        fields.push(("best_bid_price".to_string(), AvroValue::Double(ticker.best_bid_price)));
        fields.push(("best_bid_amount".to_string(), AvroValue::Double(ticker.best_bid_amount)));
        fields.push(("best_ask_price".to_string(), AvroValue::Double(ticker.best_ask_price)));
        fields.push(("best_ask_amount".to_string(), AvroValue::Double(ticker.best_ask_amount)));
        fields.push(("last_price".to_string(), AvroValue::Double(ticker.last_price)));
        fields.push(("delta".to_string(), AvroValue::Double(ticker.delta)));
        fields.push(("volume_24h".to_string(), AvroValue::Double(ticker.volume_24h)));
        fields.push(("value_24h".to_string(), AvroValue::Double(ticker.value_24h)));
        fields.push(("low_price_24h".to_string(), AvroValue::Double(ticker.low_price_24h)));
        fields.push(("high_price_24h".to_string(), AvroValue::Double(ticker.high_price_24h)));
        fields.push(("change_24h".to_string(), AvroValue::Double(ticker.change_24h)));
        fields.push(("index_price".to_string(), AvroValue::Double(ticker.index_price)));
        fields.push(("forward".to_string(), AvroValue::Double(ticker.forward)));
        fields.push(("funding_mark".to_string(), AvroValue::Double(ticker.funding_mark)));
        fields.push(("funding_rate".to_string(), AvroValue::Double(ticker.funding_rate)));
        fields.push(("collar_low".to_string(), AvroValue::Double(ticker.collar_low)));
        fields.push(("collar_high".to_string(), AvroValue::Double(ticker.collar_high)));
        fields.push(("realised_funding_24h".to_string(), AvroValue::Double(ticker.realised_funding_24h)));
        fields.push(("average_funding_rate_24h".to_string(), AvroValue::Double(ticker.average_funding_rate_24h)));
        fields.push(("open_interest".to_string(), AvroValue::Double(ticker.open_interest)));
        
        // Handle processing_timestamp field (optional)
        let processing_timestamp_value = match ticker.processing_timestamp {
            Some(ts) => AvroValue::Union(1, Box::new(AvroValue::Double(ts))),
            None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        };
        fields.push(("processing_timestamp".to_string(), processing_timestamp_value));
        
        // Return the fields directly, not wrapped in an AvroValue::Record
        Ok(fields)
    }
}