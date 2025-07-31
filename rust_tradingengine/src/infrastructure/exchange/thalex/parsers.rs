use anyhow::{anyhow, Result};
use serde_json::Value;
use crate::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use crate::domain::model::ack::Ack;
use crate::domain::model::ticker::Ticker;
use crate::domain::model::trade::Trade;

/// Parses JSON data for Kafka
pub struct ThaleParser;

impl ThaleParser {
    /// Parses JSON for an Order Acknowledgement
    pub fn parse_ack_json(data: &Value) -> Result<Ack> {
        // Get current time for processing_timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        // Create the Ack struct with proper enum types from the JSON
        let ack = Ack {
            order_id: data["order_id"].as_str().unwrap_or_default().to_string(),
            client_order_id: data["client_order_id"].as_u64(),
            instrument_name: data["instrument_name"].as_str().unwrap_or_default().to_string(),
            direction: match data["direction"].as_str().unwrap_or_default() {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                _ => OrderSide::Buy, // Default
            },
            price: if data["price"].is_null() { 
                None 
            } else { 
                Some(data["price"].as_f64().unwrap_or_default()) 
            },
            amount: data["amount"].as_f64().unwrap_or_default(),
            filled_amount: data["filled_amount"].as_f64().unwrap_or_default(),
            remaining_amount: data["remaining_amount"].as_f64().unwrap_or_default(),
            status: match data["status"].as_str().unwrap_or_default() {
                "open" => OrderStatus::Open,
                "partially_filled" => OrderStatus::PartiallyFilled,
                "cancelled" => OrderStatus::Cancelled,
                "cancelled_partially_filled" => OrderStatus::CancelledPartiallyFilled,
                "filled" => OrderStatus::Filled,
                _ => OrderStatus::Open, // Default
            },
            order_type: match data["order_type"].as_str().unwrap_or_default() {
                "limit" => OrderType::Limit,
                "market" => OrderType::Market,
                _ => OrderType::Limit, // Default
            },
            time_in_force: match data["time_in_force"].as_str().unwrap_or_default() {
                "good_till_cancelled" => TimeInForce::GTC,
                "immediate_or_cancel" => TimeInForce::IOC,
                _ => TimeInForce::GTC, // Default
            },
            change_reason: data["change_reason"].as_str().unwrap_or_default().to_string(),
            delete_reason: if data["delete_reason"].is_null() {
                None
            } else {
                Some(data["delete_reason"].as_str().unwrap_or_default().to_string())
            },
            insert_reason: if data["insert_reason"].is_null() {
                None
            } else {
                Some(data["insert_reason"].as_str().unwrap_or_default().to_string())
            },
            create_time: data["create_time"].as_f64().unwrap_or_default(),
            persistent: data["persistent"].as_bool().unwrap_or_default(),
            processing_timestamp: Some(now),
        };
        
        Ok(ack)
    }
    
    /// Parses the ticker data
    pub fn parse_ticker_json(ticker: &Ticker) -> Result<Value> {
        let ticker_json = serde_json::json!({
            "instrument_name": ticker.instrument_name,
            "timestamp": ticker.mark_timestamp,
            "last_price": ticker.last_price,
            "mark_price": ticker.mark_price,
            "index_price": ticker.index_price,
            "funding_rate": ticker.funding_rate,
            "open_interest": ticker.open_interest,
            "processing_timestamp": ticker.processing_timestamp
        });
        
        Ok(ticker_json)
    }
    
    /// Parses a trade message from JSON
    pub fn parse_trade_json(data: &Value) -> Result<Trade> {
        // Get current time for processing_timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        // Extract fields from the trade notification JSON
        let trade_id = match data.get("trade_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => {
                // Generate a trade_id if not provided
                format!("trade-{}", uuid::Uuid::new_v4())
            }
        };
        
        let order_id = data.get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        let client_order_id = data.get("client_order_id")
            .and_then(|v| v.as_u64());
        
        let instrument_name = data.get("instrument_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing instrument_name"))?
            .to_string();
        
        let price = data.get("price")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| anyhow!("Missing price"))?;
        
        let amount = data.get("amount")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| anyhow!("Missing amount"))?;
        
        let maker_taker = data.get("maker_taker")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        let time = data.get("timestamp")
            .or_else(|| data.get("time"))
            .and_then(|v| v.as_f64())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
            });
        
        // Create the Trade struct
        let trade = Trade {
            trade_id,
            order_id,
            client_order_id,
            instrument_name,
            price,
            amount,
            maker_taker,
            time,
            processing_timestamp: Some(now),
        };
        
        Ok(trade)
    }
    
    /// Extracts trades (fills) from order notification JSON
    pub fn extract_trades_from_order(order_data: &Value) -> Result<Vec<Trade>> {
        let mut trades = Vec::new();
        
        // Get current time for processing_timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
            
        // Check if the order has fills
        if let Some(fills) = order_data.get("fills").and_then(|v| v.as_array()) {
            // Get required data from the order
            let order_id = order_data.get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
                
            let client_order_id = order_data.get("client_order_id")
                .and_then(|v| v.as_u64());
                
            let instrument_name = order_data.get("instrument_name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing instrument_name"))?
                .to_string();
            
            // Process each fill
            for fill in fills {
                // Extract trade information from the fill
                let trade_id = match fill.get("trade_id").and_then(|v| v.as_str()) {
                    Some(id) => id.to_string(),
                    None => {
                        // Generate a trade_id if not provided
                        format!("trade-{}", uuid::Uuid::new_v4())
                    }
                };
                
                let price = fill.get("price")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| anyhow!("Missing price in fill"))?;
                
                let amount = fill.get("amount")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| anyhow!("Missing amount in fill"))?;
                
                let maker_taker = fill.get("maker_taker")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                
                let time = fill.get("time")
                    .and_then(|v| v.as_f64())
                    .unwrap_or_else(|| {
                        order_data.get("create_time")
                            .and_then(|v| v.as_f64())
                            .unwrap_or_else(|| {
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs_f64()
                            })
                    });
                
                // Create a Trade from the fill data
                let trade = Trade {
                    trade_id,
                    order_id: order_id.clone(),
                    client_order_id,
                    instrument_name: instrument_name.clone(),
                    price,
                    amount,
                    maker_taker,
                    time,
                    processing_timestamp: Some(now),
                };
                
                trades.push(trade);
            }
        }
        
        Ok(trades)
    }
    
    /// Creates JSON payload from a Trade model
    pub fn trade_to_json(trade: &Trade) -> Value {
        serde_json::json!({
            "trade_id": trade.trade_id,
            "order_id": trade.order_id,
            "client_order_id": trade.client_order_id,
            "instrument_name": trade.instrument_name,
            "price": trade.price,
            "amount": trade.amount,
            "maker_taker": trade.maker_taker,
            "time": trade.time,
            "processing_timestamp": trade.processing_timestamp
        })
    }
}