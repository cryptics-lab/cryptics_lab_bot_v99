use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::domain::enums::*;
use crate::domain::model::exchange::*;
use crate::domain::model::order::{Order, order_from_data, side_to_string};
use crate::domain::model::quote::SideQuote;
use crate::infrastructure::exchange::thalex::client::ThalexClient;
use crate::infrastructure::kafka::producer::KafkaProducer;

use super::config;
use super::market_data::MarketDataManager;

/// Manages order creation, modification, and cancellation
pub struct OrderManager {
    /// Client connection
    pub client: Arc<Mutex<ThalexClient>>,
    
    /// Market data manager reference
    pub market_data: Arc<MarketDataManager>,
    
    /// Orders organized by side [bids, asks]
    pub orders: RwLock<Vec<Vec<Order>>>,
    
    /// Client order ID counter
    pub client_order_id: RwLock<u64>,
    
    /// Portfolio positions
    pub portfolio: RwLock<HashMap<String, f64>>,
    
    /// Kafka producer for messaging
    pub kafka_producer: Option<Arc<KafkaProducer>>,
}

impl OrderManager {
    pub fn new(client: Arc<Mutex<ThalexClient>>, market_data: Arc<MarketDataManager>, kafka_producer: Option<Arc<KafkaProducer>>) -> Self {
        Self {
            client,
            market_data,
            orders: RwLock::new(vec![vec![], vec![]]),  // Initialize empty orders for bids and asks
            client_order_id: RwLock::new(100),          // Start with ID 100
            portfolio: RwLock::new(HashMap::new()),
            kafka_producer,
        }
    }

    /// Create quotes based on current market conditions
    pub async fn make_quotes(&self) -> Result<Vec<Vec<SideQuote>>> {
        let index_guard = self.market_data.index_price.read().await;
        let index = index_guard.ok_or_else(|| anyhow!("Index price not initialized"))?;
        
        let tick_guard = self.market_data.tick.read().await;
        let tick = tick_guard.ok_or_else(|| anyhow!("Tick size not initialized"))?;

        // Create bid quotes
        let mut bids = Vec::with_capacity(config::BID_SIZES.len());
        for (lvl, &amt) in config::BID_SIZES.iter().enumerate() {
            let price = self.market_data.round_to_tick(index - (config::SPREAD + config::BID_STEP * lvl as f64) * tick).await?;
            bids.push(SideQuote::new(price, amt));
        }

        // Create ask quotes
        let mut asks = Vec::with_capacity(config::ASK_SIZES.len());
        for (lvl, &amt) in config::ASK_SIZES.iter().enumerate() {
            let price = self.market_data.round_to_tick(index + (config::SPREAD + config::ASK_STEP * lvl as f64) * tick).await?;
            asks.push(SideQuote::new(price, amt));
        }

        Ok(vec![bids, asks])
    }

    /// Adjust quotes to match the desired state
    pub async fn adjust_quotes(&self, desired: Vec<Vec<SideQuote>>) -> Result<()> {
        let sides = [OrderSide::Buy, OrderSide::Sell];
        let mut orders_guard = self.orders.write().await;
        
        for (side_i, side) in sides.iter().enumerate() {
            let side_orders = &mut orders_guard[side_i];
            let side_quotes = &desired[side_i];
            
            // Cancel excess orders
            for i in side_quotes.len()..side_orders.len() {
                if side_orders[i].is_open() {
                    info!("Cancelling {}-{} {}", side_to_string(side), i, side_orders[i].id);
                    let mut client = self.client.lock().await;
                    client.cancel(
                        None, 
                        Some(side_orders[i].id), 
                        Some(side_orders[i].id)
                    ).await?;
                }
            }
            
            // Adjust orders for each level
            for (q_lvl, q) in side_quotes.iter().enumerate() {
                let needs_new_order = q_lvl >= side_orders.len() || 
                    (side_orders[q_lvl].status.is_some() && !side_orders[q_lvl].is_open());
                
                if needs_new_order {
                    // Create a new order for this level
                    let mut id_guard = self.client_order_id.write().await;
                    let client_order_id = *id_guard;
                    *id_guard += 1;
                    
                    // Update the order list
                    if q_lvl >= side_orders.len() {
                        side_orders.push(Order::new(client_order_id, q.price, q.amount, None));
                    } else {
                        side_orders[q_lvl] = Order::new(client_order_id, q.price, q.amount, None);
                    }
                    
                    // Send the order to the exchange
                    info!("Inserting {} {}-{} {}@{}", client_order_id, side_to_string(side), q_lvl, q.amount, q.price);
                    let perp_name = self.market_data.perp_name.read().await.clone()
                        .ok_or_else(|| anyhow!("Perpetual name not initialized"))?;
                    
                    let order_request = OrderRequest {
                        symbol: perp_name,
                        side: side.clone(),
                        order_type: OrderType::Limit,
                        quantity: q.amount,
                        price: Some(q.price),
                        client_order_id: Some(client_order_id),
                        time_in_force: Some(TimeInForce::GTC),
                    };
                    
                    let mut client = self.client.lock().await;
                    client.insert(order_request, Some(client_order_id)).await?;
                } else if side_orders[q_lvl].is_open() {
                    // Check if we need to amend the order
                    let tick_guard = self.market_data.tick.read().await;
                    let tick = tick_guard.ok_or_else(|| anyhow!("Tick size not initialized"))?;
                    
                    if (side_orders[q_lvl].price - q.price).abs() > config::AMEND_THRESHOLD * tick {
                        info!("Amending {} {}-{} {} -> {}", 
                            side_orders[q_lvl].id, 
                            side_to_string(side), 
                            q_lvl, 
                            side_orders[q_lvl].price, 
                            q.price
                        );
                        
                        let mut client = self.client.lock().await;
                        client.amend(
                            q.amount,
                            q.price,
                            None,
                            Some(side_orders[q_lvl].id),
                            Some(side_orders[q_lvl].id)
                        ).await?;
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Process order updates
    pub async fn handle_orders(&self, notification: &Value) -> Result<()> {
        if let Some(orders_array) = notification.as_array() {
            let mut orders_guard = self.orders.write().await;
            
            for order_data in orders_array {
                match order_from_data(order_data) {
                    Ok(order) => {
                        // Publish to Kafka if producer exists
                        if let Some(kafka_producer) = &self.kafka_producer {
                            if let Err(e) = kafka_producer.publish_order_notification(order_data).await {
                                // println!("\n \n THIS GOES WRONG: !{:?} \n \n \n", order_data);
                                warn!("Failed to publish to Kafka: {}", e);
                            }
                        }
                        
                        if !self.update_order(&order, &mut orders_guard) {
                            error!("Didn't find order: {:?}", order);
                        }
                    },
                    Err(e) => {
                        error!("Failed to parse order data: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Update order in collection if it exists
    fn update_order(&self, order: &Order, orders: &mut Vec<Vec<Order>>) -> bool {
        for side in 0..2 {
            for i in 0..orders[side].len() {
                if orders[side][i].id == order.id {
                    orders[side][i] = order.clone();
                    return true;
                }
            }
        }
        false
    }


    /// Process portfolio updates
    pub async fn handle_portfolio(&self, notification: &Value) -> Result<()> {
        if let Some(portfolio_array) = notification.as_array() {
            let mut portfolio_guard = self.portfolio.write().await;
            
            for position in portfolio_array {
                if let (Some(instrument), Some(position_amount)) = (
                    position.get("instrument_name").and_then(|v| v.as_str()),
                    position.get("position").and_then(|v| v.as_f64())
                ) {
                    portfolio_guard.insert(instrument.to_string(), position_amount);
                    debug!("Portfolio update: {}={}", instrument, position_amount);
                }
            }
        }
        Ok(())
    }

    /// Process trade updates
    pub async fn handle_trades(&self, notification: &Value) -> Result<()> {
        if let Some(trades_array) = notification.as_array() {
            for trade in trades_array {
                // Look for trades with our label
                if let Some(label) = trade.get("label").and_then(|v| v.as_str()) {
                    if label == config::LABEL {
                        let direction = trade.get("direction").and_then(|v| v.as_str()).unwrap_or("unknown");
                        let amount = trade.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let price = trade.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        
                        info!("Trade executed: {} {} @ {}", direction, amount, price);
                    }
                }
            }
        }
        Ok(())
    }
}