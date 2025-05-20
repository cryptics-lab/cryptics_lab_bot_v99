use anyhow::Result;
use log::{debug, error, info};
use serde_json::Value;
use std::sync::Arc;

use crate::domain::constants::*;

use super::market_data::MarketDataManager;
use super::order_manager::OrderManager;

/// Handles WebSocket notifications and routes them to appropriate handlers
pub struct NotificationHandler {
    pub market_data: Arc<MarketDataManager>,
    pub order_manager: Arc<OrderManager>,
}

impl NotificationHandler {
    pub fn new(market_data: Arc<MarketDataManager>, order_manager: Arc<OrderManager>) -> Self {
        Self {
            market_data,
            order_manager,
        }
    }

    /// Process result callback
    pub async fn result_callback(&self, result: &Value, cid: u64) -> Result<()> {
        match cid {
            CALL_ID_INSTRUMENT => {
                debug!("Instrument result: {}", result);
            }
            CALL_ID_SUBSCRIBE => {
                info!("Sub successful: {}", result);
            }
            CALL_ID_LOGIN => {
                info!("Login result: {}", result);
            }
            CALL_ID_SET_COD => {
                info!("Set cancel on disconnect result: {}", result);
            }
            _ if cid > 99 => {
                debug!("Trade request result: {}", result);
            }
            _ => {
                info!("cid={}: result={}", cid, result);
            }
        }
        Ok(())
    }

    /// Process error callback
    pub async fn error_callback(&self, error: &Value, cid: u64) -> Result<()> {
        error!("cid={}: error={}", cid, error);
        Ok(())
    }

    /// Route notifications to the appropriate handler
    pub async fn handle_notification(&self, channel: &str, notification: &Value) -> Result<()> {
        match channel {
            c if c.starts_with("ticker.") => {
                self.market_data.handle_ticker(notification).await?;
            }
            c if c.starts_with("price_index.") => {
                if let Some(price) = notification.get("price").and_then(|v| v.as_f64()) {
                    self.market_data.handle_index(price).await?;
                }
            }
            "session.orders" => {
                self.order_manager.handle_orders(notification).await?;
            }
            "account.portfolio" => {
                self.order_manager.handle_portfolio(notification).await?;
            }
            "account.trade_history" => {
                self.order_manager.handle_trades(notification).await?;
            }
            _ => {
                error!("Unknown notification channel: {}", channel);
            }
        }
        Ok(())
    }
}