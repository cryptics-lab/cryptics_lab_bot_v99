use anyhow::{anyhow, Result};
use log::{debug, error};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

use crate::infrastructure::kafka::KafkaProducer;
use crate::domain::model::ticker::Ticker;

/// Handles market data updates and processing
pub struct MarketDataManager {
    /// Current ticker data
    pub ticker: RwLock<Option<Ticker>>,
    
    /// Current index price
    pub index_price: RwLock<Option<f64>>,
    
    /// Tick size for the instrument
    pub tick: RwLock<Option<f64>>,
    
    /// Instrument name
    pub perp_name: RwLock<Option<String>>,
    
    /// Notification for quoting logic
    pub quote_notify: Arc<Notify>,
    
    /// Kafka producer for sending market data
    pub kafka_producer: Option<Arc<KafkaProducer>>,
}

impl MarketDataManager {
    pub fn new(quote_notify: Arc<Notify>, kafka_producer: Option<Arc<KafkaProducer>>) -> Self {
        Self {
            ticker: RwLock::new(None),
            index_price: RwLock::new(None),
            tick: RwLock::new(None),
            perp_name: RwLock::new(None),
            quote_notify,
            kafka_producer,
        }
    }

    /// Set ticker information
    pub async fn set_instrument_info(&self, name: String, tick_size: f64) -> Result<()> {
        let mut tick_guard = self.tick.write().await;
        let mut perp_name_guard = self.perp_name.write().await;
        
        *tick_guard = Some(tick_size);
        *perp_name_guard = Some(name);
        
        Ok(())
    }

    /// Get the channels to subscribe for market data
    pub async fn get_public_channels(&self) -> Result<Vec<String>> {
        let perp_name = self.perp_name.read().await;
        let name = perp_name
            .as_ref()
            .ok_or_else(|| anyhow!("perp_name not set"))?;
            
        Ok(vec![
            format!("ticker.{}.raw", name),
            format!("price_index.{}", super::config::UNDERLYING),
        ])
    }

    /// Round a value to the nearest tick
    pub async fn round_to_tick(&self, value: f64) -> Result<f64> {
        let tick_guard = self.tick.read().await;
        let tick = tick_guard.ok_or_else(|| anyhow!("Tick size not initialized"))?;
        Ok(tick * (value / tick).round())
    }

    /// Process ticker updates and send to Kafka
    pub async fn handle_ticker(&self, notification: &Value) -> Result<()> {
        // Get the instrument name for the Ticker
        let instrument_name = self.perp_name.read().await.clone()
            .unwrap_or_else(|| "BTC-PERPETUAL".to_string());
        
        match Ticker::from_json(notification, instrument_name.clone()) {
            Ok(ticker) => {
                debug!("Ticker update: mark_price={}, index={}, funding_rate={}", 
                    ticker.mark_price, ticker.index_price, ticker.funding_rate);
                
                // Send to Kafka if enabled
                if let Some(kafka_producer) = &self.kafka_producer {
                    // Use the ticker directly since we don't have a separate TickerData type
                    let kafka_ticker_data = ticker.clone();
                    
                    // Clone the producer reference and tick_data for use in async task
                    let kafka_producer_clone = kafka_producer.clone();
                    let ticker_data_clone = kafka_ticker_data.clone();
                    
                    // Spawn a task to handle the Kafka send without blocking
                    tokio::spawn(async move {
                        if let Err(e) = kafka_producer_clone.send_ticker(&ticker_data_clone).await {
                            error!("Failed to send ticker data to Kafka: {:?}", e);
                        } else {
                            debug!("Successfully sent ticker data to Kafka");
                        }
                    });
                }
                
                // Store the ticker for trading logic
                let mut ticker_guard = self.ticker.write().await;
                *ticker_guard = Some(ticker);
                
                // Update index price for easier access
                let mut index_guard = self.index_price.write().await;
                if let Some(ticker) = &*ticker_guard {
                    *index_guard = Some(ticker.index_price);
                }
                
                // Notify the quote task about the new data
                self.quote_notify.notify_one();
                Ok(())
            },
            Err(e) => {
                error!("Failed to parse ticker: {}", e);
                Err(e)
            }
        }
    }

    /// Process index price updates
    pub async fn handle_index(&self, price: f64) -> Result<()> {
        debug!("Index price update: {}", price);
        
        let mut index_guard = self.index_price.write().await;
        *index_guard = Some(price);
        
        // Update the ticker's index price if it exists
        let mut ticker_guard = self.ticker.write().await;
        if let Some(ticker) = &mut *ticker_guard {
            ticker.index_price = price;
        }
        
        // Notify the quote task about the new data
        self.quote_notify.notify_one();
        Ok(())
    }
}
