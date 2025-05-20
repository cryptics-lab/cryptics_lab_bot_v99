// Standard library imports
use std::sync::Arc;

// External crate imports
use anyhow::{anyhow, Result};
use futures_util::SinkExt;
use log::{debug, error, info, warn};
use serde_json::Value;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio::time::{Duration, Instant};
use tungstenite::Message;

// Internal crate imports 
use crate::infrastructure::exchange::thalex::client::ThalexClient;
use crate::infrastructure::exchange::thalex::models::InstrumentResponse;
use crate::infrastructure::kafka::KafkaProducer;
use crate::config_loader::AppConfig;
use crate::domain::constants::*;

// Import our modular components
use crate::strategies::thalex_market_maker::{
    config,
    MarketDataManager,
    OrderManager,
    NotificationHandler,
};


/// Main Thalex market maker implementation
pub struct ThalexQuoter {
    /// Client connection
    pub client: Arc<Mutex<ThalexClient>>,
    
    /// Condition variable for quotation
    pub quote_notify: Arc<Notify>,
    
    /// Market data manager
    pub market_data: Arc<MarketDataManager>,
    
    /// Order manager
    pub order_manager: Arc<OrderManager>,
    
    /// Notification handler
    pub notification_handler: Arc<NotificationHandler>,
}

impl ThalexQuoter {
    pub async fn new(client: Arc<Mutex<ThalexClient>>, config: Option<Arc<AppConfig>>) -> Self {
        // Initialize Kafka producer using the provided config
        let kafka_producer = if let Some(config) = config.clone() {
            debug!("AppConfig provided, initializing Kafka producer");
            
            match KafkaProducer::new(
                config.kafka_bootstrap_servers(),
                config.kafka_schema_registry_url(),
                std::collections::HashMap::from([
                    ("ticker".to_string(), config.topics.ticker.clone()),
                    ("ack".to_string(), config.topics.ack.clone()),
                    ("trade".to_string(), config.topics.trade.clone()),
                    ("index".to_string(), config.topics.index.clone()),
                ]),
                "../schemas".to_string()
            ).await {
                Ok(producer) => {
                    info!("Kafka producer initialized successfully");
                    Some(Arc::new(producer))
                }
                Err(e) => {
                    error!("Failed to initialize Kafka producer: {:?}", e);
                    if let Some(source) = e.source() {
                        error!("Caused by: {:?}", source);
                    }
                    None
                }
            }
        } else {
            debug!("No configuration provided, skipping Kafka producer initialization");
            None
        };

        // Create shared components
        let quote_notify = Arc::new(Notify::new());
        let market_data = Arc::new(MarketDataManager::new(
            quote_notify.clone(),
            kafka_producer.clone()
        ));
        let order_manager = Arc::new(OrderManager::new(
            client.clone(),
            market_data.clone(),
            kafka_producer
        ));
        let notification_handler = Arc::new(NotificationHandler::new(
            market_data.clone(),
            order_manager.clone()
        ));

        Self {
            client,
            quote_notify,
            market_data,
            order_manager,
            notification_handler,
        }
    }

    /// Task to periodically ping the WebSocket connection
    pub async fn ping_task(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), anyhow::Error> {
        let mut interval = tokio::time::interval(Duration::from_secs(config::PING_INTERVAL_SEC));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut client = self.client.lock().await;
                    if let Some(socket) = &mut client.socket {
                        if let Err(e) = socket.send(Message::Ping(vec![])).await {
                            error!("Ping failed: {}", e);
                            return Err(anyhow!("Ping failed"));
                        } else {
                            debug!("Ping sent");
                        }
                    } else {
                        warn!("No active socket in ping task");
                        return Err(anyhow!("No active socket"));
                    }
                }
                _ = shutdown.recv() => {
                    info!("Ping task received shutdown signal");
                    return Ok(());
                }
            }
        }
    }

    /// Task to update quotes based on market data
    pub async fn quote_task(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        info!("Quote task started");
        let mut last_update = Instant::now();
    
        loop {
            tokio::select! {
                _ = self.quote_notify.notified() => {
                    let has_ticker = self.market_data.ticker.read().await.is_some();
                    let has_index = self.market_data.index_price.read().await.is_some();
        
                    if has_ticker && has_index {
                        // Throttle: e.g., 1 update per 100ms
                        if last_update.elapsed() >= Duration::from_millis(100) {
                            let quotes = self.order_manager.make_quotes().await?;
                            self.order_manager.adjust_quotes(quotes).await?;
                            last_update = Instant::now();
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Quote task received shutdown signal");
                    return Ok(());
                }
            }
        }
    }

    /// Fetch and set instrument information
    pub async fn await_instruments(&self, client: &mut ThalexClient) -> Result<()> {
        client.instruments(Some(CALL_ID_INSTRUMENTS)).await?;

        match client.receive().await? {
            Some(msg) => {
                let parsed: InstrumentResponse = serde_json::from_str(&msg)?;
                for instr in parsed.result {
                    if instr.type_field == config::TYPE && instr.underlying == config::UNDERLYING {
                        self.market_data.set_instrument_info(instr.instrument_name, instr.tick_size).await?;
                        return Ok(());
                    }
                }
                Err(anyhow!("Perpetual BTCUSD not found"))
            },
            None => Err(anyhow!("No message received")),
        }
    }

    /// Task to listen for WebSocket messages
    pub async fn listen_task(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        {
            let mut client = self.client.lock().await;

            // Initialize instrument data
            self.await_instruments(&mut client).await?;

            // Set cancel on disconnect
            client
                .set_cancel_on_disconnect(config::TIMEOUT_SEC, Some(CALL_ID_SET_COD))
                .await?;

            // Subscribe to private channels
            let private_channels = config::CHANNELS.iter().map(|x| x.to_string()).collect();
            client
                .private_subscribe(private_channels, Some(CALL_ID_SUBSCRIBE))
                .await?;

            // Subscribe to public channels
            let public_channels = self.market_data.get_public_channels().await?;
            client
                .public_subscribe(public_channels, Some(CALL_ID_SUBSCRIBE))
                .await?;
        }

        loop {
            tokio::select! {
                // Get the next message
                msg_result = async {
                    let mut client = self.client.lock().await;
                    client.receive().await
                } => {
                    match msg_result {
                        Ok(Some(msg)) => {
                            debug!("Raw Message Thalex:{}", msg);
                            match serde_json::from_str::<Value>(&msg) {
                                Ok(parsed) => {
                                    if let Some(channel) = parsed.get("channel_name").and_then(|v| v.as_str()) {
                                        if let Some(notification) = parsed.get("notification") {
                                            self.notification_handler.handle_notification(channel, notification).await?;
                                        }
                                    } else if let Some(result) = parsed.get("result") {
                                        let cid = parsed.get("id").and_then(|v| v.as_u64()).unwrap_or_default();
                                        self.notification_handler.result_callback(result, cid).await?;
                                    } else if let Some(error) = parsed.get("error") {
                                        let cid = parsed.get("id").and_then(|v| v.as_u64()).unwrap_or_default();
                                        self.notification_handler.error_callback(error, cid).await?;
                                    } else {
                                        warn!("Unhandled message: {}", msg);
                                    }
                                },
                                Err(e) => {
                                    error!("Failed to parse JSON: {}", e);
                                }
                            }
                        },
                        Ok(None) => {
                            debug!("No message received");
                        },
                        Err(e) => {
                            error!("Error receiving message: {}", e);
                            return Err(e);
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Listen task received shutdown signal");
                    return Ok(());
                }
            }
        }
    }
}
