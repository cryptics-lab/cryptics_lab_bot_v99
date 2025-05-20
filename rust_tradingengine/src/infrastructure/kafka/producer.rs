use anyhow::{anyhow, Context, Result};
use apache_avro::Schema;
use log::{debug, error, info, warn};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use reqwest;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

use crate::domain::model::ack::Ack;
use crate::domain::model::ticker::Ticker;
use crate::domain::model::trade::Trade;
use crate::infrastructure::exchange::thalex::parsers::ThaleParser;
use crate::infrastructure::kafka::helper::{SchemaHelper, AvroConverter};

/// Cached schema info
struct SchemaInfo {
    id: i32,
    schema: Schema,
}

/// Kafka producer with schema registry support
pub struct KafkaProducer {
    /// Kafka producer client
    producer: FutureProducer,
    
    /// Topic names
    topics: HashMap<String, String>,
    
    /// Schema helper
    schema_helper: SchemaHelper,
    
    /// Schema registry URL
    schema_registry_url: String,
    
    /// Cached schemas (topic -> SchemaInfo)
    cached_schemas: RwLock<HashMap<String, SchemaInfo>>,
    
    /// Schema Registry settings
    sr_settings: Arc<SrSettings>,
}

impl KafkaProducer {
    /// Creates a new Kafka producer with the given configuration
    pub async fn new(bootstrap_servers: &str, schema_registry_url: &str, topics: HashMap<String, String>, schema_dir: String) -> Result<Self> {
        // Set up Kafka producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .context("Failed to create Kafka producer")?;
        
        // Create schema helper
        let schema_helper = SchemaHelper::new(schema_dir);
        
        // Create Schema Registry settings
        let sr_settings = Arc::new(SrSettings::new(schema_registry_url.to_string()));
        
        // Create the KafkaProducer
        let producer = Self {
            producer,
            topics,
            schema_helper,
            schema_registry_url: schema_registry_url.to_string(),
            cached_schemas: RwLock::new(HashMap::new()),
            sr_settings,
        };
        
        // Preload schemas for common topics during initialization
        info!("Preloading schemas from registry...");
        for topic_type in ["ticker", "ack", "trade", "index"] {
            if let Err(e) = producer.preload_schema(topic_type).await {
                warn!("Failed to preload schema for {}: {}", topic_type, e);
            }
        }
        
        Ok(producer)
    }
    
    /// Helper method to encode data in Confluent format
    async fn encode_confluent_format(&self, record_name: &str, value: Vec<(String, apache_avro::types::Value)>, topic: &str) -> Result<Vec<u8>> {
        // Create subject name strategy for the topic
        let subject_strategy = SubjectNameStrategy::TopicNameStrategy(
            topic.to_string(),
            false // is it a key
        );
        
        // Convert Vec<(String, Value)> to Vec<(&str, Value)> for the encoder
        let data: Vec<(&str, apache_avro::types::Value)> = value
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        
        // Create encoder with the shared settings
        // We need to clone and unwrap the Arc to get the SrSettings
        let sr_settings = (*self.sr_settings).clone();
        let encoder = AvroEncoder::new(sr_settings);
        
        // Encode with the Confluent format
        match encoder.encode(data, subject_strategy).await {
            Ok(payload) => {
                debug!("Successfully encoded {} with Confluent format, size: {} bytes", record_name, payload.len());
                Ok(payload)
            },
            Err(e) => {
                error!("Failed to encode {} with Confluent format: {}", record_name, e);
                Err(anyhow!("Failed to encode {} value: {}", record_name, e))
            }
        }
    }
    
    /// Preload schema for a given topic type
    /// First tries to get the schema ID from registry, and if not found, registers it
    async fn preload_schema(&self, topic_type: &str) -> Result<(String, i32)> {
        let topic = self.get_topic(topic_type);
        info!("Preloading schema for topic type: {} (topic: {})", topic_type, topic);
        
        // First try to get existing schema ID
        match self.get_schema_id(&topic).await {
            Ok(schema_id) => {
                info!("Found existing schema for {} with ID: {}", topic_type, schema_id);
                Ok((topic, schema_id))
            },
            Err(_) => {
                // Schema doesn't exist, load from file and register
                info!("Schema not found in registry for {}. Registering from file.", topic_type);
                self.create_topic_with_schema(topic_type).await
            }
        }
    }
    
    /// Gets the topic name for a given topic type
    pub fn get_topic(&self, topic_type: &str) -> String {
        self.topics.get(topic_type)
            .cloned()
            .unwrap_or_else(|| format!("cryptics.thalex.{}.avro", topic_type))
    }
    
    /// Register a schema with the schema registry
    pub async fn register_schema(&self, topic: &str, schema_content: &str) -> Result<i32> {
        let subject = format!("{}-value", topic);
        let register_url = format!("{}/subjects/{}/versions", self.schema_registry_url, subject);
        
        info!("Registering schema for topic: {}", topic);
        
        // Parse schema to ensure it's valid
        let _schema = Schema::parse_str(schema_content)
            .context("Failed to parse Avro schema")?;
        
        // Register schema
        let schema_request = serde_json::json!({
            "schema": schema_content
        });
        
        let client = reqwest::Client::new();
        let response = client.post(&register_url)
            .json(&schema_request)
            .send()
            .await
            .context("Failed to send schema registration request")?;
        
        let status = response.status();
        if status.is_success() {
            let registration_result = response.json::<serde_json::Value>().await?;
            let schema_id = registration_result["id"].as_i64().unwrap_or(0) as i32;
            info!("Schema registration successful with ID: {}", schema_id);
            Ok(schema_id)
        } else {
            let error_text = response.text().await?;
            if status.as_u16() == 409 {
                info!("Schema already exists: {}", error_text);
                
                // Try to get the schema ID from the registry
                let get_url = format!("{}/subjects/{}/versions/latest", self.schema_registry_url, subject);
                let get_response = client.get(&get_url).send().await?;
                
                if get_response.status().is_success() {
                    let schema_info = get_response.json::<serde_json::Value>().await?;
                    let schema_id = schema_info["id"].as_i64().unwrap_or(0) as i32;
                    info!("Retrieved existing schema ID: {}", schema_id);
                    Ok(schema_id)
                } else {
                    Err(anyhow!("Schema already exists but couldn't retrieve ID"))
                }
            } else {
                error!("Schema registration failed with status: {}, error: {}", 
                        status, error_text);
                Err(anyhow!("Failed to register schema: {}", error_text))
            }
        }
    }
    
    /// Fetch schema from registry and cache it
    async fn fetch_and_cache_schema(&self, schema_id: i32) -> Result<Schema> {
        // Get the schema from the registry
        let schema_url = format!("{}/schemas/ids/{}", self.schema_registry_url, schema_id);
        let client = reqwest::Client::new();
        let response = client.get(&schema_url).send().await
            .context("Failed to fetch schema from registry")?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Failed to get schema from registry"));
        }
        
        let schema_response = response.json::<serde_json::Value>().await?;
        let schema_str = schema_response["schema"].as_str().unwrap();
        
        // Parse the schema
        let schema = Schema::parse_str(schema_str)
            .context("Failed to parse schema from registry")?;
        
        Ok(schema)
    }
    
    /// Get schema for a topic (from cache or registry)
    async fn get_schema_for_topic(&self, topic: &str, schema_id: i32) -> Result<Schema> {
        // Check cache first
        let cache_key = format!("{}:{}", topic, schema_id);
        {
            let cache = self.cached_schemas.read().unwrap();
            if let Some(schema_info) = cache.get(&cache_key) {
                return Ok(schema_info.schema.clone());
            }
        }
        
        // Not in cache, fetch from registry
        let schema = self.fetch_and_cache_schema(schema_id).await?;
        
        // Cache it
        {
            let mut cache = self.cached_schemas.write().unwrap();
            cache.insert(cache_key, SchemaInfo {
                id: schema_id,
                schema: schema.clone(),
            });
        }
        
        Ok(schema)
    }
    
    /// Get schema ID and topic for a topic type from cache, or load from registry
    async fn get_cached_schema(&self, topic_type: &str) -> Result<(String, i32)> {
        let topic = self.get_topic(topic_type);
        
        // Check if we have this schema in cache
        {
            let cache = self.cached_schemas.read().unwrap();
            for (key, schema_info) in cache.iter() {
                if key.starts_with(&format!("{}:", topic)) {
                    debug!("Found cached schema for topic {}: {}", topic, key);
                    return Ok((topic.clone(), schema_info.id));
                }
            }
        }
        
        // Not in cache, warn and load it
        warn!("Schema for {} not found in cache. This is unexpected since schemas should be preloaded.", topic_type);
        self.preload_schema(topic_type).await
    }
    
    /// Create a topic with a schema loaded from file
    pub async fn create_topic_with_schema(&self, topic_type: &str) -> Result<(String, i32)> {
        let topic = self.get_topic(topic_type);
        
        // Load schema from file
        let schema_content = self.schema_helper.get_schema_content(topic_type)?;
        debug!("Loaded schema for {}: {}", topic_type, schema_content);
        
        // Register schema with the registry
        let schema_id = self.register_schema(&topic, &schema_content).await?;
        
        // Cache the schema
        let schema = Schema::parse_str(&schema_content)?;
        let cache_key = format!("{}:{}", topic, schema_id);
        {
            let mut cache = self.cached_schemas.write().unwrap();
            cache.insert(cache_key, SchemaInfo {
                id: schema_id,
                schema,
            });
        }
        
        Ok((topic, schema_id))
    }
    
    /// Get schema ID for an existing topic
    pub async fn get_schema_id(&self, topic: &str) -> Result<i32> {
        let subject = format!("{}-value", topic);
        let url = format!("{}/subjects/{}/versions/latest", self.schema_registry_url, subject);
        
        let client = reqwest::Client::new();
        let response = client.get(&url).send().await?;
        
        if response.status().is_success() {
            let schema_info = response.json::<serde_json::Value>().await?;
            let schema_id = schema_info["id"].as_i64().unwrap_or(0) as i32;
            
            // Fetch and cache the schema
            let schema = self.fetch_and_cache_schema(schema_id).await?;
            let cache_key = format!("{}:{}", topic, schema_id);
            {
                let mut cache = self.cached_schemas.write().unwrap();
                cache.insert(cache_key, SchemaInfo {
                    id: schema_id,
                    schema,
                });
            }
            
            Ok(schema_id)
        } else {
            Err(anyhow!("Failed to get schema ID for topic: {}", topic))
        }
    }
    
    /// Serialize an Ack to Avro bytes with schema ID
    async fn serialize_ack_to_avro(&self, ack: &Ack, topic: &str, _schema_id: i32) -> Result<Vec<u8>> {
        // Convert Ack to Avro field vector using our AvroConverter
        let avro_fields = AvroConverter::ack_to_avro_value(ack);
        debug!("Successfully converted Ack to Avro fields");
        
        // Encode using the Confluent format helper
        self.encode_confluent_format("ack", avro_fields, topic).await
    }

    /// Publish an Ack to Kafka using Apache Avro serialization
    pub async fn publish_ack(&self, ack: &Ack, topic: &str, schema_id: i32) -> Result<()> {
        // Serialize the Ack to Avro bytes
        let kafka_payload = match self.serialize_ack_to_avro(ack, topic, schema_id).await {
            Ok(payload) => payload,
            Err(e) => {
                error!("Failed to serialize Ack to Avro: {}", e);
                error!("Ack data: {:?}", ack);
                return Err(anyhow!("Failed to serialize Ack: {}", e));
            }
        };
        
        // Send to Kafka
        let delivery_result = self.producer
            .send(
                FutureRecord::to(topic)
                    .payload(&kafka_payload)
                    .key(&format!("ack-{}", Uuid::new_v4())),
                Duration::from_secs(5),
            )
            .await;
        
        match delivery_result {
            Ok((partition, offset)) => {
                debug!("Successfully sent Ack to topic: {}, partition: {}, offset: {}", 
                      topic, partition, offset);
                Ok(())
            },
            Err((err, _)) => {
                error!("Failed to send Ack message: {}, Ack data: {:?}", err, ack);
                Err(anyhow!("Failed to send Ack message: {}", err))
            }
        }
    }
    
    /// Parse JSON data and publish as an Ack, and extract any trades if present
    pub async fn publish_order_notification(&self, order_data: &Value) -> Result<()> {
        // Parse the order data into our Ack format and publish it
        let ack = ThaleParser::parse_ack_json(order_data)?;
        
        // Get topic and schema ID from cache
        let topic_type = "ack";
        let (topic, schema_id) = self.get_cached_schema(topic_type).await?;
        
        // Publish the Ack
        self.publish_ack(&ack, &topic, schema_id).await?;
        
        // Check for and publish any trades in the order data
        if let Ok(trades) = ThaleParser::extract_trades_from_order(order_data) {
            if !trades.is_empty() {
                info!("Found {} trades in order notification", trades.len());
                
                // Publish each trade
                for trade in trades {
                    self.send_trade(&trade).await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Send ticker data to Kafka
    pub async fn send_ticker(&self, ticker: &Ticker) -> Result<()> {
        let topic_type = "ticker";
        // Use cached schema
        let (topic, _) = self.get_cached_schema(topic_type).await?;
        
        // Convert ticker to Avro field vector directly
        let avro_fields = AvroConverter::ticker_to_avro_value(ticker)?;
        
        // Encode using the Confluent format helper - passing the fields directly
        let kafka_payload = self.encode_confluent_format("ticker", avro_fields, &topic).await?;
        
        // Send to Kafka
        let delivery_result = self.producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&kafka_payload)
                    .key(&format!("ticker-{}-{}", ticker.instrument_name, Uuid::new_v4())),
                Duration::from_secs(5),
            )
            .await;
        
        match delivery_result {
            Ok((partition, offset)) => {
                debug!("Successfully sent Ticker to topic: {}, partition: {}, offset: {}", 
                      topic, partition, offset);
                Ok(())
            },
            Err((err, _)) => {
                Err(anyhow!("Failed to send Ticker message: {}", err))
            }
        }
    }
    
    /// Send trade data to Kafka
    pub async fn send_trade(&self, trade: &Trade) -> Result<()> {
        let topic_type = "trade";
        // Use cached schema
        let (topic, _) = self.get_cached_schema(topic_type).await?;
        
        // Convert trade to Avro field vector
        let avro_fields = AvroConverter::trade_to_avro_value(trade)?;
        
        // Encode using the Confluent format helper
        let kafka_payload = self.encode_confluent_format("trade", avro_fields, &topic).await?;
        
        // Send to Kafka
        let delivery_result = self.producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&kafka_payload)
                    .key(&format!("trade-{}", Uuid::new_v4())),
                Duration::from_secs(5),
            )
            .await;
        
        match delivery_result {
            Ok((partition, offset)) => {
                debug!("Successfully sent Trade to topic: {}, partition: {}, offset: {}", 
                      topic, partition, offset);
                Ok(())
            },
            Err((err, _)) => {
                Err(anyhow!("Failed to send Trade message: {}", err))
            }
        }
    }
    
    /// Send an Ack to Kafka
    pub async fn send_ack(&self, ack: &Ack) -> Result<()> {
        let topic_type = "ack";
        // Use cached schema
        let (topic, schema_id) = self.get_cached_schema(topic_type).await?;
        self.publish_ack(ack, &topic, schema_id).await
    }
    
    /// Parse JSON data and publish based on topic type
    pub async fn publish_json_data(&self, data: &Value, topic_type: &str) -> Result<()> {
        match topic_type {
            "ack" => {
                let ack = ThaleParser::parse_ack_json(data)?;
                self.send_ack(&ack).await
            },
            "trade" => {
                let trade = ThaleParser::parse_trade_json(data)?;
                self.send_trade(&trade).await
            },
            "ticker" => {
                if let Some(instrument_name) = data.get("instrument_name").and_then(|v| v.as_str()) {
                    match Ticker::from_json(data, instrument_name.to_string()) {
                        Ok(ticker) => {
                            self.send_ticker(&ticker).await
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    Err(anyhow!("Missing instrument_name in ticker data"))
                }
            },
            _ => Err(anyhow!("Unsupported topic type: {}", topic_type))
        }
    }
}