use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Kafka configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
    /// Kafka broker addresses
    pub bootstrap_servers: String,
    
    /// Schema registry URL
    pub schema_registry_url: String,
    
    /// Topic mapping
    pub topics: HashMap<String, String>,
    
    /// Schema directory
    pub schema_dir: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let mut topics = HashMap::new();
        topics.insert("ticker".to_string(), "cryptics.thalex.ticker.avro2".to_string());
        topics.insert("ack".to_string(), "cryptics.thalex.ack.avro2".to_string());
        topics.insert("trade".to_string(), "cryptics.thalex.trade.avro2".to_string());
        topics.insert("index".to_string(), "cryptics.thalex.index.avro2".to_string());
        
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            schema_registry_url: "http://localhost:8081".to_string(),
            topics,
            schema_dir: Some("./schemas".to_string()),
        }
    }
}