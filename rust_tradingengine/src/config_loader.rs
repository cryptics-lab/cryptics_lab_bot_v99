use anyhow::{anyhow, Result};
use log::{debug, info};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Top-level configuration structure containing all config sections
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub topics: TopicsConfig,
    pub app: AppInfo,
    // Add more sections as needed
}

/// Kafka connection configuration
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub bootstrap_servers_internal: String,
    pub schema_registry_url: String,
    pub schema_registry_url_internal: String,
    pub connect_url: String,
    pub connect_url_internal: String,
    
    // Market maker specific configurations
    #[serde(default = "default_ack_topic")]
    pub ack_topic: String,
    
    #[serde(default = "default_trade_topic")]
    pub trade_topic: String,
    
    #[serde(default = "default_ticker_topic")]
    pub ticker_topic: String,
    
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    
    #[serde(default = "default_skip_local_schemas")]
    pub skip_local_schemas: bool,
}

fn default_ack_topic() -> String {
    "cryptics.thalex.ack".to_string()
}

fn default_trade_topic() -> String {
    "cryptics.thalex.trade".to_string()
}

fn default_ticker_topic() -> String {
    "cryptics.thalex.ticker".to_string()
}

fn default_timeout_ms() -> u64 {
    5000
}

fn default_skip_local_schemas() -> bool {
    false
}

/// Kafka topics configuration
#[derive(Debug, Clone, Deserialize)]
pub struct TopicsConfig {
    pub ticker: String,
    pub ack: String, 
    pub trade: String,
    pub index: String,
    pub base_name: String,
}

/// Application information
#[derive(Debug, Clone, Deserialize)]
pub struct AppInfo {
    pub rust_running_in_docker: bool,
    // Add more app settings as needed
}

impl AppConfig {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        
        // Read the file
        let config_str = fs::read_to_string(path)
            .map_err(|e| anyhow!("Failed to read config file '{}': {}", path.display(), e))?;
        
        // Parse the TOML
        let config: AppConfig = toml::from_str(&config_str)
            .map_err(|e| anyhow!("Failed to parse config file '{}': {}", path.display(), e))?;
        
        info!("Loaded configuration from {}", path.display());
        debug!("Running in Docker: {}", config.app.rust_running_in_docker);
        
        Ok(config)
    }
    
    /// Helper to get the appropriate Kafka bootstrap servers URL based on Docker status
    pub fn kafka_bootstrap_servers(&self) -> &str {
        if self.app.rust_running_in_docker {
            &self.kafka.bootstrap_servers_internal
        } else {
            &self.kafka.bootstrap_servers
        }
    }
    
    /// Helper to get the appropriate Schema Registry URL based on Docker status
    pub fn kafka_schema_registry_url(&self) -> &str {
        if self.app.rust_running_in_docker {
            &self.kafka.schema_registry_url_internal
        } else {
            &self.kafka.schema_registry_url
        }
    }
    
    /// Helper to get the appropriate Kafka Connect URL based on Docker status
    pub fn kafka_connect_url(&self) -> &str {
        if self.app.rust_running_in_docker {
            &self.kafka.connect_url_internal
        } else {
            &self.kafka.connect_url
        }
    }
}
