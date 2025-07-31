use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use apache_avro::{Schema, Reader};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rdkafka::config::RDKafkaLogLevel;
use tokio;

use cryptics_lab_bot::infrastructure::kafka::KafkaProducer;
use cryptics_lab_bot::infrastructure::kafka::helper::SchemaHelper;
use cryptics_lab_bot::domain::model::ticker::Ticker;

/// This test is similar to 10_kafka_ticker_test.rs but structured as a proper test
/// 
/// Note: This test requires a running Kafka and Schema Registry
#[tokio::test]
#[ignore] // This test requires a running Kafka cluster, so it's ignored by default
async fn test_kafka_ticker_integration() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let topic_prefix = "test.kafka.ticker.integration";
    
    // Create topic map
    let mut topics = HashMap::new();
    topics.insert("ticker".to_string(), format!("{}.ticker", topic_prefix));
    
    // Initialize schema helper to verify schemas
    let _schema_helper = SchemaHelper::new(schema_dir.to_string());
    
    // Create sample JSON data for ticker
    let json_data = json!({
        "instrument_name": "BTC-PERPETUAL",
        "mark_price": 50000.0,
        "mark_timestamp": 1645543210.123,
        "best_bid_price": 49950.0,
        "best_bid_amount": 0.5,
        "best_ask_price": 50050.0,
        "best_ask_amount": 0.3,
        "last_price": 49975.0,
        "index": 50010.0,
        "funding_rate": 0.0002,
        "open_interest": 500.0,
        // Additional fields with default values
        "delta": 0.1,
        "volume_24h": 1000.0,
        "value_24h": 50000000.0,
        "low_price_24h": 48000.0,
        "high_price_24h": 51000.0,
        "change_24h": 2.5,
        "forward": 0.0,
        "funding_mark": 0.0001,
        "collar_low": 48000.0,
        "collar_high": 52000.0,
        "realised_funding_24h": 0.0003,
        "average_funding_rate_24h": 0.0002
    });
    
    // Create Ticker from JSON
    let ticker = Ticker::from_json(&json_data, "BTC-PERPETUAL".to_string())?;
    
    // Create first producer
    let producer = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic name
    let _ticker_topic = producer.get_topic("ticker");
    
    // Create topic with schema (or retrieve existing schema ID if it exists)
    let (topic, schema_id) = producer.create_topic_with_schema("ticker").await?;
    
    // Publish the ticker
    producer.send_ticker(&ticker).await?;
    
    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-ticker-test-group-1")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to the topic
    consumer.subscribe(&[&topic])?;
    
    // Try to consume the message
    match tokio::time::timeout(
        Duration::from_secs(5),
        consumer.recv()
    ).await {
        Ok(result) => {
            match result {
                Ok(message) => {
                    if let Some(payload) = message.payload() {
                        // Validate that the payload follows the expected format
                        if payload.len() >= 5 && payload[0] == 0 {
                            let received_schema_id = i32::from_be_bytes([
                                payload[1], payload[2], payload[3], payload[4]
                            ]);
                            
                            assert_eq!(received_schema_id, schema_id, "Schema ID mismatch");
                            
                            // Decode the message
                            let avro_data = &payload[5..];
                            
                            // Get the schema from the registry
                            let schema_url = format!("{}/schemas/ids/{}", schema_registry_url, received_schema_id);
                            let client = reqwest::Client::new();
                            let response = client.get(&schema_url).send().await?;
                            
                            if response.status().is_success() {
                                let schema_response = response.json::<serde_json::Value>().await?;
                                let schema_str = schema_response["schema"].as_str().unwrap();
                                
                                // Parse the schema
                                let schema = Schema::parse_str(schema_str)?;
                                
                                // Decode the data
                                match Reader::with_schema(&schema, &avro_data[..]) {
                                    Ok(mut reader) => {
                                        if let Some(Ok(value)) = reader.next() {
                                            // Verify specific fields to ensure proper serialization
                                            match &value {
                                                apache_avro::types::Value::Record(fields) => {
                                                    let field_map: HashMap<_, _> = fields.iter().cloned().collect();
                                                    
                                                    match &field_map["instrument_name"] {
                                                        apache_avro::types::Value::String(s) => assert_eq!(s, "BTC-PERPETUAL"),
                                                        _ => panic!("Expected String for instrument_name"),
                                                    }
                                                    
                                                    match &field_map["mark_price"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50000.0),
                                                        _ => panic!("Expected Double for mark_price"),
                                                    }
                                                    
                                                    match &field_map["mark_timestamp"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 1645543210.123),
                                                        _ => panic!("Expected Double for mark_timestamp"),
                                                    }
                                                    
                                                    match &field_map["index_price"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50010.0),
                                                        _ => panic!("Expected Double for index_price"),
                                                    }
                                                    
                                                    match &field_map["funding_rate"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 0.0002),
                                                        _ => panic!("Expected Double for funding_rate"),
                                                    }
                                                    
                                                    match &field_map["open_interest"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 500.0),
                                                        _ => panic!("Expected Double for open_interest"),
                                                    }
                                                },
                                                _ => panic!("Expected Record, got {:?}", value)
                                            }
                                        } else {
                                            panic!("Could not read value from Avro reader");
                                        }
                                    },
                                    Err(e) => {
                                        panic!("Error decoding Avro data: {:?}", e);
                                    }
                                }
                            } else {
                                panic!("Failed to get schema from registry");
                            }
                        } else {
                            panic!("Received payload does not match expected format");
                        }
                    } else {
                        panic!("Empty payload");
                    }
                },
                Err(e) => {
                    panic!("Error consuming message: {}", e);
                }
            }
        },
        Err(_) => {
            panic!("Timeout waiting for message");
        }
    }
    
    Ok(())
}
