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
use cryptics_lab_bot::infrastructure::exchange::thalex::parsers::ThaleParser;

/// This test is similar to 11_kafka_trade_test.rs but structured as a proper test
/// 
/// Note: This test requires a running Kafka and Schema Registry
#[tokio::test]
#[ignore] // This test requires a running Kafka cluster, so it's ignored by default
async fn test_kafka_trade_integration() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let topic_prefix = "test.kafka.trade.integration";
    
    // Create topic map
    let mut topics = HashMap::new();
    topics.insert("trade".to_string(), format!("{}.trade", topic_prefix));
    
    // Initialize schema helper to verify schemas
    let _schema_helper = SchemaHelper::new(schema_dir.to_string());
    
    // Create sample JSON data for a direct trade
    let json_data = json!({
        "trade_id": "trd-123456",
        "order_id": "ord-789012",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "price": 50000.0,
        "amount": 0.1,
        "maker_taker": "maker",
        "timestamp": 1645543210.123
    });
    
    // Use the ThaleParser to parse the JSON data into a Trade
    let trade = ThaleParser::parse_trade_json(&json_data)?;
    
    // Create Kafka producer
    let producer = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic name
    let _trade_topic = producer.get_topic("trade");
    
    // Create topic with schema (or retrieve existing schema ID if it exists)
    let (topic, schema_id) = producer.create_topic_with_schema("trade").await?;
    
    // Publish the trade
    producer.send_trade(&trade).await?;
    
    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-trade-test-group-1")
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
                                                    
                                                    match &field_map["trade_id"] {
                                                        apache_avro::types::Value::String(s) => assert_eq!(s, "trd-123456"),
                                                        _ => panic!("Expected String for trade_id"),
                                                    }
                                                    
                                                    match &field_map["order_id"] {
                                                        apache_avro::types::Value::String(s) => assert_eq!(s, "ord-789012"),
                                                        _ => panic!("Expected String for order_id"),
                                                    }
                                                    
                                                    match &field_map["client_order_id"] {
                                                        apache_avro::types::Value::Union(1, box_value) => {
                                                            match &**box_value {
                                                                apache_avro::types::Value::Long(v) => assert_eq!(*v, 42),
                                                                _ => panic!("Expected Long in Union for client_order_id"),
                                                            }
                                                        },
                                                        _ => panic!("Expected Union for client_order_id"),
                                                    }
                                                    
                                                    match &field_map["instrument_name"] {
                                                        apache_avro::types::Value::String(s) => assert_eq!(s, "BTC-PERPETUAL"),
                                                        _ => panic!("Expected String for instrument_name"),
                                                    }
                                                    
                                                    match &field_map["price"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50000.0),
                                                        _ => panic!("Expected Double for price"),
                                                    }
                                                    
                                                    match &field_map["amount"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 0.1),
                                                        _ => panic!("Expected Double for amount"),
                                                    }
                                                    
                                                    match &field_map["maker_taker"] {
                                                        apache_avro::types::Value::String(s) => assert_eq!(s, "maker"),
                                                        _ => panic!("Expected String for maker_taker"),
                                                    }
                                                    
                                                    match &field_map["time"] {
                                                        apache_avro::types::Value::Double(v) => assert_eq!(*v, 1645543210.123),
                                                        _ => panic!("Expected Double for time"),
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

/// Test extracting trades from order notification and publishing them
#[tokio::test]
#[ignore] // This test requires a running Kafka cluster, so it's ignored by default
async fn test_publish_order_notification() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let topic_prefix = "test.kafka.order.integration";
    
    // Create topic map
    let mut topics = HashMap::new();
    topics.insert("trade".to_string(), format!("{}.trade", topic_prefix));
    topics.insert("ack".to_string(), format!("{}.ack", topic_prefix));
    
    // Create sample order data with fills
    let order_data = json!({
        "order_id": "ord-123456",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "direction": "buy",
        "price": 50000.0,
        "amount": 0.2,
        "filled_amount": 0.2,
        "remaining_amount": 0.0,
        "status": "filled",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "fill",
        "create_time": 1645543210.123,
        "persistent": true,
        "fills": [
            {
                "trade_id": "trd-111111",
                "price": 50000.0,
                "amount": 0.1,
                "maker_taker": "maker",
                "time": 1645543220.456
            },
            {
                "trade_id": "trd-222222",
                "price": 50000.0,
                "amount": 0.1,
                "maker_taker": "maker",
                "time": 1645543230.789
            }
        ]
    });
    
    // Create Kafka producer
    let producer = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic names
    let ack_topic = producer.get_topic("ack");
    let trade_topic = producer.get_topic("trade");
    
    // Ensure topics exist with schemas
    let _ = producer.create_topic_with_schema("ack").await?;
    let _ = producer.create_topic_with_schema("trade").await?;
    
    // Publish the order notification (should create 1 ack and 2 trades)
    producer.publish_order_notification(&order_data).await?;
    
    // Create consumer for both topics
    let consumer_all: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-order-notification-test-group")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to both topics
    consumer_all.subscribe(&[&ack_topic, &trade_topic])?;
    
    // Count how many messages we receive (should be 1 Ack + 2 Trades = 3 messages)
    let mut ack_count = 0;
    let mut trade_count = 0;
    
    for _ in 0..3 {
        match tokio::time::timeout(
            Duration::from_secs(10),
            consumer_all.recv()
        ).await {
            Ok(result) => {
                match result {
                    Ok(message) => {
                        let topic_name = message.topic();
                        if topic_name == ack_topic {
                            ack_count += 1;
                        } else if topic_name == trade_topic {
                            trade_count += 1;
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
    }
    
    // Verify message counts
    assert_eq!(ack_count, 1, "Expected 1 Ack message");
    assert_eq!(trade_count, 2, "Expected 2 Trade messages");
    
    Ok(())
}
