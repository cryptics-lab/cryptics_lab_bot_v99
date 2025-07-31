use anyhow::{anyhow, Result};
use serde_json::json;
use tokio;
use std::collections::HashMap;
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rdkafka::config::RDKafkaLogLevel;

use apache_avro::{Schema, Reader};

// Import the KafkaProducer and domain types
use cryptics_lab_bot::infrastructure::kafka::KafkaProducer;
use cryptics_lab_bot::infrastructure::kafka::helper::SchemaHelper;
use cryptics_lab_bot::infrastructure::exchange::thalex::parsers::ThaleParser;

async fn kafka_trade_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let topic_prefix = "test.kafka.trade";
    
    println!("=== KAFKA TRADE TEST ===");
    
    // Create topic map
    let mut topics = HashMap::new();
    topics.insert("trade".to_string(), format!("{}.trade", topic_prefix));
    topics.insert("ack".to_string(), format!("{}.ack", topic_prefix));
    
    // Initialize schema helper to verify schemas
    let _schema_helper = SchemaHelper::new(schema_dir.to_string());
    println!("1. Schema helper initialized");
    
    // Create sample JSON data for a direct trade
    let json_data1 = json!({
        "trade_id": "TR12345",
        "order_id": "ORD67890",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "price": 104750.0,
        "amount": 0.75,
        "maker_taker": "maker",
        "timestamp": 1747578855.2247293
    });
    
    // Use the ThaleParser to parse the JSON data into a Trade
    let trade = ThaleParser::parse_trade_json(&json_data1)?;
    println!("2. Successfully parsed JSON into Trade:");
    println!("   - Trade ID: {}", trade.trade_id);
    println!("   - Order ID: {}", trade.order_id);
    println!("   - Price: {}", trade.price);
    println!("   - Amount: {}", trade.amount);
    
    // First producer: Create the producer
    println!("3. Creating Kafka producer...");
    let producer = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic name
    let trade_topic = producer.get_topic("trade");
    println!("   Using topic: {}", trade_topic);
    
    // Check if topic exists in schema registry
    println!("4. Checking if topic exists in schema registry...");
    match producer.get_schema_id(&trade_topic).await {
        Ok(schema_id) => {
            println!("   Topic exists with schema ID: {}", schema_id);
        },
        Err(_) => {
            println!("   Topic doesn't exist yet, will be created with schema");
        }
    }
    
    // Create topic with schema (or retrieve existing schema ID if it exists)
    let (topic, schema_id) = producer.create_topic_with_schema("trade").await?;
    println!("5. Got topic with schema ID: {}", schema_id);
    
    // Publish the trade directly
    println!("6. Publishing trade message to topic...");
    producer.send_trade(&trade).await?;
    println!("   Trade message published successfully");
    
    // Create consumer
    println!("7. Creating consumer to verify the message...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-trade-test-group-1")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to the topic
    consumer.subscribe(&[&topic])?;
    
    println!("   Waiting for message (5 seconds)...");
    
    // Try to consume the message
    match tokio::time::timeout(
        Duration::from_secs(5),
        consumer.recv()
    ).await {
        Ok(result) => {
            match result {
                Ok(message) => {
                    println!("   Received message!");
                    if let Some(payload) = message.payload() {
                        println!("   Payload length: {} bytes", payload.len());
                        
                        // Validate that the payload follows the expected format
                        if payload.len() >= 5 && payload[0] == 0 {
                            let received_schema_id = i32::from_be_bytes([
                                payload[1], payload[2], payload[3], payload[4]
                            ]);
                            
                            println!("   Received message with schema ID: {}", received_schema_id);
                            println!("   Message verified successfully!");
                            
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
                                            println!("   Successfully decoded Avro data:");
                                            println!("   ----------");
                                            match &value {
                                                apache_avro::types::Value::Record(fields) => {
                                                    for (field_name, field_value) in fields {
                                                        if field_name == "trade_id" || 
                                                           field_name == "order_id" || 
                                                           field_name == "price" ||
                                                           field_name == "amount" {
                                                            println!("   {}: {:?}", field_name, field_value);
                                                        }
                                                    }
                                                },
                                                _ => println!("   Unexpected value type: {:?}", value)
                                            }
                                            println!("   ----------");
                                        } else {
                                            println!("   Could not read value from Avro reader");
                                        }
                                    },
                                    Err(e) => {
                                        println!("   Error decoding Avro data: {:?}", e);
                                    }
                                }
                            }
                        } else {
                            println!("   Received payload doesn't match expected format");
                        }
                    } else {
                        println!("   Empty payload");
                    }
                },
                Err(e) => {
                    println!("   Error consuming message: {}", e);
                }
            }
        },
        Err(_) => {
            println!("   Timeout waiting for message");
        }
    }
    
    // Now test extracting trades from order data with fills
    println!("\n8. Testing extraction of trades from order data with fills...");
    
    // Create sample order data with fills
    let order_data_with_fills = json!({
        "order_id": "ORD98765",
        "client_order_id": 43,
        "instrument_name": "ETH-PERPETUAL",
        "direction": "buy",
        "price": 5230.5,
        "amount": 2.5,
        "filled_amount": 1.5,
        "remaining_amount": 1.0,
        "status": "partially_filled",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "existing",
        "create_time": 1747578900.0,
        "persistent": true,
        "fills": [
            {
                "trade_id": "FILL123",
                "price": 5230.5,
                "amount": 1.0,
                "maker_taker": "taker",
                "time": 1747578910.0
            },
            {
                "trade_id": "FILL124",
                "price": 5230.5,
                "amount": 0.5,
                "maker_taker": "taker",
                "time": 1747578920.0
            }
        ]
    });
    
    // Extract trades from the order data
    let extracted_trades = ThaleParser::extract_trades_from_order(&order_data_with_fills)?;
    println!("9. Extracted {} trades from order data:", extracted_trades.len());
    for (i, trade) in extracted_trades.iter().enumerate() {
        println!("   Trade #{}: ID={}, Amount={}, Price={}", i+1, trade.trade_id, trade.amount, trade.price);
    }
    
    // Test the publish_order_notification method which should now handle trades
    println!("10. Testing publish_order_notification with order containing fills...");
    
    // Create a consumer for both ack and trade topics
    let ack_topic = producer.get_topic("ack");
    
    let consumer_all: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-trade-test-group-all")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to both topics
    consumer_all.subscribe(&[&ack_topic, &trade_topic])?;
    
    // Publish the order notification
    producer.publish_order_notification(&order_data_with_fills).await?;
    println!("    Order notification published successfully");
    
    // Count how many messages we receive (should be 1 Ack + 2 Trades = 3 messages)
    println!("11. Waiting for messages (10 seconds)...");
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
                            println!("    Received ACK message");
                        } else if topic_name == trade_topic {
                            trade_count += 1;
                            println!("    Received TRADE message");
                        }
                    },
                    Err(e) => {
                        println!("    Error consuming message: {}", e);
                    }
                }
            },
            Err(_) => {
                println!("    Timeout waiting for message");
                break;
            }
        }
    }
    
    println!("12. Message count: {} ACK(s), {} TRADE(s)", ack_count, trade_count);
    if ack_count == 1 && trade_count == 2 {
        println!("    Test PASSED! Received expected number of messages");
    } else {
        println!("    Test FAILED! Expected 1 ACK and 2 TRADE messages");
    }
    
    println!("=== TEST COMPLETE ===");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match kafka_trade_test().await {
        Ok(_) => {
            println!("Test completed successfully!");
            Ok(())
        },
        Err(e) => {
            eprintln!("Test failed: {}", e);
            if let Some(source) = e.source() {
                eprintln!("Caused by: {}", source);
            }
            Err(anyhow!("Test failed"))
        }
    }
}