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

async fn kafka_producer_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let topic_prefix = "test.kafka.producer";
    
    println!("=== KAFKA PRODUCER TEST ===");
    
    // Create topic map
    let mut topics = HashMap::new();
    topics.insert("ack".to_string(), format!("{}.ack", topic_prefix));
    
    // Initialize schema helper to verify schemas
    let _schema_helper = SchemaHelper::new(schema_dir.to_string());
    println!("1. Schema helper initialized");
    
    // Create sample JSON data for first test
    let json_data1 = json!({
        "amount": 0.2,
        "change_reason": "insert",
        "client_order_id": 100,
        "create_time": 1747562209.1035967,
        "delete_reason": null,
        "direction": "buy",
        "filled_amount": 0.0,
        "insert_reason": "client_request",
        "instrument_name": "BTC-PERPETUAL",
        "order_id": "001F39C500000000",
        "order_type": "limit",
        "persistent": false,
        "price": 103855.0,
        "remaining_amount": 0.2,
        "status": "open",
        "time_in_force": "good_till_cancelled"
    });
    
    // Use the ThaleParser to parse the JSON data into an Ack
    let ack1 = ThaleParser::parse_ack_json(&json_data1)?;
    println!("2. Successfully parsed JSON into Ack:");
    println!("   - Order ID: {}", ack1.order_id);
    println!("   - Direction: {:?}", ack1.direction);
    println!("   - Status: {:?}", ack1.status);
    
    // First producer: Create the producer, which will check if topic exists, and if not, create it with schema
    println!("3. Creating first producer (will create topic and register schema)...");
    let producer1 = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic name
    let ack_topic = producer1.get_topic("ack");
    println!("   Using topic: {}", ack_topic);
    
    // Check if topic exists in schema registry
    println!("4. Checking if topic exists in schema registry...");
    match producer1.get_schema_id(&ack_topic).await {
        Ok(schema_id) => {
            println!("   Topic exists with schema ID: {}", schema_id);
        },
        Err(_) => {
            println!("   Topic doesn't exist yet, will be created with schema");
        }
    }
    
    // Create topic with schema (or retrieve existing schema ID if it exists)
    let (topic, schema_id1) = producer1.create_topic_with_schema("ack").await?;
    println!("5. Got topic with schema ID: {}", schema_id1);
    
    // Publish the first Ack
    println!("6. Publishing first Ack message to topic...");
    producer1.publish_ack(&ack1, &topic, schema_id1).await?;
    println!("   First message published successfully");
    
    // Verify consumption of first message
    println!("7. Verifying first message reception...");
    
    // Create consumer
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-producer-test-group-1")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to the topic
    consumer1.subscribe(&[&topic])?;
    
    println!("   Waiting for first message (5 seconds)...");
    
    // Try to consume the first message
    match tokio::time::timeout(
        Duration::from_secs(5),
        consumer1.recv()
    ).await {
        Ok(result) => {
            match result {
                Ok(message) => {
                    println!("   Received first message!");
                    if let Some(payload) = message.payload() {
                        println!("   Payload length: {} bytes", payload.len());
                        
                        // Validate that the payload follows the expected format
                        if payload.len() >= 5 && payload[0] == 0 {
                            let received_schema_id = i32::from_be_bytes([
                                payload[1], payload[2], payload[3], payload[4]
                            ]);
                            
                            println!("   Received message with schema ID: {}", received_schema_id);
                            println!("   First message verified successfully!");
                        } else {
                            println!("   Received payload doesn't match expected format");
                        }
                    } else {
                        println!("   Empty payload");
                    }
                },
                Err(e) => {
                    println!("   Error consuming first message: {}", e);
                }
            }
        },
        Err(_) => {
            println!("   Timeout waiting for first message");
        }
    }
    
    // Create a second producer that should find the existing topic and schema
    println!("\n8. Creating second producer (should find existing schema)...");
    let producer2 = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get the schema ID from the registry (should work this time)
    let schema_id2 = producer2.get_schema_id(&topic).await?;
    println!("9. Retrieved schema ID from registry: {}", schema_id2);
    
    // Create different data for the second message
    let json_data2 = json!({
        "amount": 0.5,
        "change_reason": "amend",
        "client_order_id": 101,
        "create_time": 1747562219.5321,
        "delete_reason": null,
        "direction": "sell",
        "filled_amount": 0.1,
        "insert_reason": "client_request",
        "instrument_name": "BTC-PERPETUAL",
        "order_id": "001F39C500000001",
        "order_type": "limit",
        "persistent": true,
        "price": 103900.0,
        "remaining_amount": 0.4,
        "status": "partially_filled",
        "time_in_force": "good_till_cancelled"
    });
    
    // Parse the second JSON data into an Ack
    let ack2 = ThaleParser::parse_ack_json(&json_data2)?;
    println!("10. Successfully parsed second JSON into Ack:");
    println!("    - Order ID: {}", ack2.order_id);
    println!("    - Direction: {:?}", ack2.direction);
    println!("    - Status: {:?}", ack2.status);
    
    // Publish the second Ack
    println!("11. Publishing second Ack message to topic...");
    producer2.publish_ack(&ack2, &topic, schema_id2).await?;
    println!("    Second message published successfully");
    
    // Verify consumption of second message
    println!("12. Verifying second message reception...");
    
    // Create another consumer with a different group ID
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-producer-test-group-2")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Subscribe to the topic
    consumer2.subscribe(&[&topic])?;
    
    println!("    Waiting for second message (5 seconds)...");
    
    // Read two messages (the first one and the second one)
    let mut message_count = 0;
    let mut last_message_data: Option<Vec<u8>> = None;
    
    for _ in 0..2 {
        match tokio::time::timeout(
            Duration::from_secs(5),
            consumer2.recv()
        ).await {
            Ok(result) => {
                match result {
                    Ok(message) => {
                        message_count += 1;
                        if let Some(payload) = message.payload() {
                            println!("    Received message #{}: {} bytes", message_count, payload.len());
                            
                            // Store the last message for verification
                            if message_count == 2 {
                                last_message_data = Some(payload.to_vec());
                            }
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
    
    // Verify we got both messages
    if message_count == 2 {
        println!("13. Successfully received both messages!");
        
        // Decode the second message
        if let Some(payload) = last_message_data {
            if payload.len() >= 5 && payload[0] == 0 {
                let received_schema_id = i32::from_be_bytes([
                    payload[1], payload[2], payload[3], payload[4]
                ]);
                
                println!("14. Second message uses schema ID: {}", received_schema_id);
                
                // Get the schema from the registry
                let schema_url = format!("{}/schemas/ids/{}", schema_registry_url, received_schema_id);
                let client = reqwest::Client::new();
                let response = client.get(&schema_url).send().await?;
                
                if response.status().is_success() {
                    let schema_response = response.json::<serde_json::Value>().await?;
                    let schema_str = schema_response["schema"].as_str().unwrap();
                    
                    // Parse the schema
                    let schema = Schema::parse_str(schema_str)?;
                    
                    // Decode the Avro data
                    let avro_data = &payload[5..];
                    match Reader::with_schema(&schema, &avro_data[..]) {
                        Ok(mut reader) => {
                            if let Some(Ok(value)) = reader.next() {
                                println!("15. Successfully decoded second message data:");
                                println!("    ----------");
                                match &value {
                                    apache_avro::types::Value::Record(fields) => {
                                        for (field_name, field_value) in fields {
                                            if field_name == "order_id" || 
                                               field_name == "direction" || 
                                               field_name == "status" ||
                                               field_name == "price" ||
                                               field_name == "amount" ||
                                               field_name == "filled_amount" {
                                                println!("    {}: {:?}", field_name, field_value);
                                            }
                                        }
                                    },
                                    _ => println!("    Unexpected value type: {:?}", value)
                                }
                                println!("    ----------");
                                
                                // Check if this is indeed the second message we sent
                                match &value {
                                    apache_avro::types::Value::Record(fields) => {
                                        let mut order_id = "";
                                        let mut status = "";
                                        
                                        for (field_name, field_value) in fields {
                                            if field_name == "order_id" {
                                                if let apache_avro::types::Value::String(s) = field_value {
                                                    order_id = s;
                                                }
                                            } else if field_name == "status" {
                                                if let apache_avro::types::Value::Enum(_, s) = field_value {
                                                    status = s;
                                                }
                                            }
                                        }
                                        
                                        if order_id == "001F39C500000001" && status == "partially_filled" {
                                            println!("16. Verified this is the second message we sent (partially_filled order)!");
                                        } else {
                                            println!("16. Warning: This doesn't match our second message.");
                                        }
                                    },
                                    _ => println!("    Not a record value")
                                }
                            } else {
                                println!("    Could not read value from Avro reader");
                            }
                        },
                        Err(e) => {
                            println!("    Error decoding Avro data: {:?}", e);
                        }
                    }
                }
            }
        }
    } else {
        println!("13. Didn't receive both messages, only got {}", message_count);
    }
    
    println!("=== TEST COMPLETE ===");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match kafka_producer_test().await {
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