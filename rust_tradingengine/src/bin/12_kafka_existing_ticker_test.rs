use anyhow::{anyhow, Result};
use serde_json::json;
use tokio;
use std::collections::HashMap;
use std::time::Duration;
use chrono;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use rdkafka::config::RDKafkaLogLevel;

use apache_avro::{Schema, Reader};

// Import the KafkaProducer and domain types
use cryptics_lab_bot::infrastructure::kafka::KafkaProducer;
use cryptics_lab_bot::infrastructure::kafka::helper::SchemaHelper;
use cryptics_lab_bot::domain::model::ticker::Ticker;

async fn existing_ticker_topic_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let schema_dir = "../../schemas";
    let existing_ticker_topic = "cryptics.thalex.ticker.avro";
    
    println!("=== KAFKA EXISTING TICKER TOPIC TEST ===");
    
    // Create topic map with the existing topic
    let mut topics = HashMap::new();
    topics.insert("ticker".to_string(), existing_ticker_topic.to_string());
    
    // Initialize schema helper to verify schemas
    let _schema_helper = SchemaHelper::new(schema_dir.to_string());
    println!("1. Schema helper initialized");
    
    // Create sample JSON data for ticker (using data from error logs)
    let json_data = json!({
        "instrument_name": "BTC-PERPETUAL",
        "mark_price": 104904.59553398524,
        "mark_timestamp": 1747578855.2247293,
        "best_bid_price": 104901.0,
        "best_bid_amount": 0.028,
        "best_ask_price": 104940.0,
        "best_ask_amount": 0.3,
        "last_price": 104913.0,
        "index": 104916.49833333334, // Note: using 'index' instead of 'index_price' for from_json method
        "funding_rate": 0.0,
        "open_interest": 3740.212,
        // Additional fields with default values
        "delta": 0.0,
        "volume_24h": 0.0,
        "value_24h": 0.0,
        "low_price_24h": 0.0,
        "high_price_24h": 0.0,
        "change_24h": 0.0,
        "forward": 0.0,
        "funding_mark": 0.0,
        "collar_low": 0.0,
        "collar_high": 0.0,
        "realised_funding_24h": 0.0,
        "average_funding_rate_24h": 0.0
    });
    
    // Create Ticker from JSON
    let ticker = Ticker::from_json(&json_data, "BTC-PERPETUAL".to_string())?;
    println!("2. Successfully created Ticker from JSON:");
    println!("   - Instrument: {}", ticker.instrument_name);
    println!("   - Mark Price: {}", ticker.mark_price);
    println!("   - Index Price: {}", ticker.index_price);
    println!("   - Last Price: {}", ticker.last_price);
    
    // Create producer that will use the existing topic
    println!("3. Creating producer with existing topic...");
    let producer = KafkaProducer::new(bootstrap_servers, schema_registry_url, topics.clone(), schema_dir.to_string()).await?;
    
    // Get topic name (should be the existing topic)
    let ticker_topic = producer.get_topic("ticker");
    println!("   Using existing topic: {}", ticker_topic);
    
    // Check if topic exists in schema registry
    println!("4. Checking for existing topic in schema registry...");
    match producer.get_schema_id(&ticker_topic).await {
        Ok(schema_id) => {
            println!("   Topic exists with schema ID: {}", schema_id);
            
            // Get schema from registry
            let schema_url = format!("{}/schemas/ids/{}", schema_registry_url, schema_id);
            let client = reqwest::Client::new();
            let response = client.get(&schema_url).send().await?;
            
            if response.status().is_success() {
                let schema_response = response.json::<serde_json::Value>().await?;
                let schema_str = schema_response["schema"].as_str().unwrap();
                println!("   Retrieved schema from registry:");
                println!("   ----------");
                println!("   {}", schema_str);
                println!("   ----------");
            }
            
            // Publish the ticker using the existing topic and schema ID
            println!("5. Publishing ticker message to existing topic...");
            
            // First approach: Using the create_topic_with_schema method
            // This should detect the existing topic and get its schema ID
            let (topic1, schema_id1) = producer.create_topic_with_schema("ticker").await?;
            println!("   create_topic_with_schema returned topic: {}, schema ID: {}", topic1, schema_id1);
            
            if topic1 == ticker_topic && schema_id1 == schema_id {
                println!("   ✓ Successfully retrieved existing topic and schema ID");
            } else {
                println!("   ⚠ Warning: Mismatch in topic or schema ID!");
            }
            
            // Second approach: Direct approach with get_schema_id and publish_ack
            let schema_id2 = producer.get_schema_id(&ticker_topic).await?;
            println!("   Direct get_schema_id returned schema ID: {}", schema_id2);
            
            // Create a different ticker for this approach
            let ticker2 = Ticker {
                instrument_name: "BTC-PERPETUAL".to_string(),
                mark_price: 104950.0,
                mark_timestamp: 1747578900.0,
                best_bid_price: 104925.0,
                best_bid_amount: 0.05,
                best_ask_price: 104975.0,
                best_ask_amount: 0.2,
                last_price: 104950.0,
                delta: 0.0,
                volume_24h: 100.0,
                value_24h: 10000000.0,
                low_price_24h: 104000.0,
                high_price_24h: 105000.0,
                change_24h: 0.5,
                index_price: 104960.0,
                forward: 0.0, 
                funding_mark: 0.0,
                funding_rate: 0.0001,
                collar_low: 0.0,
                collar_high: 0.0,
                realised_funding_24h: 0.0,
                average_funding_rate_24h: 0.0,
                open_interest: 3745.0,
                processing_timestamp: Some(chrono::Utc::now().timestamp_millis() as f64), // Added missing field as Option
            };
            
            // Publish using the ticker's send_ticker method
            println!("6. Publishing to existing topic using direct method...");
            match producer.send_ticker(&ticker2).await {
                Ok(_) => println!("   Ticker published successfully to existing topic!"),
                Err(e) => println!("   Error publishing ticker: {}", e),
            }
            
            // Verify consumption of the message
            println!("7. Verifying message reception from existing topic...");
            
            // Create consumer
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", "existing-ticker-test-group")
                .set("bootstrap.servers", bootstrap_servers)
                .set("auto.offset.reset", "latest") // latest to only get new messages
                .set("enable.auto.commit", "false")
                .set_log_level(RDKafkaLogLevel::Debug)
                .create()?;
            
            // Subscribe to the topic
            consumer.subscribe(&[&ticker_topic])?;
            
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
                                    
                                    // Verify schema ID matches
                                    if received_schema_id == schema_id {
                                        println!("   ✓ Schema ID in message matches the expected schema ID");
                                    } else {
                                        println!("   ⚠ Warning: Schema ID mismatch! Expected {}, got {}", 
                                                 schema_id, received_schema_id);
                                    }
                                    
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
                                                                // Try to display as many fields as possible
                                                                println!("   {}: {:?}", field_name, field_value);
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
        },
        Err(e) => {
            println!("   Error: Topic doesn't exist or couldn't get schema ID: {}", e);
        }
    }
    
    println!("=== TEST COMPLETE ===");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match existing_ticker_topic_test().await {
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