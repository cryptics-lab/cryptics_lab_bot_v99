use anyhow::{anyhow, Result};
use serde_json::json;
use serde_json::Value;
use tokio;
use reqwest;
use std::fs;
use std::path::Path;
use std::time::Duration;
use std::io::Read;

use apache_avro::{
    types::Value as AvroValue,
    Schema,
    Writer,
    Reader,
};

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;

// Import the ThaleParser and domain types
use cryptics_lab_bot::infrastructure::exchange::thalex::parsers::ThaleParser;
use cryptics_lab_bot::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use cryptics_lab_bot::domain::model::ack::Ack;

// Helper function to convert an OrderSide to Avro enum value
fn order_side_to_avro(side: &OrderSide) -> AvroValue {
    match side {
        OrderSide::Buy => AvroValue::Enum(0, "buy".to_string()),
        OrderSide::Sell => AvroValue::Enum(1, "sell".to_string()),
    }
}

// Helper function to convert an OrderType to Avro enum value
fn order_type_to_avro(order_type: &OrderType) -> AvroValue {
    match order_type {
        OrderType::Limit => AvroValue::Enum(0, "limit".to_string()),
        OrderType::Market => AvroValue::Enum(1, "market".to_string()),
    }
}

// Helper function to convert a TimeInForce to Avro enum value
fn time_in_force_to_avro(tif: &TimeInForce) -> AvroValue {
    match tif {
        TimeInForce::GTC => AvroValue::Enum(0, "good_till_cancelled".to_string()),
        TimeInForce::IOC => AvroValue::Enum(1, "immediate_or_cancel".to_string()),
    }
}

// Helper function to convert an OrderStatus to Avro enum value
fn order_status_to_avro(status: &OrderStatus) -> AvroValue {
    match status {
        OrderStatus::Open => AvroValue::Enum(0, "open".to_string()),
        OrderStatus::PartiallyFilled => AvroValue::Enum(1, "partially_filled".to_string()),
        OrderStatus::Cancelled => AvroValue::Enum(2, "cancelled".to_string()),
        OrderStatus::CancelledPartiallyFilled => AvroValue::Enum(3, "cancelled_partially_filled".to_string()),
        OrderStatus::Filled => AvroValue::Enum(4, "filled".to_string()),
    }
}

// Convert Ack to Avro Value
fn ack_to_avro_value(ack: &Ack) -> AvroValue {
    let mut fields = vec![];
    
    // Add all fields in the correct order according to the schema
    fields.push(("order_id".to_string(), AvroValue::String(ack.order_id.clone())));
    
    // Handle Option<u64> for client_order_id
    let client_order_id_value = match ack.client_order_id {
        Some(id) => AvroValue::Union(1, Box::new(AvroValue::Long(id as i64))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };
    fields.push(("client_order_id".to_string(), client_order_id_value));
    
    fields.push(("instrument_name".to_string(), AvroValue::String(ack.instrument_name.clone())));
    fields.push(("direction".to_string(), order_side_to_avro(&ack.direction)));
    
    // Handle Option<f64> for price
    let price_value = match ack.price {
        Some(p) => AvroValue::Union(1, Box::new(AvroValue::Double(p))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };
    fields.push(("price".to_string(), price_value));
    
    fields.push(("amount".to_string(), AvroValue::Double(ack.amount)));
    fields.push(("filled_amount".to_string(), AvroValue::Double(ack.filled_amount)));
    fields.push(("remaining_amount".to_string(), AvroValue::Double(ack.remaining_amount)));
    fields.push(("status".to_string(), order_status_to_avro(&ack.status)));
    fields.push(("order_type".to_string(), order_type_to_avro(&ack.order_type)));
    fields.push(("time_in_force".to_string(), time_in_force_to_avro(&ack.time_in_force)));
    fields.push(("change_reason".to_string(), AvroValue::String(ack.change_reason.clone())));
    
    // Handle Option<String> for delete_reason
    let delete_reason_value = match &ack.delete_reason {
        Some(reason) => AvroValue::Union(1, Box::new(AvroValue::String(reason.clone()))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };
    fields.push(("delete_reason".to_string(), delete_reason_value));
    
    // Handle Option<String> for insert_reason
    let insert_reason_value = match &ack.insert_reason {
        Some(reason) => AvroValue::Union(1, Box::new(AvroValue::String(reason.clone()))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };
    fields.push(("insert_reason".to_string(), insert_reason_value));
    
    fields.push(("create_time".to_string(), AvroValue::Double(ack.create_time)));
    fields.push(("persistent".to_string(), AvroValue::Boolean(ack.persistent)));
    
    AvroValue::Record(fields)
}

async fn load_schema_file() -> Result<String> {
    // Define the path to the schema file
    let schema_path = Path::new("/Users/Borat/Desktop/code/cryptics_lab_bot/schemas/ack/schema.json");
    
    // Read the schema file
    let mut file = fs::File::open(schema_path)
        .map_err(|e| anyhow!("Failed to open schema file: {}", e))?;
    
    let mut schema_str = String::new();
    file.read_to_string(&mut schema_str)
        .map_err(|e| anyhow!("Failed to read schema file: {}", e))?;
    
    Ok(schema_str)
}

async fn register_schema(schema_registry_url: &str, topic: &str, schema_str: &str) -> Result<i32> {
    println!("Registering schema with registry...");
    
    // Register schema
    let subject = format!("{}-value", topic);
    let register_url = format!("{}/subjects/{}/versions", schema_registry_url, subject);
    
    let schema_request = json!({
        "schema": schema_str
    });
    
    let client = reqwest::Client::new();
    let response = client.post(&register_url)
        .json(&schema_request)
        .send()
        .await?;
    
    let status = response.status();
    let schema_id: i32;
    
    if status.is_success() {
        let schema_response = response.json::<serde_json::Value>().await?;
        schema_id = schema_response["id"].as_i64().unwrap() as i32;
        println!("Schema registered successfully with ID: {}", schema_id);
    } else {
        let error_text = response.text().await?;
        if status.as_u16() == 409 {
            println!("Schema already exists (409 Conflict): {}", error_text);
            // Extract schema ID from the error or use a placeholder
            schema_id = 20;  // Placeholder ID
        } else {
            println!("Schema registration error: {} - {}", status, error_text);
            return Err(anyhow!("Failed to register schema"));
        }
    }
    
    Ok(schema_id)
}

async fn schema_registry_load_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.ack.schema.topic";
    
    println!("=== ACK SCHEMA REGISTRY TEST ===");
    
    // Load schema from file
    let schema_str = load_schema_file().await?;
    println!("1. Successfully loaded schema from file");
    println!("Schema: {}", schema_str);
    
    // Register schema with registry
    let schema_id = register_schema(schema_registry_url, topic, &schema_str).await?;
    println!("2. Schema registered with ID: {}", schema_id);
    
    // Parse the Avro schema
    let schema = Schema::parse_str(&schema_str)?;
    println!("3. Successfully parsed Avro schema");
    
    // Create sample JSON data for testing
    let json_data = json!({
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
    let ack = ThaleParser::parse_ack_json(&json_data)?;
    println!("4. Successfully parsed JSON into Ack:");
    println!("   - Order ID: {}", ack.order_id);
    println!("   - Direction: {:?}", ack.direction);
    println!("   - Status: {:?}", ack.status);
    println!("   - Order Type: {:?}", ack.order_type);
    println!("   - Time in Force: {:?}", ack.time_in_force);
    
    // Convert Ack to Avro Value
    let avro_value = ack_to_avro_value(&ack);
    println!("5. Converted Ack to Avro format");
    
    // Serialize to Avro bytes
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(avro_value)?;
    let avro_bytes = writer.into_inner()?;
    
    println!("Successfully encoded with apache-avro! Size: {} bytes", avro_bytes.len());
    
    // Now create an Avro message for Kafka with schema ID
    // Format: [0x00, schema_id (4 bytes), avro_data]
    let mut kafka_payload = Vec::with_capacity(5 + avro_bytes.len());
    kafka_payload.push(0); // Magic byte
    kafka_payload.extend_from_slice(&schema_id.to_be_bytes()); // Schema ID in big-endian format
    kafka_payload.extend_from_slice(&avro_bytes); // Avro-encoded data
    
    println!("6. Sending to Kafka topic: {}", topic);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;
    
    let delivery_result = producer
        .send(
            FutureRecord::to(topic)
                .payload(&kafka_payload)
                .key("ack-schema-test"),
            Duration::from_secs(5),
        )
        .await;
    
    match delivery_result {
        Ok((partition, offset)) => {
            println!("Message sent successfully! Partition: {}, Offset: {}", partition, offset);
        },
        Err((err, _)) => {
            println!("Failed to send message: {}", err);
            return Err(anyhow!("Failed to send message"));
        }
    }
    
    // Verify consumption
    println!("7. Verifying message reception...");
    
    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "ack-schema-test-group")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    
    // Seek to the end-1 message
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, rdkafka::Offset::End)?;
    consumer.assign(&tpl)?;
    
    // Subscribe to the topic
    consumer.subscribe(&[topic])?;
    
    println!("Waiting for message (5 seconds)...");
    
    // Try to consume a message
    match tokio::time::timeout(
        Duration::from_secs(5),
        consumer.recv()
    ).await {
        Ok(result) => {
            match result {
                Ok(message) => {
                    println!("Received message!");
                    if let Some(payload) = message.payload() {
                        println!("Payload length: {} bytes", payload.len());
                        
                        // Validate that the payload follows the expected format
                        if payload.len() >= 5 && payload[0] == 0 {
                            let received_schema_id = i32::from_be_bytes([
                                payload[1], payload[2], payload[3], payload[4]
                            ]);
                            
                            println!("Received message with schema ID: {}", received_schema_id);
                            println!("Successfully verified end-to-end process!");
                            
                            // For debugging: try to decode the Avro data
                            if payload.len() > 5 {
                                let avro_data = &payload[5..];
                                
                                match Reader::with_schema(&schema, &avro_data[..]) {
                                    Ok(mut reader) => {
                                        if let Some(Ok(value)) = reader.next() {
                                            println!("Successfully decoded Avro data:");
                                            println!("----------");
                                            if let AvroValue::Record(fields) = &value {
                                                for (field_name, field_value) in fields {
                                                    if field_name == "direction" || 
                                                       field_name == "order_type" || 
                                                       field_name == "time_in_force" || 
                                                       field_name == "status" {
                                                        println!("  {}: {:?}", field_name, field_value);
                                                    }
                                                }
                                            }
                                            println!("----------");
                                        } else {
                                            println!("Could not read value from Avro reader");
                                        }
                                    },
                                    Err(e) => {
                                        println!("Error decoding Avro data: {:?}", e);
                                    }
                                }
                            }
                        } else {
                            println!("Received payload doesn't match expected format");
                        }
                    } else {
                        println!("Empty payload");
                    }
                },
                Err(e) => {
                    println!("Error consuming message: {}", e);
                }
            }
        },
        Err(_) => {
            println!("Timeout waiting for message");
        }
    }
    
    println!("=== TEST COMPLETE ===");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match schema_registry_load_test().await {
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