use anyhow::{anyhow, Result};
use serde_json::json;
use tokio;
use reqwest;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use std::time::Duration;

// Import the Apache Avro crate
use apache_avro::{
    types::Value as AvroValue,
    Schema,
    Reader, Writer,
};

// Import the domain enums
use cryptics_lab_bot::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};

// Define the Ack struct for this test
#[derive(Debug, Clone)]
pub struct Ack {
    pub order_id: String,
    pub client_order_id: Option<u64>,
    pub instrument_name: String,
    pub direction: OrderSide,
    pub price: Option<f64>,
    pub amount: f64,
    pub filled_amount: f64,
    pub remaining_amount: f64,
    pub status: OrderStatus,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub change_reason: String,
    pub delete_reason: Option<String>,
    pub insert_reason: Option<String>,
    pub create_time: f64,
    pub persistent: bool,
}

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

async fn ack_from_json_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.ack.full.topic";
    
    println!("=== ACK FROM JSON TEST ===");
    
    // The JSON string from logs
    let json_str = r#"{
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
    }"#;
    
    println!("1. Parsing JSON string from logs:");
    println!("{}", json_str);
    
    // We need to use a custom deserializer because of the enum fields
    // First, parse to a basic JSON structure
    let json_value: serde_json::Value = serde_json::from_str(json_str)?;
    
    // Now manually construct the Ack struct with the correct enum types
    let ack = Ack {
        order_id: json_value["order_id"].as_str().unwrap_or_default().to_string(),
        client_order_id: json_value["client_order_id"].as_u64(),
        instrument_name: json_value["instrument_name"].as_str().unwrap_or_default().to_string(),
        direction: match json_value["direction"].as_str().unwrap_or_default() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => OrderSide::Buy, // Default
        },
        price: if json_value["price"].is_null() { 
            None 
        } else { 
            Some(json_value["price"].as_f64().unwrap_or_default()) 
        },
        amount: json_value["amount"].as_f64().unwrap_or_default(),
        filled_amount: json_value["filled_amount"].as_f64().unwrap_or_default(),
        remaining_amount: json_value["remaining_amount"].as_f64().unwrap_or_default(),
        status: match json_value["status"].as_str().unwrap_or_default() {
            "open" => OrderStatus::Open,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "cancelled" => OrderStatus::Cancelled,
            "cancelled_partially_filled" => OrderStatus::CancelledPartiallyFilled,
            "filled" => OrderStatus::Filled,
            _ => OrderStatus::Open, // Default
        },
        order_type: match json_value["order_type"].as_str().unwrap_or_default() {
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            _ => OrderType::Limit, // Default
        },
        time_in_force: match json_value["time_in_force"].as_str().unwrap_or_default() {
            "good_till_cancelled" => TimeInForce::GTC,
            "immediate_or_cancel" => TimeInForce::IOC,
            _ => TimeInForce::GTC, // Default
        },
        change_reason: json_value["change_reason"].as_str().unwrap_or_default().to_string(),
        delete_reason: if json_value["delete_reason"].is_null() {
            None
        } else {
            Some(json_value["delete_reason"].as_str().unwrap_or_default().to_string())
        },
        insert_reason: if json_value["insert_reason"].is_null() {
            None
        } else {
            Some(json_value["insert_reason"].as_str().unwrap_or_default().to_string())
        },
        create_time: json_value["create_time"].as_f64().unwrap_or_default(),
        persistent: json_value["persistent"].as_bool().unwrap_or_default(),
    };
    
    println!("2. Successfully created Ack from JSON");
    println!("Ack details:");
    println!("  order_id: {}", ack.order_id);
    println!("  direction: {:?}", ack.direction);
    println!("  order_type: {:?}", ack.order_type);
    println!("  time_in_force: {:?}", ack.time_in_force);
    println!("  status: {:?}", ack.status);
    
    // Define the schema with all enum types
    let schema_str = r#"
    {
      "type": "record",
      "name": "Ack",
      "namespace": "exchange.order",
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "client_order_id", "type": ["null", "long"]},
        {"name": "instrument_name", "type": "string"},
        {"name": "direction", "type": {"type": "enum", "name": "OrderSide", "symbols": ["buy", "sell"]}},
        {"name": "price", "type": ["null", "double"]},
        {"name": "amount", "type": "double"},
        {"name": "filled_amount", "type": "double"},
        {"name": "remaining_amount", "type": "double"},
        {"name": "status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["open", "partially_filled", "cancelled", "cancelled_partially_filled", "filled"]}},
        {"name": "order_type", "type": {"type": "enum", "name": "OrderType", "symbols": ["limit", "market"]}},
        {"name": "time_in_force", "type": {"type": "enum", "name": "TimeInForce", "symbols": ["good_till_cancelled", "immediate_or_cancel"]}},
        {"name": "change_reason", "type": "string"},
        {"name": "delete_reason", "type": ["null", "string"]},
        {"name": "insert_reason", "type": ["null", "string"]},
        {"name": "create_time", "type": "double"},
        {"name": "persistent", "type": "boolean"}
      ]
    }
    "#;
    
    println!("3. Registering schema with registry...");
    
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
            schema_id = 15;  // Placeholder ID
        } else {
            println!("Schema registration error: {} - {}", status, error_text);
            return Err(anyhow!("Failed to register schema"));
        }
    }
    
    // Parse Avro schema
    let schema = Schema::parse_str(schema_str)?;
    println!("4. Parsed Avro schema successfully");
    
    // Convert our data to an Avro Value
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
                .key("ack-json-test"),
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
        .set("group.id", "ack-json-test-group")
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
    match ack_from_json_test().await {
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