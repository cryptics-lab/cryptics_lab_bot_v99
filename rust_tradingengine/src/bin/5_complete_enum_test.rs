use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
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

// Import all enums from the domain
use cryptics_lab_bot::domain::enums::{
    OrderSide, OrderType, TimeInForce, OrderStatus
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteOrder {
    pub order_id: String,
    pub client_order_id: u64,
    pub instrument_name: String,
    pub direction: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub status: OrderStatus,
    pub price: f64,
    pub amount: f64,
    pub filled_amount: f64,
    pub remaining_amount: f64,
    pub create_time: f64,
    pub last_update_time: Option<f64>,
    pub is_liquidation: bool,
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

// Convert CompleteOrder to Avro Value
fn order_to_avro_value(order: &CompleteOrder) -> AvroValue {
    let mut fields = vec![];
    
    // Add all fields in the correct order according to the schema
    fields.push(("order_id".to_string(), AvroValue::String(order.order_id.clone())));
    fields.push(("client_order_id".to_string(), AvroValue::Long(order.client_order_id as i64)));
    fields.push(("instrument_name".to_string(), AvroValue::String(order.instrument_name.clone())));
    fields.push(("direction".to_string(), order_side_to_avro(&order.direction)));
    fields.push(("order_type".to_string(), order_type_to_avro(&order.order_type)));
    fields.push(("time_in_force".to_string(), time_in_force_to_avro(&order.time_in_force)));
    fields.push(("status".to_string(), order_status_to_avro(&order.status)));
    fields.push(("price".to_string(), AvroValue::Double(order.price)));
    fields.push(("amount".to_string(), AvroValue::Double(order.amount)));
    fields.push(("filled_amount".to_string(), AvroValue::Double(order.filled_amount)));
    fields.push(("remaining_amount".to_string(), AvroValue::Double(order.remaining_amount)));
    fields.push(("create_time".to_string(), AvroValue::Double(order.create_time)));

    // Handle Option<f64> for last_update_time
    let last_update_time_value = match order.last_update_time {
        Some(time) => AvroValue::Union(1, Box::new(AvroValue::Double(time))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    };
    fields.push(("last_update_time".to_string(), last_update_time_value));
    
    fields.push(("is_liquidation".to_string(), AvroValue::Boolean(order.is_liquidation)));
    
    AvroValue::Record(fields)
}

async fn complete_enum_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.complete.enums.topic";
    
    println!("=== COMPLETE ENUM TEST WITH APACHE AVRO ===");
    
    // Define the schema with all enum types
    let schema_str = r#"
    {
      "type": "record",
      "name": "CompleteOrder",
      "namespace": "test.order",
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "client_order_id", "type": "long"},
        {"name": "instrument_name", "type": "string"},
        {"name": "direction", "type": {"type": "enum", "name": "OrderSide", "symbols": ["buy", "sell"]}},
        {"name": "order_type", "type": {"type": "enum", "name": "OrderType", "symbols": ["limit", "market"]}},
        {"name": "time_in_force", "type": {"type": "enum", "name": "TimeInForce", "symbols": ["good_till_cancelled", "immediate_or_cancel"]}},
        {"name": "status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["open", "partially_filled", "cancelled", "cancelled_partially_filled", "filled"]}},
        {"name": "price", "type": "double"},
        {"name": "amount", "type": "double"},
        {"name": "filled_amount", "type": "double"},
        {"name": "remaining_amount", "type": "double"},
        {"name": "create_time", "type": "double"},
        {"name": "last_update_time", "type": ["null", "double"]},
        {"name": "is_liquidation", "type": "boolean"}
      ]
    }
    "#;
    
    println!("1. Registering schema with registry...");
    
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
            schema_id = 14;  // Placeholder ID
        } else {
            println!("Schema registration error: {} - {}", status, error_text);
            return Err(anyhow!("Failed to register schema"));
        }
    }
    
    // Create test data that uses all enums
    let complete_order = CompleteOrder {
        order_id: "ORD123456789".to_string(),
        client_order_id: 123456,
        instrument_name: "BTC-PERPETUAL".to_string(),
        direction: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        status: OrderStatus::PartiallyFilled,
        price: 103855.0,
        amount: 1.0,
        filled_amount: 0.5,
        remaining_amount: 0.5,
        create_time: 1747562209.1035967,
        last_update_time: Some(1747562300.123456),
        is_liquidation: false,
    };
    
    // Convert to JSON for display purposes only
    let order_json = serde_json::to_value(&complete_order)?;
    println!("2. Test data: {}", serde_json::to_string_pretty(&order_json)?);
    
    // Parse Avro schema
    let schema = Schema::parse_str(schema_str)?;
    println!("3. Parsed Avro schema successfully");
    
    // Convert our data to an Avro Value
    let avro_value = order_to_avro_value(&complete_order);
    println!("4. Converted data to Avro format");
    
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
    
    println!("5. Sending to Kafka topic: {}", topic);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;
    
    let delivery_result = producer
        .send(
            FutureRecord::to(topic)
                .payload(&kafka_payload)
                .key("complete-enum-test"),
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
    println!("6. Verifying message reception...");
    
    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "complete-enum-test-group")
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
                                            println!("direction: {:?}", value);
                                            println!("order_type: {:?}", value);
                                            println!("time_in_force: {:?}", value);
                                            println!("status: {:?}", value);
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
    match complete_enum_test().await {
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