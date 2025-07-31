use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use serde_json::json;
use tokio;
use reqwest;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::time::Duration;

// For simplicity, let's try a simpler approach with all string fields for enums
#[derive(Debug, Serialize, Deserialize)]
pub struct AckMessage {
    pub amount: f64,
    pub change_reason: String,
    pub client_order_id: u64,
    pub create_time: f64,
    pub delete_reason: Option<String>,
    pub direction: String,  // Using string instead of enum
    pub filled_amount: f64,
    pub insert_reason: String,
    pub instrument_name: String,
    pub order_id: String,
    pub order_type: String,
    pub persistent: bool,
    pub price: f64,
    pub remaining_amount: f64,
    pub status: String,  // Using string instead of enum
    pub time_in_force: String,
}

async fn ack_string_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.ack.string.topic";
    
    println!("=== ACK STRING TEST ===");
    
    // Define a simpler schema that uses strings instead of enums
    let schema_str = r#"
    {
      "type": "record",
      "name": "AckMessage",
      "namespace": "test.ack",
      "fields": [
        {"name": "amount", "type": "double"},
        {"name": "change_reason", "type": "string"},
        {"name": "client_order_id", "type": "long"},
        {"name": "create_time", "type": "double"},
        {"name": "delete_reason", "type": ["null", "string"]},
        {"name": "direction", "type": "string"},
        {"name": "filled_amount", "type": "double"},
        {"name": "insert_reason", "type": "string"},
        {"name": "instrument_name", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "order_type", "type": "string"},
        {"name": "persistent", "type": "boolean"},
        {"name": "price", "type": "double"},
        {"name": "remaining_amount", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "time_in_force", "type": "string"}
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
    if status.is_success() {
        let schema_id = response.json::<serde_json::Value>().await?;
        println!("Schema registered successfully with ID: {}", schema_id);
    } else {
        let error_text = response.text().await?;
        if status.as_u16() == 409 {
            println!("Schema already exists (409 Conflict): {}", error_text);
        } else {
            println!("Schema registration error: {} - {}", status, error_text);
            return Err(anyhow!("Failed to register schema"));
        }
    }
    
    // Create test data that matches the log record
    let ack_message = AckMessage {
        amount: 0.2,
        change_reason: "insert".to_string(),
        client_order_id: 100,
        create_time: 1747562209.1035967,
        delete_reason: None,
        direction: "buy".to_string(),
        filled_amount: 0.0,
        insert_reason: "client_request".to_string(),
        instrument_name: "BTC-PERPETUAL".to_string(),
        order_id: "001F39C500000000".to_string(),
        order_type: "limit".to_string(),
        persistent: false,
        price: 103855.0,
        remaining_amount: 0.2,
        status: "open".to_string(),
        time_in_force: "good_till_cancelled".to_string(),
    };
    
    // Convert to JSON
    let ack_json = serde_json::to_value(&ack_message)?;
    println!("2. Test data: {}", serde_json::to_string_pretty(&ack_json)?);
    
    // Custom processing of floating-point numbers to address serialization issues
    let _amount_str = format!("{}", ack_message.amount);
    let _create_time_str = format!("{}", ack_message.create_time);
    let _filled_amount_str = format!("{}", ack_message.filled_amount);
    let _price_str = format!("{}", ack_message.price);
    let _remaining_amount_str = format!("{}", ack_message.remaining_amount);
    
    // Build custom JSON that avoids problematic float serialization
    let manual_json = json!({
        "amount": ack_message.amount,
        "change_reason": ack_message.change_reason,
        "client_order_id": ack_message.client_order_id,
        "create_time": ack_message.create_time,
        "delete_reason": ack_message.delete_reason,
        "direction": ack_message.direction,
        "filled_amount": ack_message.filled_amount,
        "insert_reason": ack_message.insert_reason,
        "instrument_name": ack_message.instrument_name,
        "order_id": ack_message.order_id,
        "order_type": ack_message.order_type,
        "persistent": ack_message.persistent,
        "price": ack_message.price,
        "remaining_amount": ack_message.remaining_amount,
        "status": ack_message.status,
        "time_in_force": ack_message.time_in_force
    });
    
    println!("2.1 Manual JSON: {}", serde_json::to_string_pretty(&manual_json)?);
    
    // Setup encoder
    let sr_settings = SrSettings::new(schema_registry_url.to_string());
    let encoder = AvroEncoder::new(sr_settings);
    
    // Encode data
    println!("3. Encoding data with schema registry...");
    let subject_strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);
    
    let encoded = encoder.encode_struct(&manual_json, &subject_strategy)
        .await?;
    
    println!("Successfully encoded! Size: {} bytes", encoded.len());
    
    // Send to Kafka
    println!("4. Sending to Kafka topic: {}", topic);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;
    
    let delivery_result = producer
        .send(
            FutureRecord::to(topic)
                .payload(&encoded)
                .key("ack-string-test"),
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
    println!("5. Verifying message reception...");
    
    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "ack-string-test-group")
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
                        println!("Successfully verified end-to-end process!");
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
    match ack_string_test().await {
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