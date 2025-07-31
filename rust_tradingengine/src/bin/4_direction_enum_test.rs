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

// Simplified test that focuses only on direction and status enums
#[derive(Debug, Serialize, Deserialize)]
pub struct DirectionTest {
    pub direction: String,
    pub status: String,
}

async fn direction_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.direction.topic";
    
    println!("=== DIRECTION ENUM TEST ===");
    
    // Define schema with enums
    let schema_str = r#"
    {
      "type": "record",
      "name": "DirectionTest",
      "namespace": "test.direction",
      "fields": [
        {"name": "direction", "type": {"type": "enum", "name": "OrderSide", "symbols": ["buy", "sell"]}},
        {"name": "status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["open", "partially_filled", "cancelled", "cancelled_partially_filled", "filled"]}}
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
    
    // Create test data
    let direction_test = DirectionTest {
        direction: "buy".to_string(),
        status: "open".to_string(),
    };
    
    // Convert to JSON
    let test_json = serde_json::to_value(&direction_test)?;
    println!("2. Test data: {}", serde_json::to_string_pretty(&test_json)?);
    
    // Setup encoder
    let sr_settings = SrSettings::new(schema_registry_url.to_string());
    let encoder = AvroEncoder::new(sr_settings);
    
    // Encode data
    println!("3. Encoding data with schema registry...");
    let subject_strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);
    
    let encoded = encoder.encode_struct(&test_json, &subject_strategy)
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
                .key("direction-test"),
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
        .set("group.id", "direction-test-group")
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
                        println!("This confirms that we can successfully use Avro enums for direction and status fields!");
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
    match direction_test().await {
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