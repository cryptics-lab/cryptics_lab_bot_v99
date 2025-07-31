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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleOrder {
    pub order_id: String,
    pub direction: Direction,
}

async fn enum_test() -> Result<()> {
    // Configuration
    let schema_registry_url = "http://localhost:8081";
    let bootstrap_servers = "localhost:9092";
    let topic = "test.enum.topic";
    
    println!("=== ENUM TEST ===");
    
    // Define a simple schema with one enum
    let schema_str = r#"
    {
      "type": "record",
      "name": "SimpleOrder",
      "namespace": "test.simple",
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "direction", "type": {"type": "enum", "name": "Direction", "symbols": ["buy", "sell"]}}
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
    let simple_order = SimpleOrder {
        order_id: "TEST12345".to_string(),
        direction: Direction::Buy,
    };
    
    // Convert to JSON 
    let order_json = serde_json::to_value(&simple_order)?;
    println!("2. Test data: {}", serde_json::to_string_pretty(&order_json)?);
    
    // Setup encoder
    let sr_settings = SrSettings::new(schema_registry_url.to_string());
    let encoder = AvroEncoder::new(sr_settings);
    
    // Encode data
    println!("3. Encoding data with schema registry...");
    let subject_strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);
    
    let encoded = encoder.encode_struct(&order_json, &subject_strategy)
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
                .key("enum-test"),
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
        .set("group.id", "enum-test-group")
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
    
    println!("6. Creating sink connector instructions...");
    println!("To create a sink connector consuming from this topic:");
    println!("
    {{
      \"name\": \"test-enum-sink\",
      \"config\": {{
        \"connector.class\": \"io.confluent.connect.jdbc.JdbcSinkConnector\",
        \"tasks.max\": \"1\",
        \"topics\": \"test.enum.topic\",
        \"connection.url\": \"jdbc:postgresql://postgres:5432/mydatabase\",
        \"connection.user\": \"postgres\",
        \"connection.password\": \"postgres\",
        \"auto.create\": \"true\",
        \"auto.evolve\": \"true\",
        \"insert.mode\": \"upsert\",
        \"pk.mode\": \"record_key\",
        \"pk.fields\": \"order_id\",
        \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
        \"value.converter\": \"io.confluent.connect.avro.AvroConverter\",
        \"value.converter.schema.registry.url\": \"http://schema-registry:8081\",
        \"transforms\": \"unwrap\",
        \"transforms.unwrap.type\": \"io.debezium.transforms.ExtractNewRecordState\"
      }}
    }}");

    
    println!("=== TEST COMPLETE ===");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match enum_test().await {
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