/// Example of loading and using Avro schemas in Rust
/// 
/// This module shows how to load schemas from files and use them
/// with the apache_avro crate for serialization and deserialization.

use apache_avro::{Schema, types::Value as AvroValue, from_avro_datum, to_avro_datum};
use serde::{Serialize, Deserialize};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::collections::HashMap;

/// Root project directory
fn project_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir).to_path_buf()
}

/// Schemas directory
fn schemas_dir() -> PathBuf {
    project_root().join("schemas")
}

/// Get path to a schema file
fn get_schema_path(model_name: &str) -> PathBuf {
    schemas_dir().join(format!("{}.avsc", model_name.to_lowercase()))
}

/// Load a schema from a file
fn load_schema(model_name: &str) -> Result<Schema, Box<dyn std::error::Error>> {
    let schema_path = get_schema_path(model_name);
    
    // Open file
    let mut file = File::open(&schema_path)
        .map_err(|e| format!("Failed to open schema file {}: {}", schema_path.display(), e))?;
    
    // Read file contents
    let mut schema_json = String::new();
    file.read_to_string(&mut schema_json)
        .map_err(|e| format!("Failed to read schema file {}: {}", schema_path.display(), e))?;
    
    // Parse schema
    let schema = Schema::parse_str(&schema_json)
        .map_err(|e| format!("Failed to parse schema from {}: {}", schema_path.display(), e))?;
    
    Ok(schema)
}

/// List all available schemas
fn list_available_schemas() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let schemas_dir = schemas_dir();
    
    if !schemas_dir.exists() {
        return Ok(vec![]);
    }
    
    let mut schema_names = Vec::new();
    
    for entry in std::fs::read_dir(schemas_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() && path.extension().map_or(false, |ext| ext == "avsc") {
            if let Some(stem) = path.file_stem() {
                if let Some(name) = stem.to_str() {
                    schema_names.push(name.to_string());
                }
            }
        }
    }
    
    Ok(schema_names)
}

/// Example of how to use a schema with a Rust structure
#[derive(Debug, Serialize, Deserialize)]
struct AckData {
    order_id: String,
    client_order_id: Option<i32>,
    instrument_name: String,
    direction: String,
    price: Option<f64>,
    amount: f64,
    filled_amount: f64,
    remaining_amount: f64,
    status: String,
    order_type: String,
    time_in_force: String,
    change_reason: String,
    delete_reason: Option<String>,
    insert_reason: Option<String>,
    create_time: f64,
    persistent: bool,
}

/// Example of serializing data using a schema
fn serialize_example() -> Result<(), Box<dyn std::error::Error>> {
    // Load the schema
    let schema = load_schema("ack")?;
    println!("Loaded schema: {}", schema.name());
    
    // Create sample data
    let ack = AckData {
        order_id: "123456".to_string(),
        client_order_id: Some(42),
        instrument_name: "BTC-PERPETUAL".to_string(),
        direction: "buy".to_string(),
        price: Some(95000.0),
        amount: 1.0,
        filled_amount: 0.5,
        remaining_amount: 0.5,
        status: "partially_filled".to_string(),
        order_type: "limit".to_string(),
        time_in_force: "good_till_cancelled".to_string(),
        change_reason: "fill".to_string(),
        delete_reason: None,
        insert_reason: None,
        create_time: 1650000000.0,
        persistent: false,
    };
    
    // Serialize to Avro
    let avro_value = serde_json::to_value(&ack)?;
    let avro_datum = to_avro_datum(&schema, avro_value)?;
    
    println!("Serialized data size: {} bytes", avro_datum.len());
    
    // Deserialize from Avro
    let reader = from_avro_datum(&schema, &mut &avro_datum[..])?;
    
    // Convert back to our struct
    let decoded_ack: AckData = serde_json::from_value(reader)?;
    println!("Deserialized: {:?}", decoded_ack);
    
    Ok(())
}

/// Main function showing examples
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // List available schemas
    let schemas = list_available_schemas()?;
    println!("Available schemas: {:?}", schemas);
    
    // Example of serializing and deserializing
    serialize_example()?;
    
    Ok(())
}
