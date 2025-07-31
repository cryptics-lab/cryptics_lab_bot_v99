# CrypticsLabBot Avro Schemas

This directory contains Avro schema files (`.avsc`) used by both Python and Rust code for serialization/deserialization.

## Schema Files

- `ticker.avsc` - Ticker data schema
- `ack.avsc` - Order acknowledgment data schema
- `trade.avsc` - Trade execution data schema
- `index.avsc` - Index price data schema

## Usage

### Generation

These schema files are generated from the Pydantic models defined in `python_pipeline/models/`. To regenerate them, run:

```
python generate_schemas.py
```

To also commit the schemas to git:

```
python generate_schemas.py --commit
```

### Python Usage

In Python code, schemas can be loaded directly:

```python
from python_pipeline.schemas import load_schema

# Load the schema
schema = load_schema('ack')

# Use it with Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=json.dumps(schema),
    to_dict=lambda obj, ctx: obj.dict()
)
```

### Rust Usage

In Rust code, schemas can be loaded using the apache_avro crate:

```rust
use apache_avro::Schema;
use std::fs::File;
use std::io::Read;

fn load_schema(model_name: &str) -> Result<Schema, Box<dyn std::error::Error>> {
    let schema_path = format!("schemas/{}.avsc", model_name.to_lowercase());
    
    // Read schema file
    let mut file = File::open(&schema_path)?;
    let mut schema_json = String::new();
    file.read_to_string(&mut schema_json)?;
    
    // Parse schema
    let schema = Schema::parse_str(&schema_json)?;
    
    Ok(schema)
}

// Use it
let schema = load_schema("ack")?;
```

See `src/examples/schema_example.rs` for a complete working example.

## Configuration

The pipeline can be configured to use either predefined schema files or generate schemas on-the-fly.
This setting is controlled in the `config.toml` file:

```toml
[pipeline]
# Set to true to use schema files, false to generate on-the-fly
use_schema_files = true
```
