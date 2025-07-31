# Development Guide

This document provides detailed instructions for setting up and extending the CrypticsLabBot system.

## Prerequisites

- Docker and Docker Compose
- Rust (2021 edition or later)
- Python 3.10+
- PostgreSQL client tools

## Local Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/cryptics_lab_bot.git
   cd cryptics_lab_bot
   ```

2. Create a `.env` file with required environment variables:
   ```bash
   # Database settings
   DB_USER=postgres
   DB_PASSWORD=postgres
   DB_NAME=cryptics
   
   # PGAdmin settings
   PGADMIN_EMAIL=admin@example.com
   PGADMIN_PASSWORD=admin
   
   # Grafana settings
   GRAFANA_USER=admin
   GRAFANA_PASSWORD=admin
   ```

3. Start the infrastructure services:
   ```bash
   cd docker
   docker-compose up -d zk broker0 broker1 broker2 schema-registry timescaledb
   ```

4. Build the Rust trading engine:
   ```bash
   cd rust_tradingengine
   cargo build --release
   ```

5. Set up the Python environment:
   ```bash
   cd python_pipeline
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   pip install -e .
   ```

## Running the System

### Option 1: Docker Compose (Full System)

Start all components with Docker Compose:
```bash
cd docker
docker-compose up -d
```

### Option 2: Individual Components

1. Start Kafka and Database services:
   ```bash
   cd docker
   docker-compose up -d zk broker0 broker1 broker2 schema-registry timescaledb
   ```

2. Run the Python Pipeline:
   ```bash
   cd python_pipeline
   python main.py
   ```

3. Run the Rust Trading Engine:
   ```bash
   cd rust_tradingengine
   RUST_LOG=info cargo run --release
   ```

4. Start Grafana (optional):
   ```bash
   cd docker
   docker-compose up -d grafana
   ```

## Debugging

### Rust Component

Run with debug logging:
```bash
RUST_LOG=debug cargo run
```

### Python Component

Run with increased verbosity:
```bash
cd python_pipeline
python -m python_pipeline.main -v
```

### Kafka Tools

Access Kafka tools through the CLI container:
```bash
docker exec -it cli-tools bash
# List topics
kafka-topics --bootstrap-server broker0:29092 --list
# Consume from a topic
kafka-avro-console-consumer --bootstrap-server broker0:29092 --topic cryptics.thalex.ticker.avro --from-beginning --property schema.registry.url=http://schema-registry:8081
```

### Database Access

Connect to TimescaleDB:
```bash
docker exec -it timescaledb psql -U postgres -d cryptics
```

Or use PGAdmin at http://localhost:5050

## Extending the System

### Adding a New Data Model

The system uses a factory pattern with base classes, making it easy to add new data models:

1. Create a new Avro schema file:
   ```bash
   mkdir -p schemas/new_model
   touch schemas/new_model/v1.avsc
   ```

2. Define your schema (example):
   ```json
   {
     "type": "record",
     "name": "NewModel",
     "namespace": "com.cryptics.avro",
     "fields": [
       {"name": "id", "type": "string", "doc": "Unique identifier"},
       {"name": "value", "type": "double", "doc": "Value field"},
       {"name": "optional_field", "type": ["null", "string"], "default": null}
     ]
   }
   ```

3. Create a new model class in `python_pipeline/models/new_model.py`:
   ```python
   from typing import ClassVar, Optional
   from pydantic import Field
   from python_pipeline.models.model_base import ModelBase

   class NewModel(ModelBase):
       model_name: ClassVar[str] = "NewModel"
       
       id: str
       value: float
       optional_field: Optional[str] = None
       
       @classmethod
       def generate(cls) -> 'NewModel':
           return cls(
               id="test-id",
               value=123.45
           )
   ```

4. Create a new producer in `python_pipeline/producers/new_model_producer.py`:
   ```python
   from typing import Dict, Any
   from python_pipeline.producers.base_producer import AvroProducerBase
   from python_pipeline.models.new_model import NewModel

   class NewModelProducer(AvroProducerBase):
       topic_key: str = "new_model"
       
       def __init__(self, config: Dict[str, Any]):
           super().__init__(config, NewModel)
       
       async def generate_and_produce(self) -> None:
           model = NewModel.generate()
           self.produce(model.id, model)
   ```

5. Create a new sink connector in `python_pipeline/consumers/new_model_sink_connector.py`:
   ```python
   from typing import Dict, Any
   from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
   from python_pipeline.models.new_model import NewModel

   class NewModelSinkConnector(BaseSinkConnector):
       topic_key: str = "new_model"
       connector_name_prefix: str = "postgres-sink"
       
       def __init__(self, config: Dict[str, Any]):
           super().__init__(
               config=config,
               model_class=NewModel,
               table_name="new_model_data",
               primary_keys=["id"]
           )
   ```

6. Register your new components in `python_pipeline/factories.py`:
   ```python
   from python_pipeline.producers.new_model_producer import NewModelProducer
   from python_pipeline.consumers.new_model_sink_connector import NewModelSinkConnector
   
   # Add to existing registration
   ProducerFactory.register("new_model", NewModelProducer)
   ConnectorFactory.register("new_model", NewModelSinkConnector)
   ```

7. Add database table to `docker/init-postgres.sql`:
   ```sql
   CREATE TABLE IF NOT EXISTS public.new_model_data (
       id UUID NOT NULL DEFAULT uuid_generate_v4(),
       value DOUBLE PRECISION NOT NULL,
       optional_field VARCHAR(100),
       time_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       PRIMARY KEY (id)
   );
   ```

8. Update config in `config.toml`:
   ```toml
   [topics]
   new_model = "cryptics.thalex.new_model.avro"
   
   [pipeline]
   enabled_models = ["ticker", "ack", "trade", "index", "new_model"]
   
   [pipeline.models.new_model]
   table_name = "new_model_data"
   ```

### Adding a New Trading Strategy

1. Create a new module in `rust_tradingengine/src/strategies/`:
   ```bash
   mkdir -p rust_tradingengine/src/strategies/new_strategy
   touch rust_tradingengine/src/strategies/new_strategy/mod.rs
   ```

2. Implement the strategy logic following the existing pattern
3. Wire up the new strategy in `main.rs`

### Adding a New Database Type

The system uses Kafka Connect JDBC sink, making it easy to switch database types:

1. Update the connector configuration in `BaseSinkConnector.get_connector_config()`:
   ```python
   # For MySQL
   config.update({
       "connection.url": f"jdbc:mysql://{self.db_host}:{self.db_port}/{self.db_name}",
       "dialect.name": "MySqlDatabaseDialect"
   })
   ```

2. Update the Docker Compose file to use the new database type:
   ```yaml
   mysql:
     image: mysql:8
     environment:
       MYSQL_ROOT_PASSWORD: ${DB_PASSWORD}
       MYSQL_DATABASE: ${DB_NAME}
     volumes:
       - ./init-mysql.sql:/docker-entrypoint-initdb.d/init-mysql.sql
   ```

3. Create the initialization SQL for the new database in `docker/init-mysql.sql`

## Testing

### Rust Tests
```bash
cd rust_tradingengine
cargo test
```

### Python Tests
```bash
cd python_pipeline
pytest
```

### Integration Tests
```bash
cd tests
./run_integration_tests.sh
```

### Schema Validation Tests

Validate Avro schemas against Schema Registry:
```bash
cd python_pipeline/tests
python test_schema_validation.py
```

## Latency Monitoring

The system includes comprehensive latency tracking to measure performance at various stages of the data flow.

### Latency Tracking Components

1. **Schema Evolution with Processing Timestamp**
   - Added `processing_timestamp` field to all message schemas (ticker, trade, ack)
   - Maintained backward compatibility with default null values
   - Implemented as union types `["null", "type"]` for proper evolution

2. **Database Latency Calculation**
   - Added latency calculation columns in the database:
     - `total_latency_ms`: End-to-end latency from exchange to database
     - `exchange_to_rust_latency_ms`: Time from exchange event to Rust processing
     - `rust_to_db_latency_ms`: Time from Rust processing to database storage
   - Implemented database triggers for automatic latency calculations

3. **Rust Instrumentation**
   - Updated domain models to include processing timestamp
   - Modified AvroConverter to handle the timestamp field
   - Added timestamp injection during message processing

### Viewing Latency Metrics

1. Access Grafana at [http://localhost:3000](http://localhost:3000)
2. Navigate to the "Latency Dashboard"
3. View the following metrics:
   - Total end-to-end latency
   - Exchange to Rust processing latency
   - Rust to database latency
   - Latency distributions

### Running Latency Queries

Query ticker data latency:
```sql
SELECT 
  mark_timestamp, 
  processing_timestamp, 
  exchange_to_rust_latency_ms, 
  rust_to_db_latency_ms, 
  total_latency_ms 
FROM ticker_data 
ORDER BY time_ts DESC 
LIMIT 10;
```

Calculate average latency over recent data:
```sql
SELECT 
  AVG(rust_to_db_latency_ms) as avg_rust_to_db, 
  AVG(exchange_to_rust_latency_ms) as avg_exchange_to_rust, 
  AVG(total_latency_ms) as avg_total 
FROM ticker_data 
WHERE time_ts > now() - interval '1 minute';
```

### Latency Monitoring Best Practices

1. Watch for large increases in latency, especially in the Rust to DB phase
2. Monitor exchange_to_rust_latency_ms to identify exchange connectivity issues
3. Use the latency distribution charts to identify outliers and inconsistencies
4. When adding new fields, always follow schema evolution best practices:
   - Always add fields with default values
   - Always use union types (`["null", "type"]`) for new fields
   - Document changes in the schemas/CHANGELOG.md file

## Kafka Connect

### Understanding Sink Connectors

The system uses Kafka Connect JDBC sink connectors to stream data from Kafka to the database:

1. **What is a Sink Connector?**
   - Sink connectors pull data from Kafka topics and send it to external systems
   - JDBC sink connector specifically writes data to relational databases
   - No custom code needed for database operations

2. **Benefits of Sink Connectors:**
   - Automatic handling of connection pooling and retries
   - Topic partitioning mapped to database operations
   - Schema evolution management
   - Simplified deployment and monitoring
   - Support for various database dialects

3. **Configuration Options:**
   - `insert.mode`: Controls how records are written (insert, upsert, update)
   - `pk.mode`: How primary keys are handled
   - `pk.fields`: Which fields to use as primary keys
   - `auto.create`: Whether to create tables automatically
   - `auto.evolve`: Whether to modify tables as schemas change

### Monitoring Connectors

Check connector status:
```bash
curl -s http://localhost:8083/connectors/postgres-sink-ticker/status | jq
```

View connector configuration:
```bash
curl -s http://localhost:8083/connectors/postgres-sink-ticker | jq
```

Reset connector (if it's in a failed state):
```bash
curl -X POST http://localhost:8083/connectors/postgres-sink-ticker/restart
```

## Deployment

See [DEPLOYMENT.md](./DEPLOYMENT.md) for production deployment instructions.
