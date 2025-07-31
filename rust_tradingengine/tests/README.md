# CrypticsLabBot Test Suite

This directory contains unit and integration tests for the CrypticsLabBot trading engine.

## Test Organization

The test suite follows the same structure as the main project:

```
tests/
├── lib.rs                      # Main test entry point
├── infrastructure/             # Tests for infrastructure components
│   ├── mod.rs                  # Infrastructure module
│   ├── kafka/                  # Kafka-related tests
│   │   ├── mod.rs              # Kafka module
│   │   ├── helper/             # Tests for Kafka helper modules
│   │   │   ├── mod.rs          # Helper module
│   │   │   └── avro_converter_tests.rs  # Tests for AvroConverter
│   │   ├── producer_tests.rs   # Tests for KafkaProducer
│   │   ├── ticker_integration_tests.rs  # Integration tests for ticker serialization
│   │   └── trade_integration_tests.rs   # Integration tests for trade serialization
│   └── exchange/               # Tests for exchange integrations
│       ├── mod.rs              # Exchange module
│       └── thalex/             # Tests for Thalex exchange
│           ├── mod.rs          # Thalex module
│           └── parsers_tests.rs  # Tests for ThaleParser
```

## Running Tests

### Unit Tests

Run the unit tests with:

```bash
cargo test
```

### Integration Tests

Some integration tests are marked with `#[ignore]` because they require a running Kafka cluster. To run these tests, use:

```bash
# Start the Kafka cluster
docker-compose up -d

# Wait for services to be ready
sleep 30

# Run the integration tests
cargo test -- --ignored
```

## Test Types

1. **Unit Tests**: Test individual components in isolation, often using mocks.
2. **Integration Tests**: Test the integration with real services like Kafka.

## Test Data

Test data is generated within the test files, using realistic examples similar to what would be encountered in production.
