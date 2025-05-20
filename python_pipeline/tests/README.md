# CrypticsLabBot Python Pipeline Tests

This directory contains tests for the CrypticsLabBot Python Pipeline.

## Test Overview

The test suite includes:

1. **Serialization Tests** - Test Avro serialization and deserialization for various model types:
   - Ticker data
   - Order acknowledgements (Ack)
   - Trade data

2. **Producer Logic Tests** - Test the business logic in the producers without relying on Kafka:
   - Ticker producer
   - Ack producer (with order state management)
   - Trade producer

## Running Tests

### Prerequisites

Make sure you have activated the virtual environment:

```bash
# If using venv
source /path/to/venv/bin/activate

# If using conda
conda activate your-environment-name
```

### Run All Tests

```bash
# From within the python_pipeline directory
cd /Users/Borat/Desktop/code/cryptics_lab_bot/python_pipeline
python -m tests.run_tests
```

Alternatively, you can run the script directly:

```bash
cd /Users/Borat/Desktop/code/cryptics_lab_bot/python_pipeline
./tests/run_tests.py
```

### Run Individual Test Modules

```bash
# Run just the Ack serialization tests
python -m unittest tests.test_ack_serialization

# Run just the producer logic tests
python -m unittest tests.test_producer_logic
```

## Test Architecture

The tests use:

1. **fastavro** - Pure Python Avro implementation for serialization/deserialization
2. **Mock objects** - To replace Kafka-dependent components
3. **Patched random functions** - To make tests deterministic

The serialization testing works by:
1. Creating model instances (either manually or via generate())
2. Serializing them to Avro bytes
3. Deserializing back to model instances
4. Comparing the original and deserialized models

The producer logic testing works by:
1. Creating mock producers that don't connect to Kafka
2. Running the same business logic as the real producers
3. Capturing the produced messages for verification
