# CrypticsLab Bot Pipeline

A modular data pipeline for the CrypticsLab trading bot. This component is responsible for ingesting, processing, and managing market data from cryptocurrency exchanges, with a focus on Thalex exchange data. The pipeline handles various data types including tickers, trades, and order acknowledgements (acks), transforming raw data into structured formats and making it available for analysis and algorithmic trading.

## Project Structure

```
python_pipeline/
├── src/                            # Source code directory
│   └── python_pipeline/            # Main package
│       ├── __init__.py             # Package initialization
│       ├── cleanup.py              # Data cleanup utilities
│       ├── main.py                 # Application entry point
│       ├── factories.py            # Factory pattern implementations
│       ├── config/                 # Configuration management
│       │   ├── __init__.py
│       │   └── settings.py         # Configuration settings using Pydantic
│       ├── consumers/              # Kafka consumer implementations
│       │   ├── __init__.py
│       │   ├── base_sink_connector.py
│       │   ├── ack_sink_connector.py
│       │   ├── trade_sink_connector.py
│       │   └── ...
│       ├── data_generators/        # Test data generation
│       │   ├── __init__.py
│       │   ├── base_generator.py
│       │   ├── ack_generator.py
│       │   └── factory.py
│       ├── models/                 # Data models
│       │   ├── __init__.py
│       │   ├── model_base.py
│       │   ├── ack.py
│       │   ├── trade.py
│       │   └── ...
│       ├── producers/              # Kafka producer implementations
│       │   ├── __init__.py
│       │   ├── base_producer.py
│       │   ├── ack_producer.py
│       │   └── ...
│       ├── services/               # Business logic services
│       │   ├── __init__.py
│       │   ├── config_service.py
│       │   ├── database_service.py
│       │   ├── migration_manager.py
│       │   └── ...
│       └── utils/                  # Utility functions
│           ├── __init__.py
│           ├── avro_utils.py
│           ├── connection_utils.py
│           └── ...
├── tests/                          # Test suite
│   ├── __init__.py
│   ├── base_test.py
│   ├── test_ack_generator.py
│   ├── test_ack_serialization.py
│   └── ...
├── pyproject.toml                  # Project metadata and dependencies
├── setup.py                        # Package setup script
└── README.md                       # This file
```

## Installation

### Development Setup

We use `uv` for fast dependency management:

```bash
# Create and activate a virtual environment (if not already done)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install uv
pip install uv

# Install the package in development mode with all dev dependencies
uv pip install -e ".[dev]"
```

## Running Tests

```bash
# Run all tests
pytest

# Run specific test files
pytest tests/test_ack_generator.py tests/test_ack_serialization.py

# Run with coverage
pytest --cov=python_pipeline
```

## Code Quality

```bash
# Run linting
ruff check .

# Auto-fix issues
ruff check --fix .
```

## Docker

To build and run the pipeline in Docker:

```bash
# From project root directory
docker build -f docker/Dockerfile.pipeline -t cryptics-pipeline .
docker run -it --name pipeline cryptics-pipeline
```

