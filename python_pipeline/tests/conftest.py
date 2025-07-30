"""
Shared test fixtures for CrypticsLabBot testing.
"""

import asyncio
import sys
import os
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Add parent of src directory to the Python path to make imports work properly
root_dir = Path(__file__).parent.parent
if root_dir.exists():
    sys.path.insert(0, str(root_dir))

# Ensure we can import from both python_pipeline and src.python_pipeline
try:
    import python_pipeline
except ImportError:
    sys.path.insert(0, str(root_dir / "src"))


@pytest.fixture
def test_config():
    """Test configuration dictionary"""
    return {
        'kafka': {
            'bootstrap_servers': 'mock-kafka:9092',
            'schema_registry_url': 'http://mock-registry:8081',
            'topic_partitions': 1,
            'topic_replicas': 1
        },
        'topics': {
            'base_name': 'cryptics.test',
            'ack': 'cryptics.test.ack.avro'
        },
        'pipeline': {
            'use_schema_files': True,
            'python_running_in_docker': False
        }
    }


@pytest.fixture
def mock_aiokafka_producer():
    """Mock for AIOKafkaProducer"""
    with patch('aiokafka.AIOKafkaProducer') as mock:
        producer_instance = AsyncMock()
        producer_instance.start = AsyncMock()
        producer_instance.stop = AsyncMock()
        producer_instance.send_and_wait = AsyncMock()
        producer_instance.flush = AsyncMock()
        mock.return_value = producer_instance
        yield mock, producer_instance


@pytest.fixture
def mock_schema_registry():
    """Mock for SchemaRegistryClient"""
    with patch('confluent_kafka.schema_registry.SchemaRegistryClient') as mock:
        registry_instance = MagicMock()
        mock.return_value = registry_instance
        yield mock, registry_instance


@pytest.fixture
def mock_schema_generator():
    """Mock for AvroSchemaGenerator"""
    with patch('python_pipeline.producers.schema_generator.AvroSchemaGenerator') as mock:
        mock.generate_schema.return_value = {
            "type": "record",
            "name": "TestModel",
            "namespace": "test.namespace",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }
        yield mock
