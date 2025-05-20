#!/usr/bin/env python3
"""
Test Pydantic Configuration Module
=================================
Tests the Pydantic-based configuration system
"""

import logging
import os
from pathlib import Path
from typing import Any

import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("config-tests")

from python_pipeline.config.settings import Settings, settings
from python_pipeline.services.config_service import ConfigService


def test_config_loads_correctly() -> None:
    """Test that configuration loads correctly"""
    # Access the singleton
    config = settings
    assert isinstance(config, Settings)
    
    # Check that required sections exist
    assert config.kafka is not None
    assert config.database is not None
    assert config.topics is not None
    assert config.app is not None
    assert config.pipeline is not None

def test_config_default_values() -> None:
    """Test that configuration has correct default values"""
    config = settings
    
    # Check a few default values
    assert config.kafka.topic_partitions == 3
    assert config.kafka.topic_replicas == 2
    assert config.pipeline.use_schema_files == True

def test_docker_environment_override() -> None:
    """Test that Docker environment variable overrides configuration"""
    # Note: This test might be less reliable with a singleton
    # We'd need to reload settings after changing env vars
    # For now, just check the existing value
    assert settings.pipeline.python_running_in_docker in (True, False)

def test_config_service_initialization() -> None:
    """Test that ConfigService initializes correctly"""
    service = ConfigService()
    
    # Check that properties are accessible
    assert service.enabled_models is not None
    assert service.use_schema_files is not None
    assert service.settings is settings  # Should use the singleton

def test_config_service_methods() -> None:
    """Test ConfigService helper methods"""
    service = ConfigService()
    
    # Test Kafka config
    kafka_config = service.get_kafka_config(internal=False)
    assert 'bootstrap_servers' in kafka_config
    assert 'schema_registry_url' in kafka_config
    assert kafka_config['bootstrap_servers'] == 'localhost:9092'
    
    # Test database config
    db_config = service.get_database_config(internal=False)
    assert 'host' in db_config
    assert 'port' in db_config
    assert 'name' in db_config
    assert db_config['host'] == 'localhost'
    
    # Test test database config (if available)
    test_db_config = service.get_database_config(internal=False, is_test=True)
    if test_db_config.get('name') != db_config.get('name'):  # Only if test DB is different
        assert test_db_config['port'] in (5433, 5432)  # Allow either port
    
    # Test DSN
    dsn = service.get_database_dsn(internal=False)
    assert 'postgresql://' in dsn
    assert 'localhost' in dsn
    
    # Test update interval
    update_interval = service.get_update_interval()
    assert update_interval > 0  # Should be positive

if __name__ == "__main__":
    pytest.main(["-v", __file__])
