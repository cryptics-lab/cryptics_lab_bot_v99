#!/usr/bin/env python3
"""
Integration tests for the Python Pipeline
========================================
Tests the complete data flow from Kafka producer to database
"""

import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("integration-tests")

from python_pipeline.producers.ticker_producer import TickerProducer
from python_pipeline.services.config_service import ConfigService
from python_pipeline.utils.connection_utils import (
    wait_for_kafka,
    wait_for_postgres,
    wait_for_schema_registry,
)


# Test fixtures
@pytest.fixture
def config_service():
    """Return a configured ConfigService instance"""
    return ConfigService()

@pytest.fixture
def postgres_connection(config_service):
    """Create and return a PostgreSQL connection"""
    dsn = config_service.get_database_dsn()
    
    # Wait for PostgreSQL to be available
    assert wait_for_postgres(dsn), "PostgreSQL is not available"
    
    # Connect to the database
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    
    yield conn
    
    # Cleanup
    conn.close()

@pytest.fixture
def kafka_services(config_service):
    """Ensure Kafka services are available"""
    kafka_config = config_service.get_kafka_config()
    bootstrap_servers = kafka_config["bootstrap_servers"]
    schema_registry_url = kafka_config["schema_registry_url"]
    
    # Wait for services
    assert wait_for_kafka(bootstrap_servers), "Kafka is not available"
    assert wait_for_schema_registry(schema_registry_url), "Schema Registry is not available"
    
    return kafka_config

@pytest.fixture
async def ticker_producer(kafka_services):
    """Create and return a configured TickerProducer"""
    producer = TickerProducer(
        bootstrap_servers=kafka_services["bootstrap_servers"],
        schema_registry_url=kafka_services["schema_registry_url"],
        topic="cryptics.thalex.ticker.avro",
        schema_path="../schemas/ticker/v1.avsc"
    )
    
    await producer.initialize()
    
    yield producer
    
    # Cleanup
    await producer.close()

# Helper functions
async def wait_for_records(conn, table_name: str, count: int, timeout: int = 30) -> bool:
    """Wait for at least 'count' records to appear in the specified table"""
    start_time = datetime.datetime.now()
    end_time = start_time + datetime.timedelta(seconds=timeout)
    
    while datetime.datetime.now() < end_time:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            current_count = cursor.fetchone()[0]
            
            if current_count >= count:
                return True
                
        # Sleep briefly before checking again
        await asyncio.sleep(1)
        
    return False

# Integration tests
@pytest.mark.asyncio
async def test_ticker_data_flow(ticker_producer, postgres_connection):
    """Test the complete flow from ticker producer to database table"""
    # Generate a unique test instrument name
    test_instrument = f"TEST-BTCUSD-{datetime.datetime.now().timestamp()}"
    
    # Prepare test data
    test_data = {
        "instrument_name": test_instrument,
        "mark_price": 50000.0,
        "mark_timestamp": datetime.datetime.now().timestamp(),
        "best_bid_price": 49950.0,
        "best_bid_amount": 1.2,
        "best_ask_price": 50050.0,
        "best_ask_amount": 1.5,
        "last_price": 50000.0,
        "delta": 0.75,
        "volume_24h": 12500.0,
        "value_24h": 625000000.0,
        "low_price_24h": 49000.0,
        "high_price_24h": 51000.0,
        "change_24h": 2.0,
        "index_price": 50010.0,
        "forward": 0.0,
        "funding_mark": 0.0001,
        "funding_rate": 0.0001,
        "collar_low": 47500.0,
        "collar_high": 52500.0,
        "realised_funding_24h": 0.0025,
        "average_funding_rate_24h": 0.0001,
        "open_interest": 45000000.0
    }
    
    # Send test data to Kafka
    logger.info(f"Sending test ticker data for {test_instrument}")
    await ticker_producer.send_data(test_data)
    await ticker_producer.flush()
    
    # Wait for data to be processed and appear in the database
    logger.info("Waiting for data to appear in database...")
    query = f"""
    SELECT * FROM ticker_data 
    WHERE instrument_name = '{test_instrument}'
    ORDER BY time_ts DESC
    LIMIT 1
    """
    
    # Wait for records to appear
    success = await wait_for_records(
        postgres_connection, 
        "ticker_data", 
        1, 
        timeout=30
    )
    
    assert success, f"Test data for {test_instrument} not found in database within timeout"
    
    # Verify the data was correctly processed
    with postgres_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        
        assert row is not None, "No data found in database"
        assert row["instrument_name"] == test_instrument, "Instrument name mismatch"
        assert abs(row["mark_price"] - test_data["mark_price"]) < 0.01, "Mark price mismatch"
        assert abs(row["best_bid_price"] - test_data["best_bid_price"]) < 0.01, "Bid price mismatch"
        assert abs(row["best_ask_price"] - test_data["best_ask_price"]) < 0.01, "Ask price mismatch"
        
        logger.info("Data successfully verified in database")

@pytest.mark.asyncio
async def test_reconnection_handling(ticker_producer, postgres_connection, config_service):
    """Test the system's ability to handle reconnections"""
    # This test would simulate connection interruptions and verify data integrity
    # Implementation would depend on how the system handles reconnections
    pass

if __name__ == "__main__":
    # Allow running with pytest or directly
    pytest.main(["-xvs", __file__])
