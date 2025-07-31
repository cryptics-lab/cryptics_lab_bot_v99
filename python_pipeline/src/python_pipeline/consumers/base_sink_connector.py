"""
Base Sink Connector for CrypticsLabBot.
Provides JDBC sink connector functionality for PostgreSQL.
"""

import json
import logging
import time
from abc import ABC
from typing import Any, Dict, List, Optional, Type

import requests

from python_pipeline.models.model_base import ModelBase

TIMEOUT_SEC: int = 10

# Configure logging
logger = logging.getLogger(__name__)


class BaseSinkConnector(ABC):
    """Base class for JDBC sink connectors"""
    
    # Class variables to be defined by subclasses
    connector_name_prefix: str = "cryptics-sink"
    topic_key: str = ""
    connector_class: str = "io.confluent.connect.jdbc.JdbcSinkConnector"
    
    def __init__(self, config: Dict[str, Any], model_class: Type[ModelBase], 
                 table_name: str, primary_keys: List[str]):
        """
        Initialize the sink connector.
        
        Args:
            config: Application configuration
            model_class: The model class to be consumed
            table_name: Name of the table to write to
            primary_keys: List of primary key fields
        """
        self.config = config
        self.model_class = model_class
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.topic_name = self.get_topic_name()
        self.connector_name = f"{self.connector_name_prefix}-{self.model_class.model_name.lower()}"
        
        # Determine if running in Docker environment
        running_in_docker = self.config.get('pipeline', {}).get('python_running_in_docker', False)
        
        # Set connection URLs based on environment
        if running_in_docker:
            self.connect_url = self.config['kafka']['connect_url_internal']
            self.schema_registry_url = self.config['kafka']['schema_registry_url_internal']
        else:
            self.connect_url = self.config['kafka']['connect_url']
            self.schema_registry_url = self.config['kafka']['schema_registry_url']
        
        # Database configuration
        # When running outside Docker, Kafka Connect needs to use the host's network
        # to connect to the database, but when running inside Docker, it should use
        # the Docker network hostname
        if running_in_docker:
            # Use Docker internal hostname when running in Docker
            self.db_host = "timescaledb"
            logger.info(f"Using Docker internal database host: {self.db_host}")
        else:
            # For local execution, Kafka Connect runs in Docker but needs to connect to host's PostgreSQL
            # Use the host machine's IP address instead of host.docker.internal
            # Using localhost from the host machine, which is accessible via 172.17.0.1 in Docker
            self.db_host = "172.17.0.1"  
            logger.info(f"Using 172.17.0.1 (host machine IP) to connect to the host's database")
        
        self.db_port = self.config['database'].get('port', 5432)
        self.db_name = self.config['database'].get('name', 'cryptics')
        self.db_user = self.config['database'].get('user', 'postgres')
        self.db_password = self.config['database'].get('password', 'postgres')
        self.db_schema = 'public'  # Use public schema as it's in the init-postgres.sql
    
    def get_topic_name(self) -> str:
        """
        Get the Kafka topic name for this connector from config.
        
        Returns:
            Topic name from config or a default based on model name
        """
        if 'topics' in self.config and self.topic_key in self.config['topics']:
            return self.config['topics'][self.topic_key]
        
        # Fallback to default topic naming
        base_name = self.config.get('topics', {}).get('base_name', 'cryptics.thalex')
        return f"{base_name}.{self.model_class.model_name.lower()}.avro"
    
    def get_jdbc_url(self) -> str:
        """
        Get the JDBC URL for connecting to PostgreSQL.
        
        Returns:
            JDBC URL string
        """
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
    
    async def wait_for_kafka_connect(self, max_retries: int = 30, retry_interval: int = 5) -> bool:
        """
        Wait for Kafka Connect to be ready.
        
        Args:
            max_retries: Maximum number of retries
            retry_interval: Time in seconds between retries
            
        Returns:
            True if Kafka Connect is ready, False otherwise
        """
        logger.debug("Waiting for Kafka Connect to be ready...")
        
        import asyncio

        import aiohttp
        
        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.connect_url}/connectors", timeout=TIMEOUT_SEC) as response:
                        if response.status == 200:
                            logger.debug("Kafka Connect is ready")
                            return True
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            
            logger.info(f"Attempt {attempt}/{max_retries} - Kafka Connect not ready yet")
            await asyncio.sleep(retry_interval)
        
        logger.error("Kafka Connect is not available after maximum retries")
        return False
    
    def get_connector_config(self) -> Dict[str, Any]:
        """
        Get the base configuration for the PostgreSQL sink connector.
        
        Returns:
            Dictionary containing the connector configuration
        """
        # For UUID primary keys, we use insert mode
        # For composite keys with timestamp, we use upsert mode
        is_uuid_primary_key = len(self.primary_keys) == 1 and self.primary_keys[0] == "id"
        
        # Always use the Docker internal schema registry URL for Kafka Connect
        # since Kafka Connect is always running inside Docker
        schema_registry_url_for_connect = self.config['kafka']['schema_registry_url_internal']
        logger.info(f"Using schema registry URL for Kafka Connect: {schema_registry_url_for_connect}")
        
        config = {
            "connector.class": self.connector_class,
            "tasks.max": "1",
            "topics": self.topic_name,
            
            # Connection settings
            "connection.url": self.get_jdbc_url(),
            "connection.user": self.db_user,
            "connection.password": self.db_password,
            "connection.attempts": "10",
            "connection.backoff.ms": "5000",
            
            # Table handling options
            "table.name.format": f"public.{self.table_name}",
            "auto.create": "false",
            "auto.evolve": "false",
            
            # Insert mode and behavior on failure
            "insert.mode": "insert" if is_uuid_primary_key else "upsert",
            
            # Dialect and mapping
            "dialect.name": "PostgreSqlDatabaseDialect",
            
            # Error and retry options
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            
            # Batch and buffer settings for performance
            "batch.size": "100",
            
            # Converter settings for Avro
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": schema_registry_url_for_connect,
        }
        
        # For upsert mode, we need to set the primary key fields
        if not is_uuid_primary_key:
            config.update({
                "pk.mode": "record_value",
                "pk.fields": ",".join(self.primary_keys),
                "delete.enabled": "false",
            })
            
        return config
    
    async def delete_connector(self) -> bool:
        """
        Delete the connector if it exists.
        
        Returns:
            True if deleted successfully or didn't exist, False otherwise
        """
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    f"{self.connect_url}/connectors/{self.connector_name}",
                    timeout=TIMEOUT_SEC
                ) as response:
                    if response.status in (204, 404):
                        if response.status == 204:
                            logger.info(f"Deleted connector '{self.connector_name}'")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Failed to delete connector: {response_text}")
                        return False
        except aiohttp.ClientError as e:
            logger.warning(f"Error while deleting connector (may not exist): {str(e)}")
            return True  # Assume it doesn't exist
    
    async def create_connector(self) -> bool:
        """
        Create the connector.
        
        Returns:
            True if created successfully, False otherwise
        """
        import aiohttp
        
        config = {
            "name": self.connector_name,
            "config": self.get_connector_config()
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.connect_url}/connectors",
                    headers={"Content-Type": "application/json"},
                    json=config,
                    timeout=TIMEOUT_SEC
                ) as response:
                    if response.status in (201, 200):
                        logger.debug(f"Connector '{self.connector_name}' created successfully")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Failed to create connector: {response_text}")
                        return False
        except aiohttp.ClientError as e:
            logger.error(f"Error while creating connector: {str(e)}")
            return False
    
    async def exists(self) -> bool:
        """
        Check if the connector already exists.
        
        Returns:
            True if the connector exists, False otherwise
        """
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.connect_url}/connectors/{self.connector_name}",
                    timeout=TIMEOUT_SEC
                ) as response:
                    return response.status == 200
        except aiohttp.ClientError:
            return False
    
    async def create(self, force: bool = True) -> bool:
        """
        Create the sink connector.
        
        Args:
            force: If True, delete the connector if it exists
            
        Returns:
            True if the connector was created successfully, False otherwise
        """
        # Wait for Kafka Connect to be ready
        if not await self.wait_for_kafka_connect():
            return False
        
        # Delete existing connector if it exists and force is True
        if force and await self.exists():
            if not await self.delete_connector():
                return False
        
        # Create connector
        return await self.create_connector()
    
    async def get_status(self) -> Optional[Dict[str, Any]]:
        """
        Get the status of the connector.
        
        Returns:
            Dictionary containing the connector status, or None if it doesn't exist
        """
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.connect_url}/connectors/{self.connector_name}/status",
                    timeout=TIMEOUT_SEC
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None
        except aiohttp.ClientError:
            return None
