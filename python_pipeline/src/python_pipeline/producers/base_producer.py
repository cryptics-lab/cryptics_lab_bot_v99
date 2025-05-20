"""
Base Avro producer for CrypticsLabBot.
"""

import json
import logging
import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Type

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from python_pipeline.models.model_base import ModelBase
from python_pipeline.producers.avro_schema_loader import get_schema_for_model
from python_pipeline.producers.schema_generator import AvroSchemaGenerator

# Configure logging
logger = logging.getLogger(__name__)


def delivery_callback(err, msg):
    """Simple delivery callback for Kafka producer"""
    if err:
        logger.error(f"Failed to deliver message: {err}")
    # Skip successful delivery logs to reduce verbosity


class AvroProducerBase(ABC):
    """Base class for Avro producers"""
    
    # Class variables to be defined by subclasses
    topic_key: str = ""
    
    def __init__(self, config: Dict[str, Any], model_class: Type[ModelBase]):
        """Initialize the Avro producer."""
        self.config = config
        self.model_class = model_class
        self.producer = None
        self.avro_serializer = None
        self.schema_registry_client = None
        self.admin_client = None
        self.topic_name = self.get_topic_name()
        
    def get_topic_name(self) -> str:
        """Get the Kafka topic name from config."""
        if 'topics' in self.config and self.topic_key in self.config['topics']:
            return self.config['topics'][self.topic_key]
        
        # Fallback to default topic naming
        base_name = self.config.get('topics', {}).get('base_name', 'cryptics.thalex')
        return f"{base_name}.{self.model_class.model_name.lower()}.avro"
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get the Avro schema for this model.
        Either generates it on-the-fly or loads from schema file based on configuration.
        """
        # Check if we should use schema files
        use_schema_files = self.config.get('pipeline', {}).get('use_schema_files', False)
        
        # Use the dedicated schema loader
        return get_schema_for_model(self.model_class, use_schema_files)
    
    def initialize(self) -> None:
        """Initialize the producer and create the Kafka topic if needed."""
        # Check if running in Docker - look in the pipeline section
        running_in_docker = self.config.get('pipeline', {}).get('python_running_in_docker', False)
        
        # Get Kafka configuration based on environment
        if running_in_docker:
            bootstrap_servers = self.config['kafka']['bootstrap_servers_internal']
            schema_registry_url = self.config['kafka']['schema_registry_url_internal']
        else:
            bootstrap_servers = self.config['kafka']['bootstrap_servers']
            schema_registry_url = self.config['kafka']['schema_registry_url']
        
        logger.info(f"Initializing producer for {self.topic_name} with bootstrap servers: {bootstrap_servers}")
        
        # Create schema registry client
        self.schema_registry_client = SchemaRegistryClient({
            'url': schema_registry_url
        })
        
        # Generate schema
        schema = self.get_schema()
        schema_str = json.dumps(schema)
        
        # Create Avro serializer with a custom to_dict function that handles Enums and None values
        def model_to_dict(obj, ctx):
            # Convert Pydantic model to dict
            data = obj.dict()
            # Get schema to check field types
            schema = self.get_schema()
            field_types = {field["name"]: field.get("type") for field in schema["fields"]}
            
            for field_name, field_value in list(data.items()):
                # Handle Enum fields - check if schema expects enum or string
                if hasattr(field_value, 'value') and hasattr(field_value, '__class__') and issubclass(field_value.__class__, str):
                    # Check if the schema type is an enum
                    field_type = field_types.get(field_name)
                    if isinstance(field_type, dict) and field_type.get("type") == "enum":
                        # Schema expects enum - DO NOT convert to string, keep as enum value
                        # data[field_name] remains unchanged as the Avro serializer will handle it
                        pass
                    else:
                        # Schema expects string - convert enum to string value
                        data[field_name] = field_value.value
                
                # Handle None values
                if field_name in field_types:
                    field_type = field_types[field_name]
                    
                    # If field is None and field type is a union with null first
                    if field_value is None and isinstance(field_type, list) and field_type[0] == "null":
                        # Keep the None value - Avro serializer will handle it as null
                        pass
                    # If field is None but field type doesn't support null
                    elif field_value is None and (not isinstance(field_type, list) or "null" not in field_type):
                        # Use appropriate default values based on the type
                        if field_type == "string":
                            data[field_name] = ""
                        elif field_type == "int":
                            data[field_name] = 0
                        elif field_type == "double":
                            data[field_name] = 0.0
                        elif field_type == "boolean":
                            data[field_name] = False
            
            return data
            
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=schema_str,
            to_dict=model_to_dict
        )
        
        # Create Kafka producer with improved reliability settings
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 100,  # Wait to batch messages
            'acks': 'all',     # Wait for all replicas
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': self.avro_serializer,
            'retries': 5,      # Retry up to 5 times
            'retry.backoff.ms': 200,  # Wait 200ms between retries
            'socket.keepalive.enable': True,  # Keep connections alive
            'socket.timeout.ms': 10000,  # 10 second timeout
            'request.timeout.ms': 30000  # 30 second request timeout
        }
        
        try:
            self.producer = SerializingProducer(producer_config)
            logger.info(f"Producer created successfully for topic: {self.topic_name}")
        except Exception as e:
            logger.error(f"Failed to create producer: {e}")
            raise
        
        # Create admin client
        try:
            self.admin_client = AdminClient({
                'bootstrap.servers': bootstrap_servers
            })
        except Exception as e:
            logger.error(f"Failed to create admin client: {e}")
            raise
        
        # Create topic if it doesn't exist
        self._create_topic(self.topic_name)
    
    def _create_topic(self, topic_name: str) -> None:
        """Create a Kafka topic if it doesn't exist."""
        # Get topic configuration
        num_partitions = self.config['kafka']['topic_partitions']
        replication_factor = self.config['kafka']['topic_replicas']
        
        topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        try:
            # Check if topic already exists
            existing_topics = self.admin_client.list_topics(timeout=10)
            if topic_name in existing_topics.topics:
                logger.info(f"Topic {topic_name} already exists")
                return
            
            futures = self.admin_client.create_topics([topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Created topic {topic}")
                except Exception as e:
                    logger.warning(f"Topic creation issue: {e}")
        except Exception as e:
            logger.warning(f"Topic operation failed: {e}")
    
    def produce(self, key: str, value: ModelBase) -> None:
        """Produce a message to Kafka."""
        if self.producer is None:
            raise RuntimeError("Producer not initialized. Call initialize() first.")
        
        try:
            # Use the SerializingProducer
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=value,
                on_delivery=delivery_callback
            )
            
            # Poll to handle callbacks
            self.producer.poll(0)
            
        except BufferError:
            # If buffer is full, flush and retry
            self.producer.flush(timeout=5)
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=value,
                on_delivery=delivery_callback
            )
            self.producer.poll(0)
                
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            
        # Periodically flush to ensure delivery
        if not hasattr(self, '_message_count'):
            self._message_count = 0
        self._message_count += 1
        
        if self._message_count % 5 == 0:  # Flush every 5 messages
            self.producer.poll(0.1)  # Poll to process callbacks
            remaining = self.producer.flush(timeout=2)
            if remaining > 0:
                logger.warning(f"Flush incomplete: {remaining} messages remaining")
    
    @abstractmethod
    async def generate_and_produce(self, **kwargs) -> None:
        """Generate model data and produce to Kafka."""
        pass
        
    def flush(self, timeout: int = 10) -> int:
        """Flush the producer to ensure all messages are sent."""
        if self.producer is None:
            return -1
            
        try:
            # Poll first to process callbacks
            self.producer.poll(1)
            result = self.producer.flush(timeout=timeout)
            
            if result > 0:
                logger.warning(f"Flush incomplete: {result} messages remaining")
            
            return result
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")
            return -1
