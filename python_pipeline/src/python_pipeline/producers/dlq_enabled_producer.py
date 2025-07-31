"""
Example of integrating Dead Letter Queue with AvroProducerBase.
This shows how the DLQ could be integrated with minimal changes.
"""

import json
import logging
import traceback
from typing import Any, Dict, Optional

from python_pipeline.models.model_base import ModelBase
from python_pipeline.producers.base_producer import AvroProducerBase
from python_pipeline.producers.dead_letter_producer import DeadLetterProducer

# Configure logging
logger = logging.getLogger(__name__)


class DLQEnabledProducerBase(AvroProducerBase):
    """Base producer with Dead Letter Queue support"""
    
    def __init__(self, config: Dict[str, Any], model_class: type[ModelBase]):
        """Initialize the DLQ-enabled producer"""
        super().__init__(config, model_class)
        
        # Create DLQ producer once config is loaded
        self.dlq_producer = None
        
    def initialize(self) -> None:
        """Initialize the producer and DLQ handler"""
        # Call the parent initialization
        super().initialize()
        
        # Set up the DLQ producer
        running_in_docker = self.config.get('pipeline', {}).get('python_running_in_docker', False)
        
        # Get bootstrap servers based on environment
        if running_in_docker:
            bootstrap_servers = self.config['kafka']['bootstrap_servers_internal']
        else:
            bootstrap_servers = self.config['kafka']['bootstrap_servers']
        
        # Initialize DLQ producer
        self.dlq_producer = DeadLetterProducer(bootstrap_servers)
        logger.info("Dead Letter Queue producer initialized")
    
    def produce(self, key: str, value: ModelBase) -> None:
        """
        Produce a message to Kafka with DLQ fallback
        
        Args:
            key: Message key
            value: Message value (a ModelBase instance)
        """
        if self.producer is None:
            raise RuntimeError("Producer not initialized. Call initialize() first.")
        
        try:
            # Try normal production
            super().produce(key, value)
            
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            
            # Send to DLQ if enabled and initialized
            if self.dlq_producer:
                try:
                    # Convert model to dict for DLQ
                    payload_dict = value.dict()
                    
                    self.dlq_producer.send_to_dlq(
                        original_topic=self.topic_name,
                        original_key=key,
                        error_reason=str(e),
                        original_payload=payload_dict,
                        stacktrace=traceback.format_exc()
                    )
                except Exception as dlq_error:
                    logger.critical(f"Failed to send to DLQ: {dlq_error}")
            else:
                logger.warning("DLQ producer not initialized, skipping DLQ")
