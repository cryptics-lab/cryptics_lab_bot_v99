"""
Dead Letter Queue Producer for CrypticsLabBot.
Handles failed message routing to dedicated DLQ topics.
"""

import datetime
import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from pydantic import BaseModel

# Configure logging
logger = logging.getLogger(__name__)


class DeadLetterMessage(BaseModel):
    """Model for encapsulating failed messages"""
    
    original_topic: str
    original_key: str
    error_reason: str
    error_timestamp: float
    original_payload: Dict[str, Any]
    stacktrace: Optional[str] = None


class DeadLetterProducer:
    """Producer for dead letter queue messages"""
    
    def __init__(self, bootstrap_servers: str):
        """
        Initialize the DLQ producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 50,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 200,
        })
        
    def send_to_dlq(self, 
                   original_topic: str, 
                   original_key: str, 
                   error_reason: str,
                   original_payload: Dict[str, Any],
                   stacktrace: Optional[str] = None) -> None:
        """
        Send a failed message to the dead letter queue
        
        Args:
            original_topic: The topic the message was originally destined for
            original_key: The key of the original message
            error_reason: Description of the error
            original_payload: The original message payload
            stacktrace: Optional stacktrace for debugging
        """
        # DLQ topic naming convention: original_topic.dlq
        dlq_topic = f"{original_topic}.dlq"
        
        # Create DLQ message
        dlq_message = DeadLetterMessage(
            original_topic=original_topic,
            original_key=original_key,
            error_reason=error_reason,
            error_timestamp=datetime.datetime.now().timestamp(),
            original_payload=original_payload,
            stacktrace=stacktrace
        )
        
        # Serialize to JSON string
        try:
            message_json = json.dumps(dlq_message.dict())
            
            # Send to DLQ topic
            self.producer.produce(
                topic=dlq_topic,
                key=original_key,
                value=message_json
            )
            
            # Ensure it's sent immediately
            self.producer.flush(timeout=5)
            
            logger.info(f"Message sent to DLQ topic {dlq_topic}: {error_reason}")
            
        except Exception as e:
            # Last resort logging if even DLQ fails
            logger.error(f"Failed to send to DLQ: {e}")
            logger.error(f"Original error: {error_reason}")
            logger.error(f"Original message: {original_payload}")
