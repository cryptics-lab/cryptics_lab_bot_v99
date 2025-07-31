#!/usr/bin/env python3
"""
Kafka Service Module
===================
Manages interactions with Kafka, including topics and schema registry.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

from python_pipeline.services.config_service import ConfigService
from python_pipeline.utils.connection_utils import wait_for_schema_registry

logger: logging.Logger = logging.getLogger("kafka-service")

class KafkaService:
    """Service for interacting with Kafka"""
    
    def __init__(self, config_service: ConfigService) -> None:
        """
        Initialize the Kafka service
        
        Args:
            config_service: The configuration service
        """
        self.config_service: ConfigService = config_service
        self.kafka_config: Dict[str, str] = config_service.get_kafka_config()
        
    async def verify_topics_exist(self, topics: List[str], timeout: int = 10) -> Dict[str, bool]:
        """
        Verify that topics exist and have data flowing
        
        Args:
            topics: List of topics to verify
            timeout: Timeout in seconds
            
        Returns:
            Dictionary mapping topics to boolean indicating if data was found
        """
        logger.info("Verifying topic data...")
        
        results: Dict[str, bool] = {}
        
        # Get Kafka configuration
        bootstrap_servers: str = self.kafka_config['bootstrap_servers']
        
        # Check each topic
        for topic in topics:
            logger.info(f"Checking for data in topic: {topic}")
            
            # Create consumer
            consumer_config: Dict[str, Any] = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'pipeline-verification-consumer',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'session.timeout.ms': 10000
            }
            
            consumer: Consumer = Consumer(consumer_config)
            
            try:
                consumer.subscribe([topic])
                
                # Poll for messages
                start_time: float = time.time()
                found_data: bool = False
                
                while time.time() - start_time < timeout and not found_data:
                    msg = consumer.poll(1.0)
                    
                    if msg and not msg.error():
                        found_data = True
                        logger.info(f"Data found in topic: {topic}")
                        break
                
                if not found_data:
                    logger.warning(f"No data found in topic: {topic}")
                
                results[topic] = found_data
                
            except Exception as e:
                logger.error(f"Error while checking topic {topic}: {str(e)}")
                results[topic] = False
            finally:
                # Ensure consumer is always closed
                try:
                    consumer.close()
                except Exception as e:
                    logger.warning(f"Error closing consumer for topic {topic}: {str(e)}")
        
        return results
    
    async def verify_schema_registry(self) -> Tuple[bool, List[str]]:
        """
        Verify Schema Registry is working
        
        Returns:
            (success: bool, subjects: List[str]) - Success flag and list of subjects
        """
        logger.info("Verifying Schema Registry...")
        
        schema_registry_url: str = self.kafka_config['schema_registry_url']
        
        try:
            # Check Schema Registry connection
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{schema_registry_url}/subjects", timeout=10) as response:
                    if response.status == 200:
                        subjects: List[str] = await response.json()
                        logger.info(f"Schema Registry is working with {len(subjects)} registered subjects")
                        return True, subjects
                    else:
                        logger.error(f"Schema Registry returned status code: {response.status}")
                        return False, []
        except Exception as e:
            logger.error(f"Failed to connect to Schema Registry: {e}")
            return False, []
