#!/usr/bin/env python3
"""
Producer Service Module
=====================
Manages Kafka producers.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

# Ensure factories.py is properly imported
import python_pipeline.factories as factories
from python_pipeline.factories import ProducerFactory
from python_pipeline.producers.base_producer import AvroProducerBase
from python_pipeline.services.config_service import ConfigService

logger: logging.Logger = logging.getLogger("producer-service")

class ProducerService:
    """Service for managing Kafka producers"""
    
    def __init__(self, config_service: ConfigService) -> None:
        """
        Initialize the producer service
        
        Args:
            config_service: The configuration service
        """
        self.config_service: ConfigService = config_service
        self.producers: Dict[str, AvroProducerBase] = {}
        self.topics: List[str] = []
    
    def setup_producers(self) -> None:
        """Set up the data producers"""
        logger.info("Setting up producers...")
        
        for model in self.config_service.enabled_models:
            try:
                logger.debug(f"Creating {model} producer...")
                # Use the dictionary version of the config
                config_dict = self.config_service.get_config()
                producer: AvroProducerBase = ProducerFactory.create(model, config_dict)
                producer.initialize()
                self.producers[model] = producer
                
                # Add topic to list
                topic: str = self.config_service.get_topic_name(model)
                self.topics.append(topic)
                
                logger.info(f"✓ {model} producer initialized")
            except Exception as e:
                logger.error(f"Failed to create {model} producer: {e}")
    
    async def run_producers(self, duration: int = 5) -> None:
        """
        Run producers for a specific duration
        
        Args:
            duration: Duration in seconds
        """
        logger.info(f"Running producers for {duration} seconds...")
        
        # Get update interval
        update_seconds: float = self.config_service.get_update_interval()
        
        start_time: float = time.time()
        
        # Reduce logging in producer classes
        producer_loggers: List[str] = [
            "python_pipeline.producers.ticker_producer",
            "python_pipeline.producers.order_producer",
            "python_pipeline.producers.index_producer"
        ]
        
        for logger_name in producer_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
        
        try:
            while time.time() - start_time < duration:
                # Process each producer
                for model_name, producer in self.producers.items():
                    await producer.generate_and_produce()
                
                # Wait for next interval
                await asyncio.sleep(update_seconds)
        finally:
            # Restore logging levels
            for logger_name in producer_loggers:
                logging.getLogger(logger_name).setLevel(logging.INFO)
    
    def flush_producers(self) -> None:
        """Flush all producers to ensure messages are sent"""
        logger.info("Flushing producers...")
        
        for model, producer in self.producers.items():
            logger.info(f"Flushing {model} producer...")
            producer.flush()
