"""
Index producer for CrypticsLabBot.
Produces PriceIndex data to Kafka.
"""

import logging
from typing import Any, Dict

from python_pipeline.models import PriceIndex
from python_pipeline.producers.base_producer import AvroProducerBase

# Configure logging
logger = logging.getLogger(__name__)


class IndexProducer(AvroProducerBase):
    """Producer for PriceIndex data"""
    
    # Topic key for configuration
    topic_key: str = "index"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the index producer.
        
        Args:
            config: Application configuration
        """
        super().__init__(config, PriceIndex)
    
    async def generate_and_produce(self) -> None:
        """Generate index data and produce to Kafka."""
        try:
            # Generate index data
            index = PriceIndex.generate(index_name="BTCUSD")
            logger.info(f"Generated index with price: {index.price}")
            
            # Send to Kafka
            self.produce(index.index_name, index)
            
        except Exception as e:
            logger.error(f"Error generating and producing index data: {e}")
