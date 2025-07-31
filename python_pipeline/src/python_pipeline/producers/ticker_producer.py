"""
Ticker producer for CrypticsLabBot.
Produces ThalexTicker data to Kafka.
"""

import logging
from typing import Any, Dict

from python_pipeline.models import ThalexTicker
from python_pipeline.producers.base_producer import AvroProducerBase

# Configure logging
logger = logging.getLogger(__name__)


class TickerProducer(AvroProducerBase):
    """Producer for ThalexTicker data"""
    
    # Topic key for configuration
    topic_key: str = "ticker"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ticker producer.
        
        Args:
            config: Application configuration
        """
        super().__init__(config, ThalexTicker)
    
    async def generate_and_produce(self) -> None:
        """Generate ticker data and produce to Kafka."""
        try:
            # Generate ticker data
            ticker = ThalexTicker.generate()
            logger.info(f"Generated ticker with mark_price: {ticker.mark_price}")
            
            # Send to Kafka
            self.produce(ticker.instrument_name, ticker)
            
        except Exception as e:
            logger.error(f"Error generating and producing ticker data: {e}")
