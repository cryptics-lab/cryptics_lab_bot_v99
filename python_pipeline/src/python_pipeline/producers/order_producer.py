"""
Order producer for CrypticsLabBot.
Produces ThalexOrder data to Kafka.
"""

import logging
import random
from typing import Any, Dict

from python_pipeline.models import ThalexOrder
from python_pipeline.producers.base_producer import AvroProducerBase

# Configure logging
logger = logging.getLogger(__name__)


class OrderProducer(AvroProducerBase):
    """Producer for ThalexOrder data"""
    
    # Topic key for configuration
    topic_key: str = "order"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the order producer.
        
        Args:
            config: Application configuration
        """
        super().__init__(config, ThalexOrder)
    
    async def generate_and_produce(self) -> None:
        """Generate order data and produce to Kafka."""
        try:
            # Generate buy and sell orders
            buy_order = ThalexOrder.generate(direction="buy", client_id=random.randint(100, 199))
            sell_order = ThalexOrder.generate(direction="sell", client_id=random.randint(200, 299))
            
            logger.info(f"Generated buy order ID: {buy_order.order_id}, price: {buy_order.price}")
            logger.info(f"Generated sell order ID: {sell_order.order_id}, price: {sell_order.price}")
            
            # Send orders to Kafka
            self.produce(buy_order.order_id, buy_order)
            self.produce(sell_order.order_id, sell_order)
            
        except Exception as e:
            logger.error(f"Error generating and producing order data: {e}")
