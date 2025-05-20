"""
Trade producer for CrypticsLabBot.
Generates and produces Trade events to Kafka.
"""

import logging
import random
from typing import Any, Dict, Optional

from python_pipeline.models.trade import MakerTakerRole, ThalexTrade
from python_pipeline.producers.base_producer import AvroProducerBase

# Configure logging
logger = logging.getLogger(__name__)


class TradeProducer(AvroProducerBase):
    """Producer for Trade data"""
    
    # Topic key for configuration
    topic_key = "trade"
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Trade producer"""
        super().__init__(config, ThalexTrade)
        self.order_ids = set()  # Track orders that have been traded
    
    async def generate_and_produce(self, **kwargs) -> None:
        """Generate and produce Trade data"""
        
        # Decide whether to use an existing order ID or create a new one
        order_id = None
        if self.order_ids and random.random() < 0.3:  # 30% chance to use existing order
            order_id = random.choice(list(self.order_ids))
        
        # Generate a trade
        trade = ThalexTrade.generate(order_id=order_id)
        
        # Add order_id to tracked orders
        self.order_ids.add(trade.order_id)
        
        # Limit the number of tracked order IDs to prevent memory issues
        if len(self.order_ids) > 100:
            # Remove a random subset of orders
            to_remove = random.sample(list(self.order_ids), 20)
            for order_id in to_remove:
                self.order_ids.remove(order_id)
        
        logger.debug(f"Generated Trade: {trade.trade_id} for order {trade.order_id} as {trade.maker_taker.value}")
        
        # Produce to Kafka
        key = trade.trade_id
        self.produce(key, trade)
