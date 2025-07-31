"""
Ack producer for CrypticsLabBot.
Generates and produces Ack events to Kafka.
Uses a data generator to create the data.
"""

import logging
import random
from typing import Any, Dict

from python_pipeline.data_generators.factory import DataGeneratorFactory
from python_pipeline.models.ack import OrderStatus, ThalexAck
from python_pipeline.producers.base_producer import AvroProducerBase

# Configure logging
logger = logging.getLogger(__name__)


class AckProducer(AvroProducerBase):
    """Producer for Ack data that leverages a dedicated data generator"""
    
    # Topic key for configuration
    topic_key = "ack"
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Ack producer"""
        super().__init__(config, ThalexAck)
        self.orders = {}  # Track order state to generate realistic updates
        self.data_generator = DataGeneratorFactory.create("ack")
    
    async def generate_and_produce(self, **kwargs) -> None:
        """Generate and produce Ack data using the data generator"""
        
        # Decide whether to create a new order or update an existing one
        if not self.orders or random.random() < 0.7:  # 70% chance to create new order
            # Generate new order Ack using the data generator
            ack = self.data_generator.generate(status=OrderStatus.OPEN)
            
            # Store order
            self.orders[ack.order_id] = {
                "status": OrderStatus.OPEN,
                "amount": ack.amount,
                "filled": 0,
                "direction": ack.direction,
                "client_order_id": ack.client_order_id
            }
            
            logger.debug(f"Generated new order Ack: {ack.order_id} ({ack.status})")
        else:
            # Update an existing order
            if not self.orders:
                return
                
            # Choose a random existing order
            order_id = random.choice(list(self.orders.keys()))
            order = self.orders[order_id]
            
            # Determine next status based on current status
            new_status = None
            
            if order["status"] == OrderStatus.OPEN:
                if random.random() < 0.3:  # 30% chance to cancel
                    new_status = OrderStatus.CANCELLED
                    order["filled"] = 0
                else:  # 70% chance to partially fill
                    new_status = OrderStatus.PARTIALLY_FILLED
                    order["filled"] = order["amount"] * random.uniform(0.1, 0.5)
            elif order["status"] == OrderStatus.PARTIALLY_FILLED:
                if random.random() < 0.3:  # 30% chance to cancel
                    new_status = OrderStatus.CANCELLED_PARTIALLY_FILLED
                else:  # 70% chance to fill completely
                    new_status = OrderStatus.FILLED
                    order["filled"] = order["amount"]
            else:
                # Order already in terminal state, skip update
                return
            
            # Generate Ack update using the data generator with custom parameters
            ack = self.data_generator.generate(
                status=new_status,
                direction=order["direction"]
            )
            
            # Override specific fields to maintain order consistency
            ack.order_id = order_id
            ack.client_order_id = order["client_order_id"]
            ack.amount = order["amount"]
            ack.filled_amount = order["filled"]
            
            if new_status in [OrderStatus.CANCELLED, OrderStatus.CANCELLED_PARTIALLY_FILLED]:
                ack.remaining_amount = 0
            elif new_status == OrderStatus.PARTIALLY_FILLED:
                ack.remaining_amount = ack.amount - ack.filled_amount
            elif new_status == OrderStatus.FILLED:
                ack.remaining_amount = 0
            
            # Update order state
            order["status"] = ack.status
            
            logger.debug(f"Generated update Ack: {ack.order_id} ({ack.status})")
        
        # Produce to Kafka
        key = ack.order_id
        self.produce(key, ack)
