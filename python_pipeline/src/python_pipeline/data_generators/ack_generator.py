"""
Ack data generator for CrypticsLabBot.
"""

import datetime
import random
import uuid

from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.models.ack import (
    OrderSide,
    OrderStatus,
    OrderType,
    ThalexAck,
    TimeInForce,
)


class AckDataGenerator(BaseDataGenerator):
    """Generator for Ack model data"""
    
    def generate(self, **kwargs) -> ThalexAck:
        """Generate sample ack data based on Thalex format"""
        # Generate a default order ID if not provided
        order_id = kwargs.get('order_id', str(uuid.uuid4()))
        
        # Basic parameters
        client_order_id = kwargs.get('client_order_id', random.randint(100, 999))
        instrument_name = kwargs.get('instrument_name', "BTC-PERPETUAL")
        direction = kwargs.get('direction', random.choice(list(OrderSide)))
        price = kwargs.get('price', random.uniform(95000, 97000))
        amount = kwargs.get('amount', random.uniform(0.1, 1.0))
        status = kwargs.get('status', random.choice(list(OrderStatus)))
        
        # Default values for amounts based on status
        filled_amount = kwargs.get('filled_amount')
        remaining_amount = kwargs.get('remaining_amount')
        
        if filled_amount is None and remaining_amount is None:
            if status == OrderStatus.OPEN:
                filled_amount = 0.0
                remaining_amount = amount
            elif status == OrderStatus.PARTIALLY_FILLED:
                filled_amount = amount * random.uniform(0.1, 0.9)
                remaining_amount = amount - filled_amount
            elif status == OrderStatus.FILLED:
                filled_amount = amount
                remaining_amount = 0.0
            elif status == OrderStatus.CANCELLED:
                filled_amount = 0.0
                remaining_amount = 0.0
            else:  # cancelled_partially_filled
                filled_amount = amount * random.uniform(0.1, 0.9)
                remaining_amount = 0.0
        elif filled_amount is None:
            filled_amount = amount - (remaining_amount or 0.0)
        elif remaining_amount is None:
            remaining_amount = amount - (filled_amount or 0.0)
        
        # Reason fields based on status
        if status == OrderStatus.OPEN:
            change_reason = kwargs.get('change_reason', "insert")
            delete_reason = kwargs.get('delete_reason', None)
            insert_reason = kwargs.get('insert_reason', "client_request")
        elif status in (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED):
            change_reason = kwargs.get('change_reason', "fill")
            delete_reason = kwargs.get('delete_reason', None)
            insert_reason = kwargs.get('insert_reason', None)
        else:  # cancelled or cancelled_partially_filled
            change_reason = kwargs.get('change_reason', "cancel")
            delete_reason = kwargs.get('delete_reason', "client_cancel")
            insert_reason = kwargs.get('insert_reason', None)
            
        # Other parameters
        order_type = kwargs.get('order_type', OrderType.LIMIT)
        time_in_force = kwargs.get('time_in_force', TimeInForce.GOOD_TILL_CANCELLED)
        create_time = kwargs.get('create_time', datetime.datetime.now().timestamp())
        persistent = kwargs.get('persistent', False)
        
        return ThalexAck(
            order_id=order_id,
            client_order_id=client_order_id,
            instrument_name=instrument_name,
            direction=direction,
            price=price,
            amount=amount,
            filled_amount=filled_amount,
            remaining_amount=remaining_amount,
            status=status,
            order_type=order_type,
            time_in_force=time_in_force,
            change_reason=change_reason,
            delete_reason=delete_reason,
            insert_reason=insert_reason,
            create_time=create_time,
            persistent=persistent
        )
