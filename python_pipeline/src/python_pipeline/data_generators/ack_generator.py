"""
Ack data generator for CrypticsLabBot.
Implements fake data generation for order acknowledgment data.
"""

import datetime
import random
import uuid
from typing import Optional

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
        if 'order_id' not in kwargs:
            order_id = str(uuid.uuid4())
        else:
            order_id = kwargs['order_id']
            
        # Use provided client_order_id if available, otherwise generate one
        if 'client_order_id' not in kwargs:
            client_order_id = random.randint(100, 999)
        else:
            client_order_id = kwargs['client_order_id']
        
        # Set instrument_name
        instrument_name = kwargs.get('instrument_name', "BTC-PERPETUAL")
        
        # Set status based on kwargs or random selection
        if 'status' in kwargs:
            # Use the provided status (string or enum)
            status_val = kwargs['status']
            if isinstance(status_val, str):
                # Handle string values
                try:
                    # Try to get the enum by value first
                    status = OrderStatus(status_val)
                except ValueError:
                    # If that fails, try to get it by name (uppercase)
                    try:
                        status = OrderStatus[status_val.upper()]
                    except KeyError:
                        # Default to OPEN if all fails
                        status = OrderStatus.OPEN
            else:
                # Already an enum
                status = status_val
        else:
            # Select a random status from the enum
            status = random.choice(list(OrderStatus))
        
        # Direction
        if 'direction' in kwargs:
            # Use the provided direction (string or enum)
            direction_val = kwargs['direction']
            if isinstance(direction_val, str):
                # Handle string values
                try:
                    # Try to get the enum by value first
                    direction = OrderSide(direction_val)
                except ValueError:
                    # If that fails, try to get it by name (uppercase)
                    try:
                        direction = OrderSide[direction_val.upper()]
                    except KeyError:
                        # Default to BUY if all fails
                        direction = OrderSide.BUY
            else:
                # Already an enum
                direction = direction_val
        else:
            # Select a random direction from the enum
            direction = random.choice(list(OrderSide))
        
        # Set price based on current BTC price range or provided value
        price = kwargs.get('price', random.uniform(95000, 97000))
        
        # Set amount from kwargs or generate random
        amount = kwargs.get('amount', random.uniform(0.1, 1.0))
        
        # Use provided values for filled_amount and remaining_amount if available
        filled_amount = kwargs.get('filled_amount')
        remaining_amount = kwargs.get('remaining_amount')
        
        # If either filled_amount or remaining_amount is not provided, calculate based on status
        if filled_amount is None or remaining_amount is None:
            if status == OrderStatus.OPEN:
                filled_amount = filled_amount or 0.0
                remaining_amount = remaining_amount or amount
                change_reason = kwargs.get('change_reason', "insert")
                delete_reason = kwargs.get('delete_reason', None)
                insert_reason = kwargs.get('insert_reason', "client_request")
            elif status == OrderStatus.PARTIALLY_FILLED:
                if filled_amount is None:
                    filled_amount = amount * random.uniform(0.1, 0.9)
                remaining_amount = remaining_amount or (amount - filled_amount)
                change_reason = kwargs.get('change_reason', "fill")
                delete_reason = kwargs.get('delete_reason', None)
                insert_reason = kwargs.get('insert_reason', None)
            elif status == OrderStatus.FILLED:
                filled_amount = filled_amount or amount
                remaining_amount = remaining_amount or 0.0
                change_reason = kwargs.get('change_reason', "fill")
                delete_reason = kwargs.get('delete_reason', None)
                insert_reason = kwargs.get('insert_reason', None)
            elif status == OrderStatus.CANCELLED:
                filled_amount = filled_amount or 0.0
                remaining_amount = remaining_amount or 0.0
                change_reason = kwargs.get('change_reason', "cancel")
                delete_reason = kwargs.get('delete_reason', random.choice(["client_cancel", "session_end"]))
                insert_reason = kwargs.get('insert_reason', None)
            else:  # cancelled_partially_filled
                if filled_amount is None:
                    filled_amount = amount * random.uniform(0.1, 0.9)
                remaining_amount = remaining_amount or 0.0
                change_reason = kwargs.get('change_reason', "cancel")
                delete_reason = kwargs.get('delete_reason', random.choice(["client_cancel", "session_end"]))
                insert_reason = kwargs.get('insert_reason', None)
        else:
            # Use provided values for change_reason, delete_reason, and insert_reason
            change_reason = kwargs.get('change_reason', "fill")
            delete_reason = kwargs.get('delete_reason', None)
            insert_reason = kwargs.get('insert_reason', None)
        
        # Get other values from kwargs or use defaults
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
