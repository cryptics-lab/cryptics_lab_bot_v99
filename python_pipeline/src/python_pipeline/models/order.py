"""
Order data model for CrypticsLabBot.
Implements Pydantic models for order data.
"""

import datetime
import json
import random
from typing import Any, ClassVar, List, Optional

from pydantic import Field

from python_pipeline.models.model_base import ModelBase


class ThalexFill(ModelBase):
    """Fill data for Thalex orders"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "Fill"
    
    # This is a placeholder until we have actual fill data
    # Will be implemented when we have the actual data structure
    
    @classmethod
    def generate(cls, **kwargs) -> 'ThalexFill':
        """Generate sample fill data"""
        return cls()


class ThalexOrder(ModelBase):
    """Order model matching the session.orders Thalex WebSocket data"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "Order"
    
    order_id: str = Field(..., description="Exchange order ID")
    instrument_name: str = Field(..., description="Instrument name")
    direction: str = Field(..., description="Order direction (buy/sell)")
    price: float = Field(..., description="Order price")
    amount: float = Field(..., description="Order amount")
    filled_amount: float = Field(..., description="Amount filled")
    remaining_amount: float = Field(..., description="Amount remaining")
    client_order_id: int = Field(..., description="Client-assigned order ID")
    status: str = Field(..., description="Order status")
    fills: str = Field(default="[]", description="Order fills (JSON string)")
    change_reason: str = Field(..., description="Reason for the change")
    insert_reason: str = Field(..., description="Reason for the insertion")
    create_time: float = Field(..., description="Creation timestamp")
    order_type: str = Field(..., description="Order type")
    time_in_force: str = Field(..., description="Time in force")
    persistent: bool = Field(..., description="Whether the order is persistent")
    
    @classmethod
    def generate(cls, direction: str = "buy", client_id: int = 100) -> 'ThalexOrder':
        """Generate a sample order based on actual Thalex data"""
        base_price = random.uniform(95000, 97000)
        
        return cls(
            order_id=f"001F377D0000000{client_id % 10}",
            instrument_name="BTC-PERPETUAL",
            direction=direction,
            price=base_price - 100 if direction == "buy" else base_price + 100,
            amount=random.uniform(0.1, 0.5),
            filled_amount=0.0,
            remaining_amount=random.uniform(0.1, 0.5),
            client_order_id=client_id,
            status="open",
            fills="[]",  # Empty JSON array as string
            change_reason="insert" if client_id % 2 == 0 else "amend",
            insert_reason="client_request",
            create_time=datetime.datetime.now().timestamp(),
            order_type="limit",
            time_in_force="good_till_cancelled",
            persistent=False
        )
