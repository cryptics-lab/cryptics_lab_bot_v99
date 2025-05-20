"""
Trade data model for CrypticsLabBot.
Implements Pydantic models for trade data.
"""

import datetime
import random
import uuid
from enum import Enum
from typing import ClassVar, Optional

from pydantic import Field

from python_pipeline.models.model_base import ModelBase


class MakerTakerRole(str, Enum):
    """Maker/Taker role enum"""
    MAKER = "maker"
    TAKER = "taker"


class ThalexTrade(ModelBase):
    """Trade model based on the Thalex trade structure"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "trade"
    
    # Fields matching exact structure in v1.avsc
    trade_id: str = Field(..., description="Unique trade identifier")
    order_id: str = Field(..., description="Exchange order ID")
    client_order_id: Optional[int] = Field(None, description="Client order ID")
    instrument_name: str = Field(..., description="Instrument name")
    price: float = Field(..., description="Trade execution price")
    amount: float = Field(..., description="Trade execution amount")
    maker_taker: MakerTakerRole = Field(..., description="Maker or taker role")
    time: float = Field(..., description="Trade timestamp")

    @classmethod
    def generate(cls, order_id: Optional[str] = None, **kwargs) -> 'ThalexTrade':
        """Generate sample trade data based on Thalex format"""
        
        # Generate a random trade ID if not provided
        trade_id = kwargs.get('trade_id', f"TRADE-{str(uuid.uuid4())[:8]}")
        
        # Generate order ID if not provided
        if not order_id:
            order_id = f"{random.randint(0, 255):02X}" + f"{random.randint(0, 16777215):06X}" + "00000000"
        
        # Optional client order ID
        client_order_id = kwargs.get('client_order_id', random.randint(100, 999) if random.random() > 0.3 else None)
        
        # Set instrument name
        instrument_name = kwargs.get('instrument_name', "BTC-PERPETUAL")
        
        # Set price and amount
        price = kwargs.get('price', random.uniform(95000, 97000))
        amount = kwargs.get('amount', random.uniform(0.1, 1.0))
        
        # Set maker/taker role
        if 'maker_taker' in kwargs:
            # Use the provided maker_taker (string or enum)
            maker_taker_val = kwargs['maker_taker']
            if isinstance(maker_taker_val, str):
                maker_taker = MakerTakerRole(maker_taker_val)
            else:
                maker_taker = maker_taker_val
        else:
            # Select a random maker_taker from the enum
            maker_taker = random.choice(list(MakerTakerRole))
        
        # Set timestamp
        time = kwargs.get('time', datetime.datetime.now().timestamp())
        
        return cls(
            trade_id=trade_id,
            order_id=order_id,
            client_order_id=client_order_id,
            instrument_name=instrument_name,
            price=price,
            amount=amount,
            maker_taker=maker_taker,
            time=time
        )
