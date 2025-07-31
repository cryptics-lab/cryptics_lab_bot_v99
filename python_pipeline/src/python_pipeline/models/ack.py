"""
Ack data model for CrypticsLabBot.
Implements Pydantic models for order acknowledgment data.
"""

from __future__ import annotations

import datetime
from enum import Enum
from typing import ClassVar, Literal, Optional

from pydantic import Field

from python_pipeline.models.model_base import ModelBase


class OrderSide(str, Enum):
    """Order side enum (buy/sell)"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Order status enum"""
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    CANCELLED_PARTIALLY_FILLED = "cancelled_partially_filled"
    FILLED = "filled"


class OrderType(str, Enum):
    """Order type enum"""
    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(str, Enum):
    """Time in force enum"""
    GOOD_TILL_CANCELLED = "good_till_cancelled"
    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"


class ThalexAck(ModelBase):
    """Ack model based on the Thalex order acknowledgment structure"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "ack"
    
    # Fields matching exact structure needed for the database
    order_id: str = Field(..., description="Exchange order ID")
    client_order_id: Optional[int] = Field(None, description="Client order ID")
    instrument_name: str = Field("BTC-PERPETUAL", description="Instrument name")
    direction: OrderSide = Field(..., description="Order direction (buy/sell)")
    price: Optional[float] = Field(None, description="Order price")
    amount: float = Field(..., description="Order amount")
    filled_amount: float = Field(..., description="Amount filled")
    remaining_amount: float = Field(..., description="Remaining amount")
    status: OrderStatus = Field(..., description="Order status")
    order_type: OrderType = Field(..., description="Order type (limit/market)")
    time_in_force: TimeInForce = Field(..., description="Time in force")
    change_reason: str = Field(..., description="Reason for change")
    delete_reason: Optional[str] = Field(None, description="Reason for deletion")
    insert_reason: Optional[str] = Field(None, description="Reason for insertion")
    create_time: float = Field(..., description="Creation timestamp")
    persistent: bool = Field(..., description="Is order persistent")

