"""
Price index model for CrypticsLabBot.
Implements Pydantic models for index data.
"""

import datetime
import random
from typing import ClassVar

from pydantic import Field, PrivateAttr

from python_pipeline.models.model_base import ModelBase


class PriceIndex(ModelBase):
    """Price index model matching the price_index.BTCUSD Thalex WebSocket data"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "Index"
    
    # Field names that match the v2.avsc schema
    index_name: str = Field(..., description="Index name (BTCUSD)")
    price: float = Field(..., description="Index price")
    timestamp: float = Field(..., description="Index timestamp")
    
    # For backward compatibility with code that might expect previous_settlement_price
    # but not serialized to Avro - using proper Pydantic PrivateAttr
    _previous_settlement_price: float = PrivateAttr(default=0.0)
    
    def __init__(self, **data):
        super().__init__(**data)
        self._previous_settlement_price = 0.0
    
    @property
    def previous_settlement_price(self) -> float:
        return self._previous_settlement_price
    
    @previous_settlement_price.setter
    def previous_settlement_price(self, value: float):
        self._previous_settlement_price = value
    
    @classmethod
    def generate(cls, index_name="BTCUSD") -> 'PriceIndex':
        """Generate a sample price index based on actual Thalex data"""
        instance = cls(
            index_name=index_name,
            price=random.uniform(95000, 97000),
            timestamp=datetime.datetime.now().timestamp()
        )
        # Set the non-Avro field
        instance._previous_settlement_price = random.uniform(93000, 94000)
        return instance
    
    # Add an alias property for backward compatibility
    @property 
    def symbol(self) -> str:
        return self.index_name
    
    @symbol.setter
    def symbol(self, value: str):
        self.index_name = value


# Create an alias for consistency in naming
ThalexIndex = PriceIndex