"""
Ticker data model for CrypticsLabBot.
Implements Pydantic models for ticker data.
"""

import datetime
import random
from typing import ClassVar

from pydantic import Field

from python_pipeline.models.model_base import ModelBase


class ThalexTicker(ModelBase):
    """Ticker model based on the exact Thalex WebSocket data structure"""
    
    # Class variables for the ModelBase
    model_name: ClassVar[str] = "Ticker"
    
    # Fields matching Thalex data format but with Avro compatibility
    instrument_name: str = "BTC-PERPETUAL"
    mark_price: float = Field(..., description="Current mark price")
    mark_timestamp: float = Field(..., description="Timestamp of mark price")
    best_bid_price: float = Field(..., description="Best bid price")
    best_bid_amount: float = Field(..., description="Best bid amount")
    best_ask_price: float = Field(..., description="Best ask price")
    best_ask_amount: float = Field(..., description="Best ask amount")
    last_price: float = Field(..., description="Last traded price")
    delta: float = Field(..., description="Delta value")
    volume_24h: float = Field(..., description="24-hour trading volume")
    value_24h: float = Field(..., description="24-hour trading value")
    low_price_24h: float = Field(..., description="24-hour low price")
    high_price_24h: float = Field(..., description="24-hour high price")
    change_24h: float = Field(..., description="24-hour price change")
    index_price: float = Field(..., description="Index price")
    forward: float = Field(..., description="Forward price")
    funding_mark: float = Field(..., description="Funding mark")
    funding_rate: float = Field(..., description="Funding rate")
    collar_low: float = Field(..., description="Lower collar limit")
    collar_high: float = Field(..., description="Upper collar limit")
    realised_funding_24h: float = Field(..., description="24-hour realized funding")
    average_funding_rate_24h: float = Field(..., description="24-hour average funding rate")
    open_interest: float = Field(..., description="Open interest")

    @classmethod
    def generate(cls) -> 'ThalexTicker':
        """Generate sample ticker data based on actual Thalex data"""
        base_price = random.uniform(95000, 97000)
        bid_price = base_price - random.uniform(10, 30)
        ask_price = base_price + random.uniform(10, 30)
        
        return cls(
            mark_price=base_price,
            mark_timestamp=datetime.datetime.now().timestamp(),
            best_bid_price=bid_price,
            best_bid_amount=random.uniform(0.01, 0.1),
            best_ask_price=ask_price,
            best_ask_amount=random.uniform(0.01, 0.1),
            last_price=base_price + random.uniform(-50, 50),
            delta=1.0,
            volume_24h=random.uniform(700, 800),
            value_24h=random.uniform(68000000, 69000000),
            low_price_24h=random.uniform(93000, 94000),
            high_price_24h=random.uniform(97000, 98000),
            change_24h=random.uniform(2000, 3500),
            index_price=base_price - random.uniform(-10, 10),
            forward=base_price - random.uniform(-10, 10),
            funding_mark=random.uniform(0, 0.001),
            funding_rate=random.uniform(0, 0.001),
            collar_low=base_price * 0.99,
            collar_high=base_price * 1.01,
            realised_funding_24h=random.uniform(0, 0.001),
            average_funding_rate_24h=random.uniform(0, 0.001),
            open_interest=random.uniform(3700, 3800)
        )
