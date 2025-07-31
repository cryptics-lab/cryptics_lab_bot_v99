"""
Ticker data generator for CrypticsLabBot.
"""

import datetime
import random

from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.models.ticker import ThalexTicker


class TickerGenerator(BaseDataGenerator):
    """Generator for Thalex ticker data"""
    
    def generate(self, **kwargs) -> ThalexTicker:
        """Generate sample ticker data based on actual Thalex data"""
        base_price = random.uniform(95000, 97000)
        bid_price = base_price - random.uniform(10, 30)
        ask_price = base_price + random.uniform(10, 30)
        
        return ThalexTicker(
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
