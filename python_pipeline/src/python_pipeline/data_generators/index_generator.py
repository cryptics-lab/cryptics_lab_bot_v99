"""
Price index data generator for CrypticsLabBot.
"""

import datetime
import random

from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.models.index import PriceIndex, ThalexIndex


class IndexGenerator(BaseDataGenerator):
    """Generator for price index data"""
    
    def generate(self, **kwargs) -> PriceIndex:
        """Generate a sample price index based on actual Thalex data"""
        index_name = kwargs.get('index_name', 'BTCUSD')
        
        instance = PriceIndex(
            symbol=index_name,
            price=random.uniform(95000, 97000),
            timestamp=datetime.datetime.now().timestamp()
        )
        
        # Set the non-Avro field
        instance._previous_settlement_price = random.uniform(93000, 94000)
        return instance
