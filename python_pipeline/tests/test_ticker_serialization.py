"""
Tests for Ticker model serialization.
"""

import datetime
import random
import unittest

from python_pipeline.models.ticker import ThalexTicker
from tests.base_test import AvroSerializationTestBase


class TickerSerializationTest(AvroSerializationTestBase):
    """Test serialization of Ticker models"""
    
    def test_generated_ticker(self):
        """Test serialization of a generated ticker"""
        # Generate a ticker
        ticker = ThalexTicker.generate()
        
        # Test serialization/deserialization
        self.assert_model_serdes(ticker)
    
    def test_custom_ticker(self):
        """Test serialization with custom values"""
        # Create a ticker with specific values
        ticker = ThalexTicker(
            instrument_name="ETH-PERPETUAL",
            mark_price=3520.75,
            mark_timestamp=datetime.datetime.now().timestamp(),
            best_bid_price=3519.50,
            best_bid_amount=10.5,
            best_ask_price=3521.25,
            best_ask_amount=8.3,
            last_price=3520.00,
            delta=0.12,
            volume_24h=12500.0,
            value_24h=44025000.0,
            low_price_24h=3480.0,
            high_price_24h=3550.0,
            change_24h=1.2,
            index_price=3520.50,
            forward=0.05,
            funding_mark=0.001,
            funding_rate=0.0001,
            collar_low=3400.0,
            collar_high=3600.0,
            realised_funding_24h=0.0002,
            average_funding_rate_24h=0.00015,
            open_interest=425.5
        )
        
        # Test serialization/deserialization
        self.assert_model_serdes(ticker)
    
    def test_null_values(self):
        """Test serialization with null values for nullable fields"""
        # Create base ticker
        ticker = ThalexTicker.generate()
        
        # Set some nullable fields to None if the schema allows
        # Since all fields in ThalexTicker are required, this is a no-op
        # but shows the pattern for other models with nullable fields
        
        # Test serialization/deserialization
        self.assert_model_serdes(ticker)


if __name__ == '__main__':
    unittest.main()
