"""
Tests for Trade model serialization.
"""

import datetime
import random
import unittest

from python_pipeline.models.trade import MakerTakerRole, ThalexTrade
from tests.base_test import AvroSerializationTestBase


class TradeSerializationTest(AvroSerializationTestBase):
    """Test serialization of Trade models"""
    
    def test_generated_trade(self):
        """Test serialization of a generated trade"""
        # Generate a trade
        trade = ThalexTrade.generate()
        
        # Test serialization/deserialization
        self.assert_model_serdes(trade)
    
    def test_custom_trade(self):
        """Test serialization with custom values"""
        # Create a trade with specific values
        trade = ThalexTrade(
            trade_id="TR123456",
            order_id="ORD789012",
            client_order_id=42,
            instrument_name="BTC-PERPETUAL",
            price=96000.0,
            amount=0.5,
            maker_taker=MakerTakerRole.MAKER,
            time=datetime.datetime.now().timestamp()
        )
        
        # Test serialization/deserialization
        self.assert_model_serdes(trade)
    
    def test_null_values(self):
        """Test serialization with null values for nullable fields"""
        # Create base trade
        trade = ThalexTrade.generate()
        
        # Set nullable fields to None
        trade.client_order_id = None
        
        # Test serialization/deserialization
        self.assert_model_serdes(trade)
    
    def test_all_maker_taker_values(self):
        """Test serialization with all possible maker/taker values"""
        for role in MakerTakerRole:
            # Generate base trade
            trade = ThalexTrade.generate()
            
            # Set the maker/taker role
            trade.maker_taker = role
            
            # Test serialization/deserialization
            self.assert_model_serdes(trade)


if __name__ == '__main__':
    unittest.main()
