"""
Unit tests for AckDataGenerator.
"""

import pytest

from python_pipeline.data_generators.ack_generator import AckDataGenerator
from python_pipeline.models.ack import OrderSide, OrderStatus, ThalexAck


class TestAckGenerator:
    
    def test_ack_generator_creates_valid_instance(self):
        """Test generator creates a valid Ack model"""
        generator = AckDataGenerator()
        ack = generator.generate()
        
        # Verify it's a valid instance of the right type
        assert isinstance(ack, ThalexAck)
        
        # Verify all required fields are present and valid
        assert isinstance(ack.order_id, str)
        assert isinstance(ack.amount, float)
        assert isinstance(ack.filled_amount, float)
        assert isinstance(ack.remaining_amount, float)
        assert isinstance(ack.status, OrderStatus)

    def test_ack_generator_status_specific_logic(self):
        """Test the generator handles different order statuses correctly"""
        generator = AckDataGenerator()
        
        # Test with OPEN status
        open_ack = generator.generate(status=OrderStatus.OPEN)
        assert open_ack.status == OrderStatus.OPEN
        assert open_ack.filled_amount == 0.0
        assert open_ack.remaining_amount == open_ack.amount
        
        # Test with FILLED status
        filled_ack = generator.generate(status=OrderStatus.FILLED)
        assert filled_ack.status == OrderStatus.FILLED
        assert filled_ack.filled_amount == filled_ack.amount
        assert filled_ack.remaining_amount == 0.0
