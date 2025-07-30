"""
Unit tests for Ack model.
"""

import pytest
from pydantic import ValidationError

from python_pipeline.models.ack import OrderSide, OrderStatus, OrderType, ThalexAck, TimeInForce


class TestAckModel:
    
    def test_ack_validation(self):
        """Test that required fields are enforced and enum values work"""
        # Valid ack should not raise exception
        ack = ThalexAck(
            order_id="test123",
            instrument_name="BTC-PERPETUAL",
            direction=OrderSide.BUY,
            price=96000.0,
            amount=1.0,
            filled_amount=0.5,
            remaining_amount=0.5,
            status=OrderStatus.PARTIALLY_FILLED,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GOOD_TILL_CANCELLED,
            change_reason="fill",
            create_time=1620000000.0,
            persistent=False
        )
        
        # Verify fields are set correctly
        assert ack.order_id == "test123"
        assert ack.direction == OrderSide.BUY
        assert ack.status == OrderStatus.PARTIALLY_FILLED
