"""
Tests for AckDataGenerator.
Verifies that the generator creates valid ThalexAck instances.
"""

import unittest

from python_pipeline.data_generators.ack_generator import AckDataGenerator
from python_pipeline.models.ack import OrderSide, OrderStatus, OrderType, ThalexAck, TimeInForce


class AckDataGeneratorTest(unittest.TestCase):
    """Test the AckDataGenerator"""
    
    def setUp(self):
        """Set up the test environment"""
        self.generator = AckDataGenerator()
    
    def test_generate_basic(self):
        """Test basic generation of an ack"""
        # Generate a basic ack
        ack = self.generator.generate()
        
        # Verify that it's a ThalexAck instance
        self.assertIsInstance(ack, ThalexAck)
        
        # Check required fields are populated
        self.assertIsNotNone(ack.order_id)
        self.assertIsNotNone(ack.direction)
        self.assertIsNotNone(ack.amount)
        self.assertIsNotNone(ack.filled_amount)
        self.assertIsNotNone(ack.remaining_amount)
        self.assertIsNotNone(ack.status)
        self.assertIsNotNone(ack.order_type)
        self.assertIsNotNone(ack.time_in_force)
        self.assertIsNotNone(ack.change_reason)
        self.assertIsNotNone(ack.create_time)
    
    def test_generate_with_status(self):
        """Test generation with specific status values"""
        # Test each possible status
        for status in OrderStatus:
            # Generate ack with the status
            ack = self.generator.generate(status=status.value)
            
            # Verify the status is set correctly
            self.assertEqual(ack.status, status)
            
            # Check that status-dependent fields match the logic
            if status == OrderStatus.OPEN:
                self.assertEqual(ack.filled_amount, 0.0)
                self.assertEqual(ack.remaining_amount, ack.amount)
                self.assertEqual(ack.change_reason, "insert")
                self.assertIsNone(ack.delete_reason)
                self.assertIsNotNone(ack.insert_reason)
            elif status == OrderStatus.PARTIALLY_FILLED:
                self.assertGreater(ack.filled_amount, 0.0)
                self.assertLess(ack.filled_amount, ack.amount)
                self.assertEqual(ack.remaining_amount, ack.amount - ack.filled_amount)
                self.assertEqual(ack.change_reason, "fill")
                self.assertIsNone(ack.delete_reason)
                self.assertIsNone(ack.insert_reason)
            elif status == OrderStatus.FILLED:
                self.assertEqual(ack.filled_amount, ack.amount)
                self.assertEqual(ack.remaining_amount, 0.0)
                self.assertEqual(ack.change_reason, "fill")
                self.assertIsNone(ack.delete_reason)
                self.assertIsNone(ack.insert_reason)
            elif status == OrderStatus.CANCELLED:
                self.assertEqual(ack.filled_amount, 0.0)
                self.assertEqual(ack.remaining_amount, 0.0)
                self.assertEqual(ack.change_reason, "cancel")
                self.assertIsNotNone(ack.delete_reason)
                self.assertIsNone(ack.insert_reason)
            elif status == OrderStatus.CANCELLED_PARTIALLY_FILLED:
                self.assertGreater(ack.filled_amount, 0.0)
                self.assertLess(ack.filled_amount, ack.amount)
                self.assertEqual(ack.remaining_amount, 0.0)
                self.assertEqual(ack.change_reason, "cancel")
                self.assertIsNotNone(ack.delete_reason)
                self.assertIsNone(ack.insert_reason)
    
    def test_generate_with_direction(self):
        """Test generation with specific direction values"""
        for direction in OrderSide:
            # Generate ack with the direction
            ack = self.generator.generate(direction=direction.value)
            
            # Verify the direction is set correctly
            self.assertEqual(ack.direction, direction)
    
    def test_generate_with_custom_values(self):
        """Test generation with custom field values"""
        # Define custom values
        custom_values = {
            'order_id': 'CUSTOM123',
            'client_order_id': 999,
            'instrument_name': 'ETH-PERPETUAL',
            'direction': OrderSide.BUY,
            'price': 2500.0,
            'amount': 5.0,
            'filled_amount': 2.5,
            'remaining_amount': 2.5,
            'status': OrderStatus.PARTIALLY_FILLED,
            'order_type': OrderType.MARKET,
            'time_in_force': TimeInForce.IMMEDIATE_OR_CANCEL,
            'change_reason': 'TEST',
            'delete_reason': 'TEST_DELETE',
            'insert_reason': 'TEST_INSERT',
            'create_time': 1600000000.0,
            'persistent': True
        }
        
        # Generate ack with custom values
        ack = self.generator.generate(**custom_values)
        
        # Verify all custom values were used
        for key, value in custom_values.items():
            self.assertEqual(getattr(ack, key), value)


if __name__ == '__main__':
    unittest.main()
