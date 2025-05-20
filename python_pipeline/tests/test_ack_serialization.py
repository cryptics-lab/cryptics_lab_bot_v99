"""
Tests for Ack model serialization.
Using the AckDataGenerator to create test data.
"""

import unittest

from python_pipeline.data_generators.ack_generator import AckDataGenerator
from python_pipeline.models.ack import OrderSide, OrderStatus
from tests.base_test import AvroSerializationTestBase


class AckSerializationTest(AvroSerializationTestBase):
    """Test serialization of Ack models using the AckDataGenerator"""
    
    def setUp(self):
        """Set up the test environment"""
        self.generator = AckDataGenerator()
    
    def test_generated_ack(self):
        """Test serialization of a generated ack"""
        # Generate an ack using the generator
        ack = self.generator.generate()
        
        # Test serialization/deserialization
        self.assert_model_serdes(ack)
    
    def test_specific_status_values(self):
        """Test serialization with specific status values"""
        for status in OrderStatus:
            # Generate ack with specific status using the generator
            ack = self.generator.generate(status=status.value)
            
            # Verify the status is set correctly
            self.assertEqual(ack.status, status)
            
            # Test serialization/deserialization
            self.assert_model_serdes(ack)
    
    def test_specific_direction_values(self):
        """Test serialization with specific direction values"""
        for direction in OrderSide:
            # Generate ack with specific direction using the generator
            ack = self.generator.generate(direction=direction.value)
            
            # Verify the direction is set correctly
            self.assertEqual(ack.direction, direction)
            
            # Test serialization/deserialization
            self.assert_model_serdes(ack)
    
    def test_nullable_fields(self):
        """Test serialization with null values for nullable fields"""
        # Generate base ack
        ack = self.generator.generate()
        
        # Set some nullable fields to None
        ack.client_order_id = None
        ack.price = None
        ack.delete_reason = None
        ack.insert_reason = None
        
        # Test serialization/deserialization
        self.assert_model_serdes(ack)


if __name__ == '__main__':
    unittest.main()
