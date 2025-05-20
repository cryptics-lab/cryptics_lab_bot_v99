"""
Base test class for Avro serialization testing.
"""

import json
import unittest
from typing import Any, Dict, Optional, Type

from python_pipeline.models.model_base import ModelBase
from python_pipeline.producers.avro_schema_loader import get_schema_for_model
from python_pipeline.utils.avro_utils import deserialize_bytes, serialize_model


class AvroSerializationTestBase(unittest.TestCase):
    """Base class for Avro serialization tests"""
    
    def assert_model_serdes(self, model: ModelBase, model_class: Optional[Type[ModelBase]] = None) -> None:
        """
        Test serialization and deserialization of a model.
        
        Args:
            model: The model instance to test
            model_class: Optional model class (if not provided, will use type(model))
        """
        if model_class is None:
            model_class = type(model)
        
        # Get schema
        schema = get_schema_for_model(model_class, use_schema_files=True)
        
        # Serialize
        serialized_bytes = serialize_model(model, schema)
        
        # Make sure we got something
        self.assertIsNotNone(serialized_bytes)
        self.assertTrue(len(serialized_bytes) > 0)
        
        # Deserialize
        deserialized_model = deserialize_bytes(serialized_bytes, schema, model_class)
        
        # Compare original and deserialized model
        original_dict = model.dict()
        deserialized_dict = deserialized_model.dict()
        
        # Debug print on failure
        if original_dict != deserialized_dict:
            print("Original:")
            print(json.dumps(original_dict, indent=2, default=str))
            print("Deserialized:")
            print(json.dumps(deserialized_dict, indent=2, default=str))
            
            # Show specific differences
            for key in original_dict:
                if key in deserialized_dict and original_dict[key] != deserialized_dict[key]:
                    print(f"Mismatch in '{key}': {original_dict[key]} != {deserialized_dict[key]}")
        
        self.assertEqual(original_dict, deserialized_dict)
