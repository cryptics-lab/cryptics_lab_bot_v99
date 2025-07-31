"""
Avro utilities for serialization and deserialization.
These utilities allow testing Avro serialization without Kafka.
"""

import enum
import io
import json
import logging
from typing import Any, Dict, Optional, Type, Union, get_args, get_origin

import fastavro
from pydantic import BaseModel

from python_pipeline.models.model_base import ModelBase
from python_pipeline.producers.avro_schema_loader import get_schema_for_model
from python_pipeline.producers.schema_generator import AvroSchemaGenerator

# Configure logging
logger = logging.getLogger(__name__)


def model_to_dict(model: ModelBase, ctx=None) -> Dict[str, Any]:
    """
    Convert a Pydantic model to a dict compatible with Avro serialization.
    Handles enums, None values, and special types.
    
    Args:
        model: The model to convert
        ctx: Context (not used, for compatibility with Confluent Kafka)
        
    Returns:
        Dictionary suitable for Avro serialization
    """
    # Convert Pydantic model to dict
    data = model.dict()
    
    # Get schema for field type handling - needed to check for enum fields
    schema = get_schema_for_model(type(model))
    field_types = {field["name"]: field.get("type") for field in schema["fields"]}
    
    # Handle enums - keeping them as enums if schema expects enum type
    for field_name, field_value in list(data.items()):
        if isinstance(field_value, enum.Enum):
            # Check if the schema defines this as an enum type
            field_type = field_types.get(field_name)
            if not (isinstance(field_type, dict) and field_type.get("type") == "enum"):
                # Only convert to string if schema doesn't expect an enum
                data[field_name] = field_value.value
    
    # Get schema for field type handling
    schema = get_schema_for_model(type(model))
    field_types = {field["name"]: field.get("type") for field in schema["fields"]}
    
    # Handle special cases for null values
    for field_name, field_value in list(data.items()):
        if field_name in field_types:
            field_type = field_types[field_name]
            
            # If field is None and field type is a union with null first
            if field_value is None and isinstance(field_type, list) and field_type[0] == "null":
                # Keep the None value - Avro serializer will handle it as null
                pass
            # If field is None but field type doesn't support null
            elif field_value is None and (not isinstance(field_type, list) or "null" not in field_type):
                # Use appropriate default values based on the type
                if field_type == "string":
                    data[field_name] = ""
                elif field_type == "int":
                    data[field_name] = 0
                elif field_type == "double":
                    data[field_name] = 0.0
                elif field_type == "boolean":
                    data[field_name] = False
    
    return data


def dict_to_model(data: Dict[str, Any], model_class: Type[ModelBase]) -> ModelBase:
    """
    Convert a dictionary to a Pydantic model instance.
    Handles enum types by converting string values to enum instances.
    
    Args:
        data: Dictionary from deserialized Avro data
        model_class: The model class to instantiate
        
    Returns:
        Instance of the model class
    """
    # Get field types from model
    field_types = {name: field.outer_type_ for name, field in model_class.__fields__.items()}
    
    # Convert values to appropriate types for model fields
    for field_name, field_value in list(data.items()):
        if field_name in field_types:
            field_type = field_types[field_name]
            
            # Handle enum fields
            if isinstance(field_type, type) and issubclass(field_type, enum.Enum):
                # Convert string value or enum symbol to enum instance
                if field_value is not None:
                    # If we got an Avro enum, it might have a '_type' and '_name' values
                    # Just ensure the value is properly converted to enum
                    if isinstance(field_value, str):
                        data[field_name] = field_type(field_value)
                    elif isinstance(field_value, dict) and '_type' in field_value and field_value['_type'] == 'enum':
                        # Extract the symbol name from an Avro enum structure
                        symbol = field_value.get('_name', '')
                        data[field_name] = field_type(symbol)
                    # Otherwise it might already be properly typed
            
            # Handle Optional[T] fields
            origin = get_origin(field_type)
            args = get_args(field_type)
            if origin is Union and type(None) in args:
                non_none_type = next((arg for arg in args if arg != type(None)), None)
                if non_none_type and isinstance(non_none_type, type) and issubclass(non_none_type, enum.Enum):
                    if field_value is not None:
                        data[field_name] = non_none_type(field_value)
    
    # Instantiate model
    return model_class(**data)


def serialize_model(model: ModelBase, schema: Optional[Dict[str, Any]] = None) -> bytes:
    """
    Serialize a Pydantic model to Avro bytes.
    
    Args:
        model: The model to serialize
        schema: Optional schema dict (if not provided, will be loaded or generated)
        
    Returns:
        Avro-serialized bytes
    """
    # Get schema if not provided
    if schema is None:
        schema = get_schema_for_model(type(model))
    
    # Convert model to dict using same logic as producer
    data = model_to_dict(model)
    
    # Use fastavro for serialization
    output = io.BytesIO()
    fastavro.schemaless_writer(output, schema, data)
    return output.getvalue()


def deserialize_bytes(bytes_data: bytes, schema: Dict[str, Any], model_class: Type[ModelBase]) -> ModelBase:
    """
    Deserialize Avro bytes to a Pydantic model instance.
    
    Args:
        bytes_data: Avro-serialized bytes
        schema: Schema dict for deserialization
        model_class: The model class to instantiate
        
    Returns:
        Instance of the model class
    """
    # Use fastavro for deserialization
    input_bytes = io.BytesIO(bytes_data)
    data = fastavro.schemaless_reader(input_bytes, schema)
    
    # Convert dict back to Pydantic model
    return dict_to_model(data, model_class)
