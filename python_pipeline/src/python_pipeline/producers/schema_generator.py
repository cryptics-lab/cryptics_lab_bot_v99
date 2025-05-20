"""
Schema generator for Avro serialization compatible with Kafka Connect JDBC Sink and TimescaleDB.
Enforces schema correctness and prevents silent data corruption by omitting defaults for required fields.
"""

import enum
import json
import os
import typing
from typing import Any, Dict, Type, get_args, get_origin

from pydantic import BaseModel


class AvroSchemaGenerator:
    """Generate Avro schemas from Pydantic models"""

    @staticmethod
    def get_avro_type(python_type: type) -> Any:
        """
        Convert Python type to Avro type. Includes support for primitives, lists, maps, optionals, enums, and nested models.
        """
        primitive_map = {
            str: "string",
            int: "int",
            float: "double",
            bool: "boolean",
        }

        # Basic primitive types
        if python_type in primitive_map:
            return primitive_map[python_type]

        # Enums as Avro enum type
        if isinstance(python_type, type) and issubclass(python_type, enum.Enum):
            # For string enums, use the values instead of names
            if issubclass(python_type, str):
                return {
                    "type": "enum",
                    "name": python_type.__name__,
                    "symbols": [e.value for e in python_type]
                }
            else:
                return {
                    "type": "enum",
                    "name": python_type.__name__,
                    "symbols": [e.name for e in python_type]
                }

        origin = get_origin(python_type)
        args = get_args(python_type)

        # List[T] → array
        if origin == list and args:
            return {
                "type": "array",
                "items": AvroSchemaGenerator.get_avro_type(args[0])
            }
        
        # Dict[str, T] → map
        if origin == dict and len(args) == 2:
            return {
                "type": "map",
                "values": AvroSchemaGenerator.get_avro_type(args[1])
            }

        # Optional[T] → ["null", T]
        if origin == typing.Union and type(None) in args:
            non_none = [arg for arg in args if arg != type(None)]
            if len(non_none) == 1:
                return ["null", AvroSchemaGenerator.get_avro_type(non_none[0])]

        # Nested Pydantic models → recursive record
        if isinstance(python_type, type) and issubclass(python_type, BaseModel):
            return AvroSchemaGenerator.generate_schema(python_type)

        # Fallback to string for unknown types
        return "string"

    @classmethod
    def generate_schema(cls, model_class: Type[BaseModel], namespace: str = "com.cryptics.avro") -> Dict[str, Any]:
        """
        Generate Avro schema from a Pydantic model. Only sets defaults for optional fields or Pydantic defaults.
        Required fields will not have a default, ensuring ingestion fails on missing values.
        """
        schema = {
            "namespace": namespace,
            "type": "record",
            "name": model_class.__name__,
            "fields": []
        }

        for field_name, field_info in model_class.__fields__.items():
            avro_type = cls.get_avro_type(field_info.outer_type_)

            field_def = {
                "name": field_name,
                "type": avro_type
            }

            # Optional field → default null
            if field_info.allow_none or (isinstance(avro_type, list) and "null" in avro_type):
                # For fields that are Optional (Union with None), set the proper JSON null as default
                # This is crucial - Avro requires the default to match the first type in the union
                field_def["default"] = None  # This will be encoded as JSON null in the schema
                
                # Ensure "null" is the first type in the union
                if isinstance(avro_type, list) and avro_type[0] != "null":
                    # If null isn't first, reorder the types
                    avro_type.remove("null")
                    avro_type.insert(0, "null")
                    field_def["type"] = avro_type

            # Explicit Pydantic default
            elif field_info.default is not None:
                field_def["default"] = field_info.default

            # For required fields: omit default to force validation upstream
            # This ensures Connect fails if required data is missing

            schema["fields"].append(field_def)

        return schema

    @classmethod
    def save_schema(cls, model_class: Type[BaseModel], output_path: str, namespace: str = "com.cryptics.avro") -> None:
        """
        Generate and save Avro schema to a file. Creates output directories if needed.
        """
        schema = cls.generate_schema(model_class, namespace)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(schema, f, indent=2)
