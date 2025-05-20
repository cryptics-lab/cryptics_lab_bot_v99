"""
Avro Schema Loader for CrypticsLabBot.
Provides functions for loading Avro schema files.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Type

from python_pipeline.models.model_base import ModelBase
from python_pipeline.producers.schema_generator import AvroSchemaGenerator

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set to INFO level to reduce logging

def get_schemas_dir() -> Path:
    """
    Get the path to the schemas directory.
    
    Returns:
        Path to the schemas directory
    """
    # Try multiple possible locations for schemas directory
    possible_paths = [
        Path("/app/schemas"),                         # Docker container path
        Path.cwd() / "schemas",                       # Current working directory
        Path.cwd().parent / "schemas",                # Parent of current directory
        Path(__file__).parent.parent.parent / "schemas", # Project root
    ]
    
    # Try to find an existing schemas directory
    for path in possible_paths:
        if path.exists():
            # Only log at debug level to reduce verbosity
            return path
    
    # Default to app path even if it doesn't exist
    schemas_dir = Path("/app/schemas")
    
    return schemas_dir

def get_schema_path(model_name: str) -> Path:
    """
    Get the path to a schema file - always finds the latest version available.
    
    Args:
        model_name: Name of the model (e.g., "ticker", "ack", "trade")
        
    Returns:
        Path to the latest schema file
    """
    schemas_dir = get_schemas_dir()
    model_dir = schemas_dir / model_name.lower()
    
    # Check if the directory exists
    if not model_dir.exists():
        logger.warning(f"Model schema directory not found: {model_dir}")
        # Default to the schemas directory with a fallback name
        return schemas_dir / model_name.lower() / "schema.avsc"
    
    # Find the latest version by checking all files matching v*.avsc
    # and sorting them by version number
    schema_files = list(model_dir.glob("v*.avsc"))
    
    if not schema_files:
        logger.warning(f"No schema files found in: {model_dir}")
        return schemas_dir / model_name.lower() / "schema.avsc"
    
    # Sort by version number (extract the number between 'v' and '.avsc')
    latest_schema = max(
        schema_files,
        key=lambda p: int(p.stem.replace('v', ''))
    )
    
    return latest_schema

def load_avro_schema(model_name: str) -> Optional[Dict[str, Any]]:
    """
    Load an Avro schema (.avsc file) from the project's schemas directory.
    
    Args:
        model_name: Name of the model (e.g., "ticker", "ack")
        
    Returns:
        Schema as a dictionary, or None if file not found or invalid
    """
    schema_dir = get_schemas_dir()
    schema_path = get_schema_path(model_name)
    
    # First check if the directory exists
    if not schema_dir.exists():
        logger.warning(f"Schemas directory not found: {schema_dir}")
        return None
    
    # Then check if the file exists
    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}")
        return None
        
    try:
        # Read and parse the .avsc file using standard JSON library
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        # Basic validation - check if it's a valid Avro schema
        if not isinstance(schema, dict) or 'type' not in schema or schema.get('type') != 'record':
            logger.error(f"Invalid Avro schema format in {schema_path}: missing required 'type':'record'")
            return None
            
        if 'fields' not in schema or not isinstance(schema['fields'], list):
            logger.error(f"Invalid Avro schema format in {schema_path}: missing 'fields' array")
            return None
        
        return schema
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON in schema file {schema_path}: {e}")
        return None
    except (IOError, OSError) as e:
        logger.error(f"File I/O error when loading schema {schema_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading schema {schema_path}: {e}")
        return None

def get_schema_for_model(model_class: Type[ModelBase], use_schema_files: bool = True) -> Dict[str, Any]:
    """
    Get the Avro schema for a model class, either by loading from file or generating on-the-fly.
    
    Args:
        model_class: The model class to get schema for
        use_schema_files: Whether to try loading from files first
        
    Returns:
        Avro schema as a dictionary
    """
    model_name = model_class.model_name
    
    if use_schema_files:
        # Try to load schema from file
        schema = load_avro_schema(model_name)
        if schema:
            return schema
        
        # Fall back to generated schema if schema file not found
        logger.warning(f"Schema file for {model_name} not found or invalid. Generating schema on-the-fly.")
    
    # Generate schema on-the-fly using the existing schema generator
    return AvroSchemaGenerator.generate_schema(model_class)
    
    # Generate schema on-the-fly using the external schema generator
    schema_gen = AvroSchemaGenerator(namespace=f"com.cryptics.avro")
    return schema_gen.to_avro_schema(model_class)
