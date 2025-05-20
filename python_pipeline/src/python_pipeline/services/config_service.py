#!/usr/bin/env python3
"""
Configuration Service Module
===========================
Centralized configuration management for the pipeline.
"""

import logging
from typing import Any, Dict, List, Optional

from python_pipeline.config.settings import settings

logger = logging.getLogger("config-service")

class ConfigService:
    """Service for managing and accessing configuration"""
    
    def __init__(self) -> None:
        """Initialize the configuration service"""
        logger.info("Loading configuration...")
        
        # Store settings
        self.settings = settings
        
        # Log important configuration settings
        self._log_config_summary()
    
    def _log_config_summary(self) -> None:
        """Log a summary of important configuration settings"""
        is_docker = self.settings.pipeline.python_running_in_docker
        logger.info(f"Running in Docker mode: {is_docker}")
        logger.info(f"Enabled models: {', '.join(self.settings.pipeline.enabled_models)}")
        logger.info(f"Using schema files: {self.settings.pipeline.use_schema_files}")
        
        # Log database settings
        db_config = self.settings.database
        logger.info(f"Database: {db_config.name} at {db_config.host}:{db_config.port}")
        
        # Log Kafka settings
        kafka_config = self.settings.kafka
        bootstrap_servers = kafka_config.bootstrap_servers_internal if is_docker else kafka_config.bootstrap_servers
        logger.info(f"Kafka: {bootstrap_servers}")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the complete configuration as a dictionary for backward compatibility"""
        return self.settings.dict()
    
    @property
    def enabled_models(self) -> List[str]:
        """Get the list of enabled models"""
        return self.settings.pipeline.enabled_models
    
    @property
    def use_schema_files(self) -> bool:
        """Get whether to use schema files"""
        return self.settings.pipeline.use_schema_files
    
    def get_topic_name(self, model: str) -> str:
        """Get the topic name for a model"""
        topics_dict = self.settings.topics.dict()
        if model in topics_dict:
            return topics_dict[model]
        raise ValueError(f"Unknown model: {model}")
    
    def get_kafka_config(self, internal: Optional[bool] = None) -> Dict[str, str]:
        """
        Get Kafka configuration
        
        Args:
            internal: Whether to use internal Docker network settings
        
        Returns:
            Dictionary with Kafka configuration settings
        """
        if internal is None:
            internal = self.settings.pipeline.python_running_in_docker
        
        kafka = self.settings.kafka
        return {
            'bootstrap_servers': kafka.bootstrap_servers_internal if internal else kafka.bootstrap_servers,
            'connect_url': kafka.connect_url_internal if internal else kafka.connect_url,
            'schema_registry_url': kafka.schema_registry_url_internal if internal else kafka.schema_registry_url
        }
    
    def get_database_config(self, internal: Optional[bool] = None, is_test: bool = False) -> Dict[str, Any]:
        """
        Get database configuration
        
        Args:
            internal: Whether to use internal Docker network settings
            is_test: Whether to use test database configuration
            
        Returns:
            Dictionary with database configuration settings
        """
        if internal is None:
            internal = self.settings.pipeline.python_running_in_docker
        
        if is_test:
            # Check if test database settings are available
            if self.settings.database.test:
                db_config = self.settings.database.test
                return {
                    'host': db_config.host_internal if internal else db_config.host,
                    'port': db_config.port,
                    'name': db_config.name,
                    'user': db_config.user,
                    'password': db_config.password,
                    'schema': db_config.db_schema
                }
            else:
                # No test database configured, return regular config
                logger.warning("No test database configuration found, using production database")
        
        # Use production database configuration
        db_config = self.settings.database
        return {
            'host': db_config.host_internal if internal else db_config.host,
            'port': db_config.port,
            'name': db_config.name,
            'user': db_config.user,
            'password': db_config.password,
            'schema': db_config.db_schema
        }
    
    def get_database_dsn(self, internal: Optional[bool] = None) -> str:
        """
        Get database connection string in DSN format
        
        Args:
            internal: Whether to use internal Docker network settings
            
        Returns:
            PostgreSQL connection string
        """
        db_config = self.get_database_config(internal)
        return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
    
    def get_update_interval(self) -> float:
        """Get producer update interval in seconds"""
        return self.settings.app.update_seconds
    
    def get_model_table_name(self, model_name: str) -> Optional[str]:
        """
        Get the table name for a specific model
        
        Args:
            model_name: Name of the model
            
        Returns:
            Table name or None if model not found
        """
        # Check if we have specific model settings
        if self.settings.pipeline.models:
            model_settings = getattr(self.settings.pipeline.models, model_name, None)
            if model_settings and model_settings.table_name:
                return model_settings.table_name
                
        # Fall back to convention
        return f"{model_name.lower()}_data"