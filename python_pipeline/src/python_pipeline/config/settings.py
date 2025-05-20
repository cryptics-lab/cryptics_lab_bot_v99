#!/usr/bin/env python3
"""
Configuration Module
==================
Centralized configuration management using Pydantic BaseSettings.
Loads configuration from config.toml file.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import toml
from pydantic import BaseSettings, Field

logger = logging.getLogger("config-settings")

class DatabaseTestSettings(BaseSettings):
    """Test database configuration settings"""
    host_internal: str = Field("timescaledb-test", description="Test database host (internal network)")
    host: str = Field("localhost", description="Test database host")
    port: int = Field(5433, description="Test database port")
    name: str = Field("cryptics_test", description="Test database name")
    user: str = Field("postgres", description="Test database username")
    password: str = Field("postgres", description="Test database password")
    db_schema: str = Field("public", description="Test database schema", alias="schema")
    
    class Config:
        env_file = ".env"
        env_prefix = "DB_TEST_"
        allow_population_by_field_name = True

class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    host_internal: str = Field("timescaledb", description="Database host (internal network)")
    host: str = Field("localhost", description="Database host")
    port: int = Field(5432, description="Database port")
    name: str = Field("cryptics", description="Database name")
    user: str = Field("postgres", description="Database username")
    password: str = Field("postgres", description="Database password")
    db_schema: str = Field("public", description="Database schema", alias="schema")
    test: Optional[DatabaseTestSettings] = None
    
    class Config:
        env_file = ".env"
        env_prefix = "DB_"
        allow_population_by_field_name = True

class TopicsSettings(BaseSettings):
    """Kafka topic settings"""
    ticker: str = Field("cryptics.thalex.ticker.avro", description="Ticker topic name")
    ack: str = Field("cryptics.thalex.ack.avro", description="Ack topic name")
    trade: str = Field("cryptics.thalex.trade.avro", description="Trade topic name")
    index: str = Field("cryptics.thalex.index.avro", description="Index topic name")
    base_name: str = Field("cryptics.thalex", description="Base name for topics")
    
    class Config:
        env_file = ".env"
        env_prefix = "TOPIC_"

class KafkaSettings(BaseSettings):
    """Kafka configuration settings"""
    bootstrap_servers_internal: str = Field("broker0:29092,broker1:29093,broker2:29094", 
                                   description="Kafka bootstrap servers (internal)")
    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    schema_registry_url_internal: str = Field("http://schema-registry:8081", 
                                    description="Schema Registry URL (internal)")
    schema_registry_url: str = Field("http://localhost:8081", description="Schema Registry URL")
    connect_url_internal: str = Field("http://kafka-connect:8083", 
                            description="Kafka Connect URL (internal)")
    connect_url: str = Field("http://localhost:8083", description="Kafka Connect URL")
    ack_topic: str = Field("cryptics.thalex.ack", description="ACK topic")
    trade_topic: str = Field("cryptics.thalex.trade", description="Trade topic")
    ticker_topic: str = Field("cryptics.thalex.ticker", description="Ticker topic")
    timeout_ms: int = Field(5000, description="Timeout in milliseconds")
    topic_partitions: int = Field(3, description="Default topic partitions")
    topic_replicas: int = Field(2, description="Default topic replication factor")
    skip_local_schemas: bool = Field(True, description="Skip local schemas")
    
    class Config:
        env_file = ".env"
        env_prefix = "KAFKA_"

class ModelConfig(BaseSettings):
    """Model-specific configuration"""
    table_name: str = Field("", description="Table name for the model")

class ModelSettings(BaseSettings):
    """Models configuration"""
    ticker: Optional[ModelConfig] = None
    ack: Optional[ModelConfig] = None
    trade: Optional[ModelConfig] = None
    index: Optional[ModelConfig] = None

class PipelineSettings(BaseSettings):
    """Pipeline configuration settings"""
    enabled_models: List[str] = Field(["ticker", "ack", "trade", "index"], 
                             description="Enabled data models")
    clear_tables: bool = Field(True, description="Clear tables before starting")
    use_schema_files: bool = Field(True, description="Use schema files instead of generating")
    python_running_in_docker: bool = Field(True, description="Running in Docker")
    run_migrations: bool = Field(True, description="Run database migrations")
    models: Optional[ModelSettings] = None
    
    class Config:
        env_file = ".env"
        env_prefix = "PIPELINE_"

class AppSettings(BaseSettings):
    """Application configuration settings"""
    debug: bool = Field(True, description="Debug mode")
    update_seconds: float = Field(1.0, description="Producer update interval in seconds")
    instruments: List[str] = Field(["BTC-PERPETUAL"], description="Trading instruments")
    rust_running_in_docker: bool = Field(False, description="Running Rust in Docker")
    
    class Config:
        env_file = ".env"
        env_prefix = "APP_"

class Settings(BaseSettings):
    """Complete configuration settings"""
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    topics: TopicsSettings = Field(default_factory=TopicsSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    app: AppSettings = Field(default_factory=AppSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    
    class Config:
        # Allow both field names and aliases to be used when loading config
        allow_population_by_field_name = True
    
    @classmethod
    def from_toml(cls, file_path: str) -> "Settings":
        """Load settings from a TOML file"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                config_dict = toml.load(f)
            
            # Handle specific aliasing for database schema
            if "database" in config_dict and "schema" in config_dict["database"]:
                # Rename schema to db_schema
                config_dict["database"]["db_schema"] = config_dict["database"]["schema"]
                del config_dict["database"]["schema"]

            # Handle schema in test database config
            if "database" in config_dict and "test" in config_dict["database"] and "schema" in config_dict["database"]["test"]:
                # Rename schema to db_schema
                config_dict["database"]["test"]["db_schema"] = config_dict["database"]["test"]["schema"]
                del config_dict["database"]["test"]["schema"]
            
            # Initialize nested models
            if "pipeline" in config_dict and "models" in config_dict["pipeline"]:
                for model, model_config in config_dict["pipeline"]["models"].items():
                    if isinstance(model_config, dict):
                        config_dict["pipeline"]["models"][model] = ModelConfig(**model_config)
            
            # Handle database test config
            if "database" in config_dict and "test" in config_dict["database"]:
                config_dict["database"]["test"] = DatabaseTestSettings(**config_dict["database"]["test"])
            
            return cls.parse_obj(config_dict)
        except Exception as e:
            logger.error(f"Error loading config from {file_path}: {e}")
            raise

def get_config_path() -> str:
    """Find the config.toml file path."""
    possible_paths = [
        "/app/config.toml",  # Docker container path
        Path.cwd() / "config.toml",  # Current working directory
        Path.cwd().parent / "config.toml",  # Parent directory
        Path(__file__).parent.parent.parent / "config.toml",  # Project root
    ]

    for path in possible_paths:
        if Path(path).exists():
            return str(path)

    raise FileNotFoundError("Could not find config.toml file")

def get_settings() -> Settings:
    """Get the settings singleton instance."""
    try:
        config_path = get_config_path()
        logger.info(f"Loading configuration from: {config_path}")
        settings = Settings.from_toml(config_path)
        logger.info("Successfully loaded configuration from TOML file")
        return settings
    except FileNotFoundError:
        logger.warning("No config file found, using environment variables and defaults")
        return Settings()

# Create a singleton instance
settings = get_settings()

