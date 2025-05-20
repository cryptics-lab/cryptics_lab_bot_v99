#!/usr/bin/env python3
"""
Pipeline Orchestrator Module
==========================
Coordinates the pipeline components.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from python_pipeline.services.config_service import ConfigService
from python_pipeline.services.connector_service import ConnectorService
from python_pipeline.services.database_service import DatabaseService
from python_pipeline.services.kafka_service import KafkaService
from python_pipeline.services.producer_service import ProducerService
from python_pipeline.utils.connection_utils import (
    wait_for_kafka,
    wait_for_postgres,
    wait_for_schema_registry,
)

PRODUCE_SECONDS: int = 3
SINK_PROCESSING_WAIT_SECONDS: int = 10

logger: logging.Logger = logging.getLogger("pipeline-orchestrator")

class PipelineOrchestrator:
    """Orchestrates the pipeline setup and execution"""
    
    def __init__(self) -> None:
        """Initialize the pipeline orchestrator"""
        # Create services
        self.config_service: ConfigService = ConfigService()
        self.kafka_service: KafkaService = KafkaService(self.config_service)
        self.connector_service: ConnectorService = ConnectorService(self.config_service)
        self.producer_service: ProducerService = ProducerService(self.config_service)
        self.database_service: DatabaseService = DatabaseService(self.config_service)
        
    async def setup_infrastructure(self) -> bool:
        """
        Set up infrastructure components
        
        Returns:
            True if setup was successful
        """
        logger.info("=== SETTING UP INFRASTRUCTURE ===")
        
        # Wait for essential services to be available
        bootstrap_servers = self.config_service.get_kafka_config()['bootstrap_servers']
        schema_registry_url = self.config_service.get_kafka_config()['schema_registry_url']
        db_dsn = self.config_service.get_database_dsn()
        
        # Wait for Kafka to be ready
        if not wait_for_kafka(bootstrap_servers):
            logger.error("Failed to connect to Kafka. Pipeline setup cannot continue.")
            return False
            
        # Wait for Schema Registry to be ready
        if not wait_for_schema_registry(schema_registry_url):
            logger.error("Failed to connect to Schema Registry. Pipeline setup cannot continue.")
            return False
            
        # Wait for PostgreSQL to be ready
        if not wait_for_postgres(db_dsn):
            logger.error("Failed to connect to PostgreSQL. Pipeline setup cannot continue.")
            return False
        
        # Remove existing connectors
        await self.connector_service.delete_all_connectors()
        
        # Verify schema registry
        schema_registry_ok: bool
        subjects: List[str]
        schema_registry_ok, subjects = await self.kafka_service.verify_schema_registry()
        if not schema_registry_ok:
            logger.error("Schema Registry check failed. Pipeline may not work correctly.")
            return False
            
        return True
    
    async def setup_producers(self) -> bool:
        """
        Set up and verify producers
        
        Returns:
            True if setup was successful
        """
        logger.info("=== SETTING UP PRODUCERS ===")
        
        # Setup producers
        self.producer_service.setup_producers()
        
        # Tasks that can be run in parallel
        run_task = self.producer_service.run_producers(duration=PRODUCE_SECONDS)
        
        # Wait for producers to run
        await run_task
        
        # Verify topic data needs to happen after producers have run
        topic_results: Dict[str, bool] = await self.kafka_service.verify_topics_exist(self.producer_service.topics)
        
        # Continue only if all topics have data
        if not all(topic_results.values()):
            logger.error("Some topics don't have data. Pipeline setup failed.")
            return False
            
        return True
    
    async def setup_connectors(self) -> bool:
        """
        Set up and verify connectors
        
        Returns:
            True if setup was successful
        """
        logger.info("=== SETTING UP CONNECTORS ===")
        
        # Setup connectors
        self.connector_service.setup_connectors()
        connector_success: bool = await self.connector_service.create_connectors()
        
        if not connector_success:
            logger.warning("Some connectors failed to create")
        
        # Wait for each connector to reach RUNNING state
        running_connectors = 0
        for connector_name in self.connector_service.connector_names:
            if await self.connector_service.wait_for_connector_running(connector_name):
                running_connectors += 1
        
        # Check if all connectors are running
        total_connectors = len(self.connector_service.connector_names)
        if running_connectors == total_connectors:
            logger.info(f"All {total_connectors} connectors are running")
            return True
        else:
            logger.warning(f"Only {running_connectors} out of {total_connectors} connectors are running")
            return running_connectors > 0  # Return true if at least one connector is running
    
    async def verify_data_flow(self) -> bool:
        """
        Verify end-to-end data flow
        
        Returns:
            True if verification was successful
        """
        logger.info("=== VERIFYING DATA FLOW ===")

        await self.producer_service.run_producers(duration=PRODUCE_SECONDS)
        
        # Add a delay to allow sink connectors time to process the data
        logger.info(f"Waiting for {SINK_PROCESSING_WAIT_SECONDS} seconds for sink connectors to process data...")
        await asyncio.sleep(int(SINK_PROCESSING_WAIT_SECONDS))  # Explicit cast to int for type safety
        
        # Check database tables directly without running producers again
        logger.info("Checking for data in database tables...")
        # Use the async version for better performance
        table_results: Dict[str, int] = await self.database_service.check_database_tables_filled_async()
        
        # Count tables with data
        tables_with_data: int = len([t for t, c in table_results.items() if c > 0])
        
        if tables_with_data == 0:
            logger.error("No tables have data. Data flow verification failed.")
            return False
            
        logger.info(f"Found data in {tables_with_data} tables. Data flow verified successfully.")
        return True
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        logger.info("=== CLEANING UP ===")
        
        # Flush producers
        self.producer_service.flush_producers()
        
        # Clean database tables - use async version for better performance
        await self.database_service.clean_database_tables_async()
    
    async def initialize_database(self) -> bool:
        """
        Initialize database schema
        
        Returns:
            True if database initialization was successful
        """
        logger.info("=== INITIALIZING DATABASE ===")
        return await self.database_service.check_schema_initialized_async()
        
    async def check_required_tables(self) -> bool:
        """
        Check if all required database tables exist
        
        Returns:
            True if all required tables exist, False otherwise
        """
        # Get list of existing tables using async method
        existing_tables = await self.database_service.list_tables_async()
        
        required_tables = ["ticker_data", "ack_data", "trade_data", "index_data"]
        missing_tables = [table for table in required_tables if table not in existing_tables]
        
        if not missing_tables:
            logger.info("All required tables exist")
            return True
        else:
            logger.error(f"Missing tables: {', '.join(missing_tables)}")
            return False
            
    async def run(self) -> bool:
        """
        Run the complete pipeline setup process
        
        Returns:
            True if pipeline setup was successful
        """
        try:
            # Step 0: Check if required tables exist
            tables_exist: bool = await self.check_required_tables()
            if not tables_exist:
                return False
            
            # Step 1: Initialize database (new step)
            db_init_success: bool = await self.initialize_database()
            if not db_init_success:
                logger.warning("Database initialization had issues but continuing")
            
            # Step 2: Setup infrastructure
            infra_success: bool = await self.setup_infrastructure()
            if not infra_success:
                logger.error("Infrastructure setup failed")
                return False
            
            # Step 3: Setup producers
            producer_success: bool = await self.setup_producers()
            if not producer_success:
                logger.error("Producer setup failed")
                return False
            
            # Step 4: Setup connectors
            connector_success: bool = await self.setup_connectors()
            if not connector_success:
                logger.warning("Connector setup had issues but continuing")
            
            # Step 5: Verify data flow
            flow_success: bool = await self.verify_data_flow()
            if not flow_success:
                logger.error("Data flow verification failed")
                return False
            
            # Step 6: Cleanup
            await self.cleanup()
            
            # Summary
            logger.info("=== PIPELINE SETUP COMPLETE ===")
            logger.info("The pipeline is ready to process real data.")
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline setup failed: {e}")
            return False
