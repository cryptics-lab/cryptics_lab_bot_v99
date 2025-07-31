#!/usr/bin/env python3
"""
Connector Service Module
======================
Manages Kafka Connect connectors.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.factories import ConnectorFactory
from python_pipeline.services.config_service import ConfigService

logger: logging.Logger = logging.getLogger("connector-service")

class ConnectorService:
    """Service for managing Kafka Connect connectors"""
    
    def __init__(self, config_service: ConfigService) -> None:
        """
        Initialize the connector service
        
        Args:
            config_service: The configuration service
        """
        self.config_service: ConfigService = config_service
        self.kafka_config: Dict[str, str] = config_service.get_kafka_config()
        self.connectors: Dict[str, BaseSinkConnector] = {}
        self.connector_names: List[str] = []
    
    async def delete_all_connectors(self) -> None:
        """Delete all existing Kafka Connect connectors"""
        logger.info("Removing existing connectors...")
        
        connect_url: str = self.kafka_config['connect_url']
        
        try:
            # Get existing connectors
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{connect_url}/connectors", timeout=10) as response:
                    if response.status == 200:
                        connectors: List[str] = await response.json()
                        logger.info(f"Found {len(connectors)} existing connectors")
                        
                        # Delete each connector
                        for connector in connectors:
                            logger.info(f"Deleting connector: {connector}")
                            try:
                                async with session.delete(f"{connect_url}/connectors/{connector}", timeout=10) as delete_response:
                                    if delete_response.status in (200, 204):
                                        logger.info(f"Deleted connector: {connector}")
                                    else:
                                        logger.error(f"Failed to delete connector {connector}: HTTP {delete_response.status}")
                            except Exception as e:
                                logger.error(f"Failed to delete connector {connector}: {e}")
                    else:
                        logger.error(f"Failed to list connectors: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Failed to manage connectors: {e}")
    
    def setup_connectors(self) -> None:
        """Set up the sink connectors"""
        logger.info("Setting up sink connectors...")
        
        for model in self.config_service.enabled_models:
            try:
                logger.debug(f"Creating {model} sink connector...")
                # Use the dictionary version of the config
                config_dict = self.config_service.get_config()
                connector: BaseSinkConnector = ConnectorFactory.create(model, config_dict)
                self.connectors[model] = connector
                self.connector_names.append(connector.connector_name)
                logger.info(f"{model} sink connector created")
            except Exception as e:
                logger.error(f"Failed to create {model} connector: {e}")
    
    async def create_connectors(self) -> bool:
        """
        Create the sink connectors in Kafka Connect asynchronously and in parallel
        
        Returns:
            True if all connectors were created successfully
        """
        logger.info("Creating connectors in Kafka Connect...")
        
        async def create_connector(name, connector):
            """Helper function to create a single connector asynchronously"""
            logger.info(f"Creating {name} connector...")
            try:
                # Check if the connector's create method is async
                if asyncio.iscoroutinefunction(connector.create):
                    success = await connector.create(force=True)
                else:
                    # If not async, run in executor to avoid blocking
                    loop = asyncio.get_event_loop()
                    success = await loop.run_in_executor(
                        None, lambda: connector.create(force=True))
                
                if success:
                    logger.info(f"{name} sink connector created")
                else:
                    logger.error(f"Failed to create {name} sink connector")
                return name, success
            except Exception as e:
                logger.error(f"Exception creating {name} sink connector: {e}")
                return name, False
        
        # Create tasks for all connectors to run in parallel
        tasks = [create_connector(name, connector) 
                for name, connector in self.connectors.items()]
        
        # Gather all results
        results = await asyncio.gather(*tasks)
        
        # Check if all were successful
        all_success = all(success for _, success in results)
        
        return all_success
    
    async def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """
        Get status of a specific connector
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Dictionary with connector status information
        """
        connect_url: str = self.kafka_config['connect_url']
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{connect_url}/connectors/{connector_name}/status", timeout=10) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Failed to get status for connector {connector_name}, status code: {response.status}")
                        return {"connector": {"state": "ERROR"}, "tasks": []}
        except Exception as e:
            logger.error(f"Error getting connector status for {connector_name}: {str(e)}")
            return {"connector": {"state": "ERROR"}, "tasks": []}
            
    async def wait_for_connector_running(self, connector_name: str, timeout: int = 30) -> bool:
        """
        Wait for connector to reach RUNNING state
        
        Args:
            connector_name: Name of the connector to check
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if connector is running, False if timed out
        """
        logger.info(f"Waiting for connector {connector_name} to reach RUNNING state...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = await self.get_connector_status(connector_name)
            connector_state = status.get("connector", {}).get("state", "")
            
            if connector_state == "RUNNING":
                # Check if all tasks are running
                tasks = status.get("tasks", [])
                all_tasks_running = all(task.get("state") == "RUNNING" for task in tasks)
                
                if all_tasks_running:
                    logger.info(f"Connector {connector_name} and all its tasks are running")
                    return True
                else:
                    logger.info(f"Connector {connector_name} is running but some tasks are not ready yet")
            else:
                logger.info(f"Connector {connector_name} is in state: {connector_state}")
            
            await asyncio.sleep(1)  # Short poll interval
        
        logger.warning(f"Timed out waiting for connector {connector_name} to reach RUNNING state")
        return False
        
    async def check_connector_statuses(self) -> Dict[str, bool]:
        """
        Check the status of sink connectors
        
        Returns:
            Dictionary mapping connector names to boolean indicating if running
        """
        logger.info("Checking connector statuses...")
        
        results: Dict[str, bool] = {}
        
        # Check each connector
        for name in self.connector_names:
            status = await self.get_connector_status(name)
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            
            if connector_state == "RUNNING":
                logger.info(f"Connector {name} is running")
                
                # Check tasks
                tasks: List[Dict[str, Any]] = status.get("tasks", [])
                all_tasks_running: bool = all(task.get("state") == "RUNNING" for task in tasks)
                
                if all_tasks_running:
                    results[name] = True
                else:
                    logger.warning(f"Some tasks for connector {name} are not running")
                    results[name] = False
            else:
                logger.warning(f"Connector {name} is in state: {connector_state}")
                results[name] = False
        
        return results