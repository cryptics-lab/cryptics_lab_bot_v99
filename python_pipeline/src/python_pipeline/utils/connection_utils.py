#!/usr/bin/env python3
"""
Connection Utilities Module
=========================
Provides utility functions for waiting on external service connections.
"""

import logging
import time
from typing import Callable

import psycopg2
import requests
from confluent_kafka.admin import AdminClient

logger = logging.getLogger("connection-utils")

def wait_for_kafka(bootstrap_servers: str, retries: int = 30, delay: int = 2) -> bool:
    """
    Wait for Kafka to be ready and accepting connections.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers string
        retries: Number of connection attempts before giving up
        delay: Delay between retries in seconds
        
    Returns:
        True if Kafka is ready, False otherwise

    Note:
        We implement this behavior as a fallback even though we also use
        depends_on with condition: service_healthy in docker-compose.
        This makes the code more resilient when deployed to Kubernetes
        or bare metal environments.
    """
    logger.info(f"Waiting for Kafka at {bootstrap_servers}...")
    
    for attempt in range(retries):
        try:
            # Create a temporary admin client to test the connection
            conf = {'bootstrap.servers': bootstrap_servers}
            admin_client = AdminClient(conf)
            
            # Request metadata to test connection
            cluster_metadata = admin_client.list_topics(timeout=5)
            if cluster_metadata is not None:
                logger.info(f"Kafka is ready at {bootstrap_servers}")
                return True
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt + 1}/{retries}): {str(e)}")
        
        time.sleep(delay)
    
    logger.error(f"Kafka did not become ready after {retries} attempts")
    return False

def wait_for_schema_registry(schema_registry_url: str, retries: int = 30, delay: int = 2) -> bool:
    """
    Wait for Schema Registry to be ready and accepting connections.
    
    Args:
        schema_registry_url: Schema Registry URL
        retries: Number of connection attempts before giving up
        delay: Delay between retries in seconds
        
    Returns:
        True if Schema Registry is ready, False otherwise
        
    Note:
        We implement this behavior as a fallback even though we also use
        depends_on with condition: service_healthy in docker-compose.
        This makes the code more resilient when deployed to Kubernetes
        or bare metal environments.
    """
    logger.info(f"Waiting for Schema Registry at {schema_registry_url}...")
    
    for attempt in range(retries):
        try:
            response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
            if response.status_code == 200:
                logger.info(f"Schema Registry is ready at {schema_registry_url}")
                return True
            else:
                logger.warning(f"Schema Registry returned status code {response.status_code} " 
                              f"(attempt {attempt + 1}/{retries})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Schema Registry not ready (attempt {attempt + 1}/{retries}): {str(e)}")
        
        time.sleep(delay)
    
    logger.error(f"Schema Registry did not become ready after {retries} attempts")
    return False

def wait_for_postgres(dsn: str, retries: int = 30, delay: int = 2) -> bool:
    """
    Wait for PostgreSQL to be ready and accepting connections.
    
    Args:
        dsn: PostgreSQL connection string
        retries: Number of connection attempts before giving up
        delay: Delay between retries in seconds
        
    Returns:
        True if PostgreSQL is ready, False otherwise
        
    Note:
        We implement this behavior as a fallback even though we also use
        depends_on with condition: service_healthy in docker-compose.
        This makes the code more resilient when deployed to Kubernetes
        or bare metal environments.
    """
    logger.info(f"Waiting for PostgreSQL...")
    
    for attempt in range(retries):
        try:
            # Create a temporary connection to test if Postgres is up
            conn = psycopg2.connect(dsn)
            conn.close()
            
            logger.info(f"PostgreSQL is ready")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"PostgreSQL not ready (attempt {attempt + 1}/{retries}): {str(e)}")
            time.sleep(delay)
    
    logger.error(f"PostgreSQL did not become ready after {retries} attempts")
    return False

def wait_for_service(
    check_function: Callable[[], bool],
    service_name: str,
    retries: int = 30,
    delay: int = 2
) -> bool:
    """
    Generic function to wait for a service to be ready.
    
    Args:
        check_function: Function that returns True if service is ready
        service_name: Name of the service (for logging)
        retries: Number of connection attempts before giving up
        delay: Delay between retries in seconds
        
    Returns:
        True if service is ready, False otherwise
    """
    logger.info(f"Waiting for {service_name}...")
    
    for attempt in range(retries):
        try:
            if check_function():
                logger.info(f"{service_name} is ready")
                return True
        except Exception as e:
            logger.warning(f"{service_name} not ready (attempt {attempt + 1}/{retries}): {str(e)}")
        
        time.sleep(delay)
    
    logger.error(f"{service_name} did not become ready after {retries} attempts")
    return False
