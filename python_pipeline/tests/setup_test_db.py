#!/usr/bin/env python3
"""
Test Database Setup Script
=========================
Sets up the test database for migration testing by checking for available 
PostgreSQL connections and creating the required database.
"""

import logging
import sys
import time

import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("db-setup")

# PostgreSQL connection parameters
TEST_DB_NAME = "cryptics_test"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSSIBLE_PORTS = [5432, 5433]  # Try standard port and alternate port

def check_postgres_connection(port):
    """
    Check if PostgreSQL is available on the specified port
    
    Args:
        port: Port number to check
        
    Returns:
        bool: True if PostgreSQL is available
    """
    try:
        conn_string = f"host={POSTGRES_HOST} port={port} dbname=postgres user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
        conn = psycopg2.connect(conn_string)
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not connect to PostgreSQL on port {port}: {e}")
        return False

def create_test_database(port):
    """
    Create the test database if it doesn't exist
    
    Args:
        port: Port to connect to
        
    Returns:
        bool: True if successful
    """
    # Connect to postgres database
    conn_string = f"host={POSTGRES_HOST} port={port} dbname=postgres user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    
    try:
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TEST_DB_NAME}'")
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"Test database '{TEST_DB_NAME}' already exists")
        else:
            # Create the test database
            logger.info(f"Creating test database '{TEST_DB_NAME}'")
            cursor.execute(f"CREATE DATABASE {TEST_DB_NAME}")
            logger.info(f"Created test database '{TEST_DB_NAME}'")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating test database: {e}")
        return False

def update_config_port(port):
    """
    Update the config.toml file with the correct port for the test database
    
    Args:
        port: Port to set in the config
    """
    import os
    from pathlib import Path

    import toml
    
    # Find project root (up one level from the script directory)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    config_path = project_root / "config.toml"
    
    # Read the current config
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
            
        # Update the test database port
        if 'database' in config and 'test' in config['database']:
            current_port = config['database']['test'].get('port')
            
            if current_port != port:
                config['database']['test']['port'] = port
                
                # Write the updated config
                with open(config_path, 'w') as f:
                    toml.dump(config, f)
                    
                logger.info(f"Updated test database port in config.toml to {port}")
            else:
                logger.info(f"Test database port already set to {port} in config.toml")
        else:
            logger.warning("Could not find database.test section in config.toml")
            
    except Exception as e:
        logger.error(f"Error updating config: {e}")

def main():
    """
    Main function that checks for PostgreSQL and creates the test database
    """
    logger.info("Checking PostgreSQL connection...")
    
    # Try each possible port
    for port in POSSIBLE_PORTS:
        if check_postgres_connection(port):
            logger.info(f"Found PostgreSQL on port {port}")
            
            # Create the test database on this port
            if create_test_database(port):
                # Update the config file with the correct port
                update_config_port(port)
                
                logger.info("=" * 50)
                logger.info(f"SUCCESS: PostgreSQL is available on port {port}")
                logger.info(f"Test database '{TEST_DB_NAME}' is ready")
                logger.info(f"Connection details: host={POSTGRES_HOST} port={port} dbname={TEST_DB_NAME} user={POSTGRES_USER}")
                logger.info("=" * 50)
                return 0
    
    # If we get here, PostgreSQL is not available on any port
    logger.error("=" * 50)
    logger.error("ERROR: PostgreSQL is not available on any of the expected ports")
    logger.error(f"Please make sure PostgreSQL is running on one of these ports: {POSSIBLE_PORTS}")
    logger.error("=" * 50)
    return 1

if __name__ == "__main__":
    sys.exit(main())
