"""
cleanup.py
Description: Utility to clean up Kafka topics and TimescaleDB tables.
This script can delete/reset topics and truncate database tables.

Usage:
- Run directly: python -m python_pipeline.cleanup [--topics] [--tables] [--all]
"""

import argparse
import logging
import time
from typing import Any, Dict, List

import psycopg2
from confluent_kafka.admin import AdminClient, NewTopic

from python_pipeline.config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_db(config: Dict[str, Any]) -> psycopg2.extensions.connection:
    """Connect to the database"""
    try:
        conn = psycopg2.connect(
            host=config["database"]["host"],
            port=config["database"]["port"],
            dbname=config["database"]["name"],
            user=config["database"]["user"],
            password=config["database"]["password"]
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def truncate_tables(config: Dict[str, Any]) -> None:
    """Truncate all tables in the database"""
    logger.info("Truncating database tables...")
    
    conn = connect_to_db(config)
    conn.autocommit = True
    
    tables = [
        "ticker_data",
        "order_events",
        "price_indices"
    ]
    
    try:
        with conn.cursor() as cursor:
            for table in tables:
                try:
                    cursor.execute(f'TRUNCATE TABLE {config["database"]["schema"]}.{table}')
                    logger.info(f"Truncated table {table}")
                except Exception as e:
                    logger.warning(f"Failed to truncate table {table}: {e}")
    finally:
        conn.close()
    
    logger.info("Table truncation completed")

def clear_topics(config: Dict[str, Any]) -> None:
    """Delete and recreate Kafka topics"""
    logger.info("Cleaning up Kafka topics...")
    
    # Create admin client
    admin_client = AdminClient({
        'bootstrap.servers': config["kafka"]["bootstrap_servers"]
    })
    
    # Get topic names
    topics = list(config["topics"].values())
    
    # Delete topics
    try:
        logger.info(f"Deleting topics: {', '.join(topics)}")
        futures = admin_client.delete_topics(topics)
        
        # Wait for deletion to complete
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Deleted topic {topic}")
            except Exception as e:
                logger.warning(f"Failed to delete topic {topic}: {e}")
        
        # Wait for topic deletion to propagate
        logger.info("Waiting for topic deletion to propagate...")
        time.sleep(5)
    except Exception as e:
        logger.warning(f"Error during topic deletion: {e}")
    
    # Recreate topics
    new_topics = []
    for topic in topics:
        new_topics.append(NewTopic(
            topic, 
            num_partitions=config["kafka"]["topic_partitions"],
            replication_factor=config["kafka"]["topic_replicas"]
        ))
    
    try:
        logger.info(f"Creating topics: {', '.join(topics)}")
        futures = admin_client.create_topics(new_topics)
        
        # Wait for creation to complete
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Created topic {topic}")
            except Exception as e:
                logger.warning(f"Failed to create topic {topic}: {e}")
    except Exception as e:
        logger.warning(f"Error during topic creation: {e}")
    
    logger.info("Topic cleanup completed")

def cleanup(clear_topics_flag: bool = False, truncate_tables_flag: bool = False) -> None:
    """Clean up Kafka topics and/or database tables"""
    # Get configuration
    config = get_config()
    
    # Clean up topics if requested
    if clear_topics_flag:
        clear_topics(config)
    
    # Truncate tables if requested
    if truncate_tables_flag:
        truncate_tables(config)
    
    logger.info("Cleanup completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up Kafka topics and TimescaleDB tables")
    parser.add_argument("--topics", action="store_true", help="Clean up Kafka topics")
    parser.add_argument("--tables", action="store_true", help="Truncate database tables")
    parser.add_argument("--all", action="store_true", help="Clean up everything")
    args = parser.parse_args()
    
    # If no specific flags are provided, show help
    if not (args.topics or args.tables or args.all):
        parser.print_help()
    else:
        cleanup(
            clear_topics_flag=args.topics or args.all,
            truncate_tables_flag=args.tables or args.all
        )
