#!/usr/bin/env python3
"""
Create Test Database Script
===========================
Creates a separate test database and applies migrations for testing.
"""

import logging
import os
import sys
from pathlib import Path
from typing import List, Tuple

import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("create-test-db")

# Project root is the parent directory of the script location
PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATIONS_PATH = os.path.join(PROJECT_ROOT, "migrations")

# Test database connection details
TEST_DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'name': 'cryptics_script_test_b74eedcf',
    'user': 'postgres',
    'password': 'postgres'
}

def create_test_database():
    """Create the test database if it doesn't exist"""
    # Connect to default postgres database
    admin_conn_string = (f"host={TEST_DB_CONFIG['host']} "
                         f"port={TEST_DB_CONFIG['port']} "
                         f"dbname=postgres "
                         f"user={TEST_DB_CONFIG['user']} "
                         f"password={TEST_DB_CONFIG['password']}")
    
    admin_conn = psycopg2.connect(admin_conn_string)
    admin_conn.autocommit = True
    cursor = admin_conn.cursor()
    
    try:
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TEST_DB_CONFIG['name']}'")
        if cursor.fetchone():
            logger.info(f"Test database {TEST_DB_CONFIG['name']} already exists")
            return
            
        # Create the test database
        logger.info(f"Creating test database: {TEST_DB_CONFIG['name']}")
        cursor.execute(f"CREATE DATABASE {TEST_DB_CONFIG['name']}")
        logger.info(f"Test database created successfully")
        
    except Exception as e:
        logger.error(f"Error creating test database: {e}")
        raise
    finally:
        cursor.close()
        admin_conn.close()

def apply_migrations():
    """Apply migrations to the test database"""
    # Connect to the test database
    test_conn_string = (f"host={TEST_DB_CONFIG['host']} "
                       f"port={TEST_DB_CONFIG['port']} "
                       f"dbname={TEST_DB_CONFIG['name']} "
                       f"user={TEST_DB_CONFIG['user']} "
                       f"password={TEST_DB_CONFIG['password']}")
    
    conn = psycopg2.connect(test_conn_string)
    conn.autocommit = False
    cursor = conn.cursor()
    
    try:
        # Create migrations table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id SERIAL PRIMARY KEY,
                migration_name VARCHAR(255) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Get list of applied migrations
        cursor.execute("SELECT migration_name FROM migrations ORDER BY id")
        applied_migrations = [row[0] for row in cursor.fetchall()]
        
        # Get list of migration files
        migration_dir = Path(MIGRATIONS_PATH)
        if not migration_dir.exists():
            logger.error(f"Migrations directory not found: {MIGRATIONS_PATH}")
            return
        
        migration_files = sorted([
            f.name for f in migration_dir.glob("*.sql")
            if f.name.startswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
        ])
        
        logger.info(f"Found {len(migration_files)} migration files")
        logger.info(f"{len(applied_migrations)} migrations already applied")
        
        # Apply pending migrations
        for migration_file in migration_files:
            if migration_file not in applied_migrations:
                migration_path = os.path.join(MIGRATIONS_PATH, migration_file)
                
                logger.info(f"Applying migration: {migration_file}")
                
                # Read and execute the migration file
                with open(migration_path, 'r') as f:
                    sql = f.read()
                    cursor.execute(sql)
                
                # Record the migration
                cursor.execute(
                    "INSERT INTO migrations (migration_name) VALUES (%s)",
                    (migration_file,)
                )
                
                logger.info(f"Successfully applied migration: {migration_file}")
                
        # Commit the transaction
        conn.commit()
        logger.info("All migrations applied successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error applying migrations: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def create_initial_schema():
    """Create the initial schema if using a fresh database"""
    # Connect to the test database
    test_conn_string = (f"host={TEST_DB_CONFIG['host']} "
                       f"port={TEST_DB_CONFIG['port']} "
                       f"dbname={TEST_DB_CONFIG['name']} "
                       f"user={TEST_DB_CONFIG['user']} "
                       f"password={TEST_DB_CONFIG['password']}")
    
    conn = psycopg2.connect(test_conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Check if tables already exist
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('ticker_data', 'trade_data', 'ack_data')
        """)
        
        count = cursor.fetchone()[0]
        if count >= 3:
            logger.info("Tables already exist, skipping initial schema creation")
            return
        
        # Create ticker_data table
        logger.info("Creating initial schema tables...")
        cursor.execute("""
            CREATE TABLE ticker_data (
                id SERIAL PRIMARY KEY,
                instrument_name VARCHAR(50) NOT NULL,
                mark_price DOUBLE PRECISION,
                mark_timestamp DOUBLE PRECISION,
                best_bid_price DOUBLE PRECISION,
                best_bid_amount DOUBLE PRECISION,
                best_ask_price DOUBLE PRECISION,
                best_ask_amount DOUBLE PRECISION,
                last_price DOUBLE PRECISION,
                delta DOUBLE PRECISION,
                volume_24h DOUBLE PRECISION, 
                value_24h DOUBLE PRECISION,
                low_price_24h DOUBLE PRECISION,
                high_price_24h DOUBLE PRECISION,
                change_24h DOUBLE PRECISION,
                index_price DOUBLE PRECISION,
                forward DOUBLE PRECISION,
                funding_mark DOUBLE PRECISION,
                funding_rate DOUBLE PRECISION,
                collar_low DOUBLE PRECISION,
                collar_high DOUBLE PRECISION,
                realised_funding_24h DOUBLE PRECISION,
                average_funding_rate_24h DOUBLE PRECISION,
                open_interest DOUBLE PRECISION,
                time_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create trade_data table
        cursor.execute("""
            CREATE TABLE trade_data (
                id SERIAL PRIMARY KEY,
                instrument_name VARCHAR(50) NOT NULL,
                order_id VARCHAR(50),
                trade_id VARCHAR(50),
                price DOUBLE PRECISION,
                amount DOUBLE PRECISION,
                time DOUBLE PRECISION,
                direction VARCHAR(10),
                fee DOUBLE PRECISION,
                time_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create ack_data table
        cursor.execute("""
            CREATE TABLE ack_data (
                id SERIAL PRIMARY KEY,
                instrument_name VARCHAR(50) NOT NULL,
                order_id VARCHAR(50),
                type VARCHAR(20) NOT NULL,
                message VARCHAR(255),
                price DOUBLE PRECISION,
                amount DOUBLE PRECISION,
                create_time DOUBLE PRECISION,
                direction VARCHAR(10),
                reduce_only BOOLEAN,
                time_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        logger.info("Initial schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating initial schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to create and set up the test database"""
    try:
        # Step 1: Create the test database
        create_test_database()
        
        # Step 2: Create initial schema tables
        create_initial_schema()
        
        # Step 3: Apply all migrations
        apply_migrations()
        
        logger.info(f"Test database {TEST_DB_CONFIG['name']} is ready for testing")
        logger.info(f"Connection string: host={TEST_DB_CONFIG['host']} port={TEST_DB_CONFIG['port']} "
                   f"dbname={TEST_DB_CONFIG['name']} user={TEST_DB_CONFIG['user']} "
                   f"password={TEST_DB_CONFIG['password']}")
        
    except Exception as e:
        logger.error(f"Failed to create test database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
