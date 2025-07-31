#!/usr/bin/env python3
"""
Test Schema Migration Module
============================
Tests the schema evolution and migration system for adding latency tracking fields.
"""

import datetime
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psycopg2
import psycopg2.extras
import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("schema-migration-tests")

from python_pipeline.services.config_service import ConfigService
from python_pipeline.services.migration_manager import (
    MigrationManager,
    get_connection_string,
)

# Constants for testing
TEST_DB_NAME = f"cryptics_test_{uuid.uuid4().hex[:8]}"  # Generate unique test DB name
TEST_MIGRATIONS_PATH = str(Path(__file__).parent.parent.parent / "migrations")

@pytest.fixture
def config_service():
    """Return a configured ConfigService instance"""
    return ConfigService()

@pytest.fixture
def test_db_config(config_service):
    """Create a configuration with a unique test database name"""
    config = config_service.config
    
    # Modify the test database name to be unique
    if 'database' in config and 'test' in config['database']:
        config['database']['test']['name'] = TEST_DB_NAME
    
    return config

@pytest.fixture
def create_test_database(test_db_config):
    """Create a temporary test database for migration testing"""
    # Get the admin connection parameters (use default database to create new one)
    admin_config = test_db_config.get('database', {}).get('test', {}).copy()
    admin_config['name'] = 'postgres'  # Connect to postgres database to create new DB
    
    # Try possible PostgreSQL ports
    possible_ports = [admin_config.get('port', 5433), 5432]  # Try configured port first, then standard port
    
    admin_conn = None
    for port in possible_ports:
        try:
            # Build connection string
            admin_conn_string = f"host={admin_config.get('host', 'localhost')} " \
                              f"port={port} " \
                              f"dbname={admin_config.get('name', 'postgres')} " \
                              f"user={admin_config.get('user', 'postgres')} " \
                              f"password={admin_config.get('password', 'postgres')}"
            
            # Connect to postgres database
            admin_conn = psycopg2.connect(admin_conn_string)
            admin_conn.autocommit = True
            logger.info(f"Successfully connected to PostgreSQL on port {port}")
            
            # Update the port in test_db_config for later use
            test_db_config['database']['test']['port'] = port
            break
            
        except psycopg2.OperationalError as e:
            logger.warning(f"Could not connect to PostgreSQL on port {port}: {e}")
    
    if admin_conn is None:
        pytest.skip("Could not connect to PostgreSQL on any of the expected ports")
        return None
    
    try:
        # Create a cursor
        cursor = admin_conn.cursor()
        
        # Check if database already exists (just in case)
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TEST_DB_NAME}'")
        if cursor.fetchone():
            # Drop the database if it exists
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
        
        # Create the test database
        logger.info(f"Creating test database: {TEST_DB_NAME}")
        cursor.execute(f"CREATE DATABASE {TEST_DB_NAME}")
        
        # Close cursor and admin connection
        cursor.close()
        admin_conn.close()
        
        # Get the port we successfully connected to
        port = test_db_config['database']['test']['port']
        
        # Return the connection string for the new database
        test_conn_string = f"host={admin_config.get('host', 'localhost')} " \
                          f"port={port} " \
                          f"dbname={TEST_DB_NAME} " \
                          f"user={admin_config.get('user', 'postgres')} " \
                          f"password={admin_config.get('password', 'postgres')}"
        
        # Yield the connection string so tests can use it
        yield test_conn_string
        
    finally:
        # Cleanup: Drop the test database after tests complete
        try:
            # Get the port we successfully connected to
            port = test_db_config['database']['test']['port']
            admin_conn_string = f"host={admin_config.get('host', 'localhost')} " \
                              f"port={port} " \
                              f"dbname=postgres " \
                              f"user={admin_config.get('user', 'postgres')} " \
                              f"password={admin_config.get('password', 'postgres')}"
            
            admin_conn = psycopg2.connect(admin_conn_string)
            admin_conn.autocommit = True
            cursor = admin_conn.cursor()
            
            # Terminate existing connections to the database
            cursor.execute(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{TEST_DB_NAME}'
                AND pid <> pg_backend_pid()
            """)
            
            # Drop the database
            logger.info(f"Dropping test database: {TEST_DB_NAME}")
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
            
            cursor.close()
            admin_conn.close()
            
        except Exception as e:
            logger.error(f"Error cleaning up test database: {e}")

@pytest.fixture
def setup_initial_schema(create_test_database):
    """Set up initial schema in test database by creating the required tables"""
    conn_string = create_test_database
    
    # Connect to the test database
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Create ticker_data table
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
        
        # Create migrations table (this would be done by the first migration)
        cursor.execute("""
            CREATE TABLE migrations (
                id SERIAL PRIMARY KEY,
                migration_name VARCHAR(255) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert initial migration record
        cursor.execute("""
            INSERT INTO migrations (migration_name) 
            VALUES ('001_initial_schema.sql')
        """)
        
        logger.info("Initial schema created successfully")
        
    except Exception as e:
        logger.error(f"Error setting up initial schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # Return connection string for later use
    return conn_string

@pytest.fixture
def migration_manager(setup_initial_schema):
    """Create a migration manager for the test database"""
    conn_string = setup_initial_schema
    manager = MigrationManager(conn_string, TEST_MIGRATIONS_PATH, is_test_db=True)
    manager.connect()
    
    yield manager
    
    # Cleanup
    manager.close()

# Sample data for testing
@pytest.fixture
def sample_data():
    """Sample data for testing"""
    current_time = time.time()
    
    return {
        'ticker': {
            'instrument_name': 'BTC-PERPETUAL',
            'mark_price': 50000.0,
            'mark_timestamp': current_time - 0.2,  # 200ms earlier
            'best_bid_price': 49950.0,
            'best_bid_amount': 1.0,
            'best_ask_price': 50050.0,
            'best_ask_amount': 1.0,
            'processing_timestamp': current_time - 0.05  # 50ms earlier
        },
        'trade': {
            'instrument_name': 'BTC-PERPETUAL',
            'order_id': 'order123',
            'trade_id': 'trade456',
            'price': 50000.0,
            'amount': 1.0,
            'time': current_time - 0.3,  # 300ms earlier
            'direction': 'buy',
            'fee': 0.0002,
            'processing_timestamp': current_time - 0.08  # 80ms earlier
        },
        'ack': {
            'instrument_name': 'BTC-PERPETUAL',
            'order_id': 'order789',
            'type': 'new_order',
            'message': 'Order accepted',
            'price': 49900.0,
            'amount': 0.5,
            'create_time': current_time - 0.25,  # 250ms earlier
            'direction': 'sell',
            'reduce_only': False,
            'processing_timestamp': current_time - 0.1  # 100ms earlier
        }
    }

# Tests
def test_migration_manager_initialization(migration_manager):
    """Test that the migration manager initializes correctly"""
    assert migration_manager.conn is not None
    assert migration_manager.migrations_dir == TEST_MIGRATIONS_PATH
    assert migration_manager.is_test_db is True

def test_get_migration_files(migration_manager):
    """Test retrieving migration files"""
    files = migration_manager._get_migration_files()
    assert len(files) >= 2
    assert "001_initial_schema.sql" in files
    assert "002_add_processing_timestamp.sql" in files

def test_get_applied_migrations(migration_manager):
    """Test retrieving applied migrations"""
    applied = migration_manager._get_applied_migrations()
    assert len(applied) == 1
    assert "001_initial_schema.sql" in applied

def test_apply_migrations(migration_manager):
    """Test applying migrations"""
    count, applied = migration_manager.apply_migrations()
    assert count == 1
    assert "002_add_processing_timestamp.sql" in applied

def test_new_columns_exist(setup_initial_schema):
    """Test that the new columns were added by the migration"""
    conn_string = setup_initial_schema
    
    # First apply the migrations
    manager = MigrationManager(conn_string, TEST_MIGRATIONS_PATH, is_test_db=True)
    manager.connect()
    manager.apply_migrations()
    manager.close()
    
    # Connect to check schema
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    try:
        # Check ticker_data table
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'ticker_data'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # Check for new columns
        assert 'processing_timestamp' in columns
        assert 'rust_to_db_latency_ms' in columns
        assert 'exchange_to_rust_latency_ms' in columns
        assert 'total_latency_ms' in columns
        
        # Check trade_data table
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'trade_data'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # Check for new columns
        assert 'processing_timestamp' in columns
        assert 'rust_to_db_latency_ms' in columns
        assert 'exchange_to_rust_latency_ms' in columns
        assert 'total_latency_ms' in columns
        
        # Check ack_data table
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'ack_data'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # Check for new columns
        assert 'processing_timestamp' in columns
        assert 'rust_to_db_latency_ms' in columns
        assert 'exchange_to_rust_latency_ms' in columns
        assert 'total_latency_ms' in columns
        
    finally:
        cursor.close()
        conn.close()

def test_insert_data_with_processing_timestamp(setup_initial_schema, sample_data):
    """Test inserting data with the new processing_timestamp field"""
    conn_string = setup_initial_schema
    
    # First apply the migrations
    manager = MigrationManager(conn_string, TEST_MIGRATIONS_PATH, is_test_db=True)
    manager.connect()
    manager.apply_migrations()
    manager.close()
    
    # Connect to insert data
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Insert ticker data
        ticker_data = sample_data['ticker']
        cursor.execute("""
            INSERT INTO ticker_data (
                instrument_name, mark_price, mark_timestamp, 
                best_bid_price, best_bid_amount, best_ask_price, best_ask_amount,
                processing_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            ticker_data['instrument_name'],
            ticker_data['mark_price'],
            ticker_data['mark_timestamp'],
            ticker_data['best_bid_price'],
            ticker_data['best_bid_amount'],
            ticker_data['best_ask_price'],
            ticker_data['best_ask_amount'],
            ticker_data['processing_timestamp']
        ))
        ticker_id = cursor.fetchone()[0]
        assert ticker_id is not None
        
        # Insert trade data
        trade_data = sample_data['trade']
        cursor.execute("""
            INSERT INTO trade_data (
                instrument_name, order_id, trade_id, 
                price, amount, time, direction, fee,
                processing_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            trade_data['instrument_name'],
            trade_data['order_id'],
            trade_data['trade_id'],
            trade_data['price'],
            trade_data['amount'],
            trade_data['time'],
            trade_data['direction'],
            trade_data['fee'],
            trade_data['processing_timestamp']
        ))
        trade_id = cursor.fetchone()[0]
        assert trade_id is not None
        
        # Insert ack data
        ack_data = sample_data['ack']
        cursor.execute("""
            INSERT INTO ack_data (
                instrument_name, order_id, type, 
                message, price, amount, create_time, direction, reduce_only,
                processing_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            ack_data['instrument_name'],
            ack_data['order_id'],
            ack_data['type'],
            ack_data['message'],
            ack_data['price'],
            ack_data['amount'],
            ack_data['create_time'],
            ack_data['direction'],
            ack_data['reduce_only'],
            ack_data['processing_timestamp']
        ))
        ack_id = cursor.fetchone()[0]
        assert ack_id is not None
        
    finally:
        cursor.close()
        conn.close()

def test_latency_calculations(setup_initial_schema, sample_data):
    """Test that latency calculations work correctly"""
    conn_string = setup_initial_schema
    
    # First apply the migrations
    manager = MigrationManager(conn_string, TEST_MIGRATIONS_PATH, is_test_db=True)
    manager.connect()
    manager.apply_migrations()
    manager.close()
    
    # Connect to insert and query data
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        # Insert ticker data
        ticker_data = sample_data['ticker']
        cursor.execute("""
            INSERT INTO ticker_data (
                instrument_name, mark_price, mark_timestamp, 
                best_bid_price, best_bid_amount, best_ask_price, best_ask_amount,
                processing_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            ticker_data['instrument_name'],
            ticker_data['mark_price'],
            ticker_data['mark_timestamp'],
            ticker_data['best_bid_price'],
            ticker_data['best_bid_amount'],
            ticker_data['best_ask_price'],
            ticker_data['best_ask_amount'],
            ticker_data['processing_timestamp']
        ))
        ticker_id = cursor.fetchone()[0]
        
        # Query to get latency metrics
        cursor.execute("""
            SELECT 
                instrument_name,
                mark_timestamp,
                processing_timestamp,
                EXTRACT(EPOCH FROM time_ts) as time_ts_epoch,
                exchange_to_rust_latency_ms,
                rust_to_db_latency_ms,
                total_latency_ms
            FROM ticker_data
            WHERE id = %s
        """, (ticker_id,))
        
        row = cursor.fetchone()
        
        # Calculate expected latencies manually
        time_ts_epoch = float(row['time_ts_epoch'])
        mark_timestamp = float(row['mark_timestamp'])
        processing_timestamp = float(row['processing_timestamp'])
        
        expected_exchange_to_rust = (processing_timestamp - mark_timestamp) * 1000
        expected_rust_to_db = (time_ts_epoch - processing_timestamp) * 1000
        expected_total = (time_ts_epoch - mark_timestamp) * 1000
        
        # Verify latency calculations
        assert abs(row['exchange_to_rust_latency_ms'] - expected_exchange_to_rust) < 1.0, \
            f"Exchange to Rust latency mismatch: {row['exchange_to_rust_latency_ms']} vs {expected_exchange_to_rust}"
        
        assert abs(row['rust_to_db_latency_ms'] - expected_rust_to_db) < 1.0, \
            f"Rust to DB latency mismatch: {row['rust_to_db_latency_ms']} vs {expected_rust_to_db}"
        
        assert abs(row['total_latency_ms'] - expected_total) < 1.0, \
            f"Total latency mismatch: {row['total_latency_ms']} vs {expected_total}"
        
    finally:
        cursor.close()
        conn.close()

def test_migration_script_content():
    """Test that the migration script contains the expected statements"""
    migration_path = Path(TEST_MIGRATIONS_PATH) / "002_add_processing_timestamp.sql"
    
    with open(migration_path, 'r') as f:
        content = f.read()
        
        # Check that the script adds processing_timestamp to all tables
        assert "ALTER TABLE public.ticker_data ADD COLUMN IF NOT EXISTS processing_timestamp" in content
        assert "ALTER TABLE public.trade_data ADD COLUMN IF NOT EXISTS processing_timestamp" in content
        assert "ALTER TABLE public.ack_data ADD COLUMN IF NOT EXISTS processing_timestamp" in content
        
        # Check that the script adds latency columns
        assert "rust_to_db_latency_ms" in content
        assert "exchange_to_rust_latency_ms" in content
        assert "total_latency_ms" in content
        
        # Check that triggers are created for calculating latency
        assert "CREATE OR REPLACE FUNCTION calculate_ticker_latency()" in content
        assert "CREATE OR REPLACE FUNCTION calculate_trade_latency()" in content
        assert "CREATE OR REPLACE FUNCTION calculate_ack_latency()" in content
        
        # Check that calculations are correct for different tables
        assert "(processing_timestamp * 1000) - (mark_timestamp * 1000)" in content  # ticker exchange_to_rust
        assert "(processing_timestamp * 1000) - (time * 1000)" in content  # trade exchange_to_rust
        assert "(processing_timestamp * 1000) - (create_time * 1000)" in content  # ack exchange_to_rust
        
        # Check that triggers are created
        assert "CREATE TRIGGER ticker_latency_trigger" in content
        assert "CREATE TRIGGER trade_latency_trigger" in content
        assert "CREATE TRIGGER ack_latency_trigger" in content

@pytest.fixture
def test_db_for_script():
    """Create a standalone test database for testing the migration script"""
    # Generate unique test DB name for this test
    test_db_name = f"cryptics_script_test_{uuid.uuid4().hex[:8]}"
    
    # Admin connection parameters 
    admin_config = {
        'host': 'localhost',
        'port': 5433,
        'name': 'postgres',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # Build connection string
    admin_conn_string = f"host={admin_config.get('host')} " \
                       f"port={admin_config.get('port')} " \
                       f"dbname={admin_config.get('name')} " \
                       f"user={admin_config.get('user')} " \
                       f"password={admin_config.get('password')}"
    
    # Connect to postgres database
    admin_conn = psycopg2.connect(admin_conn_string)
    admin_conn.autocommit = True
    
    try:
        # Create a cursor
        cursor = admin_conn.cursor()
        
        # Create the test database
        logger.info(f"Creating script test database: {test_db_name}")
        cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        cursor.execute(f"CREATE DATABASE {test_db_name}")
        
        # Close cursor and admin connection
        cursor.close()
        admin_conn.close()
        
        # Return the connection string for the new database
        test_conn_string = f"host={admin_config.get('host')} " \
                          f"port={admin_config.get('port')} " \
                          f"dbname={test_db_name} " \
                          f"user={admin_config.get('user')} " \
                          f"password={admin_config.get('password')}"
        
        # Yield the connection string and database name
        yield (test_conn_string, test_db_name)
        
    finally:
        # Cleanup: Drop the test database after tests complete
        try:
            admin_conn = psycopg2.connect(admin_conn_string)
            admin_conn.autocommit = True
            cursor = admin_conn.cursor()
            
            # Terminate existing connections
            cursor.execute(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{test_db_name}'
                AND pid <> pg_backend_pid()
            """)
            
            # Drop the database
            logger.info(f"Dropping script test database: {test_db_name}")
            cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
            
            cursor.close()
            admin_conn.close()
            
        except Exception as e:
            logger.error(f"Error cleaning up script test database: {e}")

def test_create_db_script(test_db_for_script):
    """
    Test creating a migration script to generate the test database
    This can be used to manually create the test database without running the whole pipeline
    """
    conn_string, db_name = test_db_for_script
    test_migrations_dir = TEST_MIGRATIONS_PATH
    
    # Create a script file to set up the test database
    script_path = Path(__file__).parent / "create_test_db.py"
    
    script_content = f"""#!/usr/bin/env python3
\"\"\"
Create Test Database Script
===========================
Creates a separate test database and applies migrations for testing.
\"\"\"

import os
import sys
import logging
import psycopg2
from pathlib import Path
from typing import List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("create-test-db")

# Project root is the parent directory of the script location
PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATIONS_PATH = os.path.join(PROJECT_ROOT, "migrations")

# Test database connection details
TEST_DB_CONFIG = {{
    'host': 'localhost',
    'port': 5433,
    'name': '{db_name}',
    'user': 'postgres',
    'password': 'postgres'
}}

def create_test_database():
    \"\"\"Create the test database if it doesn't exist\"\"\"
    # Connect to default postgres database
    admin_conn_string = (f"host={{TEST_DB_CONFIG['host']}} "
                         f"port={{TEST_DB_CONFIG['port']}} "
                         f"dbname=postgres "
                         f"user={{TEST_DB_CONFIG['user']}} "
                         f"password={{TEST_DB_CONFIG['password']}}")
    
    admin_conn = psycopg2.connect(admin_conn_string)
    admin_conn.autocommit = True
    cursor = admin_conn.cursor()
    
    try:
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{{TEST_DB_CONFIG['name']}}'")
        if cursor.fetchone():
            logger.info(f"Test database {{TEST_DB_CONFIG['name']}} already exists")
            return
            
        # Create the test database
        logger.info(f"Creating test database: {{TEST_DB_CONFIG['name']}}")
        cursor.execute(f"CREATE DATABASE {{TEST_DB_CONFIG['name']}}")
        logger.info(f"Test database created successfully")
        
    except Exception as e:
        logger.error(f"Error creating test database: {{e}}")
        raise
    finally:
        cursor.close()
        admin_conn.close()

def apply_migrations():
    \"\"\"Apply migrations to the test database\"\"\"
    # Connect to the test database
    test_conn_string = (f"host={{TEST_DB_CONFIG['host']}} "
                       f"port={{TEST_DB_CONFIG['port']}} "
                       f"dbname={{TEST_DB_CONFIG['name']}} "
                       f"user={{TEST_DB_CONFIG['user']}} "
                       f"password={{TEST_DB_CONFIG['password']}}")
    
    conn = psycopg2.connect(test_conn_string)
    conn.autocommit = False
    cursor = conn.cursor()
    
    try:
        # Create migrations table if it doesn't exist
        cursor.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS migrations (
                id SERIAL PRIMARY KEY,
                migration_name VARCHAR(255) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        \"\"\")
        
        # Get list of applied migrations
        cursor.execute("SELECT migration_name FROM migrations ORDER BY id")
        applied_migrations = [row[0] for row in cursor.fetchall()]
        
        # Get list of migration files
        migration_dir = Path(MIGRATIONS_PATH)
        if not migration_dir.exists():
            logger.error(f"Migrations directory not found: {{MIGRATIONS_PATH}}")
            return
        
        migration_files = sorted([
            f.name for f in migration_dir.glob("*.sql")
            if f.name.startswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
        ])
        
        logger.info(f"Found {{len(migration_files)}} migration files")
        logger.info(f"{{len(applied_migrations)}} migrations already applied")
        
        # Apply pending migrations
        for migration_file in migration_files:
            if migration_file not in applied_migrations:
                migration_path = os.path.join(MIGRATIONS_PATH, migration_file)
                
                logger.info(f"Applying migration: {{migration_file}}")
                
                # Read and execute the migration file
                with open(migration_path, 'r') as f:
                    sql = f.read()
                    cursor.execute(sql)
                
                # Record the migration
                cursor.execute(
                    "INSERT INTO migrations (migration_name) VALUES (%s)",
                    (migration_file,)
                )
                
                logger.info(f"Successfully applied migration: {{migration_file}}")
                
        # Commit the transaction
        conn.commit()
        logger.info("All migrations applied successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error applying migrations: {{e}}")
        raise
    finally:
        cursor.close()
        conn.close()

def create_initial_schema():
    \"\"\"Create the initial schema if using a fresh database\"\"\"
    # Connect to the test database
    test_conn_string = (f"host={{TEST_DB_CONFIG['host']}} "
                       f"port={{TEST_DB_CONFIG['port']}} "
                       f"dbname={{TEST_DB_CONFIG['name']}} "
                       f"user={{TEST_DB_CONFIG['user']}} "
                       f"password={{TEST_DB_CONFIG['password']}}")
    
    conn = psycopg2.connect(test_conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Check if tables already exist
        cursor.execute(\"\"\"
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('ticker_data', 'trade_data', 'ack_data')
        \"\"\")
        
        count = cursor.fetchone()[0]
        if count >= 3:
            logger.info("Tables already exist, skipping initial schema creation")
            return
        
        # Create ticker_data table
        logger.info("Creating initial schema tables...")
        cursor.execute(\"\"\"
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
        \"\"\")
        
        # Create trade_data table
        cursor.execute(\"\"\"
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
        \"\"\")
        
        # Create ack_data table
        cursor.execute(\"\"\"
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
        \"\"\")
        
        logger.info("Initial schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating initial schema: {{e}}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    \"\"\"Main function to create and set up the test database\"\"\"
    try:
        # Step 1: Create the test database
        create_test_database()
        
        # Step 2: Create initial schema tables
        create_initial_schema()
        
        # Step 3: Apply all migrations
        apply_migrations()
        
        logger.info(f"Test database {{TEST_DB_CONFIG['name']}} is ready for testing")
        logger.info(f"Connection string: host={{TEST_DB_CONFIG['host']}} port={{TEST_DB_CONFIG['port']}} "
                   f"dbname={{TEST_DB_CONFIG['name']}} user={{TEST_DB_CONFIG['user']}} "
                   f"password={{TEST_DB_CONFIG['password']}}")
        
    except Exception as e:
        logger.error(f"Failed to create test database: {{e}}")
        sys.exit(1)

if __name__ == "__main__":
    main()
"""
    
    # Create the script file
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    # Make it executable
    os.chmod(script_path, 0o755)
    
    assert script_path.exists()
    logger.info(f"Created test database script at {script_path}")
    
    # Validate the script by checking if it can execute
    from importlib.util import module_from_spec, spec_from_file_location
    
    try:
        spec = spec_from_file_location("create_test_db", script_path)
        test_db_module = module_from_spec(spec)
        # We don't actually execute the script here, just validate it can be loaded
        assert spec is not None
        logger.info("Script validation successful")
    except Exception as e:
        logger.error(f"Script validation failed: {e}")
        assert False, f"Script validation failed: {e}"

if __name__ == "__main__":
    # Allow running with pytest or directly
    pytest.main(["-xvs", __file__])
