"""
Tests for migration manager service.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import psycopg2
import pytest

from src.python_pipeline.services.migration_manager import MigrationManager, get_connection_string

# Test configuration
TEST_CONN_STRING = "host=localhost port=5433 dbname=cryptics_test user=postgres password=postgres"
TEST_MIGRATIONS_DIR = "/app/migrations"

@pytest.fixture
def mock_cursor():
    """Create a mock cursor"""
    cursor = MagicMock()
    cursor.fetchone.return_value = (False,)  # migrations table does not exist
    cursor.fetchall.return_value = []  # no migrations applied
    return cursor

@pytest.fixture
def mock_conn(mock_cursor):
    """Create a mock connection"""
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    conn.closed = False
    return conn

@pytest.fixture
def migration_manager(mock_conn):
    """Create a migration manager with mocked connection"""
    with patch('psycopg2.connect', return_value=mock_conn):
        manager = MigrationManager(TEST_CONN_STRING, TEST_MIGRATIONS_DIR, is_test_db=True)
        manager.connect()
        yield manager

def test_get_migration_files(migration_manager):
    """Test getting migration files"""
    # Setup mock directory structure
    migration_files = ["001_initial_schema.sql", "002_add_processing_timestamp.sql"]
    
    with patch('pathlib.Path.exists', return_value=True), \
         patch('pathlib.Path.glob', return_value=[Path(f) for f in migration_files]):
        
        # Test getting migration files
        files = migration_manager._get_migration_files()
        
        # Verify results
        assert len(files) == 2
        assert files == migration_files

def test_get_applied_migrations_no_table(migration_manager, mock_cursor):
    """Test getting applied migrations when table does not exist"""
    # Setup mock cursor to indicate migrations table does not exist
    mock_cursor.fetchone.return_value = (False,)
    
    # Call the method
    applied = migration_manager._get_applied_migrations()
    
    # Verify results
    assert len(applied) == 0
    # Verify table creation was attempted
    mock_cursor.execute.assert_any_call("""
                CREATE TABLE IF NOT EXISTS migrations (
                    id SERIAL PRIMARY KEY,
                    migration_name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)

def test_get_applied_migrations_with_table(migration_manager, mock_cursor):
    """Test getting applied migrations when table exists"""
    # Setup mock cursor to indicate migrations table exists
    mock_cursor.fetchone.return_value = (True,)
    mock_cursor.fetchall.return_value = [("001_initial_schema.sql",)]
    
    # Call the method
    applied = migration_manager._get_applied_migrations()
    
    # Verify results
    assert len(applied) == 1
    assert applied[0] == "001_initial_schema.sql"

def test_execute_migration(migration_manager, mock_cursor):
    """Test executing a migration file"""
    migration_file = "001_initial_schema.sql"
    migration_content = "CREATE TABLE test (id SERIAL PRIMARY KEY);"
    
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data=migration_content)):
        
        # Call the method
        migration_manager._execute_migration(migration_file)
        
        # Verify SQL was executed
        mock_cursor.execute.assert_any_call(migration_content)
        
        # Verify migration was recorded
        mock_cursor.execute.assert_any_call(
            "INSERT INTO migrations (migration_name) VALUES (%s)",
            (migration_file,)
        )
        
        # Verify commit was called
        migration_manager.conn.commit.assert_called_once()

def test_apply_migrations(migration_manager):
    """Test applying migrations"""
    # Mock methods
    migration_manager._get_migration_files = MagicMock(return_value=[
        "001_initial_schema.sql",
        "002_add_processing_timestamp.sql"
    ])
    migration_manager._get_applied_migrations = MagicMock(return_value=[
        "001_initial_schema.sql"
    ])
    migration_manager._execute_migration = MagicMock()
    
    # Call the method
    count, applied = migration_manager.apply_migrations()
    
    # Verify results
    assert count == 1
    assert applied == ["002_add_processing_timestamp.sql"]
    
    # Verify _execute_migration was called with the right file
    migration_manager._execute_migration.assert_called_once_with("002_add_processing_timestamp.sql")

def test_get_connection_string():
    """Test building connection string from config"""
    config = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'name': 'cryptics',
            'user': 'postgres',
            'password': 'postgres',
            'test': {
                'host': 'localhost',
                'port': 5433,
                'name': 'cryptics_test',
                'user': 'test_user',
                'password': 'test_pass'
            }
        }
    }
    
    # Test production connection string
    conn_string = get_connection_string(config)
    assert "host=localhost" in conn_string
    assert "port=5432" in conn_string
    assert "dbname=cryptics" in conn_string
    
    # Test test connection string
    test_conn_string = get_connection_string(config, is_test=True)
    assert "host=localhost" in test_conn_string
    assert "port=5433" in test_conn_string
    assert "dbname=cryptics_test" in test_conn_string
    assert "user=test_user" in test_conn_string
