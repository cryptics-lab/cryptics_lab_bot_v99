#!/usr/bin/env python3
"""
Database Service Module
=====================
Handles interactions with the PostgreSQL database.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import asyncpg for async database operations
import asyncpg
import psycopg2

from python_pipeline.services.config_service import ConfigService
from python_pipeline.services.migration_manager import (
    MigrationManager,
    get_connection_string,
)

logger: logging.Logger = logging.getLogger("database-service")

class DatabaseService:
    """Service for database operations"""
    
    def __init__(self, config_service: ConfigService):
        """
        Initialize the database service
        
        Args:
            config_service: Configuration service
        """
        self.config_service = config_service
        self.config = config_service.get_config()
        self.db_config = self.config.get('database', {})
        self.pipeline_config = self.config.get('pipeline', {})
        self.model_configs = self.pipeline_config.get('models', {})
        
        # Whether to clear tables before starting
        self.clear_tables = self.pipeline_config.get('clear_tables', False)
        
        # Get migrations directory
        project_root = Path(__file__).parent.parent.parent
        self.migrations_dir = os.path.join(project_root, "migrations")
        
        # Run migrations if configured
        self.run_migrations = self.pipeline_config.get('run_migrations', True)
        if self.run_migrations:
            self._apply_migrations()
    
    def _apply_migrations(self) -> None:
        """Apply database migrations to both production and test databases"""
        # Only apply migrations if enabled
        if not self.run_migrations:
            return
            
        # Apply migrations to production database
        logger.info("Applying migrations to production database...")
        db_config = self.config_service.get_database_config()
        prod_conn_string = f"host={db_config['host']} port={db_config['port']} dbname={db_config['name']} user={db_config['user']} password={db_config['password']}"
        migration_manager = MigrationManager(prod_conn_string, self.migrations_dir)
        count, applied = migration_manager.apply_migrations()
        logger.info(f"Applied {count} migrations to production database: {', '.join(applied) if applied else 'none'}")
        
        # Apply migrations to test database if enabled and available
        test_db = self.config.get('database', {}).get('test')
        if test_db:
            logger.info("Checking test database connection...")
            # Use is_test=True when calling get_database_config
            test_db_config = self.config_service.get_database_config(is_test=True)
            test_conn_string = f"host={test_db_config['host']} port={test_db_config['port']} dbname={test_db_config['name']} user={test_db_config['user']} password={test_db_config['password']}"
            
            # Try to connect to the test database
            test_connection_available = False
            try:
                test_conn = psycopg2.connect(test_conn_string)
                test_conn.close()
                test_connection_available = True
            except Exception as e:
                logger.warning(f"Test database is not available: {e}")
                logger.warning("Skipping test database migrations. This is OK for production.")
            
            # Only attempt migrations if connection is available
            if test_connection_available:
                logger.info("Applying migrations to test database...")
                try:
                    test_migration_manager = MigrationManager(test_conn_string, self.migrations_dir, is_test_db=True)
                    test_count, test_applied = test_migration_manager.apply_migrations()
                    logger.info(f"Applied {test_count} migrations to test database: {', '.join(test_applied) if test_applied else 'none'}")
                except Exception as e:
                    logger.warning(f"Failed to apply migrations to test database: {e}")
                    logger.warning("Continuing without test database migrations. This is OK for production.")
    
    def get_connection(self, is_test: bool = False) -> psycopg2.extensions.connection:
        """
        Get a database connection
        
        Args:
            is_test: Whether to connect to the test database
            
        Returns:
            PostgreSQL connection
            
        Raises:
            Exception: If connection fails and is_test is False
        """
        db_config = self.config_service.get_database_config(is_test=is_test)
        conn_string = f"host={db_config['host']} port={db_config['port']} dbname={db_config['name']} user={db_config['user']} password={db_config['password']}"
        
        try:
            conn = psycopg2.connect(conn_string)
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            if is_test:
                logger.warning("Test database connection failed - this is OK for production")
                raise
            else:
                logger.error("Production database connection failed - cannot continue")
                raise
    
    async def get_async_connection(self, is_test: bool = False) -> asyncpg.Connection:
        """
        Get an async database connection using asyncpg
        
        Args:
            is_test: Whether to connect to the test database
            
        Returns:
            Asyncpg connection
            
        Raises:
            Exception: If connection fails and is_test is False
        """
        db_config = self.config_service.get_database_config(is_test=is_test)
        dsn = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        
        try:
            conn = await asyncpg.connect(dsn)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database using asyncpg: {e}")
            if is_test:
                logger.warning("Test database connection failed - this is OK for production")
                raise
            else:
                logger.error("Production database connection failed - cannot continue")
                raise
    
    def check_schema_initialized(self) -> bool:
        """
        Check if the database schema is initialized
        
        Returns:
            True if schema is initialized
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check for critical tables
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('ticker_data', 'trade_data', 'ack_data')
            """)
            
            count = cursor.fetchone()[0]
            if count < 3:
                logger.error("Missing critical tables in the database schema")
                return False
                
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error checking database schema: {e}")
            return False
    
    async def check_schema_initialized_async(self) -> bool:
        """
        Check if the database schema is initialized (async version)
        
        Returns:
            True if schema is initialized
        """
        try:
            conn = await self.get_async_connection()
            
            # Check for critical tables
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('ticker_data', 'trade_data', 'ack_data')
            """)
            
            if count < 3:
                logger.error("Missing critical tables in the database schema")
                await conn.close()
                return False
            
            await conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error checking database schema: {e}")
            return False
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database
        
        Returns:
            List of table names
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            return tables
            
        except Exception as e:
            logger.error(f"Error listing database tables: {e}")
            return []
    
    async def list_tables_async(self) -> List[str]:
        """
        List all tables in the database (async version)
        
        Returns:
            List of table names
        """
        try:
            conn = await self.get_async_connection()
            
            rows = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            
            tables = [row['table_name'] for row in rows]
            
            await conn.close()
            return tables
            
        except Exception as e:
            logger.error(f"Error listing database tables: {e}")
            return []
    
    def check_database_tables_filled(self) -> Dict[str, int]:
        """
        Check if database tables have data
        
        Returns:
            Dictionary with table names and row counts
        """
        results: Dict[str, int] = {}
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get model table names from configuration
            model_configs = self.pipeline_config.get('models', {})
            
            # Check each enabled model's table
            for model_name in self.pipeline_config.get('enabled_models', []):
                if model_name in model_configs:
                    table_name = model_configs[model_name].get('table_name')
                    if table_name:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                            count = cursor.fetchone()[0]
                            results[table_name] = count
                            logger.info(f"Table {table_name} has {count} rows")
                        except Exception as table_e:
                            logger.error(f"Error counting rows in {table_name}: {table_e}")
                            results[table_name] = -1
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking database tables: {e}")
        
        return results
    
    async def check_database_tables_filled_async(self) -> Dict[str, int]:
        """
        Check if database tables have data (async implementation with concurrent queries)
        
        Returns:
            Dictionary with table names and row counts
        """
        results = {}
        
        try:
            # Get list of tables to check
            tables_to_check = []
            for model_name in self.pipeline_config.get('enabled_models', []):
                # Use the convention table_name = model_name + "_data"
                table_name = f"{model_name.lower()}_data"
                tables_to_check.append(table_name)
            
            # Add the additional tables that are populated by triggers
            tables_to_check.append("orders")
            tables_to_check.append("order_status")
            
            # Create a connection pool
            db_config = self.config_service.get_database_config()
            dsn = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
            
            # Define the check function with its own connection
            async def check_table(table_name):
                try:
                    # Create a new connection for each check
                    conn = await asyncpg.connect(dsn)
                    try:
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                        logger.info(f"Table {table_name} has {count} rows")
                        return table_name, count
                    finally:
                        await conn.close()
                except Exception as e:
                    logger.error(f"Error counting rows in {table_name}: {e}")
                    return table_name, -1
            
            # Run all checks concurrently with separate connections
            tasks = [check_table(table) for table in tables_to_check]
            completed_tasks = await asyncio.gather(*tasks)
            
            # Convert results to dictionary
            results = dict(completed_tasks)
            
        except Exception as e:
            logger.error(f"Error checking database tables: {e}")
        
        return results
    
    def clean_database_tables(self) -> None:
        """Clean all data from database tables"""
        # Skip if clear_tables is False
        if not self.clear_tables:
            logger.info("Skipping database table cleaning (clear_tables=False)")
            return
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get model table names from configuration
            model_configs = self.pipeline_config.get('models', {})
            
            # First truncate the order_status table (due to foreign key constraint)
            try:
                cursor.execute("TRUNCATE TABLE order_status")
                logger.info("Table order_status truncated")
            except Exception as table_e:
                logger.error(f"Error truncating order_status: {table_e}")
                
            # Then truncate the orders table with CASCADE option
            try:
                cursor.execute("TRUNCATE TABLE orders CASCADE")
                logger.info("Table orders truncated")
            except Exception as table_e:
                logger.error(f"Error truncating orders: {table_e}")
            
            # Truncate each enabled model's table
            for model_name in self.pipeline_config.get('enabled_models', []):
                if model_name in model_configs:
                    table_name = model_configs[model_name].get('table_name')
                    if table_name:
                        try:
                            cursor.execute(f"TRUNCATE TABLE {table_name}")
                            logger.info(f"Table {table_name} truncated")
                        except Exception as table_e:
                            logger.error(f"Error truncating {table_name}: {table_e}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error cleaning database tables: {e}")
    
    async def clean_database_tables_async(self) -> None:
        """Clean all data from database tables (async version)"""
        # Skip if clear_tables is False
        if not self.clear_tables:
            logger.info("Skipping database table cleaning (clear_tables=False)")
            return
            
        try:
            # Get list of tables to truncate using the same convention
            tables_to_truncate = []
            for model_name in self.pipeline_config.get('enabled_models', []):
                # Use the convention table_name = model_name + "_data"
                table_name = f"{model_name.lower()}_data"
                tables_to_truncate.append(table_name)
            
            # Add the additional tables that are populated by triggers
            # Note: order_status must be truncated before orders due to the foreign key constraint
            tables_to_truncate.append("order_status")
            tables_to_truncate.append("orders")
            
            # Create a connection pool
            db_config = self.config_service.get_database_config()
            dsn = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
            
            # Define the truncate function with its own connection
            async def truncate_table(table_name):
                try:
                    # Create a new connection for each truncate operation
                    conn = await asyncpg.connect(dsn)
                    try:
                        if table_name == "orders":
                            # For the orders table, use CASCADE to handle the foreign key constraint
                            await conn.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                        else:
                            await conn.execute(f"TRUNCATE TABLE {table_name}")
                        logger.info(f"Table {table_name} truncated")
                        return True
                    finally:
                        await conn.close()
                except Exception as e:
                    logger.error(f"Error truncating {table_name}: {e}")
                    return False
            
            # Run all truncations concurrently with separate connections
            tasks = [truncate_table(table) for table in tables_to_truncate]
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Error cleaning database tables: {e}")
