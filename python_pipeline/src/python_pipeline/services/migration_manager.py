import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2

logger = logging.getLogger(__name__)

class MigrationManager:
    """
    Manages database migrations from SQL files.
    Keeps track of applied migrations in a database table.
    """
    
    def __init__(
        self, 
        conn_string: str, 
        migrations_dir: str,
        is_test_db: bool = False
    ):
        """
        Initialize the migration manager
        
        Args:
            conn_string: PostgreSQL connection string
            migrations_dir: Directory containing migration SQL files
            is_test_db: Whether this is the test database
        """
        self.conn_string = conn_string
        self.migrations_dir = migrations_dir
        self.is_test_db = is_test_db
        self.conn = None
        
    def connect(self) -> None:
        """Establish database connection"""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(self.conn_string)
                self.conn.autocommit = False  # We want transaction control for migrations
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
    
    def close(self) -> None:
        """Close database connection"""
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            self.conn = None
    
    def _get_migration_files(self) -> List[str]:
        """Get sorted list of migration files"""
        migration_dir = Path(self.migrations_dir)
        if not migration_dir.exists():
            logger.error(f"Migrations directory not found: {self.migrations_dir}")
            return []
        
        migration_files = sorted([
            f.name for f in migration_dir.glob("*.sql")
            if f.name.startswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
        ])
        
        return migration_files
    
    def _get_applied_migrations(self) -> List[str]:
        """Get list of already applied migrations"""
        self.connect()
        cursor = self.conn.cursor()
        
        # First check if migrations table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'migrations'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info("Migrations table does not exist yet, creating it...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    id SERIAL PRIMARY KEY,
                    migration_name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
            self.conn.commit()
            return []
        
        # Get applied migrations
        cursor.execute("SELECT migration_name FROM migrations ORDER BY id")
        applied_migrations = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        return applied_migrations
    
    def _execute_migration(self, migration_file: str) -> None:
        """Execute a single migration file"""
        migration_path = Path(self.migrations_dir) / migration_file
        
        if not migration_path.exists():
            logger.error(f"Migration file not found: {migration_path}")
            return
        
        logger.info(f"Applying migration: {migration_file}")
        
        self.connect()
        cursor = self.conn.cursor()
        
        try:
            # Read and execute the migration file
            with open(migration_path, 'r') as f:
                sql = f.read()
                cursor.execute(sql)
            
            # Record the migration
            cursor.execute(
                "INSERT INTO migrations (migration_name) VALUES (%s)",
                (migration_file,)
            )
            
            # Commit the transaction
            self.conn.commit()
            logger.info(f"Successfully applied migration: {migration_file}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Migration failed: {migration_file}, Error: {e}")
            raise
        finally:
            cursor.close()
    
    def apply_migrations(self) -> Tuple[int, List[str]]:
        """
        Apply all pending migrations
        
        Returns:
            Tuple containing count of applied migrations and list of applied migration names
        """
        db_type = "TEST" if self.is_test_db else "PRODUCTION"
        logger.info(f"Starting migrations for {db_type} database")
        
        migration_files = self._get_migration_files()
        applied_migrations = self._get_applied_migrations()
        
        logger.info(f"Found {len(migration_files)} migration files")
        logger.info(f"{len(applied_migrations)} migrations already applied")
        
        # Determine pending migrations
        pending_migrations = [
            m for m in migration_files if m not in applied_migrations
        ]
        
        logger.info(f"Applying {len(pending_migrations)} pending migrations")
        
        applied = []
        for migration in pending_migrations:
            try:
                self._execute_migration(migration)
                applied.append(migration)
            except Exception as e:
                logger.error(f"Failed to apply migration {migration}: {e}")
                # Don't continue with remaining migrations if one fails
                break
                
        logger.info(f"Applied {len(applied)} migrations successfully")
        self.close()
        
        return len(applied), applied

def get_connection_string(config: dict, is_test: bool = False) -> str:
    """
    Build a connection string from configuration
    
    Args:
        config: Database configuration dictionary
        is_test: Whether to use test database configuration
        
    Returns:
        PostgreSQL connection string
    """
    if is_test:
        # Use test database configuration
        db_config = config.get('database', {}).get('test', {})
        
        # Check if we're running in Docker
        is_docker = os.environ.get('python_running_in_docker', 'false').lower() == 'true'
        
        host = db_config.get('host_internal' if is_docker else 'host', 'localhost')
        port = db_config.get('port', 5433)
        name = db_config.get('name', 'cryptics_test')
        user = db_config.get('user', 'postgres')
        password = db_config.get('password', 'postgres')
    else:
        # Use production database configuration
        db_config = config.get('database', {})
        
        # Check if we're running in Docker
        is_docker = os.environ.get('python_running_in_docker', 'false').lower() == 'true'
        
        host = db_config.get('host_internal' if is_docker else 'host', 'localhost')
        port = db_config.get('port', 5432)
        name = db_config.get('name', 'cryptics')
        user = db_config.get('user', 'postgres')
        password = db_config.get('password', 'postgres')
    
    return f"host={host} port={port} dbname={name} user={user} password={password}"
