import sqlite3
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from edgetrailresearch.data.storage.config import DB_PATH, DB_POOL_SIZE, DB_MAX_OVERFLOW

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(
            f'sqlite:///{DB_PATH}',
            poolclass=QueuePool,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
            pool_timeout=30
        )
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool."""
        connection = self.engine.raw_connection()
        try:
            yield connection
        finally:
            connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a query and return results."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_many(self, query: str, params_list: list) -> None:
        """Execute multiple queries in a transaction."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name=?
        """
        result = self.execute_query(query, (table_name,))
        return len(result) > 0
    
    def get_table_info(self, table_name: str) -> list:
        """Get information about a table's structure."""
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)

# Create a singleton instance
db_manager = DatabaseManager() 