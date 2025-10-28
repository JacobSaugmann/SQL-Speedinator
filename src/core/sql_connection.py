"""
SQL Server Connection Manager
Handles connections to SQL Server with proper error handling and connection pooling
"""

import pyodbc
import logging
import time
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

class SQLServerConnection:
    """Manages SQL Server connections with error handling and retry logic"""
    
    def __init__(self, server_name: str, config):
        """Initialize SQL Server connection
        
        Args:
            server_name (str): SQL Server instance name
            config: Configuration manager instance
        """
        self.server_name = server_name
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self._connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build connection string from configuration"""
        driver = self.config.sql_driver
        timeout = self.config.connection_timeout
        
        # Use Windows Authentication by default, SQL Auth only if explicitly chosen
        if self.config.use_windows_auth:
            self.logger.info("Using Windows Authentication for SQL Server connection")
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={self.server_name};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={timeout};"
                f"CommandTimeout={self.config.query_timeout};"
            )
        else:
            self.logger.info("Using SQL Server Authentication")
            username = self.config.sql_username
            password = self.config.sql_password
            
            if not username or not password:
                raise ValueError(
                    "SQL Server Authentication selected but SQL_USERNAME or SQL_PASSWORD not provided. "
                    "Either set USE_WINDOWS_AUTH=true or provide SQL credentials."
                )
            
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={self.server_name};"
                f"UID={username};"
                f"PWD={password};"
                f"Connection Timeout={timeout};"
                f"CommandTimeout={self.config.query_timeout};"
            )
        
        return connection_string
    
    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            self.logger.info(f"Connecting to SQL Server: {self.server_name}")
            self.connection = pyodbc.connect(
                self._connection_string,
                timeout=self.config.connection_timeout
            )
            self.connection.timeout = self.config.query_timeout
            self.logger.info("Successfully connected to SQL Server")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to SQL Server: {e}")
            return False
    
    def disconnect(self):
        """Close connection to SQL Server"""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Disconnected from SQL Server")
            except Exception as e:
                self.logger.error(f"Error disconnecting from SQL Server: {e}")
            finally:
                self.connection = None
    
    def test_connection(self) -> bool:
        """Test if connection is working"""
        if not self.connection:
            return False
            
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, parameters: Optional[tuple] = None, 
                     fetch_results: bool = True) -> Optional[List[Dict[str, Any]]]:
        """Execute SQL query and return results
        
        Args:
            query (str): SQL query to execute
            parameters (tuple, optional): Query parameters
            fetch_results (bool): Whether to fetch and return results
            
        Returns:
            List of dictionaries with query results or None
        """
        if not self.connection:
            self.logger.error("No active connection to SQL Server")
            return None
        
        try:
            cursor = self.connection.cursor()
            
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            if fetch_results:
                # Get column names
                columns = [column[0] for column in cursor.description] if cursor.description else []
                
                # Fetch all rows
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                results = []
                for row in rows:
                    results.append(dict(zip(columns, row)))
                
                cursor.close()
                return results
            else:
                cursor.close()
                return None
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            self.logger.error(f"Query: {query}")
            if cursor:
                cursor.close()
            return None
    
    def execute_query_with_retry(self, query: str, parameters: Optional[tuple] = None,
                               max_retries: int = 3, retry_delay: int = 1) -> Optional[List[Dict[str, Any]]]:
        """Execute query with retry logic for transient failures"""
        for attempt in range(max_retries):
            try:
                return self.execute_query(query, parameters)
            except Exception as e:
                self.logger.warning(f"Query attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    if not self.test_connection():
                        self.logger.info("Reconnecting to SQL Server...")
                        self.disconnect()
                        if not self.connect():
                            self.logger.error("Failed to reconnect")
                            return None
                else:
                    self.logger.error(f"Query failed after {max_retries} attempts")
                    return None
        
        return None
    
    def get_server_info(self) -> Optional[List[Dict[str, Any]]]:
        """Get basic server information"""
        query = """
        SELECT
            @@SERVERNAME as server_name,
            @@VERSION as version,
            CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)) as product_version,
            CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR(50)) as product_level,
            CAST(SERVERPROPERTY('Edition') AS VARCHAR(200)) as edition,
            SERVERPROPERTY('EngineEdition') as engine_edition,
            CAST(SERVERPROPERTY('MachineName') AS VARCHAR(100)) as machine_name,
            CAST(SERVERPROPERTY('InstanceName') AS VARCHAR(100)) as instance_name,
            CAST(SERVERPROPERTY('Collation') AS VARCHAR(100)) as collation
        """
        return self.execute_query(query)
    
    def change_database(self, database_name: str) -> bool:
        """Change current database context
        
        Args:
            database_name (str): Target database name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            query = f"USE [{database_name}]"
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            self.logger.debug(f"Changed to database: {database_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to change to database {database_name}: {str(e)}")
            return False
    
    def __enter__(self):
        """Context manager entry"""
        if self.connect():
            return self
        else:
            raise Exception("Failed to establish SQL Server connection")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()