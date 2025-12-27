"""
Base Analyzer Abstract Class
Provides common interface for all SQL Server performance analyzers
"""

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, Optional


class BaseAnalyzer(ABC):
    """
    Abstract base class for all SQL Server performance analyzers.
    
    All analyzers must inherit from this class and implement the analyze() method.
    This ensures a consistent interface across all analyzers.
    
    Attributes:
        connection: SQLServerConnection instance for database access
        config: ConfigManager instance for configuration values
        logger: Logger instance for this analyzer class
        
    Example:
        >>> class MyAnalyzer(BaseAnalyzer):
        ...     def analyze(self) -> Dict[str, Any]:
        ...         return {'result': 'data', 'error': None}
    """
    
    def __init__(self, connection, config):
        """
        Initialize base analyzer with dependencies.
        
        Args:
            connection: SQLServerConnection instance
            config: ConfigManager instance
            
        Raises:
            TypeError: If connection or config is None
        """
        if connection is None:
            raise TypeError("connection cannot be None")
        if config is None:
            raise TypeError("config cannot be None")
            
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def analyze(self) -> Dict[str, Any]:
        """
        Execute the analysis and return results.
        
        This method must be implemented by all subclasses.
        
        Returns:
            Dict[str, Any]: Dictionary containing:
                - Normal case: {'key1': value1, 'key2': value2, ...}
                - Error case: {'error': 'error message', ...}
                
        Raises:
            Should not raise - catch exceptions and return {'error': str(e)}
            
        Example:
            >>> analyzer = MyAnalyzer(connection, config)
            >>> result = analyzer.analyze()
            >>> if 'error' in result:
            ...     print(f"Analysis failed: {result['error']}")
            ... else:
            ...     print(f"Analysis succeeded: {result}")
        """
        pass
    
    def _safe_execute_query(self, query: str, description: str = "Query") -> Optional[List[Dict[str, Any]]]:
        """
        Safely execute a SQL query with error handling and logging.
        
        Args:
            query: SQL query string to execute
            description: Human-readable description of the query
            
        Returns:
            List of result dictionaries, or None if query fails
        """
        try:
            self.logger.debug(f"{description}: Executing query...")
            result = self.connection.execute_query(query)
            if result:
                self.logger.debug(f"{description}: Retrieved {len(result)} rows")
            return result
        except Exception as e:
            self.logger.error(f"{description}: Query failed - {e}")
            return None
