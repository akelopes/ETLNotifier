from abc import ABC, abstractmethod
from typing import List, Any, Dict

class DataSource(ABC):
    @abstractmethod
    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as a list of dictionaries.
        
        Args:
            query: Dictionary containing query parameters and configuration
            
        Returns:
            List of dictionaries containing the query results
            
        Raises:
            NotImplementedError: If the data source hasn't implemented this method
        """
        pass

    @abstractmethod
    def __enter__(self):
        """
        Set up the data source connection when entering a context.
        Used for resource management in 'with' statements.
        
        Returns:
            Self for context manager
            
        Raises:
            NotImplementedError: If the data source hasn't implemented this method
        """
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the data source connection when exiting a context.
        Used for resource management in 'with' statements.
        
        Args:
            exc_type: Type of exception that occurred, if any
            exc_val: Exception instance that occurred, if any
            exc_tb: Traceback of exception that occurred, if any
            
        Raises:
            NotImplementedError: If the data source hasn't implemented this method
        """
        pass