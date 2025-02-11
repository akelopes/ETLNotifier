from typing import List, Any, Dict
import pyodbc
from .base import DataSource

class DatabaseSource(DataSource):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.cursor = None

    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: Dictionary containing the SQL query under the 'sql' key
            
        Returns:
            List of dictionaries containing the query results
            
        Raises:
            ValueError: If SQL query is missing from the query dict
            pyodbc.Error: If there's a database error
        """
        sql = query.get("sql")
        if not sql:
            raise ValueError("SQL query is required for database source")
            
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def __enter__(self):
        """Set up database connection when entering context"""
        self.connection = pyodbc.connect(self.connection_string)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up database resources when exiting context"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()