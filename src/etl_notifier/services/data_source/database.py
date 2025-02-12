from typing import List, Any, Dict
import pyodbc
from .base import DataSource

class DatabaseSource(DataSource):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        if not self.connection:
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.cursor = None
        self.connection = None

    def __del__(self):
        self.disconnect()

    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        sql = query.get("sql")
        if not sql:
            raise ValueError("SQL query is required for database source")
            
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]