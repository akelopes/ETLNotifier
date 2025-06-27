from typing import List, Any, Dict
from abc import ABC, abstractmethod
from etl_notifier.models import NotificationRecord
import pyodbc
from etl_notifier.strategies.data_source import DataSource


class DatabaseSource(DataSource, ABC):
    def __init__(self, connection_string: str):
        super().__init__()
        self.connection_string = connection_string
        self.connection = pyodbc.connect(self.connection_string)
        self.cursor = self.connection.cursor()

    def _disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.cursor = None
        self.connection = None

    def __del__(self):
        self._disconnect()

    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        sql = query.get("sql")
        if not sql:
            raise ValueError("SQL query is required for database source")

        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    @abstractmethod
    def get_records(self, query: str) -> List[NotificationRecord]:
        pass
