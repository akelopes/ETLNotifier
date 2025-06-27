import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from etl_notifier.strategies.data_source.database_source import DatabaseSource
from etl_notifier.models import NotificationRecord

# Dummy subclass for testing abstract base
class DummyDatabaseSource(DatabaseSource):
    def get_records(self, query: str):
        return []

@pytest.fixture
def mock_cursor():
    cursor = MagicMock()
    cursor.description = [("id",), ("value",)]
    cursor.fetchall.return_value = [(1, "foo"), (2, "bar")]
    return cursor

@pytest.fixture
def mock_connection(mock_cursor):
    connection = MagicMock()
    connection.cursor.return_value = mock_cursor
    return connection

def test_execute_query_returns_expected_data(mock_connection):
    query = {"sql": "SELECT id, value FROM test_table"}

    with patch("pyodbc.connect", return_value=mock_connection) as mock_connect:
        db = DummyDatabaseSource(connection_string="DSN=mock")
        result = db._execute_query(query)

    mock_connect.assert_called_once_with("DSN=mock")
    assert isinstance(result, list)
    assert result == [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]

def test_execute_query_raises_error_without_sql():
    with patch("pyodbc.connect") as _:
        db = DummyDatabaseSource(connection_string="mock")
        with pytest.raises(ValueError, match="SQL query is required"):
            db._execute_query({})

def test_connect_and_disconnect(mock_connection):
    with patch("pyodbc.connect", return_value=mock_connection):
        db = DummyDatabaseSource("mock_connection")
        assert db.connection is not None
        assert db.cursor is not None

        db._disconnect()
        assert db.connection is None
        assert db.cursor is None
        mock_connection.close.assert_called_once()
        mock_connection.cursor().close.assert_called_once()
