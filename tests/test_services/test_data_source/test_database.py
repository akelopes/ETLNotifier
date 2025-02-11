import pytest
from unittest.mock import patch

from etl_notifier.services.data_source.database import DatabaseSource

class TestDatabaseSource:
    def test_execute_query(self, mock_db_cursor):
        source = DatabaseSource("test_connection_string")
        source.cursor = mock_db_cursor

        query = {"sql": "SELECT * FROM test_table"}
        results = source.execute_query(query)

        assert len(results) == 1
        assert results[0]["AccountName"] == "TestAccount"
        assert results[0]["Environment"] == "Production"
        
    def test_context_manager(self):
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = mock_connect.return_value
            mock_cursor = mock_connection.cursor.return_value

            with DatabaseSource("test_connection_string") as source:
                assert source.connection == mock_connection
                assert source.cursor == mock_cursor

            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()

    def test_invalid_query(self, mock_db_cursor):
        source = DatabaseSource("test_connection_string")
        source.cursor = mock_db_cursor

        with pytest.raises(ValueError):
            source.execute_query({})