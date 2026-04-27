import pytest
from unittest.mock import patch

from etl_notifier.services.data_source.database import DatabaseSource


class TestDatabaseSource:
    @pytest.fixture
    def source(self, mock_db_cursor):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.return_value.cursor.return_value = mock_db_cursor
            yield DatabaseSource("test_connection_string")

    def test_execute_query_returns_row_dicts(self, source, mock_db_cursor):
        source.cursor = mock_db_cursor
        results = source.execute_query({"sql": "SELECT * FROM test_table"})
        assert len(results) == 1
        assert results[0]["AccountName"] == "TestAccount"
        assert results[0]["Environment"] == "Production"

    def test_execute_query_missing_sql_raises(self, source, mock_db_cursor):
        source.cursor = mock_db_cursor
        with pytest.raises(ValueError):
            source.execute_query({})

    def test_context_manager_disconnects_on_exit(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connection = mock_connect.return_value
            mock_cursor = mock_connection.cursor.return_value
            with DatabaseSource("test_connection_string") as source:
                assert source.connection == mock_connection
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
