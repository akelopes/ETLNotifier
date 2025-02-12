import pytest
from unittest.mock import patch, Mock
from etl_notifier.services.data_source.database import DatabaseSource

class TestDatabaseSource:
    @pytest.fixture
    def mock_db_cursor(self):
        cursor = Mock()
        cursor.description = [('AccountName',), ('Environment',)]
        cursor.fetchall.return_value = [('TestAccount', 'Production')]
        return cursor

    def test_execute_query(self, mock_db_cursor):
        with patch('pyodbc.connect') as mock_connect:
            mock_connect.return_value.cursor.return_value = mock_db_cursor
            source = DatabaseSource("test_connection_string")
            query = {"sql": "SELECT * FROM test_table"}
            results = source.execute_query(query)
            
            assert len(results) == 1
            assert results[0]["AccountName"] == "TestAccount"
            assert results[0]["Environment"] == "Production"
            mock_db_cursor.execute.assert_called_once_with("SELECT * FROM test_table")

    def test_connect(self):
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = mock_connect.return_value
            mock_cursor = mock_connection.cursor.return_value
            
            source = DatabaseSource("test_connection_string")
            
            mock_connect.assert_called_once_with("test_connection_string")
            assert source.connection == mock_connection
            assert source.cursor == mock_cursor

    def test_disconnect(self):
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = mock_connect.return_value
            mock_cursor = mock_connection.cursor.return_value
            
            source = DatabaseSource("test_connection_string")
            source.disconnect()
            
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
            assert source.cursor is None
            assert source.connection is None


    def test_invalid_query(self, mock_db_cursor):
        with patch('pyodbc.connect') as mock_connect:
            mock_connect.return_value.cursor.return_value = mock_db_cursor
            source = DatabaseSource("test_connection_string")
            
            with pytest.raises(ValueError):
                source.execute_query({})
            
    def test_reconnect(self):
        with patch('pyodbc.connect') as mock_connect:
            source = DatabaseSource("test_connection_string")
            mock_connect.reset_mock()
            
            source.connect()
            mock_connect.assert_not_called()