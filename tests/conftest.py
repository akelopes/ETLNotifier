import os
import sys
import pytest
from datetime import datetime
import json
import pyodbc
from unittest.mock import Mock, MagicMock

# Add the src directory to the Python path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, src_path)

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.strategy import NotificationStrategy
from etl_notifier.services.data_source.database import DatabaseSource

@pytest.fixture
def sample_notification_record():
    """Create a sample notification record for testing"""
    return NotificationRecord(
        account_name="TestAccount",
        environment="Production",
        start_time=datetime(2025, 1, 1),
        error_message="Test error message"
    )

@pytest.fixture
def mock_cache_file(tmp_path):
    """Create a temporary cache file"""
    cache_file = tmp_path / "test_cache.json"
    cache_file.write_text("{}")
    return str(cache_file)

@pytest.fixture
def mock_queries_file(tmp_path):
    """Create a sample queries configuration file"""
    queries_file = tmp_path / "test_queries.yml"
    queries_content = """
notification:
    type: teams
    webhook_url: ${ETL_TEAMS_WEBHOOK_URL}

sources:
    database:
        type: database
        connection_string: ${ETL_DB_CONNECTION_STRING}

queries:
    test_db_query:
        source: database
        query:
            sql: "SELECT * FROM test_table"
        message_single: "Test message for {} in {}: {}"
        message_multiple: "Multiple test messages"
"""
    queries_file.write_text(queries_content)
    return str(queries_file)

@pytest.fixture
def mock_notification_strategy():
    """Create a mock notification strategy"""
    class MockNotificationStrategy(NotificationStrategy):
        def __init__(self):
            self.sent_messages = []

        def send_notification(self, message: str) -> None:
            self.sent_messages.append(message)

    return MockNotificationStrategy()

@pytest.fixture
def mock_db_cursor():
    """Create a mock database cursor"""
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.description = [
        ("AccountName", str),
        ("Environment", str),
        ("StartTime", datetime),
        ("errorMessage", str)
    ]
    cursor.fetchall.return_value = [
        ("TestAccount", "Production", datetime(2025, 1, 1), "Test error")
    ]
    return cursor

@pytest.fixture
def mock_db_connection():
    """Create a mock database connection"""
    connection = MagicMock(spec=pyodbc.Connection)
    connection.cursor.return_value = mock_db_cursor()
    return connection

@pytest.fixture
def mock_data_source():
    """Create a mock data source"""
    class MockDataSource(DatabaseSource):
        def __init__(self):
            self.executed_queries = []

        def execute_query(self, query):
            self.executed_queries.append(query)
            return [{
                "AccountName": "TestAccount",
                "Environment": "Production",
                "StartTime": datetime(2025, 1, 1),
                "errorMessage": "Test error"
            }]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    return MockDataSource()

@pytest.fixture
def env_vars():
    """Set up environment variables for testing"""
    original_environ = dict(os.environ)
    os.environ.update({
        'ETL_TEAMS_WEBHOOK_URL': 'http://test-webhook.url',
        'ETL_DB_CONNECTION_STRING': 'test-connection-string',
        'ETL_CACHE_TYPE': 'memory',
        'ETL_CACHE_FILE': 'test-cache.json',
        'ETL_SLEEP_TIME': '300'
    })
    yield
    os.environ.clear()
    os.environ.update(original_environ)