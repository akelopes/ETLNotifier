import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, Mock

# Native/cloud deps unavailable in dev/CI without drivers or Azure SDK installed
for _mod in ("pyodbc", "azure", "azure.identity"):
    sys.modules.setdefault(_mod, MagicMock())
import pyodbc  # noqa: E402 — must come after mock

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.cache.base import CacheStrategy
from etl_notifier.services.data_source.base import DataSource


@pytest.fixture
def sample_notification_record():
    return NotificationRecord(
        account_name="TestAccount",
        environment="Production",
        start_time=datetime(2025, 1, 1),
        error_message="Test error message",
    )


@pytest.fixture
def sample_etl_records():
    return [
        NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1),
            url="http://pipeline/1",
            error_message="Test error",
        ),
        NotificationRecord(
            account_name="AnotherAccount",
            environment="Staging",
            start_time=datetime(2025, 1, 2),
            url="http://pipeline/2",
            error_message="Another error",
        ),
    ]


@pytest.fixture
def mock_cache_strategy():
    strategy = Mock(spec=CacheStrategy)
    strategy.load.return_value = {}
    return strategy


@pytest.fixture
def mock_etl_config():
    return {
        "notifications": {
            "teams_main": {"type": "teams", "webhook_url": "http://test-webhook.url"},
        },
        "sources": {
            "database": {"type": "database", "connection_string": "test-connection-string"},
        },
        "queries": {
            "test_query": {
                "source": "database",
                "notifications": ["teams_main"],
                "query": {"sql": "SELECT * FROM test_table"},
                "message_single": "Pipeline {account} in {env}: {errorMessage}",
                "message_multiple": "Multiple issues:",
            }
        },
    }


@pytest.fixture
def mock_cache_file(tmp_path):
    cache_file = tmp_path / "test_cache.json"
    cache_file.write_text("{}")
    return str(cache_file)


@pytest.fixture
def mock_db_cursor():
    cursor = MagicMock()
    cursor.description = [
        ("AccountName", str),
        ("Environment", str),
        ("StartTime", datetime),
        ("errorMessage", str),
    ]
    cursor.fetchall.return_value = [
        ("TestAccount", "Production", datetime(2025, 1, 1), "Test error")
    ]
    return cursor


@pytest.fixture
def mock_data_source():
    class MockDataSource(DataSource):
        def __init__(self):
            self.executed_queries = []

        def execute_query(self, query):
            self.executed_queries.append(query)
            return [{
                "AccountName": "TestAccount",
                "Environment": "Production",
                "StartTime": datetime(2025, 1, 1),
                "PipelineURL": "http://pipeline/1",
                "errorMessage": "Test error",
                "over_hour": None,
            }]

        def disconnect(self):
            pass

    return MockDataSource()


@pytest.fixture
def env_vars(monkeypatch):
    monkeypatch.setenv("ETL_TEAMS_WEBHOOK_URL", "http://test-webhook.url")
    monkeypatch.setenv("ETL_DB_CONNECTION_STRING", "test-connection-string")
    monkeypatch.setenv("ETL_SLEEP_TIME", "300")
