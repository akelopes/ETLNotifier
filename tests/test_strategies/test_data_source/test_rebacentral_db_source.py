import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from etl_notifier.strategies.data_source.rebacentral_db_source import RebaCentralDbSource
from etl_notifier.models import RebaCentralNotificationRecord

@pytest.fixture
def sample_query_result():
    return [
        {
            "AccountName": "TeamX",
            "Environment": "Prod",
            "StartTime": datetime(2025, 6, 1, 10, 0),
            "PipelineURL": "http://example.com/pipeline",
            "errorMessage": "Something failed",
            "requiresConfirmation": True
        },
        {
            "AccountName": "TeamY",
            "Environment": "Dev",
            "StartTime": datetime(2025, 6, 1, 12, 30),
            "errorMessage": "Another error",
            "requiresConfirmation": False
        }
    ]

def test_get_records_parses_all_fields(sample_query_result):
    source = RebaCentralDbSource(connection_string="fake")

    with patch.object(source, "_execute_query", return_value=sample_query_result):
        records = source.get_records({"sql": "SELECT * FROM dummy"})

    assert isinstance(records, list)
    assert len(records) == 2
    for r in records:
        assert isinstance(r, RebaCentralNotificationRecord)

    assert records[0].account_name == "TeamX"
    assert records[0].url == "http://example.com/pipeline"
    assert records[0].requires_confirmation is True

    assert records[1].account_name == "TeamY"
    assert records[1].url is None  # Optional field not included in mock
    assert records[1].requires_confirmation is False

def test_get_records_empty_result():
    source = RebaCentralDbSource(connection_string="fake")

    with patch.object(source, "_execute_query", return_value=[]):
        records = source.get_records({"sql": "SELECT * FROM empty"})

    assert records == []
