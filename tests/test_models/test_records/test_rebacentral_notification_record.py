import pytest
from datetime import datetime
from etl_notifier.models import RebaCentralNotificationRecord


@pytest.fixture
def sample_record():
    return RebaCentralNotificationRecord(
        account_name="TestAccount",
        environment="Test",
        start_time=datetime(2025, 1, 1),
        url="http://test-url",
        error_message="Test error",
        requires_confirmation=True,
    )


def test_rebacentral_notification_record_properties(sample_record):

    assert sample_record.account_name == "TestAccount"
    assert sample_record.environment == "Test"
    assert sample_record.url == "http://test-url"
    assert sample_record.error_message == "Test error"
    assert sample_record.requires_confirmation is True
    assert isinstance(sample_record.start_time, datetime)


def test_rebacentral_notification_record_get_unique_key(sample_record):

    expected_key = "TestAccount|Test|2025-01-01 00:00:00"
    assert sample_record.get_unique_key() == expected_key


def test_rebacentral_notification_record_asdict(sample_record):
    expected_dict = {
        "account_name": "TestAccount",
        "environment": "Test",
        "start_time": datetime(2025, 1, 1),
        "url": "http://test-url",
        "error_message": "Test error",
        "requires_confirmation": True,
    }
    assert sample_record.asdict() == expected_dict
