import pytest
from datetime import datetime

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.formatter import NotificationFormatter


@pytest.fixture
def record():
    return NotificationRecord(
        account_name="TestAccount",
        environment="Production",
        start_time=datetime(2025, 1, 1),
        url="http://pipeline/1",
        error_message="Test error message",
    )


@pytest.fixture
def record_no_url():
    return NotificationRecord(
        account_name="TestAccount",
        environment="Production",
        start_time=datetime(2025, 1, 1),
    )


class TestNotificationFormatter:
    def test_single_notification_contains_intro(self, record):
        msg = NotificationFormatter.format_single_notification(record, "{account} {env}")
        assert NotificationFormatter.MESSAGE_INTRO in msg

    def test_single_notification_interpolates_account_and_env(self, record):
        msg = NotificationFormatter.format_single_notification(record, "{account} - {env}")
        assert "TestAccount" in msg
        assert "Production" in msg

    def test_single_notification_interpolates_error_message(self, record):
        msg = NotificationFormatter.format_single_notification(record, "{errorMessage}")
        assert "Test error message" in msg

    def test_single_notification_interpolates_url(self, record):
        msg = NotificationFormatter.format_single_notification(record, "{url}")
        assert "http://pipeline/1" in msg

    def test_single_notification_empty_string_for_none_fields(self, record_no_url):
        msg = NotificationFormatter.format_single_notification(
            record_no_url, "{url}|{errorMessage}|{over_hour}"
        )
        assert "None" not in msg
        assert "||" in msg

    def test_multiple_notifications_contains_intro_and_template(self, record):
        records = [record]
        msg = NotificationFormatter.format_multiple_notifications(records, "Multiple failures:")
        assert NotificationFormatter.MESSAGE_INTRO in msg
        assert "Multiple failures:" in msg

    def test_multiple_notifications_with_url_renders_link(self, record):
        msg = NotificationFormatter.format_multiple_notifications([record], "Header:")
        assert "[**TestAccount**" in msg
        assert "http://pipeline/1" in msg

    def test_multiple_notifications_without_url_no_link(self, record_no_url):
        msg = NotificationFormatter.format_multiple_notifications([record_no_url], "Header:")
        assert "TestAccount" in msg

    def test_multiple_notifications_lists_all_records(self, record):
        second = NotificationRecord(
            account_name="OtherAccount",
            environment="Staging",
            start_time=datetime(2025, 1, 2),
            url="http://pipeline/2",
        )
        msg = NotificationFormatter.format_multiple_notifications([record, second], "Header:")
        assert "TestAccount" in msg
        assert "OtherAccount" in msg
        assert "Production" in msg
        assert "Staging" in msg

    def test_multiple_notifications_empty_list(self):
        msg = NotificationFormatter.format_multiple_notifications([], "Header:")
        assert NotificationFormatter.MESSAGE_INTRO in msg
        assert "Header:" in msg
        assert "\n\n-" not in msg
