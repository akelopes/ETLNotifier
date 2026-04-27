import pytest
from datetime import datetime

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.teams_strategy import TeamsNotificationStrategy


@pytest.fixture
def strategy():
    return TeamsNotificationStrategy(webhook_url="http://test-webhook.url")


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


class TestTeamsFormatting:
    def test_single_contains_intro(self, strategy, record):
        msg = strategy._format_single(record, "{account} {env}")
        assert strategy.MESSAGE_INTRO in msg

    def test_single_interpolates_fields(self, strategy, record):
        msg = strategy._format_single(record, "{account} - {env} - {errorMessage} - {url}")
        assert "TestAccount" in msg
        assert "Production" in msg
        assert "Test error message" in msg
        assert "http://pipeline/1" in msg

    def test_single_empty_string_for_none_fields(self, strategy, record_no_url):
        msg = strategy._format_single(record_no_url, "{url}|{errorMessage}|{over_hour}")
        assert "None" not in msg
        assert "||" in msg

    def test_multiple_contains_intro_and_template(self, strategy, record):
        msg = strategy._format_multiple([record], "Multiple failures:")
        assert strategy.MESSAGE_INTRO in msg
        assert "Multiple failures:" in msg

    def test_multiple_with_url_renders_link(self, strategy, record):
        msg = strategy._format_multiple([record], "Header:")
        assert "[**TestAccount**" in msg
        assert "http://pipeline/1" in msg

    def test_multiple_without_url_no_link(self, strategy, record_no_url):
        msg = strategy._format_multiple([record_no_url], "Header:")
        assert "TestAccount" in msg

    def test_multiple_lists_all_records(self, strategy, record):
        second = NotificationRecord(
            account_name="OtherAccount",
            environment="Staging",
            start_time=datetime(2025, 1, 2),
            url="http://pipeline/2",
        )
        msg = strategy._format_multiple([record, second], "Header:")
        assert "TestAccount" in msg
        assert "OtherAccount" in msg
        assert "Production" in msg
        assert "Staging" in msg

    def test_multiple_empty_list(self, strategy):
        msg = strategy._format_multiple([], "Header:")
        assert strategy.MESSAGE_INTRO in msg
        assert "Header:" in msg
        assert "\n\n-" not in msg

    def test_format_routes_to_single_for_one_record(self, strategy, record):
        msg = strategy._format([record], "{account}", "multiple")
        assert "TestAccount" in msg
        assert "multiple" not in msg

    def test_format_routes_to_multiple_for_many_records(self, strategy, record):
        second = NotificationRecord("Other", "Staging", datetime(2025, 1, 2), url="http://x")
        msg = strategy._format([record, second], "single", "Multiple:")
        assert "Multiple:" in msg
        assert "single" not in msg
