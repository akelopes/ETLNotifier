import pytest
import requests
from datetime import datetime
from unittest.mock import Mock, patch

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.teams_strategy import TeamsNotificationStrategy


@pytest.fixture
def strategy():
    return TeamsNotificationStrategy(webhook_url="http://test-webhook.url")


@pytest.fixture
def records():
    return [
        NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1),
            url="http://pipeline/1",
            error_message="Test error",
        )
    ]


class TestTeamsNotificationStrategy:
    def test_send_notification_posts_to_webhook(self, strategy, records):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = Mock()
            strategy.send_notification(records, "{account} {env}", "Multiple:")
            mock_post.assert_called_once()
            assert mock_post.call_args[0][0] == "http://test-webhook.url"

    def test_send_notification_calls_raise_for_status(self, strategy, records):
        with patch("requests.post") as mock_post:
            strategy.send_notification(records, "{account}", "Multiple:")
            mock_post.return_value.raise_for_status.assert_called_once()

    def test_send_notification_raises_on_http_error(self, strategy, records):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
            with pytest.raises(requests.exceptions.HTTPError):
                strategy.send_notification(records, "{account}", "Multiple:")

    def test_build_payload_structure(self, strategy):
        payload = strategy._build_payload("Test message")
        assert payload["type"] == "message"
        assert len(payload["attachments"]) == 1
        attachment = payload["attachments"][0]
        assert attachment["contentType"] == "application/vnd.microsoft.teams.card.o365connector"
        assert attachment["content"]["content"] == "Test message"
