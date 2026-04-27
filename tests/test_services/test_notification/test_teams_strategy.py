import pytest
import requests
from unittest.mock import Mock, patch

from etl_notifier.services.notification.teams_strategy import TeamsNotificationStrategy


class TestTeamsNotificationStrategy:
    @pytest.fixture
    def strategy(self):
        return TeamsNotificationStrategy(webhook_url="http://test-webhook.url")

    def test_send_notification_posts_to_webhook(self, strategy):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = Mock()
            strategy.send_notification("Test message")
            mock_post.assert_called_once_with(
                "http://test-webhook.url",
                json=strategy._build_payload("Test message"),
            )

    def test_send_notification_calls_raise_for_status(self, strategy):
        with patch("requests.post") as mock_post:
            strategy.send_notification("Test message")
            mock_post.return_value.raise_for_status.assert_called_once()

    def test_send_notification_raises_on_http_error(self, strategy):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
            with pytest.raises(requests.exceptions.HTTPError):
                strategy.send_notification("Test message")

    def test_build_payload_structure(self, strategy):
        payload = strategy._build_payload("Test message")
        assert payload["type"] == "message"
        assert len(payload["attachments"]) == 1
        attachment = payload["attachments"][0]
        assert attachment["contentType"] == "application/vnd.microsoft.teams.card.o365connector"
        assert attachment["content"]["content"] == "Test message"

    def test_build_payload_message_content(self, strategy):
        msg = "Pipeline **Acct** - **Prod** failed"
        payload = strategy._build_payload(msg)
        assert payload["attachments"][0]["content"]["content"] == msg
