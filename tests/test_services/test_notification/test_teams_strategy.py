import os 
import pytest
import requests
from unittest.mock import patch
from etl_notifier.services.notification.teams_strategy import TeamsNotificationStrategy

class TestTeamsNotificationStrategy:
    @pytest.fixture
    def strategy(self, env_vars):
        return TeamsNotificationStrategy(os.environ['ETL_TEAMS_WEBHOOK_URL'])

    def test_send_notification_successful(self, strategy, mock_notification_strategy):
        with patch('requests.post') as mock_post:
            mock_response = mock_post.return_value
            mock_response.raise_for_status.return_value = None
            strategy.send_notification("Test message")
            mock_post.assert_called_once_with(
                os.environ['ETL_TEAMS_WEBHOOK_URL'],
                json=strategy._create_teams_payload("Test message")
            )
            mock_response.raise_for_status.assert_called_once()

    def test_send_notification_failed_request(self, strategy):
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.RequestException("Test error")
            with pytest.raises(requests.exceptions.RequestException):
                strategy.send_notification("Test message")

    def test_create_teams_payload_structure(self, strategy):
        message = "Test message"
        payload = strategy._create_teams_payload(message)
        assert payload["type"] == "message"
        assert len(payload["attachments"]) == 1
        assert payload["attachments"][0]["contentType"] == "application/vnd.microsoft.teams.card.o365connector"
        assert payload["attachments"][0]["content"]["$schema"] == "http://adaptivecards.io/schemas/adaptive-card.json"
        assert payload["attachments"][0]["content"]["@context"] == "http://schema.org/extensions"
        assert payload["attachments"][0]["content"]["version"] == "1.2"
        assert payload["attachments"][0]["content"]["content"] == message

    def test_send_notification_http_error(self, strategy):
        with patch('requests.post') as mock_post:
            mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
            with pytest.raises(requests.exceptions.HTTPError):
                strategy.send_notification("Test message")

    def test_webhook_url_initialization(self, env_vars):
        webhook_url = os.environ['ETL_TEAMS_WEBHOOK_URL']
        strategy = TeamsNotificationStrategy(webhook_url)
        assert strategy.webhook_url == webhook_url