import requests
from typing import Dict, Any

from .strategy import NotificationStrategy

class TeamsNotificationStrategy(NotificationStrategy):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_notification(self, message: str) -> None:
        """
        Send a notification to Microsoft Teams via webhook.
        
        Args:
            message: The message to send to Teams
            
        Raises:
            requests.exceptions.RequestException: If the request to Teams fails
        """
        payload = self._create_teams_payload(message)
        response = requests.post(self.webhook_url, json=payload)
        response.raise_for_status()

    def _create_teams_payload(self, message: str) -> Dict[str, Any]:
        """
        Create the Teams message card payload.
        
        Args:
            message: The message to include in the Teams card
            
        Returns:
            Dict containing the Teams message card payload
        """
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.teams.card.o365connector",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "@context": "http://schema.org/extensions",
                        "version": "1.2",
                        "content": message,
                    },
                }
            ],
        }