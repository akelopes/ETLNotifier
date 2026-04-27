from typing import Any, Dict

import requests

from .strategy import NotificationStrategy


class TeamsNotificationStrategy(NotificationStrategy):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_notification(self, message: str) -> None:
        response = requests.post(self.webhook_url, json=self._build_payload(message))
        response.raise_for_status()

    def _build_payload(self, message: str) -> Dict[str, Any]:
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
