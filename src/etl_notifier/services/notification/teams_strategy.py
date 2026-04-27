from typing import Any, Dict, List

import requests

from ...models.notification_record import NotificationRecord
from .strategy import NotificationStrategy


class TeamsNotificationStrategy(NotificationStrategy):
    MESSAGE_INTRO = "\r **[ETL Notifier]** [Automated Message] \n\n"

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_notification(
        self,
        records: List[NotificationRecord],
        template_single: str,
        template_multiple: str,
    ) -> None:
        message = self._format(records, template_single, template_multiple)
        response = requests.post(self.webhook_url, json=self._build_payload(message))
        response.raise_for_status()

    def _format(self, records: List[NotificationRecord], template_single: str, template_multiple: str) -> str:
        if len(records) == 1:
            return self._format_single(records[0], template_single)
        return self._format_multiple(records, template_multiple)

    def _format_single(self, record: NotificationRecord, template: str) -> str:
        return self.MESSAGE_INTRO + template.format_map({
            "account": record.account_name,
            "env": record.environment,
            "url": record.url or "",
            "errorMessage": record.error_message or "",
            "over_hour": record.over_hour or "",
        })

    def _format_multiple(self, records: List[NotificationRecord], template: str) -> str:
        lines = []
        for record in records:
            if record.url:
                lines.append(f" \n\n- [**{record.account_name}**: **{record.environment}**]({record.url})")
            else:
                lines.append(f" \n\n- **{record.account_name}**: **{record.environment}({record.url})**")
        return self.MESSAGE_INTRO + template + "".join(lines)

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
