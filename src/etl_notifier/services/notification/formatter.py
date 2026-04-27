from typing import List

from ...models.notification_record import NotificationRecord


class NotificationFormatter:
    MESSAGE_INTRO = "\r **[ETL Notifier]** [Automated Message] \n\n"

    @classmethod
    def format_single_notification(cls, record: NotificationRecord, template: str) -> str:
        return cls.MESSAGE_INTRO + template.format_map({
            "account": record.account_name,
            "env": record.environment,
            "url": record.url or "",
            "errorMessage": record.error_message or "",
            "over_hour": record.over_hour or "",
        })

    @classmethod
    def format_multiple_notifications(cls, records: List[NotificationRecord], template: str) -> str:
        lines = []
        for record in records:
            if record.url:
                lines.append(f" \n\n- [**{record.account_name}**: **{record.environment}**]({record.url})")
            else:
                lines.append(f" \n\n- **{record.account_name}**: **{record.environment}({record.url})**")
        return cls.MESSAGE_INTRO + template + "".join(lines)
