from typing import List
from etl_notifier.models import NotificationRecord


class MarkdownFormatter:
    MESSAGE_INTRO = "\r **[ETL Notifier]** [Automated Message] \n\n"

    @classmethod
    def format_single_message(cls, record: NotificationRecord, template: str) -> str:
        """
        Format a single notification message.

        Args:
            record: The notification record to format
            template: The message template to use

        Returns:
            Formatted message string
        """
        message = cls.MESSAGE_INTRO

        return message + template.format_map(record.asdict())

    @classmethod
    def format_multiple_messages(
        cls,
        records: List[NotificationRecord],
        lines_template: str,
        intro_template: str = "",
    ) -> str:
        """
        Format a message for multiple notifications.

        Args:
            records: List of notification records to format
            template: The message template to use

        Returns:
            Formatted message string containing all notifications
        """
        message = cls.MESSAGE_INTRO + intro_template
        lines = []
        for record in records:
            lines.append(lines_template.format_map(record.asdict()))

        return message + "".join(lines)
