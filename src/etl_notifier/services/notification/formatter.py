from typing import List
from ...models.notification_record import NotificationRecord

class NotificationFormatter:
    MESSAGE_INTRO = "\r **[ETL Notifier]** [Automated Message] \n\n"

    @classmethod
    def format_single_notification(cls, record: NotificationRecord, template: str) -> str:
        """
        Format a single notification message.
        
        Args:
            record: The notification record to format
            template: The message template to use
            
        Returns:
            Formatted message string
        """
        message = cls.MESSAGE_INTRO
        if record.error_message:
            return message + template.format(
                record.account_name, record.environment, record.error_message
            )
        return message + template.format(record.account_name, record.environment)

    @classmethod
    def format_multiple_notifications(
        cls, records: List[NotificationRecord], template: str
    ) -> str:
        """
        Format a message for multiple notifications.
        
        Args:
            records: List of notification records to format
            template: The message template to use
            
        Returns:
            Formatted message string containing all notifications
        """
        message = cls.MESSAGE_INTRO + template
        lines = []
        for record in records:
            lines.append(
                f" \n\n- **{record.account_name}**: **{record.environment}**"
            )
        return message + "".join(lines)