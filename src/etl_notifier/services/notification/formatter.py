from typing import List
from etl_notifier.models.notification_record import NotificationRecord

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
        if '{' not in template:
            return cls.MESSAGE_INTRO + template
        
        message = cls.MESSAGE_INTRO
        format_args = {
            'account': record.account_name,
            'env': record.environment,
            'url': record.url ,
            'errorMessage': record.error_message or ''
        }

        return message + template.format_map(format_args)

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
            if record.url:
                lines.append(
                    f" \n\n- [**{record.account_name}**: **{record.environment}**]({record.url})"
                )
            else: 
                lines.append(
                    f" \n\n- **{record.account_name}**: **{record.environment}**"
                )
        return message + "".join(lines)