import pytest
from datetime import datetime
from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.formatter import NotificationFormatter

class TestNotificationFormatter:
    def test_format_single_notification_with_error(self, sample_etl_records):
        template = "Error in {} - {}: {}"
        message = NotificationFormatter.format_single_notification(
            sample_etl_records[0], template
        )

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert "TestAccount1" in message
        assert "Prod" in message
        assert "Test error 1" in message

    def test_format_single_notification_without_error(self):
        record = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1)
        )
        template = "Status for {} in {}"
        message = NotificationFormatter.format_single_notification(record, template)

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert "TestAccount" in message
        assert "Production" in message
        assert "None" not in message

    def test_format_multiple_notifications(self, sample_etl_records):
        template = "Multiple issues detected:"
        message = NotificationFormatter.format_multiple_notifications(
            sample_etl_records, template
        )

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert "TestAccount1" in message
        assert "TestAccount2" in message
        assert "Prod" in message
        assert "Dev" in message
        assert message.count("**") >= 8

    def test_format_multiple_notifications_single_record(self, sample_etl_records):
        template = "Multiple issues detected:"
        message = NotificationFormatter.format_multiple_notifications(
            [sample_etl_records[0]], template
        )

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert "TestAccount1" in message
        assert "Prod" in message
        assert message.count("**") >= 4

    def test_format_multiple_notifications_empty_list(self):
        template = "Multiple issues detected:"
        message = NotificationFormatter.format_multiple_notifications([], template)

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert message.count("\n\n-") == 0

    @pytest.mark.parametrize("template", [
        "Error in {} - {}: {}",
        "{} has error in {}: {}",
        "Issue: {} {} {}",
        "** {} ** encountered in {} : {}"
    ])
    def test_format_single_notification_different_templates(self, sample_etl_records, template):
        message = NotificationFormatter.format_single_notification(
            sample_etl_records[0], template
        )

        assert NotificationFormatter.MESSAGE_INTRO in message
        assert "TestAccount1" in message
        assert "Prod" in message
        assert "Test error 1" in message

    def test_format_preserves_markdown(self, sample_etl_records):
        template = "**Error** in **{}** - *{}*: {}"
        message = NotificationFormatter.format_single_notification(
            sample_etl_records[0], template
        )

        assert "**Error**" in message
        assert "*Prod*" in message