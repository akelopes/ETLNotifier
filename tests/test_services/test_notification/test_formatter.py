import pytest
from datetime import datetime

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.formatter import NotificationFormatter

class TestNotificationFormatter:
    @pytest.fixture
    def sample_record(self):
        """Create a sample notification record for testing"""
        return NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1),
            error_message="Test error message"
        )

    def test_format_single_notification_with_error(self, sample_record):
        """Test formatting a single notification with error message"""
        template = "Error in {} - {}: {}"
        message = NotificationFormatter.format_single_notification(
            sample_record, template
        )

        # Verify message contains intro
        assert NotificationFormatter.MESSAGE_INTRO in message
        
        # Verify message contains record information
        assert "TestAccount" in message
        assert "Production" in message
        assert "Test error message" in message

    def test_format_single_notification_without_error(self):
        """Test formatting a single notification without error message"""
        record = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1)
        )
        template = "Status for {} in {}"
        
        message = NotificationFormatter.format_single_notification(
            record, template
        )

        # Verify message structure
        assert NotificationFormatter.MESSAGE_INTRO in message
        assert "TestAccount" in message
        assert "Production" in message
        assert "None" not in message  # Should not include None in message

    def test_format_multiple_notifications(self, sample_record):
        """Test formatting multiple notifications"""
        records = [
            sample_record,
            NotificationRecord(
                account_name="AnotherAccount",
                environment="Staging",
                start_time=datetime(2025, 1, 1),
                error_message="Another error"
            )
        ]
        template = "Multiple issues detected:"

        message = NotificationFormatter.format_multiple_notifications(
            records, template
        )

        # Verify message structure
        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert "TestAccount" in message
        assert "AnotherAccount" in message
        assert "Production" in message
        assert "Staging" in message
        
        # Verify formatting
        assert message.count("**") >= 8  # Should have bold formatting

    def test_format_multiple_notifications_single_record(self, sample_record):
        """Test formatting multiple notifications with single record"""
        records = [sample_record]
        template = "Multiple issues detected:"

        message = NotificationFormatter.format_multiple_notifications(
            records, template
        )

        # Verify message structure
        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert "TestAccount" in message
        assert "Production" in message
        assert message.count("**") >= 4  # Should have bold formatting

    def test_format_multiple_notifications_empty_list(self):
        """Test formatting multiple notifications with empty list"""
        records = []
        template = "Multiple issues detected:"

        message = NotificationFormatter.format_multiple_notifications(
            records, template
        )

        # Verify basic message structure
        assert NotificationFormatter.MESSAGE_INTRO in message
        assert template in message
        assert message.count("\n\n-") == 0  # Should not have any list items

    @pytest.mark.parametrize("template", [
        "Error in {} - {}: {}",           # Standard template
        "{} has error in {}: {}",         # Different order
        "Issue: {} {} {}",                # Minimal formatting
        "** {} ** encountered in {} : {}"  # With markdown
    ])
    def test_format_single_notification_different_templates(self, sample_record, template):
        """Test formatting with different template strings"""
        message = NotificationFormatter.format_single_notification(
            sample_record, template
        )

        # Verify all components are present regardless of template
        assert NotificationFormatter.MESSAGE_INTRO in message
        assert "TestAccount" in message
        assert "Production" in message
        assert "Test error message" in message

    def test_format_preserves_markdown(self, sample_record):
        """Test that markdown formatting in templates is preserved"""
        template = "**Error** in **{}** - *{}*: {}"
        message = NotificationFormatter.format_single_notification(
            sample_record, template
        )

        # Verify markdown is preserved
        assert "**Error**" in message
        assert "*Production*" in message