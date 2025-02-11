import pytest
from datetime import datetime

from etl_notifier.models.notification_record import NotificationRecord

class TestNotificationRecord:
    def test_create_notification_record_with_error(self):
        """Test creating a notification record with an error message"""
        record = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1),
            error_message="Test error message"
        )
        
        assert record.account_name == "TestAccount"
        assert record.environment == "Production"
        assert record.start_time == datetime(2025, 1, 1)
        assert record.error_message == "Test error message"

    def test_create_notification_record_without_error(self):
        """Test creating a notification record without an error message"""
        record = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1)
        )
        
        assert record.account_name == "TestAccount"
        assert record.environment == "Production"
        assert record.start_time == datetime(2025, 1, 1)
        assert record.error_message is None

    def test_get_unique_key(self):
        """Test generating a unique key for the notification record"""
        record = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=datetime(2025, 1, 1)
        )
        
        expected_key = "TestAccount|Production|2025-01-01 00:00:00"
        assert record.get_unique_key() == expected_key

    def test_get_unique_key_with_special_characters(self):
        """Test generating a unique key with special characters in fields"""
        record = NotificationRecord(
            account_name="Test|Account",  # Contains delimiter
            environment="Prod|uction",    # Contains delimiter
            start_time=datetime(2025, 1, 1)
        )
        
        # The method should still work with special characters
        key = record.get_unique_key()
        assert isinstance(key, str)
        assert "2025-01-01 00:00:00" in key

    @pytest.mark.parametrize("account_name,environment", [
        ("", "Production"),           # Empty account name
        ("TestAccount", ""),          # Empty environment
        ("   ", "Production"),        # Whitespace account name
        ("TestAccount", "   "),       # Whitespace environment
    ])
    def test_notification_record_with_empty_values(self, account_name, environment):
        """Test creating notification records with empty or whitespace values"""
        record = NotificationRecord(
            account_name=account_name,
            environment=environment,
            start_time=datetime(2025, 1, 1)
        )
        
        # Even with empty values, should still generate a unique key
        key = record.get_unique_key()
        assert isinstance(key, str)
        assert "2025-01-01 00:00:00" in key

    def test_notification_record_equality(self):
        """Test that two notification records with same values are equal"""
        time = datetime(2025, 1, 1)
        record1 = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=time,
            error_message="Error"
        )
        record2 = NotificationRecord(
            account_name="TestAccount",
            environment="Production",
            start_time=time,
            error_message="Error"
        )
        
        assert record1 == record2

    def test_notification_record_inequality(self):
        """Test that two notification records with different values are not equal"""
        time = datetime(2025, 1, 1)
        record1 = NotificationRecord(
            account_name="TestAccount1",
            environment="Production",
            start_time=time,
            error_message="Error"
        )
        record2 = NotificationRecord(
            account_name="TestAccount2",
            environment="Production",
            start_time=time,
            error_message="Error"
        )
        
        assert record1 != record2