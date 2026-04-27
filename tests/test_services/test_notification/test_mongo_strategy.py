import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.notification.mongo_strategy import MongoNotificationStrategy


@pytest.fixture
def mock_col():
    with patch("etl_notifier.services.notification.mongo_strategy.MongoClient") as mock_client:
        col = MagicMock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = col
        yield col


@pytest.fixture
def strategy(mock_col):
    return MongoNotificationStrategy("mongodb://test", "CHUCK", "queue")


@pytest.fixture
def record():
    return NotificationRecord(
        account_name="TestAccount",
        environment="Prod",
        start_time=datetime(2026, 4, 27),
        run_id="abc-123-run-id",
    )


class TestMongoNotificationStrategy:
    def test_send_notification_inserts_one_doc_per_record(self, strategy, mock_col, record):
        second = NotificationRecord("Other", "UAT", datetime(2026, 4, 27), run_id="xyz-456")
        strategy.send_notification([record, second], "", "")
        mock_col.insert_many.assert_called_once()
        docs = mock_col.insert_many.call_args[0][0]
        assert len(docs) == 2

    def test_doc_shape(self, strategy, mock_col, record):
        strategy.send_notification([record], "", "")
        doc = mock_col.insert_many.call_args[0][0][0]
        assert doc["type"] == "ETLFailure"
        assert doc["source"] == "etlnotifier"
        assert doc["status"] == "completed"
        assert doc["prompt"] == "Use /etl-triage to analyze ADF failure abc-123-run-id for TestAccount (Prod)"
        for field in ("id", "handoffId"):
            uuid.UUID(doc[field])  # raises if not a valid UUID

    def test_timestamps_are_utc_datetimes(self, strategy, mock_col, record):
        strategy.send_notification([record], "", "")
        doc = mock_col.insert_many.call_args[0][0][0]
        for field in ("createdAt", "startedAt", "completedAt"):
            assert isinstance(doc[field], datetime)
            assert doc[field].tzinfo == timezone.utc

    def test_id_and_handoff_are_unique_per_doc(self, strategy, mock_col, record):
        second = NotificationRecord("Other", "UAT", datetime(2026, 4, 27), run_id="xyz-456")
        strategy.send_notification([record, second], "", "")
        docs = mock_col.insert_many.call_args[0][0]
        assert docs[0]["id"] != docs[1]["id"]
        assert docs[0]["handoffId"] != docs[1]["handoffId"]

    def test_send_notification_propagates_insert_error(self, strategy, mock_col, record):
        mock_col.insert_many.side_effect = Exception("connection refused")
        with pytest.raises(Exception, match="connection refused"):
            strategy.send_notification([record], "", "")
