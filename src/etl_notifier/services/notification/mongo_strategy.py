import uuid
from datetime import datetime, timezone
from typing import List

from pymongo import MongoClient

from ...models.notification_record import NotificationRecord
from .strategy import NotificationStrategy


class MongoNotificationStrategy(NotificationStrategy):
    def __init__(self, connection_string: str, database: str, collection: str):
        self._client = MongoClient(connection_string)
        self._col = self._client[database][collection]

    def send_notification(
        self,
        records: List[NotificationRecord],
        template_single: str,
        template_multiple: str,
    ) -> None:
        now = datetime.now(timezone.utc)
        self._col.insert_many([self._build_doc(r, now) for r in records])

    def _build_doc(self, record: NotificationRecord, now: datetime) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "type": "ETLFailure",
            "prompt": f"Use etl-triage to analyze ADF failure {record.run_id}",
            "status": "completed",
            "createdAt": now,
            "source": "etlnotifier",
            "startedAt": now,
            "completedAt": now,
            "handoffId": str(uuid.uuid4()),
        }
