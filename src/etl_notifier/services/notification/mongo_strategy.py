import uuid
from datetime import datetime, timezone
from typing import List, Optional

from pymongo import MongoClient

from ...models.notification_record import NotificationRecord
from .strategy import NotificationStrategy


class MongoNotificationStrategy(NotificationStrategy):
    def __init__(self, connection_string: str, database: str, collection: str, environments: Optional[List[str]] = None):
        self._client = MongoClient(connection_string)
        self._col = self._client[database][collection]
        self._environments = [e.lower() for e in environments] if environments is not None else None

    def send_notification(
        self,
        records: List[NotificationRecord],
        template_single: str,
        template_multiple: str,
    ) -> None:
        filtered = [r for r in records if not self._environments or r.environment.lower() in self._environments]
        if not filtered:
            return
        now = datetime.now(timezone.utc)
        self._col.insert_many([self._build_doc(r, now) for r in filtered])

    def _build_doc(self, record: NotificationRecord, now: datetime) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "type": "ETLFailure",
            "prompt": f"Use /etl-triage to analyze ADF failure {record.run_id} for {record.account_name} ({record.environment})",
            "status": "completed",
            "createdAt": now,
            "source": "etlnotifier",
            "startedAt": now,
            "completedAt": now,
            "handoffId": str(uuid.uuid4()),
        }
