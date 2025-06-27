from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from etl_notifier.models import NotificationRecord


@dataclass
class RebaCentralNotificationRecord(NotificationRecord):
    account_name: str
    environment: str
    start_time: datetime
    url: Optional[str] = None
    error_message: Optional[str] = None

    def get_unique_key(self) -> str:
        return f"{self.account_name}|{self.environment}|{self.start_time}"
