from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class NotificationRecord:
    account_name: str
    environment: str
    start_time: datetime
    error_message: Optional[str] = None

    def get_unique_key(self) -> str:
        return f"{self.account_name}|{self.environment}|{self.start_time}"