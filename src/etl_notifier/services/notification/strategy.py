from abc import ABC, abstractmethod
from typing import List

from ...models.notification_record import NotificationRecord


class NotificationStrategy(ABC):
    @abstractmethod
    def send_notification(
        self,
        records: List[NotificationRecord],
        template_single: str,
        template_multiple: str,
    ) -> None:
        pass
