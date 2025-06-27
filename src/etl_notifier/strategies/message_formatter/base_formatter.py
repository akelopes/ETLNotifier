from typing import List
from abc import ABC, abstractmethod
from etl_notifier.models import NotificationRecord


class Formatter(ABC):

    @classmethod
    @abstractmethod
    def format_single_message(cls, record: NotificationRecord):
        pass

    @classmethod
    @abstractmethod
    def format_multiple_messages(cls, records: List[NotificationRecord]):
        pass
