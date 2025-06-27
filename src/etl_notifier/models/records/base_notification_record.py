from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
from typing import Dict


@dataclass(kw_only=True)
class NotificationRecord(ABC):
    requires_confirmation: bool = False

    @abstractmethod
    def get_unique_key(self) -> str:
        pass

    def asdict(self) -> Dict:
        return asdict(self)
