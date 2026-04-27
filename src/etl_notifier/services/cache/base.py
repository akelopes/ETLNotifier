from abc import ABC, abstractmethod
from typing import Any, Dict


class CacheStrategy(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def save(self, data: Dict[str, Any]) -> None:
        pass
