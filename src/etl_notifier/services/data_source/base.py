from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DataSource(ABC):
    @abstractmethod
    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass
