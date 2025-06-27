from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def get_records(self) -> None:
        pass
