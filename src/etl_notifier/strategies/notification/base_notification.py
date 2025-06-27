from abc import ABC, abstractmethod


class Notification(ABC):

    @abstractmethod
    def __init__(self):
        """
        Init should be done without parameters, any parameters should be collected from os.getenv() or from a config file.
        """
        pass

    @abstractmethod
    def send_notification(self, message: str) -> None:
        """
        Send a notification using the implemented strategy.

        Args:
            message: The message to send

        Raises:
            NotImplementedError: If the strategy hasn't implemented this method
        """
        pass
