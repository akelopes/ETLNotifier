from abc import ABC, abstractmethod

class NotificationStrategy(ABC):
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