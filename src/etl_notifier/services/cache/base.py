from abc import ABC, abstractmethod
from typing import Dict, Any

class CacheStrategy(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Any]:
        """
        Load data from the cache.
        
        Returns:
            Dictionary containing the cached data
            
        Raises:
            CacheError: If there's an error loading the cache
        """
        pass

    @abstractmethod
    def save(self, data: Dict[str, Any]) -> None:
        """
        Save data to the cache.
        
        Args:
            data: Dictionary containing the data to cache
            
        Raises:
            CacheError: If there's an error saving to the cache
        """
        pass