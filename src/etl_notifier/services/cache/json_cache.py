import json
import os
from typing import Dict, Any

from .base import CacheStrategy
from .exceptions import CacheLoadError, CacheSaveError


class JsonFileCache(CacheStrategy):
    def __init__(self, file_path: str):
        """
        Initialize JSON file cache.

        Args:
            file_path: Path to the JSON cache file
        """
        self.file_path = file_path

    def load(self) -> Dict[str, Any]:
        """
        Load data from JSON file cache.

        Returns:
            Dictionary containing the cached data

        Raises:
            CacheLoadError: If there's an error loading the cache
        """
        try:
            if not os.path.exists(self.file_path):
                with open(self.file_path, "w+") as f:
                    json.dump({}, f)
                return {}

            with open(self.file_path, "r+") as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            raise CacheLoadError(f"Invalid JSON in cache file: {str(e)}")
        except Exception as e:
            raise CacheLoadError(f"Error loading cache: {str(e)}")

    def save(self, data: Dict[str, Any]) -> None:
        """
        Save data to JSON file cache.

        Args:
            data: Dictionary containing the data to cache

        Raises:
            CacheSaveError: If there's an error saving to the cache
        """
        try:
            with open(self.file_path, "w+") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            raise CacheSaveError(f"Error saving to cache: {str(e)}")
