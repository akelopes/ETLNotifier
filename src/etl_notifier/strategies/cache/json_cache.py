import json
import os
from typing import Dict, Any

from etl_notifier.strategies.cache import Cache, CacheLoadError, CacheSaveError

class JsonFileCache(Cache):
    def __init__(self):
        self.file_path = os.getenv("ETL_CACHE_FILE", "cache.json")

    def load(self, cache_key: str = None) -> Dict[str, Any]:
        """
        Load data from JSON file cache.

        Returns:
            Dictionary containing the cached data

        Raises:
            CacheLoadError: If there's an error loading the cache
        """
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, "r+") as f:
                    return json.load(f).get(cache_key, {})
            return {}
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
