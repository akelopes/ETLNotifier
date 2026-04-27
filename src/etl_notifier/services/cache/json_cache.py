import json
import os
from typing import Any, Dict

from .base import CacheStrategy
from .exceptions import CacheLoadError, CacheSaveError


class JsonFileCache(CacheStrategy):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.file_path):
                with open(self.file_path, "w+") as f:
                    json.dump({}, f)
                return {}
            with open(self.file_path, "r+") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise CacheLoadError(f"Invalid JSON in cache file: {e}")
        except Exception as e:
            raise CacheLoadError(f"Error loading cache: {e}")

    def save(self, data: Dict[str, Any]) -> None:
        try:
            with open(self.file_path, "w+") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            raise CacheSaveError(f"Error saving to cache: {e}")
