import json
import os
import pytest

from etl_notifier.services.cache.json_cache import JsonFileCache
from etl_notifier.services.cache.exceptions import CacheLoadError, CacheSaveError


class TestJsonFileCache:
    @pytest.fixture
    def cache_path(self, tmp_path):
        return str(tmp_path / "test_cache.json")

    @pytest.fixture
    def cache(self, cache_path):
        return JsonFileCache(cache_path)

    def test_load_creates_file_if_missing(self, cache, cache_path):
        data = cache.load()
        assert data == {}
        assert os.path.exists(cache_path)

    def test_load_existing_cache(self, cache, cache_path):
        expected = {"query": {"key1": "confirmed", "key2": "pending"}}
        with open(cache_path, "w") as f:
            json.dump(expected, f)
        assert cache.load() == expected

    def test_load_invalid_json_raises(self, cache, cache_path):
        with open(cache_path, "w") as f:
            f.write("not valid json {")
        with pytest.raises(CacheLoadError, match="Invalid JSON in cache file"):
            cache.load()

    def test_save_writes_data(self, cache, cache_path):
        data = {"query": {"key1": "confirmed"}}
        cache.save(data)
        with open(cache_path, "r") as f:
            assert json.load(f) == data

    def test_save_overwrites_existing(self, cache, cache_path):
        with open(cache_path, "w") as f:
            json.dump({"old": "data"}, f)
        cache.save({"new": "data"})
        with open(cache_path, "r") as f:
            assert json.load(f) == {"new": "data"}

    def test_save_permission_error_raises(self, cache, monkeypatch):
        monkeypatch.setattr("builtins.open", lambda *a, **kw: (_ for _ in ()).throw(PermissionError("denied")))
        with pytest.raises(CacheSaveError, match="Error saving to cache"):
            cache.save({"key": "value"})
