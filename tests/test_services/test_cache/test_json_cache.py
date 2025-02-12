import os
import pytest
import json
from etl_notifier.services.cache.json_cache import JsonFileCache
from etl_notifier.services.cache.exceptions import CacheLoadError, CacheSaveError

class TestJsonFileCache:
    @pytest.fixture
    def cache(self, mock_cache_file):
        return JsonFileCache(mock_cache_file)

    def test_load_nonexistent_cache(self, cache):
        data = cache.load()
        assert data == {}

    def test_load_valid_cache(self, cache, mock_cache_file, sample_etl_records):
        with open(mock_cache_file, "w") as f:
            json.dump([record.__dict__ for record in sample_etl_records], f)
        data = cache.load()
        assert data == [record.__dict__ for record in sample_etl_records]

    def test_load_invalid_json(self, cache, mock_cache_file):
        with open(mock_cache_file, "w") as f:
            f.write("invalid json content")
        with pytest.raises(CacheLoadError) as exc_info:
            cache.load()
        assert "Invalid JSON in cache file" in str(exc_info.value)

    def test_save_new_cache(self, cache, mock_cache_file, sample_etl_records):
        cache.save([record.__dict__ for record in sample_etl_records])
        with open(mock_cache_file, "r") as f:
            saved_data = json.load(f)
        assert saved_data == [record.__dict__ for record in sample_etl_records]

    def test_save_existing_cache(self, cache, mock_cache_file, sample_etl_records):
        initial_data = {"initial": "data"}
        with open(mock_cache_file, "w") as f:
            json.dump(initial_data, f)
        cache.save([record.__dict__ for record in sample_etl_records])
        with open(mock_cache_file, "r") as f:
            saved_data = json.load(f)
        assert saved_data == [record.__dict__ for record in sample_etl_records]

    def test_save_to_nonexistent_directory(self, tmp_path, sample_etl_records):
        deep_path = tmp_path / "deep" / "nested" / "path"
        cache = JsonFileCache(str(deep_path / "cache.json"))
        cache.save([record.__dict__ for record in sample_etl_records])
        with open(str(deep_path / "cache.json"), "r") as f:
            saved_data = json.load(f)
        assert saved_data == [record.__dict__ for record in sample_etl_records]

    def test_save_with_permission_error(self, cache, mock_cache_file, sample_etl_records, monkeypatch):
        def mock_open(*args, **kwargs):
            raise PermissionError("Permission denied")
        monkeypatch.setattr("builtins.open", mock_open)
        with pytest.raises(CacheSaveError) as exc_info:
            cache.save([record.__dict__ for record in sample_etl_records])
        assert "Error saving to cache" in str(exc_info.value)