from etl_notifier.strategies.cache import JsonFileCache
import json
import pytest
import os


@pytest.fixture
def cache_file():
    return "cache.json"

def test_load_returns_empty_if_file_does_not_exist():
    cache = JsonFileCache()
    result = cache.load("nonexistent_key")

    assert result == {}

def test_load_returns_query_cache_if_exists(cache_file):
    data = {
        "query_a": {
            "key1": "confirmed",
            "key2": "pending"
        },
        "query_b": {
            "keyX": "pending"
        }
    }

    with open(cache_file, "w+") as f:
        json.dump(data, f)

    cache = JsonFileCache()
    result = cache.load("query_a")

    assert result == {"key1": "confirmed", "key2": "pending"}

def test_save_overwrites_existing_keys(cache_file):
    
    initial_data = {
        "query": {"k": "pending"}
    }

    with open(cache_file, "w+") as f:
        json.dump(initial_data, f)

    cache = JsonFileCache()
    updated = {
        "query": {
            "k": "confirmed"
        }
    }

    cache.save(updated)

    final_data = cache.load("query")

    assert final_data["k"] == "confirmed"

def test_save_creates_file_if_not_exists(monkeypatch):
    cache_file = "tmp.json"
    monkeypatch.setenv("ETL_CACHE_FILE", str(cache_file))
    
    assert not os.path.isfile(cache_file)

    cache = JsonFileCache()
    cache.save({"query": {"k1": "confirmed"}})

    assert os.path.isfile(cache_file)

    with open(cache_file, "r") as f:
        data = json.load(f)
        assert "query" in data
        assert data["query"]["k1"] == "confirmed"
        
    os.remove(cache_file)