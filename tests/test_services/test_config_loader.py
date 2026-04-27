import pytest
import yaml
from unittest.mock import patch

from etl_notifier.services.config_loader import ConfigLoader


VALID_CONFIG = """
notifications:
    teams_main:
        type: teams
        webhook_url: ${ETL_TEAMS_WEBHOOK_URL}

sources:
    database:
        type: database
        connection_string: ${ETL_DB_CONNECTION_STRING}

queries:
    database_failures:
        source: database
        notifications: [teams_main]
        query:
            sql: SELECT * FROM test_table
        message_single: "Pipeline {account} in {env}: {errorMessage}"
        message_multiple: "Multiple issues"
"""


class TestConfigLoader:
    @pytest.fixture
    def valid_config_file(self, tmp_path):
        f = tmp_path / "valid_config.yml"
        f.write_text(VALID_CONFIG)
        return str(f)

    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("ETL_TEAMS_WEBHOOK_URL", "http://test-webhook.url")
        monkeypatch.setenv("ETL_DB_CONNECTION_STRING", "test-connection-string")

    def test_load_valid_config(self, valid_config_file):
        config = ConfigLoader.load_queries(valid_config_file)
        assert config["notifications"]["teams_main"]["type"] == "teams"
        assert config["notifications"]["teams_main"]["webhook_url"] == "http://test-webhook.url"
        assert config["sources"]["database"]["connection_string"] == "test-connection-string"
        assert "database_failures" in config["queries"]

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_queries("nonexistent.yml")

    def test_load_invalid_yaml(self, tmp_path):
        f = tmp_path / "invalid.yml"
        f.write_text("this is not valid:\n    - yaml: [content")
        with pytest.raises(yaml.YAMLError):
            ConfigLoader.load_queries(str(f))

    @pytest.mark.parametrize("content,match", [
        # Missing notifications section
        ("""
sources:
    db:
        type: database
queries:
    q:
        source: db
        notifications: []
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""", "notifications"),
        # Missing sources section
        ("""
notifications:
    t:
        type: teams
        webhook_url: test
queries:
    q:
        source: db
        notifications: [t]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""", "sources"),
        # Missing queries section
        ("""
notifications:
    t:
        type: teams
        webhook_url: test
sources:
    db:
        type: database
""", "queries"),
    ])
    def test_missing_required_sections(self, tmp_path, content, match):
        f = tmp_path / "config.yml"
        f.write_text(content)
        with pytest.raises(ValueError, match=match):
            ConfigLoader.load_queries(str(f))

    def test_missing_notification_sink_type(self, tmp_path):
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    teams_main:
        webhook_url: test
sources:
    db:
        type: database
queries:
    q:
        source: db
        notifications: [teams_main]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        with pytest.raises(ValueError, match="must specify a 'type'"):
            ConfigLoader.load_queries(str(f))

    def test_missing_source_type(self, tmp_path):
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    t:
        type: teams
        webhook_url: test
sources:
    db:
        connection_string: test
queries:
    q:
        source: db
        notifications: [t]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        with pytest.raises(ValueError, match="must specify a 'type'"):
            ConfigLoader.load_queries(str(f))

    def test_query_references_undefined_source(self, tmp_path):
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    t:
        type: teams
        webhook_url: test
sources:
    db:
        type: database
queries:
    q:
        source: nonexistent
        notifications: [t]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        with pytest.raises(ValueError, match="references undefined source"):
            ConfigLoader.load_queries(str(f))

    def test_query_references_undefined_notification_sink(self, tmp_path):
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    t:
        type: teams
        webhook_url: test
sources:
    db:
        type: database
queries:
    q:
        source: db
        notifications: [nonexistent_sink]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        with pytest.raises(ValueError, match="references undefined notification sink"):
            ConfigLoader.load_queries(str(f))

    def test_query_missing_notifications_field(self, tmp_path):
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    t:
        type: teams
        webhook_url: test
sources:
    db:
        type: database
queries:
    q:
        source: db
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        with pytest.raises(ValueError, match="must specify 'notifications'"):
            ConfigLoader.load_queries(str(f))

    def test_env_var_interpolation(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_WEBHOOK", "http://my-webhook.url")
        monkeypatch.setenv("MY_CONN", "my-conn-string")
        f = tmp_path / "config.yml"
        f.write_text("""
notifications:
    t:
        type: teams
        webhook_url: ${MY_WEBHOOK}
sources:
    db:
        type: database
        connection_string: ${MY_CONN}
queries:
    q:
        source: db
        notifications: [t]
        query:
            sql: SELECT 1
        message_single: "t"
        message_multiple: "t"
""")
        config = ConfigLoader.load_queries(str(f))
        assert config["notifications"]["t"]["webhook_url"] == "http://my-webhook.url"
        assert config["sources"]["db"]["connection_string"] == "my-conn-string"

    def test_missing_env_var_raises(self, valid_config_file, monkeypatch):
        monkeypatch.delenv("ETL_TEAMS_WEBHOOK_URL", raising=False)
        with pytest.raises(ValueError, match="Environment variable not set"):
            ConfigLoader.load_queries(valid_config_file)

    def test_resolve_env_var_plain_string_unchanged(self):
        assert ConfigLoader._resolve_env_var("plain_string") == "plain_string"

    def test_resolve_env_var_partial_pattern_unchanged(self):
        assert ConfigLoader._resolve_env_var("${incomplete") == "${incomplete"
