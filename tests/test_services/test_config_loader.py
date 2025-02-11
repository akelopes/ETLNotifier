import pytest
import os
from unittest.mock import patch
import yaml

from etl_notifier.services.config_loader import ConfigLoader

class TestConfigLoader:
    @pytest.fixture
    def valid_config_file(self, tmp_path):
        """Create a valid configuration file for testing"""
        config_content = """
notification:
    type: teams
    webhook_url: ${ETL_TEAMS_WEBHOOK_URL}

sources:
    database:
        type: database
        connection_string: ${ETL_DB_CONNECTION_STRING}

queries:
    database_failures:
        source: database
        query:
            sql: SELECT * FROM test_table
        message_single: "Test message for {}"
        message_multiple: "Multiple test messages"
"""
        config_file = tmp_path / "valid_config.yml"
        config_file.write_text(config_content)
        return str(config_file)

    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        """Setup environment variables for all tests"""
        monkeypatch.setenv('ETL_TEAMS_WEBHOOK_URL', 'http://test-webhook.url')
        monkeypatch.setenv('ETL_DB_CONNECTION_STRING', 'test-connection-string')

    def test_load_valid_config(self, valid_config_file):
        """Test loading a valid configuration file"""
        config = ConfigLoader.load_queries(valid_config_file)

        assert 'notification' in config
        assert 'sources' in config
        assert 'queries' in config
        assert config['notification']['type'] == 'teams'
        assert config['notification']['webhook_url'] == 'http://test-webhook.url'
        assert config['sources']['database']['connection_string'] == 'test-connection-string'

    def test_load_nonexistent_file(self):
        """Test loading a non-existent configuration file"""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_queries("nonexistent.yml")

    def test_load_invalid_yaml(self, tmp_path):
        """Test loading an invalid YAML file"""
        invalid_yaml = """
        this is not valid:
            - yaml: [content
        """
        config_file = tmp_path / "invalid.yml"
        config_file.write_text(invalid_yaml)

        with pytest.raises(yaml.YAMLError):
            ConfigLoader.load_queries(str(config_file))

    def test_missing_required_sections(self, tmp_path):
        """Test configuration with missing required sections"""
        test_cases = [
            # Missing notification section
            """
sources:
    database:
        type: database
queries:
    test:
        source: database
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
            """,
            # Missing sources section
            """
notification:
    type: teams
    webhook_url: test
queries:
    test:
        source: database
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
            """,
            # Missing queries section
            """
notification:
    type: teams
    webhook_url: test
sources:
    database:
        type: database
            """
        ]

        for i, content in enumerate(test_cases):
            config_file = tmp_path / f"missing_sections_{i}.yml"
            config_file.write_text(content)
            
            with pytest.raises(ValueError) as exc_info:
                ConfigLoader.load_queries(str(config_file))
            assert "must contain" in str(exc_info.value)

    def test_missing_required_fields(self, tmp_path):
        """Test configuration with missing required fields in sections"""
        test_cases = [
            # Missing notification type
            """
notification:
    webhook_url: test
sources:
    database:
        type: database
queries:
    test:
        source: database
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
            """,
            # Missing source type
            """
notification:
    type: teams
    webhook_url: test
sources:
    database:
        connection_string: test
queries:
    test:
        source: database
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
            """
        ]

        for i, content in enumerate(test_cases):
            config_file = tmp_path / f"missing_fields_{i}.yml"
            config_file.write_text(content)
            
            with pytest.raises(ValueError) as exc_info:
                ConfigLoader.load_queries(str(config_file))
            assert "must specify" in str(exc_info.value)

    def test_invalid_query_configuration(self, tmp_path):
        """Test invalid query configurations"""
        content = """
notification:
    type: teams
    webhook_url: test
sources:
    database:
        type: database
queries:
    test:
        source: nonexistent_source
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
        """
        
        config_file = tmp_path / "invalid_query.yml"
        config_file.write_text(content)
        
        with pytest.raises(ValueError) as exc_info:
            ConfigLoader.load_queries(str(config_file))
        assert "references undefined source" in str(exc_info.value)

    def test_environment_variable_interpolation(self, tmp_path):
        """Test environment variable interpolation in configuration"""
        content = """
notification:
    type: teams
    webhook_url: ${TEST_WEBHOOK_URL}
sources:
    database:
        type: database
        connection_string: ${TEST_CONNECTION_STRING}
queries:
    test:
        source: database
        query:
            sql: SELECT 1
        message_single: "Test"
        message_multiple: "Test"
        """
        
        config_file = tmp_path / "env_vars.yml"
        config_file.write_text(content)
        
        with patch.dict(os.environ, {
            'TEST_WEBHOOK_URL': 'http://test-webhook.url',
            'TEST_CONNECTION_STRING': 'test-connection-string'
        }):
            config = ConfigLoader.load_queries(str(config_file))
            
        assert config['notification']['webhook_url'] == 'http://test-webhook.url'
        assert config['sources']['database']['connection_string'] == 'test-connection-string'

    def test_missing_environment_variable(self, valid_config_file):
        """Test handling of missing environment variables"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConfigLoader.load_queries(valid_config_file)
            assert "Environment variable not set" in str(exc_info.value)

    def test_non_string_values(self):
        """Test processing of non-string values"""
        test_values = [
            123,
            True,
            None,
            ["list", "of", "values"],
            {"key": "value"}
        ]
        
        for value in test_values:
            result = ConfigLoader._interpolate_env_vars(value)
            assert result == value