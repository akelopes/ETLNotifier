import os
from typing import Dict, Any
import yaml


class ConfigLoader:
    CONFIG_FILE = "config.yml"

    @staticmethod
    def load_queries(file_path: str) -> Dict[str, Any]:
        """
        Load and parse the YAML configuration file.

        Args:
            file_path: Path to the YAML configuration file

        Returns:
            Dictionary containing the parsed configuration

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If there's an error parsing the YAML file
            ValueError: If the configuration format is invalid
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, "r") as file:
            config = yaml.safe_load(file)

        # Validate configuration structure
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")

        # Validate and process notification section
        if "notification" not in config:
            raise ValueError("Configuration must contain 'notification' section")

        notification_config = config["notification"]
        if "type" not in notification_config:
            raise ValueError("Notification section must specify a 'type'")
        if "webhook_url" not in notification_config:
            raise ValueError("Notification section must specify a 'webhook_url'")

        # Process the configuration recursively to interpolate environment variables
        processed_config = ConfigLoader._process_config_section(config)

        # Validate sources section
        if "sources" not in processed_config:
            raise ValueError("Configuration must contain 'sources' section")

        # Validate each source configuration
        for source_name, source_config in processed_config["sources"].items():
            if "type" not in source_config:
                raise ValueError(f"Source '{source_name}' must specify a 'type'")

        # Validate queries section
        if "queries" not in processed_config:
            raise ValueError("Configuration must contain 'queries' section")

        # Validate each query configuration
        for query_name, query_config in processed_config["queries"].items():
            if "source" not in query_config:
                raise ValueError(f"Query '{query_name}' must specify a 'source'")
            if query_config["source"] not in processed_config["sources"]:
                raise ValueError(
                    f"Query '{query_name}' references undefined source '{query_config['source']}'"
                )
            if "query" not in query_config:
                raise ValueError(f"Query '{query_name}' must contain a 'query' section")
            if "message_single" not in query_config:
                raise ValueError(f"Query '{query_name}' must specify 'message_single'")
            if "message_multiple" not in query_config:
                raise ValueError(
                    f"Query '{query_name}' must specify 'message_multiple'"
                )

        return processed_config

    @staticmethod
    def _process_config_section(config_section: Any) -> Any:
        """
        Process a configuration section, handling nested structures.

        Args:
            config_section: Section of configuration to process

        Returns:
            Processed configuration section
        """
        if isinstance(config_section, dict):
            return {
                key: ConfigLoader._process_config_section(value)
                for key, value in config_section.items()
            }
        elif isinstance(config_section, list):
            return [
                ConfigLoader._process_config_section(item) for item in config_section
            ]
        elif isinstance(config_section, str):
            return ConfigLoader._interpolate_env_vars(config_section)
        return config_section

    @staticmethod
    def _interpolate_env_vars(value: str) -> str:
        """
        Interpolate environment variables in a string value.

        Args:
            value: String that may contain environment variable references

        Returns:
            String with environment variables replaced with their values

        Example:
            "${MY_VAR}" gets replaced with the value of MY_VAR environment variable
        """
        if not isinstance(value, str):
            return value

        if value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ValueError(f"Environment variable not set: {env_var}")
            return env_value

        return value

    @staticmethod
    def load_config():
        pass
