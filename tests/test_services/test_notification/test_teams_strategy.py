import os
from typing import Dict, Any
import yaml

class ConfigLoader:
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

        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)

        # Validate configuration structure
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")

        # Validate notification section
        if "notification" not in config:
            raise ValueError("Configuration must contain 'notification' section")
        
        notification_config = config["notification"]
        if "type" not in notification_config:
            raise ValueError("Notification section must specify a 'type'")
        if "webhook_url" not in notification_config:
            raise ValueError("Notification section must specify a 'webhook_url'")

        # Process the configuration recursively to interpolate environment variables
        config = ConfigLoader._process_config_values(config)

        # Validate sources section
        if "sources" not in config:
            raise ValueError("Configuration must contain 'sources' section")

        # Validate queries section
        if "queries" not in config:
            raise ValueError("Configuration must contain 'queries' section")

        # Validate each source configuration
        for source_name, source_config in config["sources"].items():
            if "type" not in source_config:
                raise ValueError(f"Source '{source_name}' must specify a 'type'")

        # Validate each query configuration
        for query_name, query_config in config["queries"].items():
            if "source" not in query_config:
                raise ValueError(f"Query '{query_name}' must specify a 'source'")
            if query_config["source"] not in config["sources"]:
                raise ValueError(
                    f"Query '{query_name}' references undefined source '{query_config['source']}'"
                )
            if "query" not in query_config:
                raise ValueError(f"Query '{query_name}' must contain a 'query' section")
            if "message_single" not in query_config:
                raise ValueError(f"Query '{query_name}' must specify 'message_single'")
            if "message_multiple" not in query_config:
                raise ValueError(f"Query '{query_name}' must specify 'message_multiple'")

        return config

    @staticmethod
    def _process_config_values(config: Any) -> Any:
        """
        Recursively process configuration values to interpolate environment variables.
        
        Args:
            config: Configuration value to process (can be dict, list, or scalar)
            
        Returns:
            Processed configuration value
        """
        if isinstance(config, dict):
            return {key: ConfigLoader._process_config_values(value) for key, value in config.items()}
        elif isinstance(config, list):
            return [ConfigLoader._process_config_values(value) for value in config]
        elif isinstance(config, str):
            return ConfigLoader.interpolate_env_vars(config)
        return config

    @classmethod
    def interpolate_env_vars(cls, value: str) -> str:
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