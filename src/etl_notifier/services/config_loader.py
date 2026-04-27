import os
from typing import Any, Dict

import yaml


class ConfigLoader:
    @staticmethod
    def load_queries(file_path: str) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, "r") as f:
            config = yaml.safe_load(f)

        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")

        processed = ConfigLoader._process_section(config)

        if "notifications" not in processed:
            raise ValueError("Configuration must contain 'notifications' section")
        for name, sink in processed["notifications"].items():
            if "type" not in sink:
                raise ValueError(f"Notification sink '{name}' must specify a 'type'")

        if "sources" not in processed:
            raise ValueError("Configuration must contain 'sources' section")
        for name, src in processed["sources"].items():
            if "type" not in src:
                raise ValueError(f"Source '{name}' must specify a 'type'")

        if "queries" not in processed:
            raise ValueError("Configuration must contain 'queries' section")
        for name, query in processed["queries"].items():
            if "source" not in query:
                raise ValueError(f"Query '{name}' must specify a 'source'")
            if query["source"] not in processed["sources"]:
                raise ValueError(f"Query '{name}' references undefined source '{query['source']}'")
            if "query" not in query:
                raise ValueError(f"Query '{name}' must contain a 'query' section")
            if "message_single" not in query:
                raise ValueError(f"Query '{name}' must specify 'message_single'")
            if "message_multiple" not in query:
                raise ValueError(f"Query '{name}' must specify 'message_multiple'")
            if "notifications" not in query:
                raise ValueError(f"Query '{name}' must specify 'notifications'")
            for sink_name in query["notifications"]:
                if sink_name not in processed["notifications"]:
                    raise ValueError(f"Query '{name}' references undefined notification sink '{sink_name}'")

        return processed

    @staticmethod
    def _process_section(section: Any) -> Any:
        if isinstance(section, dict):
            return {k: ConfigLoader._process_section(v) for k, v in section.items()}
        if isinstance(section, list):
            return [ConfigLoader._process_section(i) for i in section]
        if isinstance(section, str):
            return ConfigLoader._resolve_env_var(section)
        return section

    @staticmethod
    def _resolve_env_var(value: str) -> str:
        if value.startswith("${") and value.endswith("}"):
            var = value[2:-1]
            result = os.getenv(var)
            if result is None:
                raise ValueError(f"Environment variable not set: {var}")
            return result
        return value
