from typing import Dict, Type, List

from etl_notifier.strategies.data_source.base_source import DataSource
from etl_notifier.strategies.message_formatter.base_formatter import Formatter
from etl_notifier.strategies.notification import Notification
from etl_notifier.config_loader import ConfigLoader
from etl_notifier.strategies.cache import Cache
from etl_notifier.models import NotificationRecord

import etl_notifier.strategies.data_source as ds


class Notifier:
    SOURCE_TYPES: Dict[str, Type[DataSource]] = {
        "rebacentral": ds.RebaCentralDbSource,
    }

    def __init__(
        self,
        queries_file: str,
        cache_strategy: Cache,
        notification_strategy: Notification,
        message_formatter: Formatter,
    ):
        self.queries_file = queries_file
        self.cache_manager = cache_strategy()
        self.notification_strategy = notification_strategy()
        self.config = ConfigLoader.load_queries(queries_file)
        self.message_formatter = message_formatter

    def _create_data_source(self, source_name: str, source_config: Dict) -> DataSource:
        source_class = self.SOURCE_TYPES.get(source_name)

        if not source_class:
            raise ValueError(f"Unknown source type: {source_name}")

        return source_class(**{k: v for k, v in source_config.items()})

    def process_records(self, query_name: str, records: List[NotificationRecord]) -> List[NotificationRecord]:
        cache_instance = self.cache_manager.load(query_name)

        notify_records = [
            r
            for r in records
            if r.get_unique_key() not in cache_instance.keys()
            and not r.requires_confirmation
        ]

        notify_records.extend(
            [
                r
                for r in records
                if r.get_unique_key() in cache_instance.keys()
                and r.requires_confirmation
                and cache_instance.get(r.get_unique_key()) == "pending"
            ]
        )

        updated_cache = {}
        updated_cache[query_name] = {
            **{
                r.get_unique_key(): "confirmed"
                for r in records
                if (
                    not r.requires_confirmation
                    or (
                        r.requiers_confirmation
                        and cache_instance.get(r.get_unique_key())
                        in ["pending", "confirmed"]
                    )
                )
            },
            **{
                r.get_unique_key(): "pending"
                for r in records
                if r.get_unique_key() not in cache_instance.keys()
                and r.requires_confirmation
            },
        }

        self.cache_manager.save(updated_cache)

        return notify_records

    def send_notification(
        self, notification_records: List[NotificationRecord], query_info: Dict[str, str]
    ):
        if not notification_records:
            return

        if len(notification_records) == 1:
            message = self.message_formatter.format_single_message(
                notification_records[0], template=query_info["message_single"]
            )
        else:
            message = self.message_formatter.format_multiple_messages(
                notification_records,
                lines_template=query_info["message_multiple_lines"],
                intro_template=query_info["message_multiple_intro"],
            )

        self.notification_strategy.send_notification(message)

    def run(self) -> None:
        """Execute the ETL notification process."""
        try:
            sources_config = self.config["sources"]
            queries_config = self.config["queries"]

            source_queries = {}
            for query_name, query_info in queries_config.items():
                source_name = query_info["source"]
                if source_name not in source_queries:
                    source_queries[source_name] = []
                source_queries[source_name].append((query_name, query_info))

            for source_name, queries in source_queries.items():
                source_config = sources_config[source_name]
                
                with self._create_data_source(source_name, source_config) as source:
                    for query_name, query_info in queries:
                        try:
                            records = source.get_records(query_info["query"])
                            self.process_records(query_name, records, query_info)
                            self.send_notification(records, query_info)
                        except Exception as e:
                            print(f"Error processing query {query_name} for source {source_name}: {str(e)}")

        except Exception as e:
            print(f"Error in ETL notification process: {str(e)}")
