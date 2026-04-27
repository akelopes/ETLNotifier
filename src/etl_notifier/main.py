#!/usr/bin/env python3
import logging
import os
import time
from typing import Dict, List, Type

from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.cache import CacheStrategy, JsonFileCache
from etl_notifier.services.config_loader import ConfigLoader
from etl_notifier.services.data_source import AzureSqlDBSource, DatabaseSource, DataSource
from etl_notifier.services.notification import NotificationFormatter, NotificationStrategy, TeamsNotificationStrategy

logger = logging.getLogger(__name__)


class ETLNotifier:
    SOURCE_TYPES: Dict[str, Type[DataSource]] = {
        "database": DatabaseSource,
        "azure_sql_db": AzureSqlDBSource,
    }
    NOTIFICATION_TYPES: Dict[str, Type[NotificationStrategy]] = {
        "teams": TeamsNotificationStrategy,
    }

    def __init__(self, config: Dict, cache_strategy: CacheStrategy):
        self.cache_manager = cache_strategy
        self.config = config
        self.notification_strategies: Dict[str, NotificationStrategy] = {
            name: self._create_notification_strategy(cfg)
            for name, cfg in config["notifications"].items()
        }

    def _create_notification_strategy(self, sink_config: Dict) -> NotificationStrategy:
        sink_type = sink_config["type"]
        cls = self.NOTIFICATION_TYPES.get(sink_type)
        if not cls:
            raise ValueError(f"Unknown notification type: {sink_type}")
        return cls(**{k: v for k, v in sink_config.items() if k != "type"})

    def _create_data_source(self, source_config: Dict) -> DataSource:
        source_type = source_config["type"]
        source_class = self.SOURCE_TYPES.get(source_type)
        if not source_class:
            raise ValueError(f"Unknown source type: {source_type}")
        return source_class(**{k: v for k, v in source_config.items() if k != "type"})

    def _get_sinks(self, query_info: Dict) -> List[NotificationStrategy]:
        return [
            self.notification_strategies[name]
            for name in query_info.get("notifications", [])
            if name in self.notification_strategies
        ]

    def process_query_results(self, query_name: str, records: list, cache: dict, query_info: dict) -> None:
        current_keys = {record.get_unique_key() for record in records}
        existing_cache = cache.get(query_name, {})

        if query_name == "failures":
            cache[query_name] = {k: "confirmed" for k in current_keys}
            new_items = [r for r in records if r.get_unique_key() in (current_keys - existing_cache.keys())]
        else:
            new_pending_items = {k: "pending" for k in (current_keys - existing_cache.keys())}
            confirmed_items = [r for r in records if existing_cache.get(r.get_unique_key()) == "pending"]
            cache[query_name] = {
                **{k: "confirmed" for k in current_keys if existing_cache.get(k) in ("pending", "confirmed")},
                **new_pending_items,
            }
            new_items = confirmed_items

        if not new_items:
            return

        if len(new_items) == 1:
            message = NotificationFormatter.format_single_notification(new_items[0], query_info["message_single"])
        else:
            message = NotificationFormatter.format_multiple_notifications(new_items, query_info["message_multiple"])

        for sink in self._get_sinks(query_info):
            sink.send_notification(message)

    def run(self) -> None:
        try:
            cache = self.cache_manager.load()
            sources_config = self.config["sources"]
            queries_config = self.config["queries"]

            source_queries: Dict[str, list] = {}
            for query_name, query_info in queries_config.items():
                source_queries.setdefault(query_info["source"], []).append((query_name, query_info))

            for source_name, queries in source_queries.items():
                source_config = sources_config[source_name]
                with self._create_data_source(source_config) as source:
                    for query_name, query_info in queries:
                        try:
                            raw_records = source.execute_query(query_info["query"])
                            records = [
                                NotificationRecord(
                                    account_name=record["AccountName"],
                                    environment=record["Environment"],
                                    start_time=record["StartTime"],
                                    url=record.get("PipelineURL"),
                                    error_message=record.get("errorMessage"),
                                    over_hour=record.get("over_hour"),
                                )
                                for record in raw_records
                            ]
                            self.process_query_results(query_name, records, cache, query_info)
                        except Exception as e:
                            logger.error("Error processing query %s: %s", query_name, e)

            self.cache_manager.save(cache)
        except Exception as e:
            logger.error("Error in ETL notification process: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = ConfigLoader.load_queries("config/queries.yml")
    notifier = ETLNotifier(config=config, cache_strategy=JsonFileCache("cache.json"))

    while True:
        try:
            notifier.run()
        except Exception as e:
            logger.error("Error in main loop: %s", e)
        finally:
            time.sleep(int(os.getenv("ETL_SLEEP_TIME", 300)))


if __name__ == "__main__":
    main()
