#!/usr/bin/env python3

import os
import time
from typing import Dict, Type
from dotenv import load_dotenv

from .models.notification_record import NotificationRecord
from .services.data_source.base import DataSource
from .services.data_source.database import DatabaseSource
from .services.notification.teams_strategy import TeamsNotificationStrategy
from .services.notification.formatter import NotificationFormatter
from .services.config_loader import ConfigLoader
from .services.cache import CacheStrategy, JsonFileCache


class ETLNotifier:
    SOURCE_TYPES: Dict[str, Type[DataSource]] = {
        "database": DatabaseSource,
    }

    def __init__(
        self,
        queries_file: str,
        cache_strategy: CacheStrategy,
        notification_strategy: TeamsNotificationStrategy,
    ):
        self.queries_file = queries_file
        self.cache_manager = cache_strategy
        self.notification_strategy = notification_strategy
        self.config = ConfigLoader.load_queries(queries_file)
        
    def _create_data_source(self, source_config: Dict) -> DataSource:
        """Create a data source instance based on configuration."""
        source_type = source_config["type"]
        source_class = self.SOURCE_TYPES.get(source_type)
        
        if not source_class:
            raise ValueError(f"Unknown source type: {source_type}")
            
        return source_class(**{
            k: v for k, v in source_config.items() 
            if k != "type"
        })

    def process_query_results(self, query_name: str, records: list, cache: dict, query_info: dict) -> None:
        """Process query results and send notifications for new items."""
        current_keys = {record.get_unique_key() for record in records}
        existing_cache = cache.get(query_name, {})

        if query_name == "failures":
            cache[query_name] = {k: "confirmed" for k in current_keys}
            new_items = [
                r for r in records 
                if r.get_unique_key() in (current_keys - existing_cache.keys())
            ]
        else:
            new_pending_items = {
                k: "pending" for k in (current_keys - existing_cache.keys())
            }
            confirmed_items = [
                r for r in records 
                if existing_cache.get(r.get_unique_key()) == "pending"
            ]

            cache[query_name] = {
                **{
                    k: "confirmed"
                    for k in current_keys
                    if existing_cache.get(k) in ("pending", "confirmed")
                },
                **new_pending_items,
            }

            new_items = confirmed_items

        if not new_items:
            return

        if len(new_items) == 1:
            message = NotificationFormatter.format_single_notification(
                new_items[0], query_info["message_single"]
            )
        else:
            message = NotificationFormatter.format_multiple_notifications(
                new_items, query_info["message_multiple"]
            )

        self.notification_strategy.send_notification(message)

    def run(self) -> None:
        """Execute the ETL notification process."""
        try:
            cache = self.cache_manager.load()
            sources_config = self.config["sources"]
            queries_config = self.config["queries"]

            # Group queries by source
            source_queries = {}
            for query_name, query_info in queries_config.items():
                source_name = query_info["source"]
                if source_name not in source_queries:
                    source_queries[source_name] = []
                source_queries[source_name].append((query_name, query_info))

            # Process each source and its queries
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
                                    error_message=record.get("errorMessage")
                                )
                                for record in raw_records
                            ]
                            self.process_query_results(
                                query_name, records, cache, query_info
                            )
                        except Exception as e:
                            print(f"Error processing query {query_name}: {str(e)}")

            self.cache_manager.save(cache)
        except Exception as e:
            print(f"Error in ETL notification process: {str(e)}")

def main():
    """Main entry point for the ETL notifier."""
    # Load environment variables
    load_dotenv()

    notifier = ETLNotifier(
        queries_file=os.getenv("ETL_QUERIES_FILE", "config/queries.yml"),
        cache_strategy=JsonFileCache(
            os.getenv("ETL_CACHE_FILE", "cache.json")
        ),
        notification_strategy=TeamsNotificationStrategy(
            webhook_url=os.getenv("ETL_TEAMS_WEBHOOK_URL")
        ),
    )

    # Run the notification process in a loop
    while True:
        try:
            notifier.run()
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
        finally:
            # Wait before next iteration (5 minutes by default)
            sleep_time = int(os.getenv("ETL_SLEEP_TIME", "300"))
            time.sleep(sleep_time)

if __name__ == "__main__":
    main()