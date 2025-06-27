#!/usr/bin/env python3
import os
import time

from dotenv import load_dotenv
from etl_notifier import Notifier
from etl_notifier.strategies.message_formatter import MarkdownFormatter
from etl_notifier.strategies.cache import JsonFileCache
from etl_notifier.strategies.notification import TeamsNotification


def main():
    """Main entry point for the ETL notifier."""

    load_dotenv()

    notifier = Notifier(
        queries_file=os.getenv("NOTIFIER_QUERIES_FILE", "config/queries.yml"),
        cache_strategy=JsonFileCache,
        notification_strategy=TeamsNotification,
        message_formatter=MarkdownFormatter,
    )

    while True:
        try:
            notifier.run()
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
        finally:
            sleep_time = int(os.getenv("NOTIFIER_SLEEP_TIME", 300))
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
