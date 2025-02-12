import pytest
from unittest.mock import Mock, patch, MagicMock
from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.data_source.database import DatabaseSource
from etl_notifier.main import ETLNotifier

class TestETLNotifier:
    def test_create_data_source(self, mock_cache_strategy, mock_notification_strategy, mock_etl_config):
        with patch("etl_notifier.main.ConfigLoader") as mock_loader, \
            patch('pyodbc.connect') as mock_connect:
                mock_connect.return_value = MagicMock()
                mock_loader.load_queries.return_value = mock_etl_config
                notifier = ETLNotifier(
                    "test.yml", mock_cache_strategy, mock_notification_strategy
                )

                source = notifier._create_data_source(
                    {"type": "database", "connection_string": "test_connection"}
                )

                assert isinstance(source, DatabaseSource)
                assert source.connection_string == "test_connection"

    def test_process_query_results_pending_to_confirmed(
        self, mock_cache_strategy, mock_notification_strategy, 
        mock_etl_config, sample_etl_records
    ):
        with patch("etl_notifier.main.ConfigLoader") as mock_loader, \
            patch("etl_notifier.services.notification.formatter.NotificationFormatter") as mock_formatter:
            mock_loader.load_queries.return_value = mock_etl_config
            mock_formatter.format_single_notification.return_value = "Test message"
            
            notifier = ETLNotifier(
                "test.yml", mock_cache_strategy, mock_notification_strategy
            )

            cache = {"test_query": {sample_etl_records[0].get_unique_key(): "pending"}}
            mock_etl_config["queries"]["test_query"]["message_single"] = "Test message for {0} in {1}: {2}"
            
            notifier.process_query_results(
                "test_query",
                [sample_etl_records[0]],
                cache,
                mock_etl_config["queries"]["test_query"],
            )

            assert mock_notification_strategy.send_notification.called
            assert cache["test_query"][sample_etl_records[0].get_unique_key()] == "confirmed"


    def test_process_query_results_no_new_items(
        self, mock_cache_strategy, mock_notification_strategy, 
        mock_etl_config, sample_etl_records
    ):
        with patch("etl_notifier.main.ConfigLoader") as mock_loader:
            mock_loader.load_queries.return_value = mock_etl_config
            notifier = ETLNotifier(
                "test.yml", mock_cache_strategy, mock_notification_strategy
            )

            cache = {
                "test_query": {
                    record.get_unique_key(): "confirmed" for record in sample_etl_records
                }
            }
            notifier.process_query_results(
                "test_query",
                sample_etl_records,
                cache,
                mock_etl_config["queries"]["test_query"],
            )

            assert not mock_notification_strategy.send_notification.called

    def test_run_successful_execution(
        self, mock_cache_strategy, mock_notification_strategy, 
        mock_etl_config, mock_data_source
    ):
        with patch("etl_notifier.main.ConfigLoader") as mock_loader, patch.dict(
            "etl_notifier.main.ETLNotifier.SOURCE_TYPES",
            {'database': Mock(return_value=mock_data_source)}
        ):
            mock_loader.load_queries.return_value = mock_etl_config
            notifier = ETLNotifier(
                "test.yml", mock_cache_strategy, mock_notification_strategy
            )
            notifier.run()

            assert mock_cache_strategy.save.called
            assert len(mock_data_source.executed_queries) > 0

    def test_run_with_query_error(
        self, mock_cache_strategy, mock_notification_strategy, mock_etl_config
    ):
        with patch("etl_notifier.main.ConfigLoader") as mock_loader, patch.dict(
            "etl_notifier.main.ETLNotifier.SOURCE_TYPES"
        ):
            mock_loader.load_queries.return_value = mock_etl_config

            mock_source = MagicMock()
            mock_source.__enter__.return_value = mock_source
            mock_source.execute_query.side_effect = Exception("Test error")

            mock_source_class = Mock(return_value=mock_source)
            ETLNotifier.SOURCE_TYPES["database"] = mock_source_class

            notifier = ETLNotifier(
                "test.yml", mock_cache_strategy, mock_notification_strategy
            )
            notifier.run()  # Should not raise exception

            assert mock_cache_strategy.save.called