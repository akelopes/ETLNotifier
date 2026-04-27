import pytest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from etl_notifier.main import ETLNotifier
from etl_notifier.models.notification_record import NotificationRecord
from etl_notifier.services.data_source.database import DatabaseSource
from etl_notifier.services.notification.strategy import NotificationStrategy


class TestETLNotifier:
    @pytest.fixture
    def mock_sink(self):
        return Mock(spec=NotificationStrategy)

    @pytest.fixture(autouse=True)
    def patch_notification_types(self, mock_sink):
        with patch.dict(ETLNotifier.NOTIFICATION_TYPES, {"teams": Mock(return_value=mock_sink)}):
            yield

    @pytest.fixture
    def notifier(self, mock_etl_config, mock_cache_strategy):
        return ETLNotifier(config=mock_etl_config, cache_strategy=mock_cache_strategy)

    # --- factory methods ---

    def test_create_data_source_returns_correct_type(self, notifier):
        with patch("pyodbc.connect"):
            source = notifier._create_data_source({"type": "database", "connection_string": "test"})
        assert isinstance(source, DatabaseSource)

    def test_create_data_source_unknown_type_raises(self, notifier):
        with pytest.raises(ValueError, match="Unknown source type"):
            notifier._create_data_source({"type": "nonexistent"})

    def test_create_notification_strategy_unknown_type_raises(self, mock_cache_strategy, mock_etl_config):
        mock_etl_config["notifications"]["bad"] = {"type": "unknown_type"}
        mock_etl_config["queries"]["test_query"]["notifications"] = ["bad"]
        with patch.dict(ETLNotifier.NOTIFICATION_TYPES, {}, clear=True):
            with pytest.raises(ValueError, match="Unknown notification type"):
                ETLNotifier(config=mock_etl_config, cache_strategy=mock_cache_strategy)

    # --- _get_sinks ---

    def test_get_sinks_returns_matching_strategies(self, notifier, mock_sink):
        sinks = notifier._get_sinks({"notifications": ["teams_main"]})
        assert sinks == [mock_sink]

    def test_get_sinks_ignores_undefined_sink_name(self, notifier):
        sinks = notifier._get_sinks({"notifications": ["nonexistent"]})
        assert sinks == []

    def test_get_sinks_empty_list(self, notifier):
        assert notifier._get_sinks({"notifications": []}) == []

    # --- process_query_results: "failures" mode ---

    def test_failures_new_item_triggers_notification(self, notifier, mock_sink, sample_etl_records):
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("failures", [sample_etl_records[0]], {}, query_info)
        mock_sink.send_notification.assert_called_once()

    def test_failures_existing_item_no_notification(self, notifier, mock_sink, sample_etl_records):
        record = sample_etl_records[0]
        cache = {"failures": {record.get_unique_key(): "confirmed"}}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("failures", [record], cache, query_info)
        mock_sink.send_notification.assert_not_called()

    def test_failures_always_marks_confirmed(self, notifier, mock_sink, sample_etl_records):
        record = sample_etl_records[0]
        cache = {}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("failures", [record], cache, query_info)
        assert cache["failures"][record.get_unique_key()] == "confirmed"

    # --- process_query_results: pending/confirmed mode ---

    def test_new_item_goes_to_pending_no_notification(self, notifier, mock_sink, sample_etl_records):
        record = sample_etl_records[0]
        cache = {}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("test_query", [record], cache, query_info)
        mock_sink.send_notification.assert_not_called()
        assert cache["test_query"][record.get_unique_key()] == "pending"

    def test_pending_item_confirmed_triggers_notification(self, notifier, mock_sink, sample_etl_records):
        record = sample_etl_records[0]
        cache = {"test_query": {record.get_unique_key(): "pending"}}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("test_query", [record], cache, query_info)
        mock_sink.send_notification.assert_called_once()
        assert cache["test_query"][record.get_unique_key()] == "confirmed"

    def test_confirmed_item_no_notification(self, notifier, mock_sink, sample_etl_records):
        record = sample_etl_records[0]
        cache = {"test_query": {record.get_unique_key(): "confirmed"}}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("test_query", [record], cache, query_info)
        mock_sink.send_notification.assert_not_called()

    def test_multiple_confirmed_items_uses_multi_formatter(self, notifier, mock_sink, sample_etl_records):
        cache = {"test_query": {r.get_unique_key(): "pending" for r in sample_etl_records}}
        query_info = mock_etl_config_query(notifications=["teams_main"])
        notifier.process_query_results("test_query", sample_etl_records, cache, query_info)
        mock_sink.send_notification.assert_called_once()
        message = mock_sink.send_notification.call_args[0][0]
        assert "Multiple issues:" in message

    # --- multi-sink routing ---

    def test_notifies_all_declared_sinks(self, mock_etl_config, mock_cache_strategy):
        sink_a = Mock(spec=NotificationStrategy)
        sink_b = Mock(spec=NotificationStrategy)
        mock_etl_config["notifications"]["teams_b"] = {"type": "teams_b", "webhook_url": "http://b.url"}
        mock_etl_config["queries"]["test_query"]["notifications"] = ["teams_main", "teams_b"]
        with patch.dict(ETLNotifier.NOTIFICATION_TYPES, {
            "teams": Mock(return_value=sink_a),
            "teams_b": Mock(return_value=sink_b),
        }):
            notifier = ETLNotifier(config=mock_etl_config, cache_strategy=mock_cache_strategy)

        record = NotificationRecord("Acct", "Prod", datetime(2025, 1, 1), error_message="err")
        cache = {"test_query": {record.get_unique_key(): "pending"}}
        notifier.process_query_results("test_query", [record], cache, mock_etl_config["queries"]["test_query"])
        sink_a.send_notification.assert_called_once()
        sink_b.send_notification.assert_called_once()

    # --- run() ---

    def test_run_saves_cache(self, notifier, mock_cache_strategy, mock_data_source):
        with patch.dict(ETLNotifier.SOURCE_TYPES, {"database": Mock(return_value=mock_data_source)}):
            notifier.run()
        mock_cache_strategy.save.assert_called_once()

    def test_run_query_error_does_not_propagate(self, notifier, mock_cache_strategy):
        mock_source = MagicMock()
        mock_source.__enter__.return_value = mock_source
        mock_source.__exit__.return_value = False
        mock_source.execute_query.side_effect = Exception("DB error")
        with patch.dict(ETLNotifier.SOURCE_TYPES, {"database": Mock(return_value=mock_source)}):
            notifier.run()
        mock_cache_strategy.save.assert_called_once()


def mock_etl_config_query(notifications):
    return {
        "notifications": notifications,
        "message_single": "Pipeline {account} in {env}: {errorMessage}",
        "message_multiple": "Multiple issues:",
    }
