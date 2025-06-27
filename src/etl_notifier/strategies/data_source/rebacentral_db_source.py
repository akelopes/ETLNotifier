from typing import List, Any, Dict
from etl_notifier.strategies.data_source import DatabaseSource
from etl_notifier.models import RebaCentralNotificationRecord


class RebaCentralDbSource(DatabaseSource):

    def get_records(self, query: str) -> List[RebaCentralNotificationRecord]:
        """Fetch records from the Reba Central database based on the provided query."""
        result = self._execute_query(query)
        records = [
            RebaCentralNotificationRecord(
                account_name=record["AccountName"],
                environment=record["Environment"],
                start_time=record["StartTime"],
                url=record.get("PipelineURL"),
                error_message=record.get("errorMessage"),
                requires_confirmation=record.get("requiresConfirmation"),
            )
            for record in result
        ]

        return records
