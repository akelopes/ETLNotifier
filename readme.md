# ETL Notifier

A Python-based notification system that monitors ETL processes and sends alerts through configurable channels (currently supporting Microsoft Teams).

## Overview

- Monitors ETL executions through database queries
- Caches results to prevent duplicate notifications
- Routes alerts to one or more notification sinks per query
- Supports multiple data sources and notification strategies

## Project Structure

```
etl_notifier/
├── src/
│   └── etl_notifier/
│       ├── models/              # Data models
│       ├── services/
│       │   ├── cache/          # Cache implementations
│       │   ├── data_source/    # Data source implementations
│       │   └── notification/   # Notification strategies
│       └── main.py             # Application entry point
├── config/
│   └── queries.yml             # Query and notification configuration
├── tests/                      # Test suite
└── scripts/                    # Utility scripts
```

## Getting Started

### Prerequisites

- Python 3.8+
- ODBC Driver 18 for SQL Server
- Access to the target database
- Microsoft Teams webhook URL (or other supported notification sink)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/akelopes/ETLNotifier.git
cd ETLNotifier
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -e ".[dev]"
```

4. Set up environment variables:
```bash
./scripts/setup_env.sh
```

5. Edit `.env` with your values:
```env
TEAMS_WEBHOOK_URL="your-teams-webhook-url"
ETL_SLEEP_TIME=300   # polling interval in seconds (default: 300)
```

### Configuration

`config/queries.yml` defines notification sinks, data sources, and queries.

```yaml
notifications:
  teams_ops:
    type: teams
    webhook_url: ${TEAMS_WEBHOOK_URL}
  teams_oncall:
    type: teams
    webhook_url: ${TEAMS_ONCALL_WEBHOOK_URL}

sources:
  my_db:
    type: azure_sql_db
    connection_string: "Driver={ODBC Driver 18 for SQL Server};Server=..."
    msi_client_id: "${MSI_CLIENT_ID}"

queries:
  failures:
    source: my_db
    notifications: [teams_ops, teams_oncall]   # fan-out to multiple sinks
    query:
      sql: "SELECT ..."
    message_single: "Pipeline [{account} - {env}]({url}) failed: {errorMessage}"
    message_multiple: "Multiple pipelines failed:"
```

**Message templates** support these named placeholders: `{account}`, `{env}`, `{url}`, `{errorMessage}`, `{over_hour}`.

**Notification behaviour** differs by query name:
- `failures` — fires immediately on first occurrence; deduplicates by run
- all others — fires only after an item is seen in two consecutive polling cycles (pending → confirmed), suppressing transient spikes

## Development

### Adding a New Data Source

1. Create a class in `src/etl_notifier/services/data_source/` implementing `DataSource`:
```python
from .base import DataSource

class NewDataSource(DataSource):
    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        ...
```

2. Register it in `ETLNotifier.SOURCE_TYPES`:
```python
SOURCE_TYPES = {
    "new_type": NewDataSource,
    ...
}
```

### Adding a New Notification Sink

1. Create a class in `src/etl_notifier/services/notification/` implementing `NotificationStrategy`:
```python
from .strategy import NotificationStrategy

class NewNotificationStrategy(NotificationStrategy):
    def send_notification(self, message: str) -> None:
        ...
```

2. Register it in `ETLNotifier.NOTIFICATION_TYPES`:
```python
NOTIFICATION_TYPES = {
    "new_type": NewNotificationStrategy,
    ...
}
```

3. Add a sink entry in `config/queries.yml` and reference it in the relevant queries.

### Running Tests

```bash
pytest tests/
```

## Architecture

- **Strategy pattern** — `NotificationStrategy`, `DataSource`, and `CacheStrategy` are abstract bases with swappable implementations
- **Registry pattern** — `SOURCE_TYPES` and `NOTIFICATION_TYPES` dicts on `ETLNotifier` map config `type` strings to classes, so adding a new implementation requires no changes to `run()` or `process_query_results()`
- **Per-query sink routing** — each query declares its own `notifications` list; `ETLNotifier` fans out to all declared sinks independently

## Additional Resources

- [ODBC Driver 18 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server)
- [Teams Incoming Webhooks](https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)
