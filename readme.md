# ETL Notifier

A Python-based notification system that monitors ETL processes and sends alerts through various channels (currently supporting Microsoft Teams).

## Overview

The ETL Notifier is designed to:
- Monitor ETL executions through database queries
- Cache results to prevent duplicate notifications
- Send formatted notifications through configurable channels
- Support multiple data sources and notification methods

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
├── scripts/                    # Utility scripts
├── .env.example               # Example environment configuration
└── README.md                  # This file
```

## Getting Started

### Prerequisites

- Python 3.8 or higher
- ODBC Driver 17 for SQL Server
- Access to the target database
- Microsoft Teams webhook URL (or other supported notification platform)

### Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd etl-notifier
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e ".[dev]"
```

4. Set up environment configuration:
```bash
./scripts/setup_env.sh
```

5. Edit the `.env` file with your configuration values:
```env
ETL_DB_CONNECTION_STRING="Driver={ODBC Driver 17 for SQL Server};Server=your-server;Database=your-db;Authentication=ActiveDirectoryInteractive;UID=your-username;"
ETL_TEAMS_WEBHOOK_URL="your-teams-webhook-url"
```

### Configuration

The application uses two main configuration files:

1. `.env` - Environment variables:
   - Database connection string
   - Webhook URLs
   - Cache settings
   - Application settings

2. `config/queries.yml` - Application configuration:
```yaml
notification:
  type: teams
  webhook_url: ${ETL_TEAMS_WEBHOOK_URL}

sources:
  database:
    type: database
    connection_string: ${ETL_DB_CONNECTION_STRING}

queries:
  database_failures:
    source: database
    query:
      sql: "YOUR_QUERY_HERE"
    message_single: "Template for single failure"
    message_multiple: "Template for multiple failures"
```

## Development

### Adding a New Data Source

1. Create a new class in `services/data_source/` implementing the `DataSource` interface:
```python
from .base import DataSource

class NewDataSource(DataSource):
    def execute_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Implementation here
        pass
```

2. Add the new source type to `ETLNotifier.SOURCE_TYPES`

### Adding a New Notification Strategy

1. Create a new class in `services/notification/` implementing the `NotificationStrategy` interface:
```python
from .strategy import NotificationStrategy

class NewNotificationStrategy(NotificationStrategy):
    def send_notification(self, message: str) -> None:
        # Implementation here
        pass
```

2. Add the new strategy type to `ETLNotifier.NOTIFICATION_TYPES`

### Running Tests

```bash
pytest tests/
```

### Code Style

The project uses:
- Black for code formatting
- isort for import sorting
- mypy for type checking
- flake8 for linting

Run all checks:
```bash
black src/ tests/
isort src/ tests/
mypy src/
flake8 src/ tests/
```

## Architecture

The project follows SOLID principles and uses several design patterns:

1. Strategy Pattern:
   - For notification implementations
   - For data source implementations
   - For cache implementations

2. Dependency Injection:
   - Components receive their dependencies through constructor injection
   - Makes testing and extending functionality easier

3. Abstract Base Classes:
   - Define clear interfaces for implementations
   - Ensure consistency across different implementations

## Contributing

1. Create a feature branch
2. Make your changes
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Troubleshooting

Common issues and solutions:

1. Database Connection Issues:
   - Verify ODBC Driver is installed
   - Check connection string format
   - Ensure proper authentication

2. Notification Failures:
   - Verify webhook URL is correct
   - Check network connectivity
   - Validate message format

3. Cache Issues:
   - Check file permissions for JSON cache
   - Verify cache directory exists
   - Monitor cache file size

## Additional Resources

- [ODBC Driver Documentation](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server)
- [Teams Webhook Documentation](https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)