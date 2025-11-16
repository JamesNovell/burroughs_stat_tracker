# Burroughs Statistics Tracker

A Dockerized service that tracks and processes Burroughs service call statistics from a SQL Server database.

## Features

- Automatically polls for new batches every 5 minutes (configurable)
- Separates statistics for Recyclers and Smart Safes
- Tracks batch-level, hourly, and daily summary statistics
- Hierarchical aggregation: Batch → Hourly → Daily
- Package tracking integration (FedEx and UPS APIs)
- Handles irregular database updates gracefully

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Comprehensive application architecture and data flow documentation
- **[DATABASE_STRUCTURE.md](DATABASE_STRUCTURE.md)** - Detailed explanation of the source database table structure and how the system interacts with it

## Setup

### 1. Configuration

**SECURITY WARNING**: Never commit `config.json` with real credentials to version control. Always use `config.json.example` as a template.

Copy `config.json.example` to `config.json` and fill in your database credentials:

```bash
cp config.json.example config.json
```

Edit `config.json` with your database connection details. This file is already in `.gitignore` to prevent accidental commits.

**If credentials have been committed to git history, they should be rotated immediately.**

### 2. Docker Setup

Build and run with Docker Compose:

```bash
docker-compose up -d
```

Or build and run manually:

```bash
docker build -t burroughs-stat-tracker .
docker run -d --name burroughs-stat-tracker -v $(pwd)/config.json:/app/config.json:ro burroughs-stat-tracker
```

### 3. View Logs

```bash
docker-compose logs -f
```

Or for manual Docker run:

```bash
docker logs -f burroughs-stat-tracker
```

### 4. Stop the Service

```bash
docker-compose down
```

Or for manual Docker run:

```bash
docker stop burroughs-stat-tracker
docker rm burroughs-stat-tracker
```

## How It Works

The service runs in a continuous polling loop:

1. **Checks for new batches** in the source database table
2. **Processes any new batches** found (separately for Recyclers and Smart Safes)
3. **Waits 5 minutes** (configurable) before checking again
4. **Repeats** indefinitely

The polling interval is configurable in `config.json` (default: 5 minutes). The service automatically handles:
- Duplicate batch detection (won't process the same batch twice)
- Irregular update intervals
- Database connection errors (retries after the poll interval)

## Configuration

The `config.json` file contains:

- **Database credentials**: Host, user, password, database name
- **Polling settings**: Interval in minutes (default: 5 minutes)
- **Table names**: Source table and destination tables for Recyclers and Smart Safes
- **Daily summary settings**: End-of-day time (default: 11:59 PM CST)
- **Tracking database**: Connection details for tracking information queries
- **FedEx API**: API credentials for FedEx package tracking
- **UPS API**: API credentials for UPS package tracking
- **Aggregation settings**: Configuration for hourly and daily aggregation

See `config.json.example` for the full configuration structure.

## Development

Run locally without Docker:

```bash
pip install -r requirements.txt
python main.py
```

## Project Structure

```
burroughs_stat_tracker/
├── main.py                    # Entry point - starts polling loop
├── app/
│   ├── __init__.py
│   ├── config/               # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py      # Loads and validates config.json
│   ├── controllers/          # Orchestration layer
│   │   ├── __init__.py
│   │   └── batch_controller.py
│   ├── services/             # Business logic
│   │   ├── __init__.py
│   │   ├── batch_stats.py    # Batch-level statistics
│   │   ├── daily_summary.py  # Daily aggregation
│   │   ├── hourly_aggregator.py # Hourly aggregation
│   │   ├── tracking.py       # Tracking database integration
│   │   ├── fedex_tracker.py  # FedEx API integration
│   │   └── ups_tracker.py    # UPS API integration
│   ├── data/                 # Data access layer
│   │   ├── __init__.py
│   │   └── database.py       # Database connections and schema
│   ├── utils/                # Utility functions
│   │   ├── __init__.py
│   │   ├── data.py           # Data processing utilities
│   │   ├── equipment.py      # Equipment type detection
│   │   ├── timezone.py       # Timezone handling
│   │   └── tracking_parser.py # Tracking number parsing
│   └── models/               # Data models (reserved for future use)
│       └── __init__.py
├── config.json               # Configuration file (not in version control)
├── config.json.example       # Example configuration template
├── requirements.txt          # Python dependencies
├── Dockerfile               # Docker image definition
├── docker-compose.yml        # Docker Compose configuration
├── ARCHITECTURE.md           # Application architecture documentation
└── DATABASE_STRUCTURE.md     # Database documentation
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture documentation.

## Notes

- The service requires network access to your SQL Server database
- Ensure `config.json` is properly configured before starting
- The service runs as a non-root user inside the container for security

