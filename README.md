# Burroughs Statistics Tracker

A Dockerized service that tracks and processes Burroughs service call statistics from a SQL Server database.

## Features

- Automatically polls for new batches every 5 minutes (configurable)
- Separates statistics for Recyclers and Smart Safes
- Tracks batch-level and daily summary statistics
- Handles irregular database updates gracefully

## Documentation

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
├── main.py                 # Small entry point - wires everything together
├── app/
│   ├── __init__.py
│   ├── config.py          # Settings, constants, configuration loading
│   ├── utils.py           # Helper functions (equipment filtering, timezone, deduplication)
│   ├── services.py        # Business logic (batch stats, daily summaries)
│   ├── controllers.py     # Orchestration (batch processing, polling)
│   └── data/
│       ├── __init__.py
│       └── db.py          # Database connections and table creation
├── config.json            # Configuration file (not in version control)
├── config.json.example    # Example configuration template
├── requirements.txt       # Python dependencies
├── Dockerfile            # Docker image definition
├── docker-compose.yml    # Docker Compose configuration
└── DATABASE_STRUCTURE.md # Database documentation
```

## Notes

- The service requires network access to your SQL Server database
- Ensure `config.json` is properly configured before starting
- The service runs as a non-root user inside the container for security

