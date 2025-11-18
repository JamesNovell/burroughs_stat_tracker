# Application Architecture

This document provides a comprehensive overview of the Burroughs Statistics Tracker application architecture, designed to help AI assistants and developers understand the codebase structure and data flow.

## Overview

The Burroughs Statistics Tracker is a Python application that monitors a SQL Server database for new batches of service call data, processes statistics, and tracks package delivery status via FedEx and UPS APIs.

## Application Flow

```
main.py
  └─> app.controllers.poll_for_batches()
       └─> app.controllers.process_batch()
            ├─> app.data.database.get_db_connection()
            ├─> app.data.database.create_tables_if_not_exist()
            ├─> app.services.tracking.TrackingService (for each record)
            │    ├─> Query tracking database (pyodbc)
            │    ├─> app.services.fedex_tracker (if FedEx tracking number)
            │    └─> app.services.ups_tracker (if UPS tracking number)
            ├─> app.services.batch_stats.process_equipment_type_stats() (Recyclers)
            ├─> app.services.batch_stats.process_equipment_type_stats() (Smart Safes)
            ├─> app.services.hourly_aggregator.aggregate_hourly_stats() (if hour boundary crossed)
            └─> app.services.daily_summary.calculate_daily_summary() (if EOD)
```

## Directory Structure

```
burroughs_stat_tracker/
├── main.py                    # Entry point - starts polling loop
├── config.json                # Configuration (not in git)
├── config.json.example        # Configuration template
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container definition
├── docker-compose.yml         # Container orchestration
├── README.md                  # User documentation
├── ARCHITECTURE.md            # This file - architecture documentation
├── DATABASE_STRUCTURE.md      # Database schema documentation
└── app/                       # Application package
    ├── __init__.py            # Package initialization
    ├── config/                # Configuration management
    │   ├── __init__.py        # Exports config constants
    │   └── settings.py        # Loads and validates config.json
    ├── controllers/           # Orchestration layer
    │   ├── __init__.py         # Exports controller functions
    │   └── batch_controller.py # Main batch processing logic
    ├── services/              # Business logic layer
    │   ├── __init__.py        # Exports service functions
    │   ├── batch_stats.py     # Batch-level statistics calculation
    │   ├── batch_service.py   # Batch query utilities
    │   ├── daily_summary.py   # Daily aggregation logic
    │   ├── hourly_aggregator.py # Hourly aggregation logic
    │   ├── tracking.py        # Tracking database integration
    │   ├── fedex_tracker.py   # FedEx API integration
    │   └── ups_tracker.py     # UPS API integration
    ├── data/                  # Data access layer
    │   ├── __init__.py        # Exports database functions
    │   └── database.py        # Database connections and schema
    ├── utils/                 # Utility functions
    │   ├── __init__.py        # Exports utility functions
    │   ├── data.py            # Data processing utilities
    │   ├── equipment.py       # Equipment type detection
    │   ├── timezone.py        # Timezone handling (CST)
    │   └── tracking_parser.py # Tracking number parsing
    └── models/                # Data models (currently unused)
        └── __init__.py        # Reserved for future use
```

## Layer Responsibilities

### Controllers (`app/controllers/`)
**Purpose**: Orchestration and coordination
- **batch_controller.py**: Main entry point for batch processing
  - `process_batch()`: Orchestrates the entire batch processing workflow
  - `poll_for_batches()`: Continuous polling loop

### Services (`app/services/`)
**Purpose**: Business logic and external integrations

- **batch_stats.py**: Calculates batch-level statistics
  - `process_equipment_type_stats()`: Processes stats for Recyclers or Smart Safes

- **batch_service.py**: Batch query utilities
  - `get_last_processed_timestamp()`: Gets last processed batch timestamp

- **daily_summary.py**: Daily aggregation
  - `calculate_daily_summary()`: Dispatcher for daily aggregation
  - `calculate_daily_summary_from_hourly()`: Aggregates from hourly stats
  - `calculate_daily_summary_from_raw()`: Aggregates from raw batch data

- **hourly_aggregator.py**: Hourly aggregation
  - `aggregate_hourly_stats()`: Aggregates batch stats into hourly summaries
  - `should_trigger_hourly_aggregation()`: Determines if aggregation should run
  - `validate_hourly_aggregation()`: Validates aggregation results

- **tracking.py**: Tracking database integration
  - `TrackingService`: Queries tracking database and updates tracking columns

- **fedex_tracker.py**: FedEx API integration
  - `is_fedex_tracking_number()`: Detects FedEx tracking numbers
  - `get_fedex_tracking_status()`: Queries FedEx API for status

- **ups_tracker.py**: UPS API integration
  - `is_ups_tracking_number()`: Detects UPS tracking numbers
  - `get_ups_tracking_status()`: Queries UPS API for status

### Data (`app/data/`)
**Purpose**: Database access and schema management
- **database.py**: Database operations
  - `get_db_connection()`: Creates database connection
  - `create_tables_if_not_exist()`: Ensures all tables exist
  - Table creation functions for stat, history, hourly, and daily tables
  - `ensure_tracking_columns_exist()`: Adds tracking columns to source table

### Utils (`app/utils/`)
**Purpose**: Reusable utility functions

- **data.py**: Data processing
  - `deduplicate_records()`: Removes duplicate records, keeps latest

- **equipment.py**: Equipment type detection
  - `is_recycler()`: Checks if equipment is a recycler
  - `filter_by_equipment_type()`: Filters calls by equipment type

- **timezone.py**: Timezone handling
  - `to_cst()`: Converts datetime to CST
  - `get_cst_date()`: Gets date in CST
  - `is_end_of_day_cst()`: Checks if datetime is at EOD

- **tracking_parser.py**: Tracking number parsing
  - `determine_tracking_number()`: Extracts tracking number from query results
  - `extract_latest_parts()`: Extracts latest parts list

### Config (`app/config/`)
**Purpose**: Configuration management
- **settings.py**: Loads and validates `config.json`
  - Exports all configuration constants
  - Validates required fields

## Data Flow

### 1. Batch Processing Flow

1. **Poll for new batch** (`batch_controller.poll_for_batches`)
   - Queries source table for latest `"Pushed At"` timestamp
   - Checks if batch has been processed

2. **Fetch batch data** (`batch_controller.process_batch`)
   - Fetches all records for latest batch
   - Deduplicates records (keeps highest ID per Service_Call_ID)

3. **Process tracking** (`TrackingService`)
   - For each record, queries tracking database using Vendor Call Number
   - Extracts tracking number and parts
   - Checks if tracking number matches in DesNote or PartNote
   - If FedEx/UPS tracking number detected, queries carrier API for status
   - Updates `querytrackingnumber`, `queryparts`, `trackingmatch`, `tracking_status` columns

4. **Calculate statistics** (`batch_stats.process_equipment_type_stats`)
   - Separates Recyclers and Smart Safes
   - Calculates batch-level metrics
   - Writes to stat tables
   - Writes closed calls to history tables

5. **Hourly aggregation** (`hourly_aggregator.aggregate_hourly_stats`)
   - Triggered when hour boundary is crossed
   - Aggregates all batches from completed hour
   - Writes to hourly stat tables

6. **Daily summary** (`daily_summary.calculate_daily_summary`)
   - Triggered at configured EOD time (default 11:59 PM CST)
   - Aggregates from hourly stats (if configured) or raw batch data
   - Calculates 24-hour rolling window metrics
   - Writes to daily summary tables

## Database Tables

### Source Table
- **`dbo.Burroughs_Open_Calls`**: Source data (read-only)
  - Updated by external system in batches
  - Contains all open service calls
  - Columns added by this app: `querytrackingnumber`, `queryparts`, `trackingmatch`, `tracking_status`

### Stat Tables (per equipment type)
- **`Burroughs_Recyclers_Stat`** / **`Burroughs_Smart_Safes_Stat`**
  - One row per batch processed
  - Batch-level statistics

### History Tables (per equipment type)
- **`Burroughs_Recyclers_Closed_Call_History`** / **`Burroughs_Smart_Safes_Closed_Call_History`**
  - One row per closed call
  - Tracks closure timestamp and final state

### Hourly Tables (per equipment type)
- **`Burroughs_Recyclers_Hourly_Stat`** / **`Burroughs_Smart_Safes_Hourly_Stat`**
  - One row per hour
  - Aggregated hourly statistics

### Daily Tables (per equipment type)
- **`Burroughs_Recyclers_Daily_Summary`** / **`Burroughs_Smart_Safes_Daily_Summary`**
  - One row per day (at EOD)
  - Daily aggregated metrics

## Configuration

All configuration is stored in `config.json` (not in git). See `config.json.example` for structure.

### Key Configuration Sections

- **database**: Main database connection (pymssql)
- **tracking_database**: Tracking database connection (pyodbc)
- **fedex_api**: FedEx API credentials
- **ups_api**: UPS API credentials
- **polling**: Polling interval settings
- **daily_summary**: EOD time configuration
- **tables**: Table name configuration
- **aggregation**: Hourly/daily aggregation settings

## External Dependencies

### APIs
- **FedEx Tracking API**: Package tracking status
- **UPS Tracking API**: Package tracking status

### Databases
- **Main Database** (pymssql): Source data and stat tables
- **Tracking Database** (pyodbc): Service call tracking information

## Error Handling

- Database errors: Logged and retried after poll interval
- API errors: Logged, processing continues without status
- Missing data: Handled gracefully with defaults
- Duplicate batches: Detected and skipped

## Timezone Handling

All time-based operations use CST (America/Chicago) timezone:
- Batch timestamps converted to CST
- EOD calculations in CST
- Hourly aggregation boundaries in CST

## Key Design Decisions

1. **Separation by Equipment Type**: Recyclers and Smart Safes processed separately
2. **Hierarchical Aggregation**: Batch → Hourly → Daily
3. **Idempotent Processing**: Batches can be processed multiple times safely
4. **Read-Only Source**: Never modifies source table, only adds tracking columns
5. **Graceful Degradation**: API failures don't stop batch processing

