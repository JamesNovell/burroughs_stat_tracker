# Quick Reference Guide

This document provides quick reference information for AI assistants and developers working with the codebase.

## Entry Points

- **`main.py`**: Application entry point → calls `poll_for_batches()`
- **`app/controllers/batch_controller.py`**: Main orchestration
  - `process_batch()`: Processes a single batch
  - `poll_for_batches()`: Continuous polling loop

## Key Functions by Category

### Batch Processing
- `app/controllers/batch_controller.process_batch()`: Main batch processing workflow
- `app/services/batch_stats.process_equipment_type_stats()`: Calculate batch-level stats
- `app/services/batch_service.get_last_processed_timestamp()`: Get last processed batch

### Aggregation
- `app/services/hourly_aggregator.aggregate_hourly_stats()`: Aggregate batches into hourly stats
- `app/services/hourly_aggregator.should_trigger_hourly_aggregation()`: Check if aggregation needed
- `app/services/daily_summary.calculate_daily_summary()`: Calculate daily summary (dispatcher)
- `app/services/daily_summary.calculate_daily_summary_from_hourly()`: Aggregate from hourly
- `app/services/daily_summary.calculate_daily_summary_from_raw()`: Aggregate from raw batches

### Tracking
- `app/services/tracking.TrackingService.query_tracking_info()`: Query tracking database
- `app/services/tracking.TrackingService.update_tracking_columns()`: Update tracking columns
- `app/services/fedex_tracker.get_fedex_tracking_status()`: Get FedEx tracking status
- `app/services/ups_tracker.get_ups_tracking_status()`: Get UPS tracking status

### Utilities
- `app/utils/data.deduplicate_records()`: Remove duplicates, keep latest
- `app/utils/equipment.is_recycler()`: Check if equipment is recycler
- `app/utils/equipment.filter_by_equipment_type()`: Filter by equipment type
- `app/utils/timezone.to_cst()`: Convert to CST timezone
- `app/utils/timezone.is_end_of_day_cst()`: Check if EOD
- `app/utils/tracking_parser.determine_tracking_number()`: Extract tracking number
- `app/utils/tracking_parser.extract_latest_parts()`: Extract latest parts

### Database
- `app/data/database.get_db_connection()`: Get database connection
- `app/data/database.create_tables_if_not_exist()`: Ensure tables exist
- `app/data/database.ensure_tracking_columns_exist()`: Add tracking columns

## Configuration Constants

All config constants are exported from `app.config`:
- Database: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- Tables: `SOURCE_TABLE`, `RECYCLERS_STAT_TABLE`, `SMART_SAFES_STAT_TABLE`, etc.
- Polling: `POLL_INTERVAL_MINUTES`
- EOD: `EOD_HOUR`, `EOD_MINUTE`
- Tracking DB: `TRACKING_DB_*` constants
- FedEx: `FEDEX_API_KEY`, `FEDEX_API_SECRET`, `FEDEX_USE_PRODUCTION`
- UPS: `UPS_CLIENT_ID`, `UPS_CLIENT_SECRET`
- Aggregation: `HOURLY_AGGREGATION_ENABLED`, `DAILY_AGGREGATION_ENABLED`, etc.

## Data Flow Summary

1. **Poll** → Check for new batch in source table
2. **Fetch** → Get all records for latest batch
3. **Deduplicate** → Keep latest record per Service_Call_ID
4. **Track** → Query tracking DB, update tracking columns, check FedEx/UPS
5. **Process** → Calculate stats for Recyclers and Smart Safes separately
6. **Aggregate** → If hour boundary crossed, aggregate hourly stats
7. **Summarize** → If EOD, calculate daily summary
8. **Repeat** → Wait and poll again

## Equipment Types

- **Recyclers**: Equipment IDs starting with `N4R`, `N9R`, `N7F`, or `RF`
- **Smart Safes**: All other equipment IDs

## Tracking Number Detection

- **FedEx**: 12/15 digit numbers or alphanumeric patterns
- **UPS**: 18-char format starting with `1Z`, or 9/11 digit numbers

## Timezone

All operations use **CST (America/Chicago)** timezone.

## Common Patterns

### Processing a Batch
```python
from app.controllers import process_batch
result = process_batch()  # Returns True if batch processed
```

### Getting Tracking Status
```python
from app.services.fedex_tracker import get_fedex_tracking_status
status = get_fedex_tracking_status("123456789012")
```

### Filtering by Equipment Type
```python
from app.utils.equipment import filter_by_equipment_type, is_recycler
recyclers = filter_by_equipment_type(calls, is_recycler_filter=True)
```

### Timezone Conversion
```python
from app.utils.timezone import to_cst, get_cst_date
cst_time = to_cst(naive_datetime)
date = get_cst_date(datetime)
```

## Error Handling Patterns

- Database errors: Logged, retried after poll interval
- API errors: Logged, processing continues
- Missing data: Defaults used (empty dict, 0, None)
- Duplicate batches: Detected and skipped

## Testing Considerations

- All functions are pure or have minimal side effects
- Database operations are idempotent
- Configuration is loaded at module import time
- Timezone operations are deterministic

