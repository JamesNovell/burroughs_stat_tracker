# Open Call Database Structure Documentation

## Overview

This document explains the structure and operation of the source database table `dbo.Burroughs_Open_Calls` and how the stat tracker system interacts with it.

## Source Table: `dbo.Burroughs_Open_Calls`

### Purpose
The source table contains all open service calls for Burroughs equipment. It is updated periodically (approximately hourly) by an external system and serves as the primary data source for the statistics tracker.

### Table Schema

The table contains the following columns (as used by the stat tracker):

| Column Name | Type | Description |
|------------|------|-------------|
| `ID` | INT | Primary key/row identifier. Used for deduplication - higher ID = more recent record |
| `Service_Call_ID` | VARCHAR | Unique identifier for each service call (e.g., "SC-12345") |
| `Appt. Status` | VARCHAR | Current appointment status (e.g., "Scheduled", "In Progress", "Completed") |
| `Appointment` | INT | Appointment number for this call (1 = first appointment, 2 = second, etc.) |
| `Open DateTime` | DATETIME | When the service call was originally opened |
| `Batch ID` | BIGINT | Identifier for which batch this record belongs to |
| `Pushed At` | DATETIME | Timestamp when this batch was pushed to the database |
| `Equipment_ID` | VARCHAR | Equipment identifier (used to distinguish Recyclers vs Smart Safes) |
| `Vendor Call Number` | VARCHAR | Vendor's call tracking number |

### Key Characteristics

#### 1. Batch-Based Updates
- The table is updated in **batches** (approximately hourly)
- All records in a batch share the same `"Pushed At"` timestamp
- All records in a batch share the same `"Batch ID"`
- The system identifies batches by querying for distinct `"Pushed At"` values

#### 2. Multiple Entries Per Call
- **A service call can appear multiple times** in the table
- Each update creates new rows with the same `Service_Call_ID` but different `ID` values
- The code deduplicates by keeping the record with the **highest `ID`** for each `Service_Call_ID`
- This allows tracking the history of a call as it progresses through appointments

#### 3. Append-Only Design
- The table is **append-only** - old records are never deleted
- New batches add new rows, but old rows remain in the table
- The system identifies the "current state" by finding the latest batch

#### 4. Call Lifecycle Tracking
- **Open calls**: Appear in every batch until they are closed
- **Closed calls**: Disappear from the table (no longer appear in the latest batch)
- The system detects closures by comparing batches:
  - If a `Service_Call_ID` was in the previous batch but NOT in the current batch → it closed
  - If a `Service_Call_ID` appears in the current batch → it's still open

### Example Data Structure

```
ID  | Service_Call_ID | Appointment | Appt. Status | Open DateTime | Batch ID | Pushed At           | Equipment_ID | Vendor Call Number
----|-----------------|-------------|--------------|---------------|----------|---------------------|--------------|-------------------
100 | SC-12345        | 1           | Scheduled    | 2025-11-14    | 5001     | 2025-11-14 10:00:00 | N4R-001     | VC-001
101 | SC-12345        | 2           | In Progress  | 2025-11-14    | 5002     | 2025-11-14 11:00:00 | N4R-001     | VC-001
102 | SC-12346        | 1           | Scheduled    | 2025-11-14    | 5002     | 2025-11-14 11:00:00 | SS-002      | VC-002
103 | SC-12347        | 1           | Scheduled    | 2025-11-14    | 5002     | 2025-11-14 11:00:00 | N9R-003     | VC-003
```

In this example:
- `SC-12345` has progressed from appointment 1 to appointment 2 (two rows with different IDs)
- `SC-12346` and `SC-12347` are new calls in batch 5002
- All records in batch 5002 have the same `Pushed At` timestamp

## How the Stat Tracker System Uses This Table

### 1. Polling for New Batches

The system polls the database every 5 minutes (configurable) to check for new batches:

```sql
-- Find the latest batch timestamp
SELECT DISTINCT TOP 1 "Pushed At" 
FROM dbo.Burroughs_Open_Calls 
ORDER BY "Pushed At" DESC;

-- Get the Batch ID for that timestamp
SELECT TOP 1 "Batch ID" 
FROM dbo.Burroughs_Open_Calls 
WHERE "Pushed At" = ?;
```

### 2. Deduplication Process

When fetching records for a batch, the system deduplicates by keeping only the most recent record for each `Service_Call_ID`:

```python
# Records are sorted by ID descending
# The first occurrence of each Service_Call_ID is kept (highest ID = most recent)
unique_calls = {}
for record in sorted(records, key=lambda x: x['ID'], reverse=True):
    call_id = record['Service_Call_ID']
    if call_id not in unique_calls:
        unique_calls[call_id] = record
```

### 3. Batch Comparison

The system compares the latest batch to the previous batch to identify:

- **New calls**: Present in latest batch, not in previous batch
- **Closed calls**: Present in previous batch, not in latest batch
- **Updated calls**: Present in both, but with different appointment numbers or statuses

### 4. Equipment Type Filtering

The system uses `Equipment_ID` prefix to separate equipment types:

- **Recyclers**: Equipment IDs starting with `N4R`, `N9R`, `N7F`, or `RF`
- **Smart Safes**: All other equipment IDs

This separation allows independent statistics tracking for each equipment type.

### 5. Query Pattern

The standard query pattern used throughout the system:

```sql
SELECT 
    "ID", 
    "Service_Call_ID", 
    "Appt. Status", 
    "Appointment", 
    "Open DateTime", 
    "Batch ID", 
    "Pushed At", 
    "Equipment_ID", 
    "Vendor Call Number"
FROM dbo.Burroughs_Open_Calls 
WHERE "Pushed At" = ?;
```

This fetches all records for a specific batch, which are then deduplicated in memory.

## Important Notes for Developers

1. **Never modify the source table** - The stat tracker is read-only. It never inserts, updates, or deletes from `dbo.Burroughs_Open_Calls`.

2. **Batch identification** - Always use `"Pushed At"` to identify batches. The `"Batch ID"` is used for tracking which batches have been processed.

3. **Deduplication is critical** - Always deduplicate records by `Service_Call_ID` using the highest `ID` value before processing.

4. **Timezone handling** - All datetime comparisons should be done in CST timezone (America/Chicago) using the helper functions in `helpers.py`.

5. **Missing data handling** - Some fields like `Equipment_ID` or `Vendor Call Number` may be NULL. Always use `.get()` with defaults when accessing these fields.

6. **Batch processing** - The system tracks processed batches by storing `BatchID` in the stat tables. This prevents duplicate processing if the script runs multiple times.

## Related Tables Created by the Stat Tracker

The stat tracker creates and maintains the following tables (separate for Recyclers and Smart Safes):

1. **Stat Tables** (`Burroughs_Recyclers_Stat`, `Burroughs_Smart_Safes_Stat`)
   - One row per batch processed
   - Contains batch-level statistics (open calls, closed calls, rates, etc.)

2. **History Tables** (`Burroughs_Recyclers_Closed_Call_History`, `Burroughs_Smart_Safes_Closed_Call_History`)
   - One row per closed call
   - Tracks when calls closed and their final state

3. **Daily Summary Tables** (`Burroughs_Recyclers_Daily_Summary`, `Burroughs_Smart_Safes_Daily_Summary`)
   - One row per day (at end of day)
   - Contains aggregated daily metrics

These tables are created automatically if they don't exist when the script runs.

## Configuration

The source table name is configurable in `config.json`:

```json
{
  "tables": {
    "source_table": "dbo.Burroughs_Open_Calls"
  }
}
```

By default, it uses `dbo.Burroughs_Open_Calls`, but this can be changed if the table structure is the same but the name differs.

