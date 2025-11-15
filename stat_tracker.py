import os
import pymssql
import json
from collections import Counter
from datetime import datetime, timedelta

# --- Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "master")
STAT_TABLE = "Burroughs_Open_Calls_Stat"
HISTORY_TABLE = "Burroughs_Closed_Call_History"
SOURCE_TABLE = "dbo.Burroughs_Open_Calls"

# --- Database Schema Management ---
def create_tables_if_not_exist(cursor):
    """Ensures the necessary statistics and history tables exist."""
    # Schema for the main statistics table
    stat_table_schema = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{STAT_TABLE}' and xtype='U')
    CREATE TABLE {STAT_TABLE} (
        StatID INT IDENTITY(1,1) PRIMARY KEY,
        Timestamp DATETIME,
        BatchID BIGINT,
        TotalOpenCalls INT,
        CallsClosedSinceLastBatch INT,
        SameDayClosures INT,
        CallsWithMultipleAppointments INT,
        AverageAppointmentNumber FLOAT,
        StatusSummary NVARCHAR(MAX),
        SameDayCloseRate FLOAT,
        AvgAppointmentsPerCompletedCall FLOAT,
        FirstTimeFixRate FLOAT,
        FourteenDayReopenRate FLOAT
    );
    """
    cursor.execute(stat_table_schema)

    # Schema for the closed call history table
    history_table_schema = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{HISTORY_TABLE}' and xtype='U')
    CREATE TABLE {HISTORY_TABLE} (
        Service_Call_ID VARCHAR(255) NOT NULL,
        ClosedTimestamp DATETIME NOT NULL,
        OpenDateTime DATETIME,
        PRIMARY KEY (Service_Call_ID, ClosedTimestamp)
    );
    """
    cursor.execute(history_table_schema)

# --- Data Processing Helpers ---
def deduplicate_records(records):
    """Given a list of records for a batch, return a dict of unique calls, keeping the most recent entry."""
    # Sort by ID descending to ensure the first one we see for a Service_Call_ID is the latest
    unique_calls = {}
    for record in sorted(records, key=lambda x: x['ID'], reverse=True):
        call_id = record['Service_Call_ID']
        if call_id not in unique_calls:
            unique_calls[call_id] = record
    return unique_calls

# --- Main Logic ---
def get_db_stats():
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("Error: Database credentials must be set as environment variables.")
        return

    conn = None
    try:
        conn = pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        conn.autocommit(True)
        cursor = conn.cursor(as_dict=True)

        # 1. Ensure tables exist
        create_tables_if_not_exist(cursor)

        # 2. Get the two most recent batches
        distinct_batches_query = f"SELECT DISTINCT TOP 2 \"Pushed At\" FROM {SOURCE_TABLE} ORDER BY \"Pushed At\" DESC;"
        cursor.execute(distinct_batches_query)
        batches = cursor.fetchall()

        if not batches:
            print(f"No data found in {SOURCE_TABLE}.")
            return

        latest_pushed_at = batches[0]["Pushed At"]
        previous_pushed_at = batches[1]["Pushed At"] if len(batches) > 1 else None

        # 3. Fetch and process records for both batches
        cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID"'
        
        cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (latest_pushed_at,))
        latest_records_raw = cursor.fetchall()
        latest_batch_id = latest_records_raw[0]['Batch ID'] if latest_records_raw else 0
        latest_calls = deduplicate_records(latest_records_raw)

        previous_calls = {}
        if previous_pushed_at:
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at,))
            previous_records_raw = cursor.fetchall()
            previous_calls = deduplicate_records(previous_records_raw)

        # 4. --- Calculate Metrics ---
        # A. Current Open Call Stats
        total_open_calls = len(latest_calls)
        status_counts = Counter(rec['Appt. Status'] for rec in latest_calls.values())
        calls_with_multi_appt = sum(1 for rec in latest_calls.values() if int(rec['Appointment']) >= 2)
        total_appointments = sum(int(rec['Appointment']) for rec in latest_calls.values())
        avg_appt_num = total_appointments / total_open_calls if total_open_calls > 0 else 0

        # B. Closed Call & Rate Stats
        closed_call_ids = set(previous_calls.keys()) - set(latest_calls.keys())
        same_day_closures = 0
        first_time_fixes = 0
        completed_call_appointment_numbers = []

        for call_id in closed_call_ids:
            prev_rec = previous_calls[call_id]
            if prev_rec['Open DateTime'].date() == latest_pushed_at.date():
                same_day_closures += 1
            if int(prev_rec['Appointment']) == 1:
                first_time_fixes += 1
            completed_call_appointment_numbers.append(int(prev_rec['Appointment']))
            
            # Log to history table
            merge_sql = f"""
            MERGE {HISTORY_TABLE} AS target
            USING (SELECT %s AS Service_Call_ID, %s AS ClosedTimestamp, %s AS OpenDateTime) AS source
            ON (target.Service_Call_ID = source.Service_Call_ID AND target.ClosedTimestamp = source.ClosedTimestamp)
            WHEN NOT MATCHED THEN
                INSERT (Service_Call_ID, ClosedTimestamp, OpenDateTime)
                VALUES (source.Service_Call_ID, source.ClosedTimestamp, source.OpenDateTime);
            """
            cursor.execute(merge_sql, (call_id, latest_pushed_at, prev_rec['Open DateTime']))

        # Calculate rates
        total_closed = len(closed_call_ids)
        same_day_close_rate = same_day_closures / total_closed if total_closed > 0 else 0
        first_time_fix_rate = first_time_fixes / total_closed if total_closed > 0 else 0
        avg_appt_per_completed = sum(completed_call_appointment_numbers) / total_closed if total_closed > 0 else 0

        # C. Reopen Rate
        newly_opened_call_ids = set(latest_calls.keys()) - set(previous_calls.keys())
        reopened_calls = 0
        fourteen_days_ago = latest_pushed_at - timedelta(days=14)

        if newly_opened_call_ids:
            # Use a single query to check all new calls against history
            placeholders = ', '.join(['%s'] * len(newly_opened_call_ids))
            check_reopen_sql = f"""
            SELECT Service_Call_ID FROM {HISTORY_TABLE}
            WHERE Service_Call_ID IN ({placeholders}) AND ClosedTimestamp >= %s;
            """
            cursor.execute(check_reopen_sql, list(newly_opened_call_ids) + [fourteen_days_ago])
            reopened_calls = cursor.rowcount

        reopen_rate = reopened_calls / len(newly_opened_call_ids) if newly_opened_call_ids else 0

        # 5. --- Log Stats to Database ---
        stats_to_insert = {
            "Timestamp": latest_pushed_at, "BatchID": latest_batch_id, "TotalOpenCalls": total_open_calls,
            "CallsClosedSinceLastBatch": total_closed, "SameDayClosures": same_day_closures,
            "CallsWithMultipleAppointments": calls_with_multi_appt, "AverageAppointmentNumber": avg_appt_num,
            "StatusSummary": json.dumps(status_counts), "SameDayCloseRate": same_day_close_rate,
            "AvgAppointmentsPerCompletedCall": avg_appt_per_completed, "FirstTimeFixRate": first_time_fix_rate,
            "FourteenDayReopenRate": reopen_rate
        }
        
        insert_cols = ", ".join(stats_to_insert.keys())
        insert_vals = ", ".join(["%s"] * len(stats_to_insert))
        insert_sql = f"INSERT INTO {STAT_TABLE} ({insert_cols}) VALUES ({insert_vals});"
        cursor.execute(insert_sql, list(stats_to_insert.values()))

        # 6. --- Print to Console ---
        print(f"\n--- Stats for Batch {latest_batch_id} ({latest_pushed_at}) ---")
        print(f"Successfully logged to {STAT_TABLE} table.")
        
        print("\n[ Current Open Calls ]")
        print(f"Total Open Calls: {total_open_calls}")
        print(f"Average Appointment Number: {avg_appt_num:.2f}")
        print(f"Calls with 2+ Appointments: {calls_with_multi_appt}")
        print("Status Breakdown:", json.dumps(status_counts))

        print("\n[ Closed Call KPIs ]")
        print(f"Calls Closed Since Last Batch: {total_closed}")
        print(f"Same-Day Close Rate: {same_day_close_rate:.2%}")
        print(f"First-Time Fix Rate: {first_time_fix_rate:.2%}")
        print(f"Average Appointments per Completed Call: {avg_appt_per_completed:.2f}")

        print("\n[ New & Reopened Call KPIs ]")
        print(f"Newly Opened Calls: {len(newly_opened_call_ids)}")
        print(f"14-Day Reopen Rate: {reopen_rate:.2%}")

        print("\n--- Awaiting Clarification ---")
        print("Remaining metrics can be implemented after clarification on:")
        print("- 'Repeat Dispatch Rate', 'Time to Dispatch', 'Unit Engagement Rate', 'Daily Volume'")

    except pymssql.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_db_stats()
