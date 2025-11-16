"""Business logic services for batch statistics and daily summaries."""
import json
from collections import Counter
from datetime import datetime, timedelta
from app.utils import filter_by_equipment_type, get_cst_date, to_cst, is_recycler, is_end_of_day_cst, CST, deduplicate_records
from app.config import SOURCE_TABLE, EOD_HOUR, EOD_MINUTE


def process_equipment_type_stats(cursor, latest_calls, previous_calls, latest_pushed_at, latest_batch_id, 
                                  stat_table, history_table, equipment_type_name, is_recycler_type):
    """Process and save statistics for a specific equipment type."""
    # Filter calls by equipment type
    latest_filtered = filter_by_equipment_type(latest_calls, is_recycler_type)
    previous_filtered = filter_by_equipment_type(previous_calls, is_recycler_type) if previous_calls else {}
    
    # A. Current Open Call Stats
    total_open_calls = len(latest_filtered)
    status_counts = Counter(rec['Appt. Status'] for rec in latest_filtered.values())
    calls_with_multi_appt = sum(1 for rec in latest_filtered.values() if int(rec['Appointment']) >= 2)
    total_appointments = sum(int(rec['Appointment']) for rec in latest_filtered.values())
    avg_appt_num = total_appointments / total_open_calls if total_open_calls > 0 else 0

    # B. Closed Call & Rate Stats
    closed_call_ids = set(previous_filtered.keys()) - set(latest_filtered.keys()) if previous_filtered else set()
    same_day_closures = 0
    first_time_fixes = 0
    completed_call_appointment_numbers = []

    for call_id in closed_call_ids:
        prev_rec = previous_filtered[call_id]
        # Compare dates in CST timezone
        open_date_cst = get_cst_date(prev_rec['Open DateTime'])
        pushed_date_cst = get_cst_date(latest_pushed_at)
        if open_date_cst and pushed_date_cst and open_date_cst == pushed_date_cst:
            same_day_closures += 1
        if int(prev_rec['Appointment']) == 1:
            first_time_fixes += 1
        completed_call_appointment_numbers.append(int(prev_rec['Appointment']))
        
        # Log to history table
        merge_sql = f"""
        MERGE {history_table} AS target
        USING (SELECT %s AS Service_Call_ID, %s AS ClosedTimestamp, %s AS OpenDateTime, %s AS Equipment_ID, %s AS VendorCallNumber) AS source
        ON (target.Service_Call_ID = source.Service_Call_ID AND target.ClosedTimestamp = source.ClosedTimestamp)
        WHEN NOT MATCHED THEN
            INSERT (Service_Call_ID, ClosedTimestamp, OpenDateTime, Equipment_ID, VendorCallNumber)
            VALUES (source.Service_Call_ID, source.ClosedTimestamp, source.OpenDateTime, source.Equipment_ID, source.VendorCallNumber);
        """
        equipment_id = prev_rec.get('Equipment_ID', None)
        vendor_call_number = prev_rec.get('Vendor Call Number', None)
        cursor.execute(merge_sql, (call_id, latest_pushed_at, prev_rec['Open DateTime'], equipment_id, vendor_call_number))

    # Calculate rates
    total_closed = len(closed_call_ids)
    same_day_close_rate = same_day_closures / total_closed if total_closed > 0 else 0
    first_time_fix_rate = first_time_fixes / total_closed if total_closed > 0 else 0
    avg_appt_per_completed = sum(completed_call_appointment_numbers) / total_closed if total_closed > 0 else 0

    # C. Reopen Rate
    newly_opened_call_ids = set(latest_filtered.keys()) - set(previous_filtered.keys()) if previous_filtered else set(latest_filtered.keys())
    reopened_calls = 0
    # Calculate 14 days ago in CST
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    fourteen_days_ago = (latest_pushed_at_cst - timedelta(days=14)).replace(tzinfo=None)  # Remove timezone for SQL query

    if newly_opened_call_ids:
        # Use a single query to check all new calls against history
        placeholders = ', '.join(['%s'] * len(newly_opened_call_ids))
        check_reopen_sql = f"""
        SELECT Service_Call_ID FROM {history_table}
        WHERE Service_Call_ID IN ({placeholders}) AND ClosedTimestamp >= %s;
        """
        # pymssql requires tuple or dict, not list - convert to tuple
        params = tuple(newly_opened_call_ids) + (fourteen_days_ago,)
        cursor.execute(check_reopen_sql, params)
        # pymssql: rowcount is unreliable for SELECT until rows are fetched
        # Must fetch all results before using rowcount or use len() on fetchall()
        reopened_results = cursor.fetchall()
        reopened_calls = len(reopened_results)

    reopen_rate = reopened_calls / len(newly_opened_call_ids) if newly_opened_call_ids else 0

    # Log Stats to Database
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
    insert_sql = f"INSERT INTO {stat_table} ({insert_cols}) VALUES ({insert_vals});"
    cursor.execute(insert_sql, list(stats_to_insert.values()))

    # Print to Console
    print(f"\n--- {equipment_type_name} Stats for Batch {latest_batch_id} ({latest_pushed_at}) ---")
    print(f"Successfully logged to {stat_table} table.")
    
    if not previous_filtered:
        print(f"\n[Note: This is the first {equipment_type_name} batch processed. No comparison data available.]")
    
    print(f"\n[ {equipment_type_name} - Current Open Calls ]")
    print(f"Total Open Calls: {total_open_calls}")
    print(f"Average Appointment Number: {avg_appt_num:.2f}")
    print(f"Calls with 2+ Appointments: {calls_with_multi_appt}")
    print("Status Breakdown:", json.dumps(status_counts))

    print(f"\n[ {equipment_type_name} - Closed Call KPIs ]")
    if previous_filtered:
        print(f"Calls Closed Since Last Batch: {total_closed}")
        print(f"Same-Day Close Rate: {same_day_close_rate:.2%}")
        print(f"First-Time Fix Rate: {first_time_fix_rate:.2%}")
        print(f"Average Appointments per Completed Call: {avg_appt_per_completed:.2f}")
    else:
        print("(No previous batch to compare - metrics will be available after next update)")

    print(f"\n[ {equipment_type_name} - New & Reopened Call KPIs ]")
    if previous_filtered:
        print(f"Newly Opened Calls: {len(newly_opened_call_ids)}")
        print(f"14-Day Reopen Rate: {reopen_rate:.2%}")
    else:
        print(f"Total Calls in Batch: {len(newly_opened_call_ids)}")
        print("(Reopen rate will be calculated after next update)")


def calculate_daily_summary(cursor, latest_pushed_at, latest_calls, stat_table, history_table, daily_table, 
                            equipment_type_name, is_recycler_type):
    """Calculate and store daily summary at configured end of day time (default 11:59 PM CST).
    Calculates metrics for the last 24 hours from the EOD time."""
    # Check if it's end of day
    if not is_end_of_day_cst(latest_pushed_at):
        return
    
    # Get the EOD time in CST for the current date
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    # Calculate the EOD datetime for this date
    eod_datetime_cst = CST.localize(datetime.combine(current_date_cst, datetime.time(EOD_HOUR, EOD_MINUTE)))
    
    # Calculate 24 hours before EOD
    start_time_cst = eod_datetime_cst - timedelta(hours=24)
    
    print(f"\n=== Calculating Daily Summary for {equipment_type_name} - {current_date_cst} ===")
    print(f"Calculating for last 24 hours: {start_time_cst.strftime('%Y-%m-%d %H:%M:%S %Z')} to {eod_datetime_cst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Filter calls by equipment type
    latest_filtered = filter_by_equipment_type(latest_calls, is_recycler_type)
    
    # Get all batches from the last 24 hours (from start_time to EOD)
    batches_today_query = f"""
    SELECT DISTINCT \"Pushed At\" 
    FROM {SOURCE_TABLE} 
    WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s
    ORDER BY \"Pushed At\" ASC;
    """
    cursor.execute(batches_today_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    batches_today = cursor.fetchall()
    
    if not batches_today:
        print(f"No batches found for {current_date_cst}")
        return
    
    # Get all unique calls that appeared in any batch today
    cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number"'
    all_calls_today = {}
    
    for batch in batches_today:
        batch_time = batch["Pushed At"]
        cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (batch_time,))
        batch_records = cursor.fetchall()
        batch_calls = deduplicate_records(batch_records)
        
        # Update all_calls_today with latest state from each batch
        for call_id, record in batch_calls.items():
            if is_recycler(record.get('Equipment_ID', '')) == is_recycler_type:
                # Keep the most recent state of each call
                if call_id not in all_calls_today or batch_time > all_calls_today[call_id]['Pushed At']:
                    all_calls_today[call_id] = record
                    all_calls_today[call_id]['Pushed At'] = batch_time
    
    # Separate into open and closed calls
    open_calls_at_eod = {}
    closed_calls_today = []
    
    # Get calls that closed in the last 24 hours from history table
    closed_today_query = f"""
    SELECT Service_Call_ID, ClosedTimestamp, OpenDateTime, Equipment_ID
    FROM {history_table}
    WHERE ClosedTimestamp >= %s AND ClosedTimestamp <= %s;
    """
    cursor.execute(closed_today_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    closed_history = cursor.fetchall()
    
    # Build set of closed call IDs
    closed_call_ids_today = {rec['Service_Call_ID'] for rec in closed_history}
    
    # Get final appointment numbers for closed calls
    # The ClosedTimestamp is when we detected the close (the batch time when it disappeared)
    # So we need the appointment number from the batch RIGHT BEFORE the close
    closed_appointment_map = {}
    for closed_rec in closed_history:
        call_id = closed_rec['Service_Call_ID']
        closed_time = closed_rec['ClosedTimestamp']
        
        # Find the last batch BEFORE the close time (not at or after)
        # This gives us the appointment number when the call was still open
        last_batch_before_close = None
        for batch in batches_today:
            batch_time = batch["Pushed At"]
            if batch_time < closed_time:
                if last_batch_before_close is None or batch_time > last_batch_before_close:
                    last_batch_before_close = batch_time
        
        if last_batch_before_close:
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s AND \"Service_Call_ID\" = %s;", 
                         (last_batch_before_close, call_id))
            call_record = cursor.fetchone()
            if call_record:
                closed_appointment_map[call_id] = int(call_record['Appointment'])
    
    # Separate calls
    # First, process calls that appeared in batches within the 24-hour window
    for call_id, record in all_calls_today.items():
        if call_id in closed_call_ids_today:
            # This call closed today - use final appointment number
            if call_id in closed_appointment_map:
                closed_calls_today.append(closed_appointment_map[call_id])
        else:
            # Still open - use latest appointment number
            open_calls_at_eod[call_id] = int(record['Appointment'])
    
    # Bug 3 Fix: Process closed calls that don't appear in any batch within the 24-hour window
    # These are calls that were opened before the window and closed within it
    closed_calls_not_in_batches = closed_call_ids_today - set(all_calls_today.keys())
    
    for call_id in closed_calls_not_in_batches:
        # Try to find appointment number from closed_appointment_map first
        if call_id in closed_appointment_map:
            closed_calls_today.append(closed_appointment_map[call_id])
        else:
            # If not found, try to get it from any batch before the close time (before the 24-hour window)
            # Find the closed record to get the close time
            closed_rec = next((rec for rec in closed_history if rec['Service_Call_ID'] == call_id), None)
            if closed_rec:
                closed_time = closed_rec['ClosedTimestamp']
                # Query for the call in any batch before the close time (before the 24-hour window)
                # Use the earliest batch time as a starting point
                before_window_query = f"""
                SELECT TOP 1 \"Appointment\"
                FROM {SOURCE_TABLE}
                WHERE \"Service_Call_ID\" = %s AND \"Pushed At\" < %s
                ORDER BY \"Pushed At\" DESC;
                """
                cursor.execute(before_window_query, (call_id, start_time_cst.replace(tzinfo=None)))
                before_window_record = cursor.fetchone()
                if before_window_record:
                    closed_calls_today.append(int(before_window_record['Appointment']))
                else:
                    # Fallback: if we can't find the appointment, use 1 as default
                    # This is conservative and indicates the call was closed
                    closed_calls_today.append(1)
    
    # Calculate averages
    total_open_at_eod = len(open_calls_at_eod)
    total_closed_eod = len(closed_calls_today)
    total_active_today = len(all_calls_today)
    
    avg_appt_open_eod = sum(open_calls_at_eod.values()) / total_open_at_eod if total_open_at_eod > 0 else 0
    avg_appt_closed_today = sum(closed_calls_today) / total_closed_eod if total_closed_eod > 0 else 0
    
    # Insert new daily summary row for this batch
    insert_daily_sql = f"""
    INSERT INTO {daily_table} (Date, Timestamp, AvgApptNum_OpenAtEndOfDay, AvgApptNum_ClosedToday, 
                               TotalOpenAtEndOfDay, TotalClosedEOD, TotalActiveToday)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_daily_sql, (
        current_date_cst, latest_pushed_at_cst.replace(tzinfo=None), avg_appt_open_eod, avg_appt_closed_today,
        total_open_at_eod, total_closed_eod, total_active_today
    ))
    
    print(f"Daily Summary for {equipment_type_name} - {current_date_cst}:")
    print(f"  Open at EOD: {total_open_at_eod} calls, Avg Appointment: {avg_appt_open_eod:.2f}")
    print(f"  Closed in last 24h: {total_closed_eod} calls, Avg Appointment: {avg_appt_closed_today:.2f}")
    print(f"  Total Active (last 24h): {total_active_today} calls")
    print(f"Successfully saved to {daily_table}")

