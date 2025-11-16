"""Daily summary calculation and storage."""
from datetime import datetime, timedelta
from config import SOURCE_TABLE, EOD_HOUR, EOD_MINUTE
from helpers import (
    filter_by_equipment_type, is_recycler, is_end_of_day_cst, 
    get_cst_date, to_cst, CST, deduplicate_records
)


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
    for call_id, record in all_calls_today.items():
        if call_id in closed_call_ids_today:
            # This call closed today - use final appointment number
            if call_id in closed_appointment_map:
                closed_calls_today.append(closed_appointment_map[call_id])
        else:
            # Still open - use latest appointment number
            open_calls_at_eod[call_id] = int(record['Appointment'])
    
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

