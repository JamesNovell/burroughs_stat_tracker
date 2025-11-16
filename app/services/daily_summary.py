"""Daily summary calculation and storage."""
from datetime import datetime, timedelta
from app.config import SOURCE_TABLE, EOD_HOUR, EOD_MINUTE, DAILY_AGGREGATE_FROM, DAILY_AGGREGATION_ENABLED
from app.utils.equipment import filter_by_equipment_type, is_recycler
from app.utils.timezone import is_end_of_day_cst, get_cst_date, to_cst, CST
from app.utils.data import deduplicate_records


def calculate_daily_summary(cursor, latest_pushed_at, latest_calls, stat_table, history_table, hourly_table, daily_table, 
                            equipment_type_name, is_recycler_type):
    """Calculate and store daily summary at configured end of day time (default 11:59 PM CST).
    Calculates metrics for the last 24 hours from the EOD time.
    Can aggregate from hourly stats (if configured) or from raw batch data."""
    # Check if it's end of day
    if not is_end_of_day_cst(latest_pushed_at):
        return
    
    if not DAILY_AGGREGATION_ENABLED:
        return
    
    # Route to appropriate aggregation method
    if DAILY_AGGREGATE_FROM == "hourly" and hourly_table:
        calculate_daily_summary_from_hourly(cursor, latest_pushed_at, hourly_table, daily_table, equipment_type_name)
    else:
        calculate_daily_summary_from_raw(cursor, latest_pushed_at, latest_calls, stat_table, history_table, daily_table,
                                        equipment_type_name, is_recycler_type)


def calculate_daily_summary_from_hourly(cursor, latest_pushed_at, hourly_table, daily_table, equipment_type_name):
    """Calculate daily summary by aggregating from hourly statistics."""
    # Get the EOD time in CST for the current date
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    # Calculate the EOD datetime for this date
    eod_datetime_cst = CST.localize(datetime.combine(current_date_cst, datetime.time(EOD_HOUR, EOD_MINUTE)))
    
    # Calculate 24 hours before EOD
    start_time_cst = eod_datetime_cst - timedelta(hours=24)
    
    print(f"\n=== Calculating Daily Summary for {equipment_type_name} - {current_date_cst} (from hourly stats) ===")
    print(f"Aggregating hours from: {start_time_cst.strftime('%Y-%m-%d %H:%M:%S %Z')} to {eod_datetime_cst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Get all hourly stats within the 24-hour window
    hourly_stats_query = f"""
    SELECT 
        Date, Hour, PeriodStart, PeriodEnd,
        TotalOpenCalls,
        TotalClosedCalls,
        TotalSameDayClosures,
        TotalCallsWithMultiAppt,
        SumAppointments,
        SumCompletedAppointments,
        AverageAppointmentNumber,
        SameDayCloseRate,
        FirstTimeFixRate,
        AvgAppointmentsPerCompletedCall,
        FourteenDayReopenRate,
        TotalFollowUpAppointments,
        TotalAppointments,
        RepeatDispatchRate
    FROM {hourly_table}
    WHERE PeriodStart >= %s AND PeriodEnd <= %s
    ORDER BY Date, Hour ASC;
    """
    cursor.execute(hourly_stats_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    hourly_stats = cursor.fetchall()
    
    if not hourly_stats:
        print(f"No hourly stats found for {current_date_cst}")
        return
    
    # Aggregate hourly stats
    # For open calls at EOD, use the latest hour's TotalOpenCalls
    # For closed calls, sum all TotalClosedCalls
    # For appointments, sum all SumAppointments and SumCompletedAppointments
    
    total_open_at_eod = 0
    total_closed_eod = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    sum_appointments_open = 0
    sum_appointments_closed = 0
    
    # Weighted rates
    total_closed_for_rates = 0
    weighted_same_day_rate = 0.0
    weighted_first_time_rate = 0.0
    weighted_avg_appt_rate = 0.0
    
    # RDR aggregation
    total_follow_up_appointments = 0
    
    latest_hour_stat = None
    
    for hour_stat in hourly_stats:
        # Use latest hour's open calls count
        total_open_at_eod = hour_stat['TotalOpenCalls'] or 0
        
        # Sum closed calls
        total_closed_eod += hour_stat['TotalClosedCalls'] or 0
        total_same_day_closures += hour_stat['TotalSameDayClosures'] or 0
        total_calls_with_multi_appt = hour_stat['TotalCallsWithMultiAppt'] or 0  # Use latest
        
        # Sum appointments
        sum_appointments_open += hour_stat['SumAppointments'] or 0
        sum_appointments_closed += hour_stat['SumCompletedAppointments'] or 0
        
        # Weighted rates
        closed_count = hour_stat['TotalClosedCalls'] or 0
        if closed_count > 0:
            total_closed_for_rates += closed_count
            weighted_same_day_rate += (hour_stat['SameDayCloseRate'] or 0) * closed_count
            weighted_first_time_rate += (hour_stat['FirstTimeFixRate'] or 0) * closed_count
            weighted_avg_appt_rate += (hour_stat['AvgAppointmentsPerCompletedCall'] or 0) * closed_count
        
        # Sum follow-up appointments from all hours
        total_follow_up_appointments += hour_stat['TotalFollowUpAppointments'] or 0
        
        latest_hour_stat = hour_stat
    
    # For unique appointments across the 24-hour window, query source table
    # Get all batches in the 24-hour window
    batches_query = f"""
    SELECT DISTINCT \"Pushed At\"
    FROM {SOURCE_TABLE}
    WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s
    ORDER BY \"Pushed At\" ASC;
    """
    cursor.execute(batches_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    batches_in_window = cursor.fetchall()
    
    # Determine equipment type from hourly_table name
    from app.config import RECYCLERS_HOURLY_TABLE
    from app.utils.equipment import is_recycler
    is_recycler_type = (hourly_table == RECYCLERS_HOURLY_TABLE)
    
    # Collect unique appointment numbers from all batches in the 24-hour window
    unique_appointments_daily = set()
    if batches_in_window:
        batch_timestamps = [batch['Pushed At'] for batch in batches_in_window]
        placeholders = ', '.join(['%s'] * len(batch_timestamps))
        appointments_query = f"""
        SELECT DISTINCT \"Appointment\", \"Equipment_ID\"
        FROM {SOURCE_TABLE}
        WHERE \"Pushed At\" IN ({placeholders});
        """
        cursor.execute(appointments_query, tuple(batch_timestamps))
        appointment_records = cursor.fetchall()
        
        # Filter by equipment type and collect unique appointments
        for record in appointment_records:
            equipment_id = record.get('Equipment_ID', '')
            if is_recycler(equipment_id) == is_recycler_type:
                unique_appointments_daily.add(int(record['Appointment']))
    
    total_appointments_daily = len(unique_appointments_daily)
    
    # Calculate daily RDR
    daily_rdr = total_follow_up_appointments / total_appointments_daily if total_appointments_daily > 0 else 0
    
    # Calculate averages
    avg_appt_open_eod = sum_appointments_open / total_open_at_eod if total_open_at_eod > 0 else 0
    avg_appt_closed_today = sum_appointments_closed / total_closed_eod if total_closed_eod > 0 else 0
    
    # Calculate weighted rates
    same_day_close_rate = weighted_same_day_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    first_time_fix_rate = weighted_first_time_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    avg_appt_per_completed = weighted_avg_appt_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    
    # Total active today = open at EOD + closed today
    total_active_today = total_open_at_eod + total_closed_eod
    
    # Insert daily summary
    insert_daily_sql = f"""
    INSERT INTO {daily_table} (Date, Timestamp, AvgApptNum_OpenAtEndOfDay, AvgApptNum_ClosedToday, 
                               TotalOpenAtEndOfDay, TotalClosedEOD, TotalActiveToday, RepeatDispatchRate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_daily_sql, (
        current_date_cst, latest_pushed_at_cst.replace(tzinfo=None), avg_appt_open_eod, avg_appt_closed_today,
        total_open_at_eod, total_closed_eod, total_active_today, daily_rdr
    ))
    
    print(f"Daily Summary for {equipment_type_name} - {current_date_cst} (from {len(hourly_stats)} hourly stats):")
    print(f"  Open at EOD: {total_open_at_eod} calls, Avg Appointment: {avg_appt_open_eod:.2f}")
    print(f"  Closed in last 24h: {total_closed_eod} calls, Avg Appointment: {avg_appt_closed_today:.2f}")
    print(f"  Total Active (last 24h): {total_active_today} calls")
    print(f"  RDR: {total_follow_up_appointments} follow-ups / {total_appointments_daily} unique appointments = {daily_rdr:.2%}")
    print(f"Successfully saved to {daily_table}")


def calculate_daily_summary_from_raw(cursor, latest_pushed_at, latest_calls, stat_table, history_table, daily_table, 
                            equipment_type_name, is_recycler_type):
    """Calculate daily summary from raw batch data (original implementation)."""
    
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
    cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number", "DesNote", "PartNote", "parts_tracking"'
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
                # Call is closed but appointment number not found in closed_appointment_map
                # Try to get it from the record itself (last known state) or query before close time
                closed_rec = next((rec for rec in closed_history if rec['Service_Call_ID'] == call_id), None)
                if closed_rec:
                    closed_time = closed_rec['ClosedTimestamp']
                    # Try to get appointment from any batch before the close time
                    before_close_query = f"""
                    SELECT TOP 1 \"Appointment\"
                    FROM {SOURCE_TABLE}
                    WHERE \"Service_Call_ID\" = %s AND \"Pushed At\" < %s
                    ORDER BY \"Pushed At\" DESC;
                    """
                    cursor.execute(before_close_query, (call_id, closed_time))
                    before_close_record = cursor.fetchone()
                    if before_close_record:
                        closed_calls_today.append(int(before_close_record['Appointment']))
                    else:
                        # Fallback: use appointment from current record (last known state)
                        # This is the appointment number from the latest batch before close
                        closed_calls_today.append(int(record['Appointment']))
                else:
                    # No closed record found, use appointment from current record as fallback
                    closed_calls_today.append(int(record['Appointment']))
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

