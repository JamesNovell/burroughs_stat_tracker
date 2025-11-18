"""
Daily summary calculation and storage.

This module calculates daily summaries from either batch aggregations or raw batch data.
Calculates metrics for the last 24 hours from the configured End-of-Day (EOD) time.
Supports two aggregation methods:
1. From batch aggregation stats (preferred, more efficient)
2. From raw batch data (fallback, more comprehensive)

Rolling daily metrics (TotalSameDayClosures, SameDayCloseRate, RepeatDispatchRate) are
persisted at EOD using the latest batch aggregation's rolling values. These metrics reset at the next EOD.
"""
import logging
from datetime import datetime, timedelta, time
from app.config import SOURCE_TABLE, EOD_HOUR, EOD_MINUTE, DAILY_AGGREGATE_FROM, DAILY_AGGREGATION_ENABLED
from app.utils.equipment import filter_by_equipment_type, is_recycler
from app.utils.timezone import is_end_of_day_cst, get_cst_date, to_cst, CST
from app.utils.data import deduplicate_records

logger = logging.getLogger(__name__)


def calculate_daily_summary(cursor, latest_pushed_at, latest_calls, stat_table, history_table, hourly_table, daily_table, 
                            equipment_type_name, is_recycler_type):
    """Calculate and store daily summary at configured end of day time (default 11:59 PM CST).
    Calculates metrics for the last 24 hours from the EOD time.
    Can aggregate from batch aggregation stats (if configured) or from raw batch data."""
    # Check if it's end of day
    if not is_end_of_day_cst(latest_pushed_at):
        logger.debug(f"[{equipment_type_name}] Not end of day ({latest_pushed_at}), skipping daily summary")
        return
    
    if not DAILY_AGGREGATION_ENABLED:
        logger.debug(f"[{equipment_type_name}] Daily aggregation disabled, skipping")
        return
    
    logger.info(f"[{equipment_type_name}] Calculating daily summary (EOD detected at {latest_pushed_at})")
    
    # Route to appropriate aggregation method
    if DAILY_AGGREGATE_FROM == "hourly" and hourly_table:
        logger.debug(f"[{equipment_type_name}] Using batch aggregation method")
        calculate_daily_summary_from_hourly(cursor, latest_pushed_at, hourly_table, daily_table, equipment_type_name)
    else:
        logger.debug(f"[{equipment_type_name}] Using raw batch data aggregation method")
        calculate_daily_summary_from_raw(cursor, latest_pushed_at, latest_calls, stat_table, history_table, daily_table,
                                        equipment_type_name, is_recycler_type)


def calculate_daily_summary_from_hourly(cursor, latest_pushed_at, hourly_table, daily_table, equipment_type_name):
    """Calculate daily summary by aggregating from batch aggregation statistics."""
    # Get the EOD time in CST for the current date
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    # Calculate the EOD datetime for this date
    eod_datetime_cst = CST.localize(datetime.combine(current_date_cst, time(EOD_HOUR, EOD_MINUTE)))
    
    # Calculate 24 hours before EOD
    start_time_cst = eod_datetime_cst - timedelta(hours=24)
    
    logger.info(f"[{equipment_type_name}] Calculating daily summary from batch aggregation stats for {current_date_cst}")
    logger.debug(f"[{equipment_type_name}] Aggregating batches from: {start_time_cst.strftime('%Y-%m-%d %H:%M:%S %Z')} to {eod_datetime_cst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Get all batch aggregation stats within the 24-hour window
    hourly_stats_query = f"""
    SELECT 
        Date, Hour, PeriodMinute, PeriodStart, PeriodEnd,
        TotalOpenCalls,
        TotalClosedCalls,
        TotalSameDayClosures,
        TotalCallsWithMultiAppt,
        TotalNotServicedYet,
        SumAppointments,
        SumCompletedAppointments,
        AverageAppointmentNumber,
        SameDayCloseRate,
        FirstTimeFixRate,
        AvgAppointmentsPerCompletedCall,
        FirstTimeFixRate_RunningTotal,
        TotalFollowUpAppointments,
        TotalAppointments,
        RepeatDispatchRate,
        BatchMissing
    FROM {hourly_table}
    WHERE PeriodStart >= %s AND PeriodEnd <= %s
    ORDER BY Date, Hour, PeriodMinute ASC;
    """
    logger.debug(f"[{equipment_type_name}] Querying batch aggregation stats from {hourly_table}")
    cursor.execute(hourly_stats_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    hourly_stats = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Retrieved {len(hourly_stats)} batch aggregation stat records")
    
    if not hourly_stats:
        logger.warning(f"[{equipment_type_name}] No batch aggregation stats found for {current_date_cst}")
        return
    
    # Aggregate batch aggregation stats
    # For open calls at EOD, use the latest hour's TotalOpenCalls
    # For closed calls, sum all TotalClosedCalls
    # For appointments, use latest hour's AverageAppointmentNumber for open calls
    # For closed appointments, sum all SumCompletedAppointments
    
    total_open_at_eod = 0
    total_closed_eod = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    total_not_serviced_yet = 0
    avg_appt_num_open_eod = 0  # Will use latest hour's AverageAppointmentNumber
    sum_appointments_closed = 0
    
    # Weighted rates
    total_closed_for_rates = 0
    weighted_same_day_rate = 0.0
    weighted_first_time_rate = 0.0
    weighted_avg_appt_rate = 0.0
    
    # RDR aggregation
    total_follow_up_appointments = 0
    
    # First-Time Fix Rate running total (use the latest hour's running total)
    first_time_fix_rate_running_total = 0
    
    latest_hour_stat = None
    
    for hour_stat in hourly_stats:
        # Sum closed calls across all periods (for calculating avg appointment number for closed calls)
        total_closed_eod += hour_stat['TotalClosedCalls'] or 0
        
        # Sum completed appointments (for closed calls average)
        sum_appointments_closed += hour_stat['SumCompletedAppointments'] or 0
        
        # For rates: use period's rates (rolling daily metrics)
        # Note: SameDayCloseRate and RepeatDispatchRate are now rolling daily metrics
        # FirstTimeFixRate and AvgAppointmentsPerCompletedCall are still period-only
        closed_count = hour_stat['TotalClosedCalls'] or 0
        if closed_count > 0:
            total_closed_for_rates += closed_count
            weighted_first_time_rate += (hour_stat['FirstTimeFixRate'] or 0) * closed_count
            weighted_avg_appt_rate += (hour_stat['AvgAppointmentsPerCompletedCall'] or 0) * closed_count
        
        # Use latest period's values (overwritten each iteration, ends with last period)
        # These are rolling daily metrics, so use the latest period's value
        total_open_at_eod = hour_stat['TotalOpenCalls'] or 0
        avg_appt_num_open_eod = hour_stat['AverageAppointmentNumber'] or 0
        total_same_day_closures = hour_stat['TotalSameDayClosures'] or 0
        total_calls_with_multi_appt = hour_stat['TotalCallsWithMultiAppt'] or 0
        total_not_serviced_yet = hour_stat['TotalNotServicedYet'] or 0
        same_day_close_rate = hour_stat['SameDayCloseRate'] or 0
        repeat_dispatch_rate = hour_stat['RepeatDispatchRate'] or 0
        first_time_fix_rate_running_total = hour_stat['FirstTimeFixRate_RunningTotal'] or 0
        
        latest_hour_stat = hour_stat
    
    # For unique appointments across the 24-hour window, query source table
    # Get all batches in the 24-hour window
    logger.debug(f"[{equipment_type_name}] Querying batches in 24-hour window for unique appointments")
    batches_query = f"""
    SELECT DISTINCT \"Pushed At\"
    FROM {SOURCE_TABLE}
    WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s
    ORDER BY \"Pushed At\" ASC;
    """
    cursor.execute(batches_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    batches_in_window = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Found {len(batches_in_window)} batches in 24-hour window")
    
    # Determine equipment type from batch aggregation table name
    from app.config import RECYCLERS_HOURLY_TABLE
    is_recycler_type = (hourly_table == RECYCLERS_HOURLY_TABLE)
    logger.debug(f"[{equipment_type_name}] Equipment type detection: is_recycler={is_recycler_type}")
    
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
        logger.debug(f"[{equipment_type_name}] Retrieved {len(appointment_records)} appointment records from source table")
        
        # Filter by equipment type and collect unique appointments
        for record in appointment_records:
            equipment_id = record.get('Equipment_ID', '')
            if is_recycler(equipment_id) == is_recycler_type:
                unique_appointments_daily.add(int(record['Appointment']))
    
    # Calculate averages
    # For open calls at EOD, use the latest hour's AverageAppointmentNumber (already calculated)
    avg_appt_open_eod = avg_appt_num_open_eod
    # For closed calls, calculate average from sum
    avg_appt_closed_today = sum_appointments_closed / total_closed_eod if total_closed_eod > 0 else 0
    
    # Use latest batch aggregation's rolling rates (already calculated in batch aggregator)
    # same_day_close_rate and repeat_dispatch_rate are already set from latest batch aggregation
    # Calculate weighted rates for FirstTimeFixRate and AvgAppointmentsPerCompletedCall (batch-only metrics)
    first_time_fix_rate = weighted_first_time_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    avg_appt_per_completed = weighted_avg_appt_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    
    # Note: same_day_close_rate and repeat_dispatch_rate are rolling daily metrics from latest hour
    # They are already set from the latest hour_stat in the loop above
    
    # Insert daily summary
    insert_daily_sql = f"""
    INSERT INTO {daily_table} (Date, Timestamp, AvgApptNum_OpenAtEndOfDay, AvgApptNum_ClosedToday, 
                               TotalOpenAtEndOfDay, TotalClosedEOD, TotalSameDayClosures, 
                               TotalCallsWithMultiAppt, TotalNotServicedYet, FirstTimeFixRate_RunningTotal, RepeatDispatchRate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_daily_sql, (
        current_date_cst, latest_pushed_at_cst.replace(tzinfo=None), avg_appt_open_eod, avg_appt_closed_today,
        total_open_at_eod, total_closed_eod, total_same_day_closures, 
        total_calls_with_multi_appt, total_not_serviced_yet, first_time_fix_rate_running_total, repeat_dispatch_rate
    ))
    
    logger.info(f"[{equipment_type_name}] Daily summary for {current_date_cst} (from {len(hourly_stats)} batch aggregations):")
    logger.info(f"[{equipment_type_name}]   Open at EOD: {total_open_at_eod} calls, Avg Appt: {avg_appt_open_eod:.2f}")
    logger.info(f"[{equipment_type_name}]   Closed in last 24h: {total_closed_eod} calls, Avg Appt: {avg_appt_closed_today:.2f}")
    logger.info(f"[{equipment_type_name}]   Same-day closures (rolling daily): {total_same_day_closures}")
    logger.info(f"[{equipment_type_name}]   Same-day close rate (rolling daily): {same_day_close_rate:.2%}")
    logger.info(f"[{equipment_type_name}]   Calls with multi-appt: {total_calls_with_multi_appt}")
    logger.info(f"[{equipment_type_name}]   Not serviced yet: {total_not_serviced_yet}")
    logger.info(f"[{equipment_type_name}]   First-Time Fix Rate (running total): {first_time_fix_rate_running_total:.2%}")
    logger.info(f"[{equipment_type_name}]   Repeat Dispatch Rate (rolling daily): {repeat_dispatch_rate:.2%}")
    logger.info(f"[{equipment_type_name}] Successfully saved daily summary to {daily_table}")


def calculate_daily_summary_from_raw(cursor, latest_pushed_at, latest_calls, stat_table, history_table, daily_table, 
                            equipment_type_name, is_recycler_type):
    """Calculate daily summary from raw batch data (original implementation)."""
    
    # Get the EOD time in CST for the current date
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    # Calculate the EOD datetime for this date
    eod_datetime_cst = CST.localize(datetime.combine(current_date_cst, time(EOD_HOUR, EOD_MINUTE)))
    
    # Calculate 24 hours before EOD
    start_time_cst = eod_datetime_cst - timedelta(hours=24)
    
    logger.info(f"[{equipment_type_name}] Calculating daily summary from raw batch data for {current_date_cst}")
    logger.debug(f"[{equipment_type_name}] Calculating for last 24 hours: {start_time_cst.strftime('%Y-%m-%d %H:%M:%S %Z')} to {eod_datetime_cst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Filter calls by equipment type
    latest_filtered = filter_by_equipment_type(latest_calls, is_recycler_type)
    logger.debug(f"[{equipment_type_name}] Filtered {len(latest_filtered)} calls for this equipment type")
    
    # Get all batches from the last 24 hours (from start_time to EOD)
    batches_today_query = f"""
    SELECT DISTINCT \"Pushed At\" 
    FROM {SOURCE_TABLE} 
    WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s
    ORDER BY \"Pushed At\" ASC;
    """
    logger.debug(f"[{equipment_type_name}] Querying batches in 24-hour window")
    cursor.execute(batches_today_query, (start_time_cst.replace(tzinfo=None), eod_datetime_cst.replace(tzinfo=None)))
    batches_today = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Found {len(batches_today)} batches in 24-hour window")
    
    if not batches_today:
        logger.warning(f"[{equipment_type_name}] No batches found for {current_date_cst}")
        return
    
    # Get all unique calls that appeared in any batch today
    cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number", "DesNote", "PartNote"'
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
    
    # Process closed calls that don't appear in any batch within the 24-hour window
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
    
    # Calculate TotalSameDayClosures from closed calls
    total_same_day_closures = 0
    current_date_cst_for_comparison = get_cst_date(latest_pushed_at)
    for closed_rec in closed_history:
        open_datetime_cst = to_cst(closed_rec['OpenDateTime'])
        if open_datetime_cst.date() == current_date_cst_for_comparison:
            total_same_day_closures += 1
    
    # Calculate TotalCallsWithMultiAppt from open calls at EOD
    total_calls_with_multi_appt = sum(1 for appt in open_calls_at_eod.values() if appt >= 2)
    
    # Calculate TotalNotServicedYet (calls with Appointment = 1) from open calls at EOD
    total_not_serviced_yet = sum(1 for appt in open_calls_at_eod.values() if appt == 1)
    
    # Calculate First-Time Fix Rate running total for the day
    # Count first-time fixes (closed calls with Appointment = 1) from closed calls today
    total_first_time_fixes = sum(1 for appt in closed_calls_today if appt == 1)
    first_time_fix_rate_running_total = total_first_time_fixes / total_closed_eod if total_closed_eod > 0 else 0
    
    avg_appt_open_eod = sum(open_calls_at_eod.values()) / total_open_at_eod if total_open_at_eod > 0 else 0
    avg_appt_closed_today = sum(closed_calls_today) / total_closed_eod if total_closed_eod > 0 else 0
    
    # Insert new daily summary row for this batch
    insert_daily_sql = f"""
    INSERT INTO {daily_table} (Date, Timestamp, AvgApptNum_OpenAtEndOfDay, AvgApptNum_ClosedToday, 
                               TotalOpenAtEndOfDay, TotalClosedEOD, TotalSameDayClosures, 
                               TotalCallsWithMultiAppt, TotalNotServicedYet, FirstTimeFixRate_RunningTotal)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_daily_sql, (
        current_date_cst, latest_pushed_at_cst.replace(tzinfo=None), avg_appt_open_eod, avg_appt_closed_today,
        total_open_at_eod, total_closed_eod, total_same_day_closures, 
        total_calls_with_multi_appt, total_not_serviced_yet, first_time_fix_rate_running_total
    ))
    
    logger.info(f"[{equipment_type_name}] Daily summary for {current_date_cst}:")
    logger.info(f"[{equipment_type_name}]   Open at EOD: {total_open_at_eod} calls, Avg Appt: {avg_appt_open_eod:.2f}")
    logger.info(f"[{equipment_type_name}]   Closed in last 24h: {total_closed_eod} calls, Avg Appt: {avg_appt_closed_today:.2f}")
    logger.info(f"[{equipment_type_name}]   Same-day closures: {total_same_day_closures}")
    logger.info(f"[{equipment_type_name}]   Calls with multi-appt: {total_calls_with_multi_appt}")
    logger.info(f"[{equipment_type_name}]   Not serviced yet: {total_not_serviced_yet}")
    logger.info(f"[{equipment_type_name}]   First-Time Fix Rate (running total): {first_time_fix_rate_running_total:.2%}")
    logger.info(f"[{equipment_type_name}] Successfully saved daily summary to {daily_table}")

