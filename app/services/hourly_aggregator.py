"""Hourly aggregation service - aggregates batch-level stats into hourly statistics."""
from datetime import datetime, timedelta
from app.config import HOURLY_AGGREGATION_ENABLED, HOURLY_VALIDATION_ENABLED, SOURCE_TABLE, RECYCLERS_STAT_TABLE
from app.utils.timezone import to_cst, get_cst_date, CST
from app.utils.equipment import is_recycler


def aggregate_hourly_stats(cursor, stat_table, hourly_table, hour_start_cst, hour_end_cst, equipment_type_name):
    """
    Aggregate all batch stats within an hour into hourly statistics.
    
    Args:
        cursor: Database cursor
        stat_table: Batch-level stat table name
        hourly_table: Hourly stat table name
        hour_start_cst: Start of hour in CST (datetime with timezone)
        hour_end_cst: End of hour in CST (datetime with timezone)
        equipment_type_name: Name for logging (e.g., "Recyclers")
    
    Returns:
        True if aggregation was successful, False otherwise
    """
    if not HOURLY_AGGREGATION_ENABLED:
        return False
    
    # Check if hourly aggregation already exists for this hour
    date_cst = hour_start_cst.date()
    hour = hour_start_cst.hour
    
    check_existing = f"""
    SELECT HourlyStatID FROM {hourly_table}
    WHERE Date = %s AND Hour = %s;
    """
    cursor.execute(check_existing, (date_cst, hour))
    existing = cursor.fetchone()
    
    if existing:
        print(f"Hourly aggregation for {equipment_type_name} - {date_cst} {hour:02d}:00 already exists. Skipping.")
        return False
    
    # Get all batches within this hour
    hour_start_naive = hour_start_cst.replace(tzinfo=None)
    hour_end_naive = hour_end_cst.replace(tzinfo=None)
    
    batches_query = f"""
    SELECT 
        BatchID,
        Timestamp,
        TotalOpenCalls,
        CallsClosedSinceLastBatch,
        SameDayClosures,
        CallsWithMultipleAppointments,
        AverageAppointmentNumber,
        SameDayCloseRate,
        AvgAppointmentsPerCompletedCall,
        FirstTimeFixRate,
        FourteenDayReopenRate,
        TotalFollowUpAppointments,
        TotalAppointments,
        RepeatDispatchRate
    FROM {stat_table}
    WHERE Timestamp >= %s AND Timestamp < %s
    ORDER BY Timestamp ASC;
    """
    cursor.execute(batches_query, (hour_start_naive, hour_end_naive))
    batches = cursor.fetchall()
    
    if not batches:
        print(f"No batches found for {equipment_type_name} hour {date_cst} {hour:02d}:00")
        return False
    
    # Aggregate batch stats
    total_open_calls = 0
    total_closed_calls = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    sum_appointments = 0
    sum_completed_appointments = 0
    
    # For rates, we need to calculate weighted averages
    total_closed_for_rates = 0
    weighted_same_day_close_rate = 0.0
    weighted_first_time_fix_rate = 0.0
    weighted_avg_appt_per_completed = 0.0
    
    # RDR aggregation
    total_follow_up_appointments = 0
    unique_appointments_hour = set()  # Track unique appointments across all batches in hour
    
    # Track the latest batch for open calls count and snapshot metrics
    latest_batch = None
    
    for batch in batches:
        batch_timestamp = batch['Timestamp']
        
        # Use latest batch's open calls count (snapshot at end of hour)
        total_open_calls = batch['TotalOpenCalls'] or 0
        
        # Sum closed calls across all batches in the hour
        closed_count = batch['CallsClosedSinceLastBatch'] or 0
        total_closed_calls += closed_count
        
        # Sum same day closures
        total_same_day_closures += batch['SameDayClosures'] or 0
        
        # Use latest batch's multi-appt count (snapshot)
        total_calls_with_multi_appt = batch['CallsWithMultipleAppointments'] or 0
        
        # Calculate sum of appointments from average * count for latest batch
        avg_appt = batch['AverageAppointmentNumber'] or 0
        if avg_appt > 0 and total_open_calls > 0:
            sum_appointments = int(avg_appt * total_open_calls)
        
        # For completed appointments, sum avg * closed count for each batch
        avg_completed = batch['AvgAppointmentsPerCompletedCall'] or 0
        if avg_completed > 0 and closed_count > 0:
            sum_completed_appointments += int(avg_completed * closed_count)
        
        # Weighted rates (weight by number of closed calls)
        if closed_count > 0:
            batch_weight = closed_count
            total_closed_for_rates += batch_weight
            
            same_day_rate = batch['SameDayCloseRate'] or 0
            first_time_rate = batch['FirstTimeFixRate'] or 0
            avg_appt_rate = batch['AvgAppointmentsPerCompletedCall'] or 0
            
            weighted_same_day_close_rate += same_day_rate * batch_weight
            weighted_first_time_fix_rate += first_time_rate * batch_weight
            weighted_avg_appt_per_completed += avg_appt_rate * batch_weight
        
        # RDR aggregation: Sum follow-up appointments from all batches
        total_follow_up_appointments += batch['TotalFollowUpAppointments'] or 0
        
        latest_batch = batch
    
    # For unique appointments across the hour, query source table for all batches
    # Determine equipment type from stat_table name
    is_recycler_type = (stat_table == RECYCLERS_STAT_TABLE)
    
    # Get all unique appointment numbers from source table for all batches in this hour
    # Get distinct batch timestamps from stat_table
    batch_timestamps = [batch['Timestamp'] for batch in batches]
    
    if batch_timestamps:
        # Query source table for all batches in this hour
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
                unique_appointments_hour.add(int(record['Appointment']))
    
    total_appointments_hour = len(unique_appointments_hour)
    
    # Calculate hourly RDR
    hourly_rdr = total_follow_up_appointments / total_appointments_hour if total_appointments_hour > 0 else 0
    
    # Calculate final rates
    same_day_close_rate = weighted_same_day_close_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    first_time_fix_rate = weighted_first_time_fix_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    avg_appt_per_completed = weighted_avg_appt_per_completed / total_closed_for_rates if total_closed_for_rates > 0 else 0
    
    # Average appointment number (from latest batch)
    avg_appt_num = latest_batch['AverageAppointmentNumber'] or 0 if latest_batch else 0
    
    # Reopen rate (use latest batch's rate as snapshot)
    reopen_rate = latest_batch['FourteenDayReopenRate'] or 0 if latest_batch else 0
    
    # New calls and reopened calls - these are harder to aggregate, use latest batch's values
    # In a more sophisticated implementation, we could track these across batches
    total_new_calls = 0  # Would need to be calculated from batch differences
    total_reopened_calls = 0  # Would need to be calculated from batch differences
    
    # Validation (if enabled)
    if HOURLY_VALIDATION_ENABLED:
        if not validate_hourly_aggregation(cursor, stat_table, hour_start_naive, hour_end_naive, 
                                          total_closed_calls, batches):
            print(f"Warning: Hourly aggregation validation failed for {equipment_type_name} hour {date_cst} {hour:02d}:00")
            # Continue anyway, but log the warning
    
    # Insert hourly aggregation
    insert_sql = f"""
    INSERT INTO {hourly_table} (
        Date, Hour, PeriodStart, PeriodEnd, Timestamp,
        TotalOpenCalls, TotalClosedCalls, TotalSameDayClosures,
        TotalCallsWithMultiAppt, TotalNewCalls, TotalReopenedCalls,
        SumAppointments, SumCompletedAppointments,
        AverageAppointmentNumber, SameDayCloseRate, FirstTimeFixRate,
        AvgAppointmentsPerCompletedCall, FourteenDayReopenRate,
        TotalFollowUpAppointments, TotalAppointments, RepeatDispatchRate,
        BatchCount
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    );
    """
    
    cursor.execute(insert_sql, (
        date_cst, hour, hour_start_naive, hour_end_naive, hour_end_naive,
        total_open_calls, total_closed_calls, total_same_day_closures,
        total_calls_with_multi_appt, total_new_calls, total_reopened_calls,
        sum_appointments, sum_completed_appointments,
        avg_appt_num, same_day_close_rate, first_time_fix_rate,
        avg_appt_per_completed, reopen_rate,
        total_follow_up_appointments, total_appointments_hour, hourly_rdr,
        len(batches)
    ))
    
    print(f"Hourly aggregation for {equipment_type_name} - {date_cst} {hour:02d}:00: "
          f"{len(batches)} batches, {total_open_calls} open, {total_closed_calls} closed")
    print(f"  RDR: {total_follow_up_appointments} follow-ups / {total_appointments_hour} unique appointments = {hourly_rdr:.2%}")
    
    return True


def validate_hourly_aggregation(cursor, stat_table, hour_start, hour_end, aggregated_closed, batches):
    """
    Validate that hourly aggregation matches batch-level data.
    
    Returns:
        True if validation passes, False otherwise
    """
    if not HOURLY_VALIDATION_ENABLED:
        return True
    
    # Sum up closed calls from batches
    total_from_batches = sum(batch['CallsClosedSinceLastBatch'] or 0 for batch in batches)
    
    # Compare with aggregated value
    if abs(total_from_batches - aggregated_closed) > 1:  # Allow 1 for rounding
        print(f"Validation warning: Closed calls mismatch - batches: {total_from_batches}, aggregated: {aggregated_closed}")
        return False
    
    return True


def should_trigger_hourly_aggregation(current_timestamp_cst, last_processed_timestamp_cst):
    """
    Determine if hourly aggregation should be triggered.
    
    Args:
        current_timestamp_cst: Current batch timestamp in CST
        last_processed_timestamp_cst: Last processed batch timestamp in CST (or None)
    
    Returns:
        Tuple of (should_trigger, hour_start_cst, hour_end_cst) or (False, None, None)
    """
    if not HOURLY_AGGREGATION_ENABLED:
        return (False, None, None)
    
    if last_processed_timestamp_cst is None:
        return (False, None, None)
    
    # Check if we've crossed an hour boundary
    current_hour = current_timestamp_cst.hour
    last_hour = last_processed_timestamp_cst.hour
    current_date = current_timestamp_cst.date()
    last_date = last_processed_timestamp_cst.date()
    
    # Crossed hour boundary (same day or different day)
    if (current_date > last_date) or (current_date == last_date and current_hour > last_hour):
        # Calculate the hour that just completed
        if current_date > last_date:
            # Crossed day boundary - aggregate the last hour of previous day
            hour_to_aggregate = 23
            date_to_aggregate = last_date
        else:
            # Same day, crossed hour - aggregate the previous hour
            hour_to_aggregate = last_hour
            date_to_aggregate = last_date
        
        # Calculate hour boundaries
        hour_start = CST.localize(datetime.combine(date_to_aggregate, datetime.time(hour_to_aggregate, 0)))
        hour_end = hour_start + timedelta(hours=1)
        
        return (True, hour_start, hour_end)
    
    return (False, None, None)

