"""
Batch aggregation service - aggregates batch-level stats into batch-level summaries.

This module aggregates batch-level statistics whenever a new batch arrives, calculating:
- Total open/closed calls (snapshot at end of batch period for open, sum for closed)
- Rolling daily metrics that accumulate from start of day:
  * TotalSameDayClosures: cumulative count from start of day
  * SameDayCloseRate: rate calculated from all closed calls today
  * RepeatDispatchRate: rate calculated from all appointments today
- Weighted averages for batch-only rates (first-time fix rate, etc.)
- Running totals for First-Time Fix Rate (accumulated throughout the day)
- AverageAppointmentNumber: snapshot at end of batch period (for open calls)

All rolling daily metrics reset at the configured EOD time.
Aggregation happens on every batch arrival, not on a time schedule.
"""
import logging
from datetime import datetime, timedelta, time
from app.config import HOURLY_AGGREGATION_ENABLED, HOURLY_VALIDATION_ENABLED, SOURCE_TABLE, RECYCLERS_STAT_TABLE
from app.utils.timezone import to_cst, get_cst_date, CST
from app.utils.equipment import is_recycler

logger = logging.getLogger(__name__)


def aggregate_batch_stats(cursor, stat_table, batch_table, last_aggregation_timestamp, current_batch_timestamp, equipment_type_name):
    """
    Aggregate all batch stats since the last aggregation into a batch-level summary.
    
    Args:
        cursor: Database cursor
        stat_table: Batch-level stat table name
        batch_table: Batch aggregation table name (formerly hourly_table)
        last_aggregation_timestamp: Timestamp of last aggregation (or None for first aggregation)
        current_batch_timestamp: Timestamp of current batch (end of aggregation period)
        equipment_type_name: Name for logging (e.g., "Recyclers")
    
    Returns:
        True if aggregation was successful, False otherwise
    """
    if not HOURLY_AGGREGATION_ENABLED:
        logger.debug(f"[{equipment_type_name}] Batch aggregation disabled, skipping")
        return False
    
    # Convert timestamps to CST for date/hour calculations
    current_batch_cst = to_cst(current_batch_timestamp)
    date_cst = current_batch_cst.date()
    hour = current_batch_cst.hour
    period_minute = 0  # Always 0 for batch-based aggregation (not time-based)
    
    # Calculate period boundaries
    if last_aggregation_timestamp:
        period_start_cst = to_cst(last_aggregation_timestamp)
        period_start_naive = period_start_cst.replace(tzinfo=None)
    else:
        # First aggregation - use current batch timestamp as start (will aggregate just this batch)
        period_start_cst = current_batch_cst
        period_start_naive = current_batch_cst.replace(tzinfo=None)
    
    period_end_cst = current_batch_cst
    period_end_naive = current_batch_cst.replace(tzinfo=None)
    
    logger.info(f"[{equipment_type_name}] Aggregating batches from {period_start_naive} to {period_end_naive} (batch timestamp: {current_batch_timestamp})")
    
    # Check if aggregation already exists for this batch timestamp
    check_existing = f"""
    SELECT HourlyStatID FROM {batch_table}
    WHERE PeriodEnd = %s;
    """
    cursor.execute(check_existing, (period_end_naive,))
    existing = cursor.fetchone()
    
    if existing:
        logger.info(f"[{equipment_type_name}] Aggregation for batch at {period_end_naive} already exists. Skipping.")
        return False
    
    # Get all batches since last aggregation
    if last_aggregation_timestamp:
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
            TotalFollowUpAppointments,
            TotalAppointments,
            RepeatDispatchRate
        FROM {stat_table}
        WHERE Timestamp > %s AND Timestamp <= %s
        ORDER BY Timestamp ASC;
        """
        logger.debug(f"[{equipment_type_name}] Querying batches since last aggregation: {period_start_naive} to {period_end_naive}")
        cursor.execute(batches_query, (period_start_naive, period_end_naive))
    else:
        # First aggregation - get all batches up to current batch
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
            TotalFollowUpAppointments,
            TotalAppointments,
            RepeatDispatchRate
        FROM {stat_table}
        WHERE Timestamp <= %s
        ORDER BY Timestamp ASC;
        """
        logger.debug(f"[{equipment_type_name}] First aggregation - querying all batches up to {period_end_naive}")
        cursor.execute(batches_query, (period_end_naive,))
    batches = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Found {len(batches)} batches in this period")
    
    # Set BatchMissing flag if no batches found
    batch_missing = len(batches) == 0
    
    # Initialize aggregation variables
    total_open_calls = 0
    total_closed_calls = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    
    if batch_missing:
        logger.warning(f"[{equipment_type_name}] No batches found for period {period_start_naive} to {period_end_naive} - marking as BatchMissing")
        # Get previous aggregation's values to use as snapshot (for open calls, etc.)
        previous_period_query = f"""
        SELECT TOP 1 
            TotalOpenCalls, TotalCallsWithMultiAppt, TotalNotServicedYet, AverageAppointmentNumber
        FROM {batch_table}
        WHERE PeriodEnd < %s
        ORDER BY PeriodEnd DESC;
        """
        cursor.execute(previous_period_query, (period_end_naive,))
        prev_period = cursor.fetchone()
        
        if prev_period:
            # Use previous aggregation's snapshot values
            total_open_calls = prev_period['TotalOpenCalls'] or 0
            total_calls_with_multi_appt = prev_period['TotalCallsWithMultiAppt'] or 0
            total_not_serviced_yet = prev_period['TotalNotServicedYet'] or 0
            avg_appt_num = prev_period['AverageAppointmentNumber'] or 0
            logger.debug(f"[{equipment_type_name}] Using previous aggregation's snapshot values: {total_open_calls} open calls")
        else:
            # No previous aggregation, use zeros
            total_not_serviced_yet = 0
            avg_appt_num = 0
            logger.debug(f"[{equipment_type_name}] No previous aggregation found, using zero values")
    
    # Aggregate batch stats (only if batches exist)
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
    
    # First-Time Fix Rate running totals (for the day)
    total_first_time_fixes_hour = 0  # First-time fixes in this hour
    total_closed_calls_hour = 0  # Closed calls in this hour
    
    # Track the latest batch for open calls count and snapshot metrics
    latest_batch = None
    
    for batch in batches:
        batch_timestamp = batch['Timestamp']
        
        # Use latest batch's open calls count (snapshot at end of hour)
        total_open_calls = batch['TotalOpenCalls'] or 0
        
        # Sum closed calls across all batches in the hour
        closed_count = batch['CallsClosedSinceLastBatch'] or 0
        total_closed_calls += closed_count
        
        # Sum same day closures (will accumulate from start of day later)
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
            
            # Calculate first-time fixes for this batch (FirstTimeFixRate * closed_count)
            first_time_fixes_batch = int(first_time_rate * closed_count)
            total_first_time_fixes_hour += first_time_fixes_batch
            total_closed_calls_hour += closed_count
        
        # RDR aggregation: Sum follow-up appointments from all batches
        total_follow_up_appointments += batch['TotalFollowUpAppointments'] or 0
        
        latest_batch = batch
    
    # For unique appointments across the batches, query source table for all batches
    # Determine equipment type from stat_table name
    is_recycler_type = (stat_table == RECYCLERS_STAT_TABLE)
    logger.debug(f"[{equipment_type_name}] Equipment type detection: is_recycler={is_recycler_type}")
    
    # Get all unique appointment numbers from source table for all batches in this aggregation
    # Get distinct batch timestamps from stat_table
    batch_timestamps = [batch['Timestamp'] for batch in batches]
    
    if batch_timestamps:
        logger.debug(f"[{equipment_type_name}] Querying source table for unique appointments across {len(batch_timestamps)} batches")
        # Query source table for all batches in this aggregation
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
                unique_appointments_hour.add(int(record['Appointment']))
    
    total_appointments_hour = len(unique_appointments_hour)
    logger.debug(f"[{equipment_type_name}] Unique appointments for this aggregation: {total_appointments_hour}")
    
    # Calculate rolling daily metrics (accumulate from start of day)
    # Get previous aggregations' same-day closures for this day
    previous_same_day_query = f"""
    SELECT SUM(TotalSameDayClosures) as PrevTotalSameDayClosures
    FROM {batch_table}
    WHERE Date = %s AND PeriodEnd < %s;
    """
    cursor.execute(previous_same_day_query, (date_cst, period_end_naive))
    prev_same_day_result = cursor.fetchone()
    prev_same_day_closures = prev_same_day_result['PrevTotalSameDayClosures'] or 0 if prev_same_day_result else 0
    
    # Rolling total: previous aggregations + current aggregation
    rolling_total_same_day_closures = prev_same_day_closures + total_same_day_closures
    logger.debug(f"[{equipment_type_name}] Rolling same-day closures: {prev_same_day_closures} (prev) + {total_same_day_closures} (this batch) = {rolling_total_same_day_closures}")
    
    # Calculate rolling same-day close rate from all closed calls today
    # Get all closed calls today (from previous aggregations + current aggregation)
    # Need to get previous aggregations' closed calls for rolling calculation
    previous_periods_closed_query = f"""
    SELECT SUM(TotalClosedCalls) as PrevTotalClosedCalls
    FROM {batch_table}
    WHERE Date = %s AND PeriodEnd < %s;
    """
    cursor.execute(previous_periods_closed_query, (date_cst, period_end_naive))
    prev_periods_result = cursor.fetchone()
    prev_total_closed_calls = prev_periods_result['PrevTotalClosedCalls'] or 0 if prev_periods_result else 0
    
    total_closed_calls_today = prev_total_closed_calls + total_closed_calls_hour
    rolling_same_day_close_rate = rolling_total_same_day_closures / total_closed_calls_today if total_closed_calls_today > 0 else 0
    logger.debug(f"[{equipment_type_name}] Rolling same-day close rate: {rolling_total_same_day_closures}/{total_closed_calls_today} = {rolling_same_day_close_rate:.2%}")
    
    # Calculate rolling RDR from all appointments today
    # Get all follow-up appointments from previous aggregations today
    previous_rdr_query = f"""
    SELECT 
        SUM(TotalFollowUpAppointments) as PrevTotalFollowUpAppointments
    FROM {batch_table}
    WHERE Date = %s AND PeriodEnd < %s;
    """
    cursor.execute(previous_rdr_query, (date_cst, period_end_naive))
    prev_rdr_result = cursor.fetchone()
    prev_follow_up_appointments = prev_rdr_result['PrevTotalFollowUpAppointments'] or 0 if prev_rdr_result else 0
    
    # Rolling total follow-up appointments
    rolling_total_follow_up_appointments = prev_follow_up_appointments + total_follow_up_appointments
    
    # Get all unique appointments from start of day (query source table for all batches today)
    # Get start of day datetime
    day_start_cst = CST.localize(datetime.combine(date_cst, time(0, 0)))
    day_start_naive = day_start_cst.replace(tzinfo=None)
    period_end_naive_for_query = period_end_naive
    
    # Query all batches from start of day to end of current batch
    all_batches_today_query = f"""
    SELECT DISTINCT \"Pushed At\"
    FROM {SOURCE_TABLE}
    WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s
    ORDER BY \"Pushed At\" ASC;
    """
    cursor.execute(all_batches_today_query, (day_start_naive, period_end_naive_for_query))
    all_batches_today = cursor.fetchall()
    
    # Get all unique appointments from all batches today
    unique_appointments_today = set()
    if all_batches_today:
        batch_timestamps_today = [batch['Pushed At'] for batch in all_batches_today]
        placeholders_today = ', '.join(['%s'] * len(batch_timestamps_today))
        all_appointments_query = f"""
        SELECT DISTINCT \"Appointment\", \"Equipment_ID\"
        FROM {SOURCE_TABLE}
        WHERE \"Pushed At\" IN ({placeholders_today});
        """
        cursor.execute(all_appointments_query, tuple(batch_timestamps_today))
        all_appointment_records = cursor.fetchall()
        
        # Filter by equipment type
        for record in all_appointment_records:
            equipment_id = record.get('Equipment_ID', '')
            if is_recycler(equipment_id) == is_recycler_type:
                unique_appointments_today.add(int(record['Appointment']))
    
    total_appointments_today = len(unique_appointments_today)
    logger.debug(f"[{equipment_type_name}] Unique appointments today (rolling): {total_appointments_today}")
    
    # Calculate rolling RDR
    rolling_rdr = rolling_total_follow_up_appointments / total_appointments_today if total_appointments_today > 0 else 0
    logger.debug(f"[{equipment_type_name}] Rolling RDR: {rolling_total_follow_up_appointments}/{total_appointments_today} = {rolling_rdr:.2%}")
    
    # Calculate batch-only rates (for reference, but we'll use rolling rates)
    batch_rdr = total_follow_up_appointments / total_appointments_hour if total_appointments_hour > 0 else 0
    logger.debug(f"[{equipment_type_name}] Batch-only RDR: {total_follow_up_appointments}/{total_appointments_hour} = {batch_rdr:.2%}")
    
    # Calculate final rates (batch-only for FirstTimeFixRate and AvgAppointmentsPerCompletedCall)
    # Note: SameDayCloseRate and RepeatDispatchRate are now rolling daily metrics
    same_day_close_rate = weighted_same_day_close_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    first_time_fix_rate = weighted_first_time_fix_rate / total_closed_for_rates if total_closed_for_rates > 0 else 0
    avg_appt_per_completed = weighted_avg_appt_per_completed / total_closed_for_rates if total_closed_for_rates > 0 else 0
    
    # Average appointment number (from latest batch, or from previous period if batch missing)
    if not batch_missing and latest_batch:
        avg_appt_num = latest_batch['AverageAppointmentNumber'] or 0
    elif batch_missing:
        # avg_appt_num already set from previous period above (or 0 if no previous period)
        pass
    else:
        avg_appt_num = 0
    
    # New calls and reopened calls - these are harder to aggregate, use latest batch's values
    # In a more sophisticated implementation, we could track these across batches
    total_new_calls = 0  # Would need to be calculated from batch differences
    total_reopened_calls = 0  # Would need to be calculated from batch differences
    
    # Calculate First-Time Fix Rate running total for the day
    # Get previous aggregations' totals for this day to accumulate
    previous_periods_query = f"""
    SELECT 
        SUM(TotalFirstTimeFixes) as PrevTotalFirstTimeFixes,
        SUM(TotalClosedCallsForFTF) as PrevTotalClosedCalls
    FROM {batch_table}
    WHERE Date = %s AND PeriodEnd < %s;
    """
    cursor.execute(previous_periods_query, (date_cst, period_end_naive))
    prev_totals = cursor.fetchone()
    prev_total_first_time_fixes = prev_totals['PrevTotalFirstTimeFixes'] or 0 if prev_totals else 0
    prev_total_closed_calls = prev_totals['PrevTotalClosedCalls'] or 0 if prev_totals else 0
    
    # Accumulate: previous day totals + current batch totals
    cumulative_first_time_fixes = prev_total_first_time_fixes + total_first_time_fixes_hour
    cumulative_closed_calls = prev_total_closed_calls + total_closed_calls_hour
    
    # Calculate running total rate
    first_time_fix_rate_running_total = cumulative_first_time_fixes / cumulative_closed_calls if cumulative_closed_calls > 0 else 0
    
    logger.debug(f"[{equipment_type_name}] First-Time Fix Rate running total: {first_time_fix_rate_running_total:.2%} "
                f"({cumulative_first_time_fixes}/{cumulative_closed_calls})")
    
    # Calculate TotalNotServicedYet (calls with Appointment = 1) from source table
    # Query the latest batch's open calls to count those with Appointment = 1
    # If batch missing, total_not_serviced_yet already set from previous period
    if not batch_missing:
        total_not_serviced_yet = 0
        if batch_timestamps and latest_batch:
            latest_batch_timestamp = latest_batch['Timestamp']
            logger.debug(f"[{equipment_type_name}] Querying source table for calls not serviced yet (Appointment = 1)")
            # Query all records with Appointment = 1, then filter by equipment type in Python
            # This approach is cleaner and avoids complex SQL string formatting
            not_serviced_query = f"""
            SELECT \"Service_Call_ID\", \"Appointment\", \"Equipment_ID\"
            FROM {SOURCE_TABLE}
            WHERE \"Pushed At\" = %s AND \"Appointment\" = 1;
            """
            cursor.execute(not_serviced_query, (latest_batch_timestamp,))
            not_serviced_records = cursor.fetchall()
            logger.debug(f"[{equipment_type_name}] Retrieved {len(not_serviced_records)} records with Appointment = 1")
            
            # Filter by equipment type
            for record in not_serviced_records:
                equipment_id = record.get('Equipment_ID', '')
                if is_recycler(equipment_id) == is_recycler_type:
                    total_not_serviced_yet += 1
            
            logger.debug(f"[{equipment_type_name}] Total not serviced yet (Appointment = 1): {total_not_serviced_yet}")
    # else: total_not_serviced_yet already set from previous period if batch_missing
    
    # Validation (if enabled) - skip if no batches
    if HOURLY_VALIDATION_ENABLED and not batch_missing:
        logger.debug(f"[{equipment_type_name}] Running validation checks")
        if not validate_batch_aggregation(cursor, stat_table, period_start_naive, period_end_naive, 
                                          total_closed_calls, batches):
            logger.warning(f"[{equipment_type_name}] Batch aggregation validation failed for period {period_start_naive} to {period_end_naive}")
            # Continue anyway, but log the warning
        else:
            logger.debug(f"[{equipment_type_name}] Validation passed")
    
    # Insert batch aggregation
    insert_sql = f"""
    INSERT INTO {batch_table} (
        Date, Hour, PeriodMinute, PeriodStart, PeriodEnd, Timestamp,
        TotalOpenCalls, TotalClosedCalls, TotalSameDayClosures,
        TotalCallsWithMultiAppt, TotalNewCalls, TotalReopenedCalls,
        TotalNotServicedYet,
        SumAppointments, SumCompletedAppointments,
        AverageAppointmentNumber, SameDayCloseRate, FirstTimeFixRate,
        AvgAppointmentsPerCompletedCall,
        TotalFirstTimeFixes, TotalClosedCallsForFTF, FirstTimeFixRate_RunningTotal,
        TotalFollowUpAppointments, TotalAppointments, RepeatDispatchRate,
        BatchCount, BatchMissing
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s
    );
    """
    
    cursor.execute(insert_sql, (
        date_cst, hour, period_minute, period_start_naive, period_end_naive, period_end_naive,
        total_open_calls, total_closed_calls, rolling_total_same_day_closures,  # Use rolling total
        total_calls_with_multi_appt, total_new_calls, total_reopened_calls,
        total_not_serviced_yet,
        sum_appointments, sum_completed_appointments,
        avg_appt_num, rolling_same_day_close_rate, first_time_fix_rate,  # Use rolling rate
        avg_appt_per_completed,
        cumulative_first_time_fixes, cumulative_closed_calls, first_time_fix_rate_running_total,
        rolling_total_follow_up_appointments, total_appointments_today, rolling_rdr,  # Use rolling RDR
        len(batches), 1 if batch_missing else 0  # BatchMissing flag
    ))
    
    if batch_missing:
        logger.warning(f"[{equipment_type_name}] Batch aggregation complete: NO BATCHES FOUND - BatchMissing flag set")
    else:
        logger.info(f"[{equipment_type_name}] Batch aggregation complete: {len(batches)} batches, {total_open_calls} open, {total_closed_calls} closed")
    logger.info(f"[{equipment_type_name}] Rolling same-day closures (daily): {rolling_total_same_day_closures}")
    logger.info(f"[{equipment_type_name}] Rolling same-day close rate (daily): {rolling_same_day_close_rate:.2%}")
    logger.info(f"[{equipment_type_name}] Rolling RDR (daily): {rolling_total_follow_up_appointments}/{total_appointments_today} = {rolling_rdr:.2%}")
    logger.debug(f"[{equipment_type_name}] Inserted batch stats into {batch_table}")
    
    return True


def validate_batch_aggregation(cursor, stat_table, period_start, period_end, aggregated_closed, batches):
    """
    Validate that batch aggregation matches batch-level data.
    
    Returns:
        True if validation passes, False otherwise
    """
    if not HOURLY_VALIDATION_ENABLED:
        return True
    
    # Sum up closed calls from batches
    total_from_batches = sum(batch['CallsClosedSinceLastBatch'] or 0 for batch in batches)
    
    # Compare with aggregated value
    if abs(total_from_batches - aggregated_closed) > 1:  # Allow 1 for rounding
        logger.warning(f"Validation warning: Closed calls mismatch - batches: {total_from_batches}, aggregated: {aggregated_closed}")
        return False
    
    return True


def get_last_batch_aggregation_timestamp(cursor, batch_table):
    """
    Get the timestamp of the most recent batch aggregation.
    
    Args:
        cursor: Database cursor
        batch_table: Batch aggregation table name
    
    Returns:
        Timestamp of last aggregation (datetime) or None if no aggregations exist
    """
    query = f"""
    SELECT TOP 1 PeriodEnd FROM {batch_table}
    ORDER BY PeriodEnd DESC;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result['PeriodEnd'] if result else None

