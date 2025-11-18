"""
Monthly aggregation service - aggregates weekly summaries into monthly statistics.

This module aggregates weekly summaries into monthly statistics, calculating:
- Total open/closed calls (snapshot at end of month for open, sum for closed)
- Rolling daily metrics from latest week's persisted values:
  * TotalSameDayClosures: from latest week's rolling total
  * SameDayCloseRate: from latest week's rolling rate
  * RepeatDispatchRate: from latest week's rolling rate
  * FirstTimeFixRate_RunningTotal: from latest week's running total
- Includes weeks that overlap with the month boundaries

Monthly summaries are persisted at the end of month (last day at EOD).
Rolling metrics reset at the next month boundary.
"""
import logging
from calendar import monthrange
from app.config import MONTHLY_AGGREGATION_ENABLED
from app.utils.timezone import to_cst, get_cst_date

logger = logging.getLogger(__name__)


def aggregate_monthly_stats(cursor, weekly_table, monthly_table, latest_pushed_at, equipment_type_name):
    """
    Aggregate weekly summaries into monthly statistics.
    
    Args:
        cursor: Database cursor
        weekly_table: Weekly summary table name
        monthly_table: Monthly summary table name
        latest_pushed_at: Timestamp of the latest batch (to determine which month to aggregate)
        equipment_type_name: Name for logging (e.g., "Recyclers")
    
    Returns:
        True if aggregation was successful, False otherwise
    """
    if not MONTHLY_AGGREGATION_ENABLED:
        logger.debug(f"[{equipment_type_name}] Monthly aggregation disabled, skipping")
        return False
    
    # Get the date in CST
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    year = current_date_cst.year
    month = current_date_cst.month
    
    # Get month boundaries
    month_start = current_date_cst.replace(day=1)
    last_day = monthrange(year, month)[1]
    month_end = current_date_cst.replace(day=last_day)
    
    logger.info(f"[{equipment_type_name}] Aggregating monthly stats for {year}-{month:02d} ({month_start} to {month_end})")
    
    # Check if monthly aggregation already exists for this month
    check_existing = f"""
    SELECT MonthlyStatID FROM {monthly_table}
    WHERE Year = %s AND Month = %s;
    """
    cursor.execute(check_existing, (year, month))
    existing = cursor.fetchone()
    
    if existing:
        logger.info(f"[{equipment_type_name}] Monthly aggregation for {year}-{month:02d} already exists. Skipping.")
        return False
    
    # Get all weekly summaries within this month
    # A week is included if its WeekStartDate or WeekEndDate falls within the month
    weekly_stats_query = f"""
    SELECT 
        WeekStartDate,
        WeekEndDate,
        Year,
        WeekNumber,
        Timestamp,
        AvgApptNum_OpenAtEndOfWeek,
        AvgApptNum_ClosedThisWeek,
        TotalOpenAtEndOfWeek,
        TotalClosedThisWeek,
        TotalSameDayClosures,
        TotalCallsWithMultiAppt,
        TotalNotServicedYet,
        FirstTimeFixRate_RunningTotal,
        RepeatDispatchRate
    FROM {weekly_table}
    WHERE (WeekStartDate >= %s AND WeekStartDate <= %s)
       OR (WeekEndDate >= %s AND WeekEndDate <= %s)
       OR (WeekStartDate < %s AND WeekEndDate > %s)
    ORDER BY WeekStartDate ASC;
    """
    logger.debug(f"[{equipment_type_name}] Querying weekly stats from {weekly_table} for month {year}-{month:02d}")
    cursor.execute(weekly_stats_query, (month_start, month_end, month_start, month_end, month_start, month_end))
    weekly_stats = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Retrieved {len(weekly_stats)} weekly stat records")
    
    if not weekly_stats:
        logger.warning(f"[{equipment_type_name}] No weekly stats found for {year}-{month:02d}")
        return False
    
    # Aggregate weekly stats
    # For open calls at end of month, use the latest week's TotalOpenAtEndOfWeek
    # For closed calls, sum all TotalClosedThisWeek
    # For averages, use latest week's values for open calls, calculate from sum for closed calls
    
    total_open_at_end_of_month = 0
    total_closed_this_month = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    total_not_serviced_yet = 0
    avg_appt_num_open_eom = 0
    sum_appointments_closed = 0
    
    latest_weekly_stat = None
    
    for weekly_stat in weekly_stats:
        # Use latest week's open calls count (overwritten each iteration, ends with last week)
        total_open_at_end_of_month = weekly_stat['TotalOpenAtEndOfWeek'] or 0
        
        # Use latest week's average appointment number for open calls (snapshot at end of month)
        avg_appt_num_open_eom = weekly_stat['AvgApptNum_OpenAtEndOfWeek'] or 0
        
        # Sum closed calls (for calculating avg appointment number for closed calls)
        total_closed_this_month += weekly_stat['TotalClosedThisWeek'] or 0
        
        # Use latest week's rolling same-day closures (rolling daily metric, persisted at end of week)
        total_same_day_closures = weekly_stat['TotalSameDayClosures'] or 0
        
        # Use latest week's values for snapshot metrics
        total_calls_with_multi_appt = weekly_stat['TotalCallsWithMultiAppt'] or 0
        total_not_serviced_yet = weekly_stat['TotalNotServicedYet'] or 0
        
        # Sum completed appointments (for closed calls average)
        closed_count = weekly_stat['TotalClosedThisWeek'] or 0
        avg_closed = weekly_stat['AvgApptNum_ClosedThisWeek'] or 0
        if closed_count > 0 and avg_closed > 0:
            sum_appointments_closed += int(avg_closed * closed_count)
        
        # For rates: use latest week's rolling rates (rolling daily metrics, persisted at end of week)
        # Note: SameDayClosures, RepeatDispatchRate are rolling daily metrics
        # FirstTimeFixRate_RunningTotal is also a rolling daily metric
        # Use latest week's rolling rates (persisted at end of week)
        first_time_fix_rate_running_total = weekly_stat['FirstTimeFixRate_RunningTotal'] or 0
        repeat_dispatch_rate = weekly_stat['RepeatDispatchRate'] or 0
        
        latest_weekly_stat = weekly_stat
    
    # Calculate averages
    avg_appt_open_eom = avg_appt_num_open_eom  # From latest week
    avg_appt_closed_month = sum_appointments_closed / total_closed_this_month if total_closed_this_month > 0 else 0
    
    # Use latest week's rolling rates (already set from latest_weekly_stat in loop)
    # first_time_fix_rate_running_total and repeat_dispatch_rate are already set from latest week
    
    # Insert monthly summary
    insert_monthly_sql = f"""
    INSERT INTO {monthly_table} (
        Year, Month, MonthStartDate, MonthEndDate, Timestamp,
        AvgApptNum_OpenAtEndOfMonth, AvgApptNum_ClosedThisMonth,
        TotalOpenAtEndOfMonth, TotalClosedThisMonth, TotalSameDayClosures,
        TotalCallsWithMultiAppt, TotalNotServicedYet,
        FirstTimeFixRate_RunningTotal, RepeatDispatchRate,
        WeekCount
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    );
    """
    cursor.execute(insert_monthly_sql, (
        year, month, month_start, month_end, latest_pushed_at_cst.replace(tzinfo=None),
        avg_appt_open_eom, avg_appt_closed_month,
        total_open_at_end_of_month, total_closed_this_month, total_same_day_closures,
        total_calls_with_multi_appt, total_not_serviced_yet,
        first_time_fix_rate_running_total, repeat_dispatch_rate,
        len(weekly_stats)
    ))
    
    logger.info(f"[{equipment_type_name}] Monthly aggregation complete for {year}-{month:02d}:")
    logger.info(f"[{equipment_type_name}]   Open at EOM: {total_open_at_end_of_month} calls, Avg Appt: {avg_appt_open_eom:.2f}")
    logger.info(f"[{equipment_type_name}]   Closed this month: {total_closed_this_month} calls, Avg Appt: {avg_appt_closed_month:.2f}")
    logger.info(f"[{equipment_type_name}]   Same-day closures: {total_same_day_closures}")
    logger.info(f"[{equipment_type_name}]   First-Time Fix Rate: {first_time_fix_rate_running_total:.2%}")
    logger.info(f"[{equipment_type_name}]   RDR: {repeat_dispatch_rate:.2%}")
    logger.info(f"[{equipment_type_name}] Successfully saved monthly summary to {monthly_table}")
    
    return True

