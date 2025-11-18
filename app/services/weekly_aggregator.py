"""
Weekly aggregation service - aggregates daily summaries into weekly statistics.

This module aggregates daily summaries into weekly statistics, calculating:
- Total open/closed calls (snapshot at end of week for open, sum for closed)
- Rolling daily metrics from latest day's persisted values:
  * TotalSameDayClosures: from latest day's rolling total
  * SameDayCloseRate: from latest day's rolling rate
  * RepeatDispatchRate: from latest day's rolling rate
  * FirstTimeFixRate_RunningTotal: from latest day's running total
- Week boundaries are configurable (default: Sunday to Saturday)

Weekly summaries are persisted at the end of week (Saturday at EOD for Sunday-start week).
Rolling metrics reset at the next week boundary.
"""
import logging
from app.config import WEEKLY_AGGREGATION_ENABLED, WEEK_STARTS_ON
from app.utils.timezone import to_cst, get_cst_date, get_week_start_end, get_week_number

logger = logging.getLogger(__name__)


def aggregate_weekly_stats(cursor, daily_table, weekly_table, latest_pushed_at, equipment_type_name):
    """
    Aggregate daily summaries into weekly statistics.
    
    Args:
        cursor: Database cursor
        daily_table: Daily summary table name
        weekly_table: Weekly summary table name
        latest_pushed_at: Timestamp of the latest batch (to determine which week to aggregate)
        equipment_type_name: Name for logging (e.g., "Recyclers")
    
    Returns:
        True if aggregation was successful, False otherwise
    """
    if not WEEKLY_AGGREGATION_ENABLED:
        logger.debug(f"[{equipment_type_name}] Weekly aggregation disabled, skipping")
        return False
    
    # Get the date in CST
    current_date_cst = get_cst_date(latest_pushed_at)
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    
    # Get week boundaries (use config value for week start)
    week_start, week_end = get_week_start_end(current_date_cst, WEEK_STARTS_ON)
    year, week_number = get_week_number(current_date_cst, WEEK_STARTS_ON)
    
    logger.info(f"[{equipment_type_name}] Aggregating weekly stats for week {week_number} of {year} ({week_start} to {week_end})")
    
    # Check if weekly aggregation already exists for this week
    check_existing = f"""
    SELECT WeeklyStatID FROM {weekly_table}
    WHERE Year = %s AND WeekNumber = %s;
    """
    cursor.execute(check_existing, (year, week_number))
    existing = cursor.fetchone()
    
    if existing:
        logger.info(f"[{equipment_type_name}] Weekly aggregation for week {week_number} of {year} already exists. Skipping.")
        return False
    
    # Get all daily summaries within this week
    daily_stats_query = f"""
    SELECT 
        Date,
        Timestamp,
        AvgApptNum_OpenAtEndOfDay,
        AvgApptNum_ClosedToday,
        TotalOpenAtEndOfDay,
        TotalClosedEOD,
        TotalSameDayClosures,
        TotalCallsWithMultiAppt,
        TotalNotServicedYet,
        FirstTimeFixRate_RunningTotal,
        RepeatDispatchRate
    FROM {daily_table}
    WHERE Date >= %s AND Date <= %s
    ORDER BY Date ASC;
    """
    logger.debug(f"[{equipment_type_name}] Querying daily stats from {daily_table} for week {week_start} to {week_end}")
    cursor.execute(daily_stats_query, (week_start, week_end))
    daily_stats = cursor.fetchall()
    logger.debug(f"[{equipment_type_name}] Retrieved {len(daily_stats)} daily stat records")
    
    if not daily_stats:
        logger.warning(f"[{equipment_type_name}] No daily stats found for week {week_number} of {year}")
        return False
    
    # Aggregate daily stats
    # For open calls at end of week, use the latest day's TotalOpenAtEndOfDay
    # For closed calls, sum all TotalClosedEOD
    # For averages, use latest day's values for open calls, calculate from sum for closed calls
    
    total_open_at_end_of_week = 0
    total_closed_this_week = 0
    total_same_day_closures = 0
    total_calls_with_multi_appt = 0
    total_not_serviced_yet = 0
    avg_appt_num_open_eow = 0
    sum_appointments_closed = 0
    
    latest_daily_stat = None
    
    for daily_stat in daily_stats:
        # Use latest day's open calls count (overwritten each iteration, ends with last day)
        total_open_at_end_of_week = daily_stat['TotalOpenAtEndOfDay'] or 0
        
        # Use latest day's average appointment number for open calls (snapshot at end of week)
        avg_appt_num_open_eow = daily_stat['AvgApptNum_OpenAtEndOfDay'] or 0
        
        # Sum closed calls (for calculating avg appointment number for closed calls)
        total_closed_this_week += daily_stat['TotalClosedEOD'] or 0
        
        # Use latest day's rolling same-day closures (rolling daily metric, persisted at EOD)
        total_same_day_closures = daily_stat['TotalSameDayClosures'] or 0
        
        # Use latest day's values for snapshot metrics
        total_calls_with_multi_appt = daily_stat['TotalCallsWithMultiAppt'] or 0
        total_not_serviced_yet = daily_stat['TotalNotServicedYet'] or 0
        
        # Sum completed appointments (for closed calls average)
        closed_count = daily_stat['TotalClosedEOD'] or 0
        avg_closed = daily_stat['AvgApptNum_ClosedToday'] or 0
        if closed_count > 0 and avg_closed > 0:
            sum_appointments_closed += int(avg_closed * closed_count)
        
        # For rates: use latest day's rolling rates (rolling daily metrics, persisted at EOD)
        # Note: SameDayClosures, RepeatDispatchRate are rolling daily metrics
        # FirstTimeFixRate_RunningTotal is also a rolling daily metric
        # Use latest day's rolling rates (persisted at EOD)
        first_time_fix_rate_running_total = daily_stat['FirstTimeFixRate_RunningTotal'] or 0
        repeat_dispatch_rate = daily_stat['RepeatDispatchRate'] or 0
        
        latest_daily_stat = daily_stat
    
    # Calculate averages
    avg_appt_open_eow = avg_appt_num_open_eow  # From latest day
    avg_appt_closed_week = sum_appointments_closed / total_closed_this_week if total_closed_this_week > 0 else 0
    
    # Use latest day's rolling rates (already set from latest_daily_stat in loop)
    # first_time_fix_rate_running_total and repeat_dispatch_rate are already set from latest day
    
    # Insert weekly summary
    insert_weekly_sql = f"""
    INSERT INTO {weekly_table} (
        WeekStartDate, WeekEndDate, Year, WeekNumber, Timestamp,
        AvgApptNum_OpenAtEndOfWeek, AvgApptNum_ClosedThisWeek,
        TotalOpenAtEndOfWeek, TotalClosedThisWeek, TotalSameDayClosures,
        TotalCallsWithMultiAppt, TotalNotServicedYet,
        FirstTimeFixRate_RunningTotal, RepeatDispatchRate,
        DayCount
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    );
    """
    cursor.execute(insert_weekly_sql, (
        week_start, week_end, year, week_number, latest_pushed_at_cst.replace(tzinfo=None),
        avg_appt_open_eow, avg_appt_closed_week,
        total_open_at_end_of_week, total_closed_this_week, total_same_day_closures,
        total_calls_with_multi_appt, total_not_serviced_yet,
        first_time_fix_rate_running_total, repeat_dispatch_rate,
        len(daily_stats)
    ))
    
    logger.info(f"[{equipment_type_name}] Weekly aggregation complete for week {week_number} of {year}:")
    logger.info(f"[{equipment_type_name}]   Open at EOW: {total_open_at_end_of_week} calls, Avg Appt: {avg_appt_open_eow:.2f}")
    logger.info(f"[{equipment_type_name}]   Closed this week: {total_closed_this_week} calls, Avg Appt: {avg_appt_closed_week:.2f}")
    logger.info(f"[{equipment_type_name}]   Same-day closures: {total_same_day_closures}")
    logger.info(f"[{equipment_type_name}]   First-Time Fix Rate: {first_time_fix_rate_running_total:.2%}")
    logger.info(f"[{equipment_type_name}]   RDR: {repeat_dispatch_rate:.2%}")
    logger.info(f"[{equipment_type_name}] Successfully saved weekly summary to {weekly_table}")
    
    return True

