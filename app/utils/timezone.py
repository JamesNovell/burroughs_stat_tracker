"""
Timezone handling utilities.

All timezone operations use Central Time (CST/CDT) which is automatically handled by pytz.
Functions convert datetimes to CST, calculate week/month boundaries, and detect end-of-period conditions.
"""
import pytz
from datetime import datetime
from app.config import EOD_HOUR, EOD_MINUTE, WEEK_STARTS_ON


# Central Time (handles CST/CDT automatically)
CST = pytz.timezone('America/Chicago')


def to_cst(dt):
    """Convert a datetime to CST timezone. If datetime is naive, assume it's already in CST."""
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(str(dt).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        # Assume naive datetime is in CST
        return CST.localize(dt)
    # Convert to CST
    return dt.astimezone(CST)


def get_cst_date(dt):
    """Get the date in CST timezone."""
    if dt is None:
        return None
    cst_dt = to_cst(dt)
    return cst_dt.date()


def is_end_of_day_cst(dt):
    """
    Check if datetime is 30 minutes after the configured EOD time in CST.
    This gives time for the batch to arrive around the hour marker.
    EOD is configured for a specific date, but trigger happens 30 minutes later.
    """
    if dt is None:
        return False
    cst_dt = to_cst(dt)
    # EOD trigger happens 30 minutes after configured EOD time
    eod_trigger_minute = EOD_MINUTE + 30
    eod_trigger_hour = EOD_HOUR
    
    # Handle minute overflow (e.g., if EOD is 23:59, trigger is 00:29 next day)
    if eod_trigger_minute >= 60:
        eod_trigger_hour = (eod_trigger_hour + 1) % 24
        eod_trigger_minute = eod_trigger_minute % 60
    
    # Check if we're at or after the trigger time
    if eod_trigger_hour == 0:  # Trigger rolled over to next day (e.g., EOD 23:59 -> trigger 00:29)
        # Check if it's the next day and at/after trigger time
        # For this case, we need to check if we're past midnight
        if cst_dt.hour == 0:
            return cst_dt.minute >= eod_trigger_minute
        elif cst_dt.hour > 0:
            # Already past the trigger time on the next day
            return True
        else:
            return False
    else:
        # Trigger is on the same day
        if cst_dt.hour > eod_trigger_hour:
            return True
        elif cst_dt.hour == eod_trigger_hour:
            return cst_dt.minute >= eod_trigger_minute
        else:
            return False


def get_week_start_end(date, week_starts_on="Sunday"):
    """
    Get the start and end dates of the week containing the given date.
    
    Args:
        date: Date object (in CST)
        week_starts_on: "Sunday" or "Monday" (default: "Sunday")
    
    Returns:
        Tuple of (week_start_date, week_end_date)
    """
    from datetime import timedelta
    
    # Get weekday (0=Monday, 6=Sunday)
    weekday = date.weekday()
    
    if week_starts_on == "Sunday":
        # Sunday = 6, Monday = 0, etc.
        days_since_sunday = (weekday + 1) % 7
        week_start = date - timedelta(days=days_since_sunday)
    else:  # Monday
        # Monday = 0, Tuesday = 1, etc.
        week_start = date - timedelta(days=weekday)
    
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def get_week_number(date, week_starts_on="Sunday"):
    """
    Get the ISO week number for a date.
    
    Args:
        date: Date object (in CST)
        week_starts_on: "Sunday" or "Monday" (default: "Sunday")
    
    Returns:
        Tuple of (year, week_number)
    """
    from datetime import timedelta
    
    # Get the first day of the year
    year_start = date.replace(month=1, day=1)
    
    # Get week start/end for the date
    week_start, _ = get_week_start_end(date, week_starts_on)
    
    # Calculate days from year start to week start
    days_from_year_start = (week_start - year_start).days
    
    # Calculate week number (1-based)
    week_number = (days_from_year_start // 7) + 1
    
    # Handle edge case: if week starts in previous year, use that year
    if week_start.year < date.year:
        return week_start.year, week_number
    
    return date.year, week_number


def is_end_of_week_cst(dt, week_starts_on=None):
    """
    Check if datetime is at the end of the week (Saturday at EOD) in CST.
    
    Args:
        dt: Datetime to check
        week_starts_on: "Sunday" or "Monday" (default: from config)
    
    Returns:
        True if it's Saturday at EOD, False otherwise
    """
    if dt is None:
        return False
    if week_starts_on is None:
        week_starts_on = WEEK_STARTS_ON
    cst_dt = to_cst(dt)
    
    # Check if it's Saturday (5 = Saturday in weekday(), 6 = Sunday)
    if week_starts_on == "Sunday":
        is_saturday = cst_dt.weekday() == 5  # Saturday
    else:  # Monday
        is_saturday = cst_dt.weekday() == 6  # Sunday (end of week if week starts Monday)
    
    # Check if it's at EOD time
    is_eod = cst_dt.hour == EOD_HOUR and cst_dt.minute >= EOD_MINUTE
    
    return is_saturday and is_eod


def is_end_of_month_cst(dt):
    """
    Check if datetime is at the end of the month (last day at EOD) in CST.
    
    Args:
        dt: Datetime to check
    
    Returns:
        True if it's the last day of the month at EOD, False otherwise
    """
    if dt is None:
        return False
    cst_dt = to_cst(dt)
    
    # Check if it's at EOD time
    is_eod = cst_dt.hour == EOD_HOUR and cst_dt.minute >= EOD_MINUTE
    
    # Check if it's the last day of the month
    from datetime import timedelta
    next_day = cst_dt.date() + timedelta(days=1)
    is_last_day = next_day.month != cst_dt.month
    
    return is_last_day and is_eod

