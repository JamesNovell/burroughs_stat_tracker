"""Timezone handling utilities."""
import pytz
from datetime import datetime
from app.config import EOD_HOUR, EOD_MINUTE


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
    """Check if datetime is at or after the configured EOD time in CST."""
    if dt is None:
        return False
    cst_dt = to_cst(dt)
    return cst_dt.hour == EOD_HOUR and cst_dt.minute >= EOD_MINUTE

