"""Helper functions for equipment type detection, timezone handling, and data processing."""
import pytz
from datetime import datetime
from config import EOD_HOUR, EOD_MINUTE


# --- Equipment Type Helpers ---
def is_recycler(equipment_id):
    """Check if equipment is a recycler based on Equipment_ID prefix."""
    if not equipment_id:
        return False
    equipment_id = str(equipment_id).upper()
    return equipment_id.startswith(('N4R', 'N9R', 'N7F', 'RF'))


def filter_by_equipment_type(calls_dict, is_recycler_filter=True):
    """Filter calls dictionary by equipment type."""
    filtered = {}
    for call_id, record in calls_dict.items():
        equipment_id = record.get('Equipment_ID', '')
        if is_recycler_filter == is_recycler(equipment_id):
            filtered[call_id] = record
    return filtered


# --- Timezone Helpers ---
CST = pytz.timezone('America/Chicago')  # Central Time (handles CST/CDT automatically)


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


# --- Data Processing Helpers ---
def deduplicate_records(records):
    """Given a list of records for a batch, return a dict of unique calls, keeping the most recent entry."""
    # Sort by ID descending to ensure the first one we see for a Service_Call_ID is the latest
    unique_calls = {}
    for record in sorted(records, key=lambda x: x['ID'], reverse=True):
        call_id = record['Service_Call_ID']
        if call_id not in unique_calls:
            unique_calls[call_id] = record
    return unique_calls

