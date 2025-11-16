"""Batch-related service functions for querying batch information."""
from app.config import RECYCLERS_STAT_TABLE, SMART_SAFES_STAT_TABLE


def get_last_processed_timestamp(cursor):
    """Get the timestamp of the last processed batch from either stat table."""
    last_recyclers_query = f"SELECT TOP 1 BatchID, Timestamp FROM {RECYCLERS_STAT_TABLE} ORDER BY Timestamp DESC;"
    last_smart_safes_query = f"SELECT TOP 1 BatchID, Timestamp FROM {SMART_SAFES_STAT_TABLE} ORDER BY Timestamp DESC;"
    cursor.execute(last_recyclers_query)
    last_recyclers = cursor.fetchone()
    cursor.execute(last_smart_safes_query)
    last_smart_safes = cursor.fetchone()
    
    # Use the most recent timestamp from either table
    last_processed = None
    if last_recyclers and last_smart_safes:
        last_processed = last_recyclers if last_recyclers['Timestamp'] >= last_smart_safes['Timestamp'] else last_smart_safes
    elif last_recyclers:
        last_processed = last_recyclers
    elif last_smart_safes:
        last_processed = last_smart_safes
    
    return last_processed['Timestamp'] if last_processed else None

