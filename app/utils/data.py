"""
Data processing utilities for batch record handling.
"""
def deduplicate_records(records):
    """Given a list of records for a batch, return a dict of unique calls, keeping the most recent entry."""
    # Sort by ID descending to ensure the first one we see for a Service_Call_ID is the latest
    unique_calls = {}
    for record in sorted(records, key=lambda x: x['ID'], reverse=True):
        call_id = record['Service_Call_ID']
        if call_id not in unique_calls:
            unique_calls[call_id] = record
    return unique_calls

