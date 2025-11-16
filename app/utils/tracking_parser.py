"""
Utility functions to determine the latest tracking number
from SQL query output fields.
"""
from typing import Optional, Dict, List


def _split_csv(value: Optional[str]) -> List[str]:
    """Split a comma-separated string into a list of trimmed values."""
    if not value:
        return []
    return [item.strip() for item in str(value).split(',')]


def determine_tracking_number(row: Dict[str, str]) -> str:
    """
    Determine the latest tracking number based on query output fields.

    Rules:
      1. If AllPackNumbers ends with a non-zero number and the corresponding
         AllTrackingStatuses value is numeric, return that number.
      2. If AllPackNumbers ends with a non-zero number but the corresponding
         tracking status ends with 'NP', tracking is not available yet.
      3. If the last AllPackNumbers value is 0 and AllBins does not end with
         'NoBin', return the highest UPS order number from UPSOrderNumbers.
      4. Otherwise return 'not available yet'.
    """
    pack_numbers = _split_csv(row.get('AllPackNumbers'))
    statuses = _split_csv(row.get('AllTrackingStatuses'))
    bins = _split_csv(row.get('AllBins'))
    ups_numbers = _split_csv(row.get('UPSOrderNumbers'))

    last_pack = pack_numbers[-1] if pack_numbers else ''
    last_status = statuses[-1] if statuses else ''
    last_bin = bins[-1] if bins else ''

    # Rule 1 & 2: Last pack non-zero
    if last_pack and last_pack != '0':
        if last_status and last_status.isdigit():
            return last_status
        if last_status and last_status.upper().endswith('NP'):
            return 'not available yet'

    # Rule 3: Last pack is 0 and last bin is not NoBin
    if last_pack == '0' and (not last_bin or last_bin.lower() != 'nobin'):
        numeric_ups = [num for num in ups_numbers if num.isdigit()]
        if numeric_ups:
            return max(numeric_ups, key=int)

    # Default
    return 'not available yet'


def extract_latest_parts(all_parts_value: Optional[str]) -> List[str]:
    """
    Extract the latest set of parts from the AllParts field.
    Each top-level set is wrapped in parentheses. Nested parentheses
    inside part descriptions are supported.
    Returns a list of parts for the last set.
    """
    if not all_parts_value:
        return []

    text = str(all_parts_value)
    depth = 0
    start_idx = None
    latest_segment: Optional[str] = None

    for idx, char in enumerate(text):
        if char == '(':
            if depth == 0:
                start_idx = idx + 1
            depth += 1
        elif char == ')':
            if depth > 0:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    latest_segment = text[start_idx:idx].strip()

    if not latest_segment:
        return []

    return [item.strip() for item in latest_segment.split('||') if item.strip()]

