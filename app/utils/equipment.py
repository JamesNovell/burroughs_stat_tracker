"""Equipment type detection and filtering utilities."""


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

