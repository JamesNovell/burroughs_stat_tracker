"""Services package - business logic services."""
from app.services.batch_stats import process_equipment_type_stats
from app.services.daily_summary import calculate_daily_summary
from app.services.hourly_aggregator import aggregate_batch_stats, get_last_batch_aggregation_timestamp
from app.services.weekly_aggregator import aggregate_weekly_stats
from app.services.monthly_aggregator import aggregate_monthly_stats
from app.services.tracking import TrackingService
from app.services.batch_service import get_last_processed_timestamp
from app.services.fedex_tracker import is_fedex_tracking_number, get_fedex_tracking_status
from app.services.ups_tracker import is_ups_tracking_number, get_ups_tracking_status

__all__ = [
    'process_equipment_type_stats',
    'calculate_daily_summary',
    'aggregate_batch_stats',
    'get_last_batch_aggregation_timestamp',
    'aggregate_weekly_stats',
    'aggregate_monthly_stats',
    'TrackingService',
    'get_last_processed_timestamp',
    'is_fedex_tracking_number',
    'get_fedex_tracking_status',
    'is_ups_tracking_number',
    'get_ups_tracking_status',
]

