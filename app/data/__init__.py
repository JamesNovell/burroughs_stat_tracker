"""Data layer for database operations."""
from app.data.database import (
    get_db_connection,
    create_tables_if_not_exist,
    create_stat_table,
    create_history_table,
    create_hourly_stat_table,
    create_daily_summary_table,
    ensure_tracking_columns_exist,
)

__all__ = [
    'get_db_connection',
    'create_tables_if_not_exist',
    'create_stat_table',
    'create_history_table',
    'create_hourly_stat_table',
    'create_daily_summary_table',
    'ensure_tracking_columns_exist',
]

