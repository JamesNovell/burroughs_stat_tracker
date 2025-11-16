"""Controller layer - orchestrates batch processing and polling."""
import time
import pymssql
import logging
from datetime import datetime
from app.config import (
    SOURCE_TABLE, RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_HOURLY_TABLE, RECYCLERS_DAILY_TABLE,
    SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_HOURLY_TABLE, SMART_SAFES_DAILY_TABLE,
    POLL_INTERVAL_MINUTES
)
from app.data.database import get_db_connection, create_tables_if_not_exist
from app.utils import deduplicate_records
from app.utils.timezone import to_cst
from app.services import (
    process_equipment_type_stats, calculate_daily_summary, get_last_processed_timestamp,
    aggregate_hourly_stats, should_trigger_hourly_aggregation
)
from app.services.tracking import TrackingService

logger = logging.getLogger(__name__)


def process_batch():
    """Main function to process and track statistics. Returns True if a batch was processed, False otherwise."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)

        # 1. Ensure tables exist
        create_tables_if_not_exist(cursor)

        # 2. Get the latest batch from source table
        # Get distinct pushed_at values and pick the most recent one
        distinct_batches_query = f"SELECT DISTINCT TOP 1 \"Pushed At\" FROM {SOURCE_TABLE} ORDER BY \"Pushed At\" DESC;"
        cursor.execute(distinct_batches_query)
        latest_batch_info = cursor.fetchone()

        if not latest_batch_info:
            print(f"No data found in {SOURCE_TABLE}.")
            return False

        latest_pushed_at = latest_batch_info["Pushed At"]
        
        # Get the Batch ID from the latest batch (all records with same Pushed At should have same Batch ID)
        batch_id_query = f"SELECT TOP 1 \"Batch ID\" FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;"
        cursor.execute(batch_id_query, (latest_pushed_at,))
        batch_id_result = cursor.fetchone()
        latest_batch_id = batch_id_result["Batch ID"] if batch_id_result else 0

        # 3. Check if this batch has already been processed (check both tables)
        check_recyclers = f"SELECT TOP 1 BatchID FROM {RECYCLERS_STAT_TABLE} WHERE BatchID = %s;"
        check_smart_safes = f"SELECT TOP 1 BatchID FROM {SMART_SAFES_STAT_TABLE} WHERE BatchID = %s;"
        cursor.execute(check_recyclers, (latest_batch_id,))
        recyclers_processed = cursor.fetchone()
        cursor.execute(check_smart_safes, (latest_batch_id,))
        smart_safes_processed = cursor.fetchone()

        if recyclers_processed and smart_safes_processed:
            print(f"Batch {latest_batch_id} (pushed at {latest_pushed_at}) has already been processed for both equipment types.")
            print("No new data to process.")
            return False

        # 4. Get the last processed batch for comparison (check both tables)
        last_processed = None
        last_processed_timestamp = get_last_processed_timestamp(cursor)
        if last_processed_timestamp:
            # Get the full record for comparison
            last_recyclers_query = f"SELECT TOP 1 BatchID, Timestamp FROM {RECYCLERS_STAT_TABLE} ORDER BY Timestamp DESC;"
            last_smart_safes_query = f"SELECT TOP 1 BatchID, Timestamp FROM {SMART_SAFES_STAT_TABLE} ORDER BY Timestamp DESC;"
            cursor.execute(last_recyclers_query)
            last_recyclers = cursor.fetchone()
            cursor.execute(last_smart_safes_query)
            last_smart_safes = cursor.fetchone()
            
            if last_recyclers and last_smart_safes:
                last_processed = last_recyclers if last_recyclers['Timestamp'] >= last_smart_safes['Timestamp'] else last_smart_safes
            elif last_recyclers:
                last_processed = last_recyclers
            elif last_smart_safes:
                last_processed = last_smart_safes

        # 5. Fetch and process records for the latest batch
        cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number", "DesNote", "PartNote", "parts_tracking"'
        
        cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (latest_pushed_at,))
        latest_records_raw = cursor.fetchall()
        latest_calls = deduplicate_records(latest_records_raw)

        # 6. Get previous batch for comparison
        previous_calls = {}
        previous_pushed_at = None
        
        if last_processed:
            # Use the Timestamp from the last processed stat (which is the Pushed At of that batch)
            previous_pushed_at = last_processed['Timestamp']
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at,))
            previous_records_raw = cursor.fetchall()
            previous_calls = deduplicate_records(previous_records_raw)
        else:
            # First run - get the second most recent batch if available for comparison
            second_batch_query = f"SELECT DISTINCT TOP 2 \"Pushed At\" FROM {SOURCE_TABLE} ORDER BY \"Pushed At\" DESC;"
            cursor.execute(second_batch_query)
            all_batches = cursor.fetchall()
            
            if len(all_batches) > 1:
                previous_pushed_at = all_batches[1]["Pushed At"]
                cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at,))
                previous_records_raw = cursor.fetchall()
                previous_calls = deduplicate_records(previous_records_raw)
        
        # 7. Process tracking for all records in the new batch
        try:
            tracking_service = TrackingService()
            print(f"\n=== Processing tracking for {len(latest_calls)} records in batch {latest_batch_id} ===")
            
            processed_count = 0
            error_count = 0
            
            for call_id, record in latest_calls.items():
                vendor_call_number = record.get('Vendor Call Number')
                
                if not vendor_call_number:
                    continue
                
                try:
                    # Query tracking database
                    tracking_result = tracking_service.query_tracking_info(vendor_call_number)
                    
                    if tracking_result:
                        tracking_number, parts_list = tracking_result
                        
                        # Get DesNote, PartNote, and parts_tracking from the record
                        des_note = record.get('DesNote')
                        part_note = record.get('PartNote')
                        parts_tracking = record.get('parts_tracking')
                        
                        # Check for match
                        tracking_match = tracking_service.check_tracking_match(
                            tracking_number, des_note, part_note, parts_tracking
                        )
                        
                        # Update columns
                        tracking_service.update_tracking_columns(
                            cursor, call_id, tracking_number, parts_list, tracking_match
                        )
                        
                        processed_count += 1
                    else:
                        # No tracking info found, set to false
                        tracking_service.update_tracking_columns(
                            cursor, call_id, '', [], False
                        )
                        processed_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing tracking for service call {call_id} (vendor call {vendor_call_number}): {str(e)}")
                    # Continue with next record
                    continue
            
            print(f"Tracking processing complete: {processed_count} processed, {error_count} errors")
            
        except Exception as e:
            logger.error(f"Error initializing tracking service: {str(e)}")
            print(f"Warning: Tracking processing failed: {str(e)}")
            # Continue with batch processing even if tracking fails

        # 8. Process stats for Recyclers
        if not recyclers_processed:
            process_equipment_type_stats(
                cursor, latest_calls, previous_calls, latest_pushed_at, latest_batch_id,
                RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, "Recyclers", True
            )
            
            # Check if hourly aggregation should be triggered
            latest_pushed_at_cst = to_cst(latest_pushed_at)
            last_processed_timestamp_cst = to_cst(last_processed['Timestamp']) if last_processed else None
            
            should_trigger, hour_start_cst, hour_end_cst = should_trigger_hourly_aggregation(
                latest_pushed_at_cst, last_processed_timestamp_cst
            )
            
            if should_trigger:
                print(f"\n=== Triggering hourly aggregation for Recyclers ===")
                aggregate_hourly_stats(
                    cursor, RECYCLERS_STAT_TABLE, RECYCLERS_HOURLY_TABLE,
                    hour_start_cst, hour_end_cst, "Recyclers"
                )
            
            # Calculate daily summary if it's end of day
            calculate_daily_summary(
                cursor, latest_pushed_at, latest_calls,
                RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_HOURLY_TABLE, RECYCLERS_DAILY_TABLE,
                "Recyclers", True
            )
        
        # 9. Process stats for Smart Safes
        if not smart_safes_processed:
            process_equipment_type_stats(
                cursor, latest_calls, previous_calls, latest_pushed_at, latest_batch_id,
                SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, "Smart Safes", False
            )
            
            # Check if hourly aggregation should be triggered
            latest_pushed_at_cst = to_cst(latest_pushed_at)
            last_processed_timestamp_cst = to_cst(last_processed['Timestamp']) if last_processed else None
            
            should_trigger, hour_start_cst, hour_end_cst = should_trigger_hourly_aggregation(
                latest_pushed_at_cst, last_processed_timestamp_cst
            )
            
            if should_trigger:
                print(f"\n=== Triggering hourly aggregation for Smart Safes ===")
                aggregate_hourly_stats(
                    cursor, SMART_SAFES_STAT_TABLE, SMART_SAFES_HOURLY_TABLE,
                    hour_start_cst, hour_end_cst, "Smart Safes"
                )
            
            # Calculate daily summary if it's end of day
            calculate_daily_summary(
                cursor, latest_pushed_at, latest_calls,
                SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_HOURLY_TABLE, SMART_SAFES_DAILY_TABLE,
                "Smart Safes", False
            )
        
        return True

    except pymssql.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False
    finally:
        if conn:
            conn.close()


def poll_for_batches(poll_interval_minutes=None):
    """
    Continuously poll for new batches, checking approximately every poll_interval_minutes
    after the last batch was processed.
    
    Args:
        poll_interval_minutes: Minutes to wait after last batch processing before next check.
                              If None, uses value from config.json (default: 5 minutes)
    """
    if poll_interval_minutes is None:
        poll_interval_minutes = POLL_INTERVAL_MINUTES
    
    print(f"Starting batch polling service (checking every ~{poll_interval_minutes} minutes after last batch)")
    print("Press Ctrl+C to stop")
    
    poll_interval_seconds = poll_interval_minutes * 60
    
    while True:
        try:
            # Try to process any available batches
            batch_processed = process_batch()
            
            if batch_processed:
                print(f"\n[{datetime.now()}] Batch processed successfully. Waiting {poll_interval_minutes} minutes before next check...")
            else:
                # No new batch found, check again after the poll interval
                print(f"\n[{datetime.now()}] No new batches found. Waiting {poll_interval_minutes} minutes before next check...")
            
            # Wait for the poll interval
            time.sleep(poll_interval_seconds)
            
        except KeyboardInterrupt:
            print("\n\nPolling stopped by user.")
            break
        except Exception as e:
            print(f"\n[{datetime.now()}] Error in polling loop: {e}")
            print(f"Waiting {poll_interval_minutes} minutes before retry...")
            time.sleep(poll_interval_seconds)

