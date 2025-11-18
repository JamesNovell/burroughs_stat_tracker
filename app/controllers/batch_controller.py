"""Controller layer - orchestrates batch processing and polling."""
import time
import pymssql
import logging
from datetime import datetime, timedelta
from app.config import (
    SOURCE_TABLE, RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_HOURLY_TABLE, RECYCLERS_DAILY_TABLE,
    RECYCLERS_WEEKLY_TABLE, RECYCLERS_MONTHLY_TABLE,
    SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_HOURLY_TABLE, SMART_SAFES_DAILY_TABLE,
    SMART_SAFES_WEEKLY_TABLE, SMART_SAFES_MONTHLY_TABLE,
    POLL_INTERVAL_MINUTES, REPROCESS_LAST_BATCH_ON_STARTUP
)
from app.data.database import get_db_connection, create_tables_if_not_exist
from app.utils import deduplicate_records
from app.utils.timezone import to_cst, is_end_of_week_cst, is_end_of_month_cst
from app.services import (
    process_equipment_type_stats, calculate_daily_summary, get_last_processed_timestamp,
    aggregate_batch_stats, get_last_batch_aggregation_timestamp,
    aggregate_weekly_stats, aggregate_monthly_stats
)
from app.services.tracking import TrackingService
from app.services.fedex_tracker import is_fedex_tracking_number, get_fedex_tracking_status
from app.services.ups_tracker import is_ups_tracking_number, get_ups_tracking_status
from app.utils.tracking_parser import extract_tracking_numbers_from_value

logger = logging.getLogger(__name__)


def process_batch():
    """Main function to process and track statistics. Returns True if a batch was processed, False otherwise."""
    logger.info("=" * 80)
    logger.info("Starting batch processing")
    logger.info("=" * 80)
    
    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        logger.debug("Database connection established")

        # 1. Ensure tables exist
        logger.debug("Ensuring tables exist")
        create_tables_if_not_exist(cursor)
        logger.debug("Table check complete")

        # 2. Get the latest batch from source table
        # IMPORTANT: Select both Pushed At and Batch ID in the same query so we
        # don't have to re-query by timestamp (which can suffer from precision issues).
        logger.debug(f"Querying {SOURCE_TABLE} for latest batch")
        distinct_batches_query = (
            f"SELECT TOP 1 \"Pushed At\", \"Batch ID\" "
            f"FROM {SOURCE_TABLE} "
            f"ORDER BY \"Pushed At\" DESC;"
        )
        cursor.execute(distinct_batches_query)
        latest_batch_info = cursor.fetchone()

        if not latest_batch_info:
            logger.warning(f"No data found in {SOURCE_TABLE}")
            return False

        latest_pushed_at = latest_batch_info["Pushed At"]
        latest_batch_id = latest_batch_info.get("Batch ID")
        logger.info(f"Found latest batch timestamp: {latest_pushed_at}")
        logger.info(f"Latest batch ID: {latest_batch_id}")
        
        # 3. Check if this batch has already been processed (check both tables)
        # NOTE: Some batches may have Batch ID = 0 (or NULL). In those cases,
        # we should NOT rely on BatchID-based de-duplication, otherwise we may
        # incorrectly skip new batches. Treat BatchID=0 as "no reliable batch id".
        logger.debug(f"Checking if batch {latest_batch_id} has already been processed")
        check_recyclers = f"SELECT TOP 1 BatchID FROM {RECYCLERS_STAT_TABLE} WHERE BatchID = %s;"
        check_smart_safes = f"SELECT TOP 1 BatchID FROM {SMART_SAFES_STAT_TABLE} WHERE BatchID = %s;"
        cursor.execute(check_recyclers, (latest_batch_id,))
        recyclers_processed = cursor.fetchone()
        cursor.execute(check_smart_safes, (latest_batch_id,))
        smart_safes_processed = cursor.fetchone()
        
        # Only skip if we have a non-zero Batch ID and both equipment types
        # have already logged stats for this BatchID.
        if latest_batch_id and recyclers_processed and smart_safes_processed:
            logger.info(f"Batch {latest_batch_id} (pushed at {latest_pushed_at}) already processed for both equipment types")
            logger.debug("No new data to process")
            return False
        elif not latest_batch_id:
            logger.warning(
                "Latest batch has Batch ID = 0 or NULL. "
                "Skipping BatchID-based de-duplication and processing it as a new batch."
            )

        # 4. Get the last processed batch for comparison (check both tables)
        logger.debug("Retrieving last processed batch timestamp")
        last_processed = None
        last_recyclers = None
        last_smart_safes = None
        last_processed_timestamp = get_last_processed_timestamp(cursor)
        if last_processed_timestamp:
            logger.debug(f"Last processed timestamp: {last_processed_timestamp}")
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
            
            if last_processed:
                logger.info(f"Last processed batch: ID={last_processed.get('BatchID')}, Timestamp={last_processed.get('Timestamp')}")
        else:
            logger.info("No previous batches found - this appears to be the first run")

        # 5. Fetch and process records for the latest batch
        logger.debug(f"Fetching records for latest batch (Pushed At: {latest_pushed_at})")
        cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number", "DesNote", "PartNote", "querytrackingnumber", "queryparts", "trackingmatch", "tracking_status"'
        
        cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (latest_pushed_at,))
        latest_records_raw = cursor.fetchall()
        logger.debug(f"Retrieved {len(latest_records_raw)} raw records from latest batch")
        latest_calls = deduplicate_records(latest_records_raw)
        logger.info(f"After deduplication: {len(latest_calls)} unique calls in latest batch")

        # 6. Get previous batch for comparison - SEPARATE FOR EACH EQUIPMENT TYPE
        previous_calls_recyclers = {}
        previous_calls_smart_safes = {}
        
        # Get Recyclers-specific previous batch
        if last_recyclers:
            previous_pushed_at_recyclers = last_recyclers['Timestamp']
            logger.debug(f"Fetching records for Recyclers previous batch (Pushed At: {previous_pushed_at_recyclers})")
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at_recyclers,))
            previous_records_raw = cursor.fetchall()
            logger.debug(f"Retrieved {len(previous_records_raw)} raw records from Recyclers previous batch")
            previous_calls_recyclers = deduplicate_records(previous_records_raw)
            logger.info(f"After deduplication: {len(previous_calls_recyclers)} unique calls in Recyclers previous batch")
        else:
            # First run - get the second most recent batch if available for comparison
            logger.debug("No last processed Recyclers batch found, checking for second most recent batch")
            second_batch_query = f"SELECT DISTINCT TOP 2 \"Pushed At\" FROM {SOURCE_TABLE} ORDER BY \"Pushed At\" DESC;"
            cursor.execute(second_batch_query)
            all_batches = cursor.fetchall()
            
            if len(all_batches) > 1:
                previous_pushed_at_recyclers = all_batches[1]["Pushed At"]
                logger.debug(f"Found second batch for Recyclers comparison (Pushed At: {previous_pushed_at_recyclers})")
                cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at_recyclers,))
                previous_records_raw = cursor.fetchall()
                logger.debug(f"Retrieved {len(previous_records_raw)} raw records from second batch")
                previous_calls_recyclers = deduplicate_records(previous_records_raw)
                logger.info(f"After deduplication: {len(previous_calls_recyclers)} unique calls in second batch")
            else:
                logger.info("Only one batch found - no previous batch for Recyclers comparison")
        
        # Get Smart Safes-specific previous batch
        if last_smart_safes:
            previous_pushed_at_smart_safes = last_smart_safes['Timestamp']
            logger.debug(f"Fetching records for Smart Safes previous batch (Pushed At: {previous_pushed_at_smart_safes})")
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at_smart_safes,))
            previous_records_raw = cursor.fetchall()
            logger.debug(f"Retrieved {len(previous_records_raw)} raw records from Smart Safes previous batch")
            previous_calls_smart_safes = deduplicate_records(previous_records_raw)
            logger.info(f"After deduplication: {len(previous_calls_smart_safes)} unique calls in Smart Safes previous batch")
        else:
            # First run - get the second most recent batch if available for comparison
            logger.debug("No last processed Smart Safes batch found, checking for second most recent batch")
            second_batch_query = f"SELECT DISTINCT TOP 2 \"Pushed At\" FROM {SOURCE_TABLE} ORDER BY \"Pushed At\" DESC;"
            cursor.execute(second_batch_query)
            all_batches = cursor.fetchall()
            
            if len(all_batches) > 1:
                previous_pushed_at_smart_safes = all_batches[1]["Pushed At"]
                logger.debug(f"Found second batch for Smart Safes comparison (Pushed At: {previous_pushed_at_smart_safes})")
                cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at_smart_safes,))
                previous_records_raw = cursor.fetchall()
                logger.debug(f"Retrieved {len(previous_records_raw)} raw records from second batch")
                previous_calls_smart_safes = deduplicate_records(previous_records_raw)
                logger.info(f"After deduplication: {len(previous_calls_smart_safes)} unique calls in second batch")
            else:
                logger.info("Only one batch found - no previous batch for Smart Safes comparison")
        
        # 7. Process tracking for all records in the new batch
        logger.info("=" * 80)
        logger.info(f"Processing tracking for {len(latest_calls)} records in batch {latest_batch_id}")
        logger.info("=" * 80)
        
        try:
            tracking_service = TrackingService()
            logger.debug("Tracking service initialized")
            
            # Start timing for tracking processing
            tracking_start_time = time.time()
            total_records = len(latest_calls)
            
            # Collect all service call tuples for batch query
            service_call_tuples = []
            records_without_vendor_call = []
            
            for call_id, record in latest_calls.items():
                vendor_call_number = record.get('Vendor Call Number')
                if vendor_call_number:
                    service_call_tuples.append((call_id, vendor_call_number))
                else:
                    records_without_vendor_call.append(call_id)
            
            records_with_vendor_call = len(service_call_tuples)
            logger.info(f"Records with vendor call number: {records_with_vendor_call}/{total_records}")
            
            # Process records without vendor call numbers first (set defaults)
            for call_id in records_without_vendor_call:
                tracking_service.update_tracking_columns(
                    cursor, call_id, '', [], False, None, None
                )
            
            # Batch query all vendor call numbers in parallel
            batch_results = {}
            if service_call_tuples:
                batch_query_start = time.time()
                batch_results = tracking_service.query_tracking_info_batch(service_call_tuples)
                batch_query_elapsed = time.time() - batch_query_start
                logger.info(f"Batch query completed in {timedelta(seconds=int(batch_query_elapsed))} "
                          f"({batch_query_elapsed/len(service_call_tuples):.2f}s per query average)")
            
            # Process results and update database
            processed_count = len(records_without_vendor_call)
            error_count = 0
            
            for idx, (call_id, record) in enumerate(latest_calls.items(), 1):
                record_start_time = time.time()
                vendor_call_number = record.get('Vendor Call Number')
                
                if not vendor_call_number:
                    # Already processed above
                    continue
                
                logger.info(f"[{idx}/{total_records}] Processing tracking for service call {call_id} (vendor: {vendor_call_number})")
                
                try:
                    # Get result from batch query
                    tracking_result = batch_results.get(call_id)
                    
                    if tracking_result:
                        tracking_number, parts_list, parts_tracking_value = tracking_result
                        logger.info(f"Service call {call_id}: Found tracking number={tracking_number}, parts={len(parts_list)}")
                        
                        # Get DesNote and PartNote from the record
                        des_note = record.get('DesNote')
                        part_note = record.get('PartNote')
                        
                        # For tracking match, use the first tracking number if there are multiple (dash-separated)
                        # Extract all tracking numbers from the tracking_number value
                        all_tracking_numbers = extract_tracking_numbers_from_value(tracking_number)
                        first_tracking_number = all_tracking_numbers[0] if all_tracking_numbers else tracking_number
                        
                        # Check for match using the first tracking number
                        tracking_match = tracking_service.check_tracking_match(
                            first_tracking_number, des_note, part_note
                        )
                        logger.info(f"Service call {call_id}: Tracking match check using first number {first_tracking_number}: match={tracking_match}")
                        
                        # Get FedEx/UPS tracking status for ALL tracking numbers (if dash-separated)
                        tracking_status = None
                        # Extract all tracking numbers from the tracking_number value (may be dash-separated)
                        all_tracking_numbers = extract_tracking_numbers_from_value(tracking_number)
                        
                        # Only query FedEx/UPS APIs if we have actual tracking numbers (not status messages)
                        if all_tracking_numbers:
                            statuses = []
                            for tn in all_tracking_numbers:
                                # Try FedEx first
                                if is_fedex_tracking_number(tn):
                                    logger.info(f"Service call {call_id}: Checking FedEx tracking number {tn}")
                                    try:
                                        fedex_status = get_fedex_tracking_status(tn)
                                        if fedex_status:
                                            statuses.append(f"FedEx {tn}: {fedex_status}")
                                            logger.info(f"Service call {call_id}: FedEx {tn} status={fedex_status}")
                                        else:
                                            statuses.append(f"FedEx {tn}: Status unavailable")
                                            logger.debug(f"Service call {call_id}: Could not retrieve FedEx status for {tn}")
                                    except Exception as e:
                                        statuses.append(f"FedEx {tn}: Error - {str(e)}")
                                        logger.warning(f"Service call {call_id}: Error getting FedEx status for {tn}: {str(e)}")
                                # Try UPS if not FedEx
                                elif is_ups_tracking_number(tn):
                                    logger.info(f"Service call {call_id}: Checking UPS tracking number {tn}")
                                    try:
                                        ups_status = get_ups_tracking_status(tn)
                                        if ups_status:
                                            statuses.append(f"UPS {tn}: {ups_status}")
                                            logger.info(f"Service call {call_id}: UPS {tn} status={ups_status}")
                                        else:
                                            statuses.append(f"UPS {tn}: Status unavailable")
                                            logger.debug(f"Service call {call_id}: Could not retrieve UPS status for {tn}")
                                    except Exception as e:
                                        statuses.append(f"UPS {tn}: Error - {str(e)}")
                                        logger.warning(f"Service call {call_id}: Error getting UPS status for {tn}: {str(e)}")
                                else:
                                    logger.debug(f"Service call {call_id}: Tracking number {tn} is not FedEx or UPS format")
                            
                            # Combine all statuses with semicolon separator
                            if statuses:
                                tracking_status = '; '.join(statuses)
                                logger.info(f"Service call {call_id}: Combined tracking status: {tracking_status}")
                        
                        # Update columns
                        tracking_service.update_tracking_columns(
                            cursor, call_id, tracking_number, parts_list, tracking_match, tracking_status, parts_tracking_value
                        )
                        
                        processed_count += 1
                        record_elapsed = time.time() - record_start_time
                        
                        # Log progress every 10 records or on last record
                        if (idx % 10 == 0) or (idx == total_records):
                            elapsed_time = time.time() - tracking_start_time
                            avg_time_per_record = elapsed_time / idx if idx > 0 else 0
                            remaining_records = total_records - idx
                            estimated_remaining = timedelta(seconds=int(avg_time_per_record * remaining_records))
                            
                            logger.info(f"Tracking progress: Row {idx}/{total_records} | "
                                      f"Processed: {processed_count} | Errors: {error_count} | "
                                      f"Elapsed: {timedelta(seconds=int(elapsed_time))} | "
                                      f"Avg: {avg_time_per_record:.2f}s/record | "
                                      f"ETA: {estimated_remaining}")
                        else:
                            logger.debug(f"Row {idx}/{total_records}: Processed in {record_elapsed:.2f}s")
                    else:
                        # No tracking info found, set to false
                        logger.info(f"Service call {call_id} (vendor {vendor_call_number}): No tracking info found - writing NULL values")
                        tracking_service.update_tracking_columns(
                            cursor, call_id, '', [], False, None, None
                        )
                        processed_count += 1
                        record_elapsed = time.time() - record_start_time
                        logger.debug(f"Row {idx}/{total_records}: Processed in {record_elapsed:.2f}s")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing tracking for service call {call_id} (vendor call {vendor_call_number}): {str(e)}", exc_info=True)
                    # Update tracking columns with defaults even on error to avoid reprocessing
                    try:
                        tracking_service.update_tracking_columns(
                            cursor, call_id, '', [], False, f"Error: {str(e)}", None
                        )
                        processed_count += 1
                    except Exception as update_error:
                        logger.error(f"Error updating tracking columns for {call_id} after error: {str(update_error)}")
                    # Continue with next record
                    record_elapsed = time.time() - record_start_time
                    logger.debug(f"Row {idx}/{total_records}: Error after {record_elapsed:.2f}s")
                    continue
            
            # Calculate final timing
            total_elapsed = time.time() - tracking_start_time
            avg_time = total_elapsed / total_records if total_records > 0 else 0
            logger.info("=" * 80)
            logger.info(f"Tracking processing complete: {processed_count}/{total_records} processed, {error_count} errors")
            logger.info(f"Total time: {timedelta(seconds=int(total_elapsed))} | "
                      f"Average: {avg_time:.2f}s per record | "
                      f"Rate: {total_records/total_elapsed:.2f} records/second")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error initializing tracking service: {str(e)}", exc_info=True)
            logger.warning("Continuing with batch processing despite tracking failure")
            # Continue with batch processing even if tracking fails

        # 8. Process stats for Recyclers
        if not recyclers_processed:
            logger.info("=" * 80)
            logger.info("Processing statistics for RECYCLERS")
            logger.info("=" * 80)
            
            process_equipment_type_stats(
                cursor, latest_calls, previous_calls_recyclers, latest_pushed_at, latest_batch_id,
                RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, "Recyclers", True
            )
            
            # Always aggregate batches since last aggregation (batch-based, not time-based)
            logger.debug("Aggregating batches for Recyclers")
            last_aggregation_timestamp = get_last_batch_aggregation_timestamp(cursor, RECYCLERS_HOURLY_TABLE)
            
            if last_aggregation_timestamp:
                logger.info(f"Aggregating Recyclers batches since last aggregation: {last_aggregation_timestamp}")
            else:
                logger.info("First batch aggregation for Recyclers - aggregating all batches up to now")
            
            aggregate_batch_stats(
                cursor, RECYCLERS_STAT_TABLE, RECYCLERS_HOURLY_TABLE,
                last_aggregation_timestamp, latest_pushed_at, "Recyclers"
            )
            
            # Calculate daily summary if it's end of day
            logger.debug("Checking if daily summary should be calculated for Recyclers")
            calculate_daily_summary(
                cursor, latest_pushed_at, latest_calls,
                RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_HOURLY_TABLE, RECYCLERS_DAILY_TABLE,
                "Recyclers", True
            )
            
            # Calculate weekly summary if it's end of week
            if is_end_of_week_cst(latest_pushed_at):
                logger.info("End of week detected - aggregating weekly stats for Recyclers")
                aggregate_weekly_stats(
                    cursor, RECYCLERS_DAILY_TABLE, RECYCLERS_WEEKLY_TABLE,
                    latest_pushed_at, "Recyclers"
                )
            
            # Calculate monthly summary if it's end of month
            if is_end_of_month_cst(latest_pushed_at):
                logger.info("End of month detected - aggregating monthly stats for Recyclers")
                aggregate_monthly_stats(
                    cursor, RECYCLERS_WEEKLY_TABLE, RECYCLERS_MONTHLY_TABLE,
                    latest_pushed_at, "Recyclers"
                )
        else:
            logger.info(f"Recyclers batch {latest_batch_id} already processed, skipping")
        
        # 9. Process stats for Smart Safes
        if not smart_safes_processed:
            logger.info("=" * 80)
            logger.info("Processing statistics for SMART SAFES")
            logger.info("=" * 80)
            
            process_equipment_type_stats(
                cursor, latest_calls, previous_calls_smart_safes, latest_pushed_at, latest_batch_id,
                SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, "Smart Safes", False
            )
            
            # Always aggregate batches since last aggregation (batch-based, not time-based)
            logger.debug("Aggregating batches for Smart Safes")
            last_aggregation_timestamp = get_last_batch_aggregation_timestamp(cursor, SMART_SAFES_HOURLY_TABLE)
            
            if last_aggregation_timestamp:
                logger.info(f"Aggregating Smart Safes batches since last aggregation: {last_aggregation_timestamp}")
            else:
                logger.info("First batch aggregation for Smart Safes - aggregating all batches up to now")
            
            aggregate_batch_stats(
                cursor, SMART_SAFES_STAT_TABLE, SMART_SAFES_HOURLY_TABLE,
                last_aggregation_timestamp, latest_pushed_at, "Smart Safes"
            )
            
            # Calculate daily summary if it's end of day
            logger.debug("Checking if daily summary should be calculated for Smart Safes")
            calculate_daily_summary(
                cursor, latest_pushed_at, latest_calls,
                SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_HOURLY_TABLE, SMART_SAFES_DAILY_TABLE,
                "Smart Safes", False
            )
            
            # Calculate weekly summary if it's end of week
            if is_end_of_week_cst(latest_pushed_at):
                logger.info("End of week detected - aggregating weekly stats for Smart Safes")
                aggregate_weekly_stats(
                    cursor, SMART_SAFES_DAILY_TABLE, SMART_SAFES_WEEKLY_TABLE,
                    latest_pushed_at, "Smart Safes"
                )
            
            # Calculate monthly summary if it's end of month
            if is_end_of_month_cst(latest_pushed_at):
                logger.info("End of month detected - aggregating monthly stats for Smart Safes")
                aggregate_monthly_stats(
                    cursor, SMART_SAFES_WEEKLY_TABLE, SMART_SAFES_MONTHLY_TABLE,
                    latest_pushed_at, "Smart Safes"
                )
        else:
            logger.info(f"Smart Safes batch {latest_batch_id} already processed, skipping")
        
        logger.info("=" * 80)
        logger.info(f"Batch {latest_batch_id} processing complete")
        logger.info("=" * 80)
        return True

    except pymssql.Error as e:
        logger.error(f"Database error during batch processing: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error during batch processing: {e}", exc_info=True)
        return False
    finally:
        if conn:
            logger.debug("Closing database connection")
            conn.close()


def reprocess_last_batch():
    """
    Reprocess the last processed batch (for troubleshoot mode).
    This function finds the last processed batch and forces it to be reprocessed.
    
    Returns:
        True if a batch was reprocessed, False otherwise
    """
    logger.info("=" * 80)
    logger.info("TROUBLESHOOT MODE: Reprocessing last batch")
    logger.info("=" * 80)
    
    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        logger.debug("Database connection established")

        # 1. Ensure tables exist
        logger.debug("Ensuring tables exist")
        create_tables_if_not_exist(cursor)
        logger.debug("Table check complete")

        # 2. Get the last processed batch from stat tables
        logger.debug("Finding last processed batch")
        last_recyclers_query = f"SELECT TOP 1 BatchID, Timestamp FROM {RECYCLERS_STAT_TABLE} ORDER BY Timestamp DESC;"
        last_smart_safes_query = f"SELECT TOP 1 BatchID, Timestamp FROM {SMART_SAFES_STAT_TABLE} ORDER BY Timestamp DESC;"
        cursor.execute(last_recyclers_query)
        last_recyclers = cursor.fetchone()
        cursor.execute(last_smart_safes_query)
        last_smart_safes = cursor.fetchone()
        
        # Find the most recent batch (could be in either table)
        last_processed = None
        if last_recyclers and last_smart_safes:
            last_processed = last_recyclers if last_recyclers['Timestamp'] >= last_smart_safes['Timestamp'] else last_smart_safes
        elif last_recyclers:
            last_processed = last_recyclers
        elif last_smart_safes:
            last_processed = last_smart_safes
        
        if not last_processed:
            logger.warning("No previously processed batches found. Cannot reprocess.")
            return False
        
        last_batch_id = last_processed.get('BatchID')
        last_pushed_at = last_processed.get('Timestamp')
        
        logger.info(f"Found last processed batch: ID={last_batch_id}, Timestamp={last_pushed_at}")
        logger.info(f"Reprocessing batch {last_batch_id}...")
        
        # 3. Fetch records for the batch to reprocess
        cols_to_fetch = '"ID", "Service_Call_ID", "Appt. Status", "Appointment", "Open DateTime", "Batch ID", "Pushed At", "Equipment_ID", "Vendor Call Number", "DesNote", "PartNote", "querytrackingnumber", "queryparts", "trackingmatch", "tracking_status"'
        
        logger.debug(f"Fetching records for batch {last_batch_id} (Pushed At: {last_pushed_at})")
        
        # Use a range query to handle timestamp precision issues
        # Query for records within 1 second of the timestamp
        time_tolerance = timedelta(seconds=1)
        time_start = last_pushed_at - time_tolerance
        time_end = last_pushed_at + time_tolerance
        
        # Also try exact match first, then fall back to range if needed
        cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (last_pushed_at,))
        batch_records_raw = cursor.fetchall()
        logger.debug(f"Retrieved {len(batch_records_raw)} raw records from batch (exact match)")
        
        # If exact match found nothing, try range query
        if not batch_records_raw:
            logger.debug(f"No exact match found, trying range query: {time_start} to {time_end}")
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" >= %s AND \"Pushed At\" <= %s;", (time_start, time_end))
            batch_records_raw = cursor.fetchall()
            logger.debug(f"Retrieved {len(batch_records_raw)} raw records from batch (range query)")
            
            # If still nothing, try querying by Batch ID
            if not batch_records_raw and last_batch_id:
                logger.debug(f"No records found with timestamp, trying Batch ID: {last_batch_id}")
                cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Batch ID\" = %s;", (last_batch_id,))
                batch_records_raw = cursor.fetchall()
                logger.debug(f"Retrieved {len(batch_records_raw)} raw records from batch (Batch ID query)")
            
            # If we found records, also check what the actual "Pushed At" values are
            if batch_records_raw:
                actual_timestamps = set(r.get('Pushed At') for r in batch_records_raw)
                logger.info(f"Found records with timestamps: {sorted(actual_timestamps)}")
        
        batch_calls = deduplicate_records(batch_records_raw)
        logger.info(f"After deduplication: {len(batch_calls)} unique calls in batch")
        
        if not batch_calls:
            logger.warning(f"No records found for batch {last_batch_id}")
            return False
        
        # 4. Get previous batch for comparison (the one before the last processed)
        previous_calls = {}
        previous_pushed_at = None
        
        # Find the batch before this one
        previous_batch_query = f"""
        SELECT DISTINCT TOP 1 \"Pushed At\" 
        FROM {SOURCE_TABLE} 
        WHERE \"Pushed At\" < %s 
        ORDER BY \"Pushed At\" DESC;
        """
        cursor.execute(previous_batch_query, (last_pushed_at,))
        previous_batch_info = cursor.fetchone()
        
        if previous_batch_info:
            previous_pushed_at = previous_batch_info["Pushed At"]
            logger.debug(f"Found previous batch for comparison (Pushed At: {previous_pushed_at})")
            cursor.execute(f"SELECT {cols_to_fetch} FROM {SOURCE_TABLE} WHERE \"Pushed At\" = %s;", (previous_pushed_at,))
            previous_records_raw = cursor.fetchall()
            logger.debug(f"Retrieved {len(previous_records_raw)} raw records from previous batch")
            previous_calls = deduplicate_records(previous_records_raw)
            logger.info(f"After deduplication: {len(previous_calls)} unique calls in previous batch")
        else:
            logger.info("No previous batch found for comparison")
        
        # 5. Process tracking for all records in the batch
        logger.info("=" * 80)
        logger.info(f"Processing tracking for {len(batch_calls)} records in batch {last_batch_id}")
        logger.info("=" * 80)
        
        try:
            tracking_service = TrackingService()
            logger.debug("Tracking service initialized")
            
            # Start timing for tracking processing
            tracking_start_time = time.time()
            total_records = len(batch_calls)
            
            # Collect all service call tuples for batch query
            service_call_tuples = []
            records_without_vendor_call = []
            
            for call_id, record in batch_calls.items():
                vendor_call_number = record.get('Vendor Call Number')
                if vendor_call_number:
                    service_call_tuples.append((call_id, vendor_call_number))
                else:
                    records_without_vendor_call.append(call_id)
            
            records_with_vendor_call = len(service_call_tuples)
            logger.info(f"Records with vendor call number: {records_with_vendor_call}/{total_records}")
            
            # Process records without vendor call numbers first (set defaults)
            for call_id in records_without_vendor_call:
                tracking_service.update_tracking_columns(
                    cursor, call_id, '', [], False, None, None
                )
            
            # Batch query all vendor call numbers in parallel
            batch_results = {}
            if service_call_tuples:
                batch_query_start = time.time()
                batch_results = tracking_service.query_tracking_info_batch(service_call_tuples)
                batch_query_elapsed = time.time() - batch_query_start
                logger.info(f"Batch query completed in {timedelta(seconds=int(batch_query_elapsed))} "
                          f"({batch_query_elapsed/len(service_call_tuples):.2f}s per query average)")
            
            # Process results and update database
            processed_count = len(records_without_vendor_call)
            error_count = 0
            
            for idx, (call_id, record) in enumerate(batch_calls.items(), 1):
                record_start_time = time.time()
                vendor_call_number = record.get('Vendor Call Number')
                
                if not vendor_call_number:
                    # Already processed above
                    continue
                
                logger.debug(f"[{idx}/{total_records}] Service call {call_id}: Processing results for vendor call {vendor_call_number}")
                
                try:
                    # Get result from batch query
                    tracking_result = batch_results.get(call_id)
                    
                    if tracking_result:
                        tracking_number, parts_list, parts_tracking_value = tracking_result
                        logger.debug(f"Service call {call_id}: Found tracking number={tracking_number}, parts={len(parts_list)}")
                        
                        des_note = record.get('DesNote')
                        part_note = record.get('PartNote')
                        
                        tracking_match = tracking_service.check_tracking_match(
                            tracking_number, des_note, part_note
                        )
                        logger.debug(f"Service call {call_id}: Tracking match={tracking_match}")
                        
                        tracking_status = None
                        # Only query FedEx/UPS APIs if tracking_number is an actual tracking number (not a status message)
                        if tracking_number and tracking_number not in ['not available yet', 'No Tracking', 'Pack Created No Tracking yet']:
                            if is_fedex_tracking_number(tracking_number):
                                logger.debug(f"Service call {call_id}: Detected FedEx tracking number {tracking_number}")
                                try:
                                    tracking_status = get_fedex_tracking_status(tracking_number)
                                    if tracking_status:
                                        logger.debug(f"Service call {call_id}: FedEx status={tracking_status}")
                                except Exception as e:
                                    logger.warning(f"Service call {call_id}: Error getting FedEx status: {str(e)}")
                            elif is_ups_tracking_number(tracking_number):
                                logger.debug(f"Service call {call_id}: Detected UPS tracking number {tracking_number}")
                                try:
                                    tracking_status = get_ups_tracking_status(tracking_number)
                                    if tracking_status:
                                        logger.debug(f"Service call {call_id}: UPS status={tracking_status}")
                                except Exception as e:
                                    logger.warning(f"Service call {call_id}: Error getting UPS status: {str(e)}")
                        
                        tracking_service.update_tracking_columns(
                            cursor, call_id, tracking_number, parts_list, tracking_match, tracking_status, parts_tracking_value
                        )
                        
                        processed_count += 1
                        record_elapsed = time.time() - record_start_time
                        
                        # Log progress every 10 records or on last record
                        if (idx % 10 == 0) or (idx == total_records):
                            elapsed_time = time.time() - tracking_start_time
                            avg_time_per_record = elapsed_time / idx if idx > 0 else 0
                            remaining_records = total_records - idx
                            estimated_remaining = timedelta(seconds=int(avg_time_per_record * remaining_records))
                            
                            logger.info(f"Tracking progress: Row {idx}/{total_records} | "
                                      f"Processed: {processed_count} | Errors: {error_count} | "
                                      f"Elapsed: {timedelta(seconds=int(elapsed_time))} | "
                                      f"Avg: {avg_time_per_record:.2f}s/record | "
                                      f"ETA: {estimated_remaining}")
                        else:
                            logger.debug(f"Row {idx}/{total_records}: Processed in {record_elapsed:.2f}s")
                    else:
                        logger.debug(f"Service call {call_id}: No tracking info found")
                        tracking_service.update_tracking_columns(
                            cursor, call_id, '', [], False, None, None
                        )
                        processed_count += 1
                        record_elapsed = time.time() - record_start_time
                        logger.debug(f"Row {idx}/{total_records}: Processed in {record_elapsed:.2f}s")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing tracking for service call {call_id} (vendor call {vendor_call_number}): {str(e)}", exc_info=True)
                    try:
                        tracking_service.update_tracking_columns(
                            cursor, call_id, '', [], False, f"Error: {str(e)}", None
                        )
                        processed_count += 1
                    except Exception as update_error:
                        logger.error(f"Error updating tracking columns for {call_id} after error: {str(update_error)}")
                    record_elapsed = time.time() - record_start_time
                    logger.debug(f"Row {idx}/{total_records}: Error after {record_elapsed:.2f}s")
                    continue
            
            # Calculate final timing
            total_elapsed = time.time() - tracking_start_time
            avg_time = total_elapsed / total_records if total_records > 0 else 0
            logger.info("=" * 80)
            logger.info(f"Tracking processing complete: {processed_count}/{total_records} processed, {error_count} errors")
            logger.info(f"Total time: {timedelta(seconds=int(total_elapsed))} | "
                      f"Average: {avg_time:.2f}s per record | "
                      f"Rate: {total_records/total_elapsed:.2f} records/second")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error initializing tracking service: {str(e)}", exc_info=True)
            logger.warning("Continuing with batch processing despite tracking failure")
        
        # 6. Reprocess stats for Recyclers (force reprocess by passing force=True equivalent)
        logger.info("=" * 80)
        logger.info("Reprocessing statistics for RECYCLERS")
        logger.info("=" * 80)
        
        process_equipment_type_stats(
            cursor, batch_calls, previous_calls, last_pushed_at, last_batch_id,
            RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, "Recyclers", True
        )
        
        # 7. Reprocess stats for Smart Safes
        logger.info("=" * 80)
        logger.info("Reprocessing statistics for SMART SAFES")
        logger.info("=" * 80)
        
        process_equipment_type_stats(
            cursor, batch_calls, previous_calls, last_pushed_at, last_batch_id,
            SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, "Smart Safes", False
        )
        
        logger.info("=" * 80)
        logger.info(f"TROUBLESHOOT MODE: Batch {last_batch_id} reprocessing complete")
        logger.info("=" * 80)
        return True

    except pymssql.Error as e:
        logger.error(f"Database error during batch reprocessing: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error during batch reprocessing: {e}", exc_info=True)
        return False
    finally:
        if conn:
            logger.debug("Closing database connection")
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
    
    # Check for troubleshoot mode on startup
    if REPROCESS_LAST_BATCH_ON_STARTUP:
        logger.warning("=" * 80)
        logger.warning("⚠ TROUBLESHOOT MODE ENABLED")
        logger.warning("⚠ Will reprocess last batch on startup")
        logger.warning("=" * 80)
        reprocess_last_batch()
        logger.warning("=" * 80)
        logger.warning("⚠ TROUBLESHOOT MODE: Reprocessing complete")
        logger.warning("⚠ Continuing with normal polling...")
        logger.warning("=" * 80)
    
    logger.info("=" * 80)
    logger.info(f"Starting batch polling service")
    logger.info(f"Poll interval: {poll_interval_minutes} minutes")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 80)
    
    poll_interval_seconds = poll_interval_minutes * 60
    
    while True:
        try:
            # Try to process any available batches
            batch_processed = process_batch()
            
            if batch_processed:
                logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch processed successfully. Waiting {poll_interval_minutes} minutes before next check...")
            else:
                # No new batch found, check again after the poll interval
                logger.debug(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No new batches found. Waiting {poll_interval_minutes} minutes before next check...")
            
            # Wait for the poll interval
            time.sleep(poll_interval_seconds)
            
        except KeyboardInterrupt:
            logger.info("=" * 80)
            logger.info("Polling stopped by user")
            logger.info("=" * 80)
            break
        except Exception as e:
            logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error in polling loop: {e}", exc_info=True)
            logger.info(f"Waiting {poll_interval_minutes} minutes before retry...")
            time.sleep(poll_interval_seconds)

