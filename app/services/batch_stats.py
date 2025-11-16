"""Batch-level statistics processing and calculation."""
import json
from collections import Counter
from datetime import timedelta
from app.utils.equipment import filter_by_equipment_type
from app.utils.timezone import get_cst_date, to_cst


def process_equipment_type_stats(cursor, latest_calls, previous_calls, latest_pushed_at, latest_batch_id, 
                                  stat_table, history_table, equipment_type_name, is_recycler_type):
    """Process and save statistics for a specific equipment type."""
    # Filter calls by equipment type
    latest_filtered = filter_by_equipment_type(latest_calls, is_recycler_type)
    previous_filtered = filter_by_equipment_type(previous_calls, is_recycler_type) if previous_calls else {}
    
    # A. Current Open Call Stats
    total_open_calls = len(latest_filtered)
    status_counts = Counter(rec['Appt. Status'] for rec in latest_filtered.values())
    calls_with_multi_appt = sum(1 for rec in latest_filtered.values() if int(rec['Appointment']) >= 2)
    total_appointments = sum(int(rec['Appointment']) for rec in latest_filtered.values())
    avg_appt_num = total_appointments / total_open_calls if total_open_calls > 0 else 0

    # B. Closed Call & Rate Stats
    closed_call_ids = set(previous_filtered.keys()) - set(latest_filtered.keys()) if previous_filtered else set()
    same_day_closures = 0
    first_time_fixes = 0
    completed_call_appointment_numbers = []

    for call_id in closed_call_ids:
        prev_rec = previous_filtered[call_id]
        # Compare dates in CST timezone
        open_date_cst = get_cst_date(prev_rec['Open DateTime'])
        pushed_date_cst = get_cst_date(latest_pushed_at)
        if open_date_cst and pushed_date_cst and open_date_cst == pushed_date_cst:
            same_day_closures += 1
        if int(prev_rec['Appointment']) == 1:
            first_time_fixes += 1
        completed_call_appointment_numbers.append(int(prev_rec['Appointment']))
        
        # Log to history table
        merge_sql = f"""
        MERGE {history_table} AS target
        USING (SELECT %s AS Service_Call_ID, %s AS ClosedTimestamp, %s AS OpenDateTime, %s AS Equipment_ID, %s AS VendorCallNumber) AS source
        ON (target.Service_Call_ID = source.Service_Call_ID AND target.ClosedTimestamp = source.ClosedTimestamp)
        WHEN NOT MATCHED THEN
            INSERT (Service_Call_ID, ClosedTimestamp, OpenDateTime, Equipment_ID, VendorCallNumber)
            VALUES (source.Service_Call_ID, source.ClosedTimestamp, source.OpenDateTime, source.Equipment_ID, source.VendorCallNumber);
        """
        equipment_id = prev_rec.get('Equipment_ID', None)
        vendor_call_number = prev_rec.get('Vendor Call Number', None)
        cursor.execute(merge_sql, (call_id, latest_pushed_at, prev_rec['Open DateTime'], equipment_id, vendor_call_number))

    # Calculate rates
    total_closed = len(closed_call_ids)
    same_day_close_rate = same_day_closures / total_closed if total_closed > 0 else 0
    first_time_fix_rate = first_time_fixes / total_closed if total_closed > 0 else 0
    avg_appt_per_completed = sum(completed_call_appointment_numbers) / total_closed if total_closed > 0 else 0

    # C. Reopen Rate
    newly_opened_call_ids = set(latest_filtered.keys()) - set(previous_filtered.keys()) if previous_filtered else set(latest_filtered.keys())
    reopened_calls = 0
    # Calculate 14 days ago in CST
    latest_pushed_at_cst = to_cst(latest_pushed_at)
    fourteen_days_ago = (latest_pushed_at_cst - timedelta(days=14)).replace(tzinfo=None)  # Remove timezone for SQL query

    if newly_opened_call_ids:
        # Use a single query to check all new calls against history
        placeholders = ', '.join(['%s'] * len(newly_opened_call_ids))
        check_reopen_sql = f"""
        SELECT Service_Call_ID FROM {history_table}
        WHERE Service_Call_ID IN ({placeholders}) AND ClosedTimestamp >= %s;
        """
        # pymssql requires tuple or dict, not list - convert to tuple
        params = tuple(newly_opened_call_ids) + (fourteen_days_ago,)
        cursor.execute(check_reopen_sql, params)
        # pymssql: rowcount is unreliable for SELECT until rows are fetched
        # Must fetch all results before using rowcount or use len() on fetchall()
        reopened_results = cursor.fetchall()
        reopened_calls = len(reopened_results)

    reopen_rate = reopened_calls / len(newly_opened_call_ids) if newly_opened_call_ids else 0

    # D. Repeat Dispatch Rate (RDR)
    # Follow-up appointments: Each time a call progresses to a new appointment number > 1
    total_follow_up_appointments = 0
    if previous_filtered:
        for call_id in latest_filtered:
            if call_id in previous_filtered:
                latest_appt = int(latest_filtered[call_id]['Appointment'])
                previous_appt = int(previous_filtered[call_id]['Appointment'])
                # Count if appointment increased AND latest appointment is > 1
                if latest_appt > previous_appt and latest_appt > 1:
                    total_follow_up_appointments += 1
    
    # Total appointments: Count of unique appointment numbers in this batch
    unique_appointments = set()
    for call_id, record in latest_filtered.items():
        unique_appointments.add(int(record['Appointment']))
    total_appointments = len(unique_appointments)
    
    # Calculate RDR
    repeat_dispatch_rate = total_follow_up_appointments / total_appointments if total_appointments > 0 else 0

    # Log Stats to Database
    stats_to_insert = {
        "Timestamp": latest_pushed_at, "BatchID": latest_batch_id, "TotalOpenCalls": total_open_calls,
        "CallsClosedSinceLastBatch": total_closed, "SameDayClosures": same_day_closures,
        "CallsWithMultipleAppointments": calls_with_multi_appt, "AverageAppointmentNumber": avg_appt_num,
        "StatusSummary": json.dumps(status_counts), "SameDayCloseRate": same_day_close_rate,
        "AvgAppointmentsPerCompletedCall": avg_appt_per_completed, "FirstTimeFixRate": first_time_fix_rate,
        "FourteenDayReopenRate": reopen_rate,
        "TotalFollowUpAppointments": total_follow_up_appointments,
        "TotalAppointments": total_appointments,
        "RepeatDispatchRate": repeat_dispatch_rate
    }
    
    insert_cols = ", ".join(stats_to_insert.keys())
    insert_vals = ", ".join(["%s"] * len(stats_to_insert))
    insert_sql = f"INSERT INTO {stat_table} ({insert_cols}) VALUES ({insert_vals});"
    cursor.execute(insert_sql, list(stats_to_insert.values()))

    # Print to Console
    print(f"\n--- {equipment_type_name} Stats for Batch {latest_batch_id} ({latest_pushed_at}) ---")
    print(f"Successfully logged to {stat_table} table.")
    
    if not previous_filtered:
        print(f"\n[Note: This is the first {equipment_type_name} batch processed. No comparison data available.]")
    
    print(f"\n[ {equipment_type_name} - Current Open Calls ]")
    print(f"Total Open Calls: {total_open_calls}")
    print(f"Average Appointment Number: {avg_appt_num:.2f}")
    print(f"Calls with 2+ Appointments: {calls_with_multi_appt}")
    print("Status Breakdown:", json.dumps(status_counts))

    print(f"\n[ {equipment_type_name} - Closed Call KPIs ]")
    if previous_filtered:
        print(f"Calls Closed Since Last Batch: {total_closed}")
        print(f"Same-Day Close Rate: {same_day_close_rate:.2%}")
        print(f"First-Time Fix Rate: {first_time_fix_rate:.2%}")
        print(f"Average Appointments per Completed Call: {avg_appt_per_completed:.2f}")
    else:
        print("(No previous batch to compare - metrics will be available after next update)")

    print(f"\n[ {equipment_type_name} - New & Reopened Call KPIs ]")
    if previous_filtered:
        print(f"Newly Opened Calls: {len(newly_opened_call_ids)}")
        print(f"14-Day Reopen Rate: {reopen_rate:.2%}")
    else:
        print(f"Total Calls in Batch: {len(newly_opened_call_ids)}")
        print("(Reopen rate will be calculated after next update)")
    
    print(f"\n[ {equipment_type_name} - Repeat Dispatch Rate (RDR) ]")
    if previous_filtered:
        print(f"Follow-up Appointments: {total_follow_up_appointments}")
        print(f"Total Unique Appointments: {total_appointments}")
        print(f"Repeat Dispatch Rate: {repeat_dispatch_rate:.2%}")
    else:
        print(f"Total Unique Appointments: {total_appointments}")
        print("(RDR will be calculated after next update)")

