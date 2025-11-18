"""
Tracking service for querying tracking database and updating tracking match columns.
"""
import pyodbc
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List, Dict
from app.config import (
    TRACKING_DB_DRIVER, TRACKING_DB_SERVER, TRACKING_DB_DATABASE,
    TRACKING_DB_USERNAME, TRACKING_DB_PASSWORD, TRACKING_DB_TRUST_SERVER_CERT,
    SOURCE_TABLE, TRACKING_MAX_WORKERS
)
from app.utils.tracking_parser import determine_tracking_number, extract_latest_parts, extract_tracking_numbers_from_value
from app.services.fedex_tracker import is_fedex_tracking_number, get_fedex_tracking_status
from app.services.ups_tracker import is_ups_tracking_number, get_ups_tracking_status

logger = logging.getLogger(__name__)

# Lightweight query to check if tracking exists (checks pack numbers only)
SERVICE_CALL_QUERY_CHECK_PACKS = """
DECLARE @CallNum INT = {CALL_NUMBER};
DECLARE @CaseNum INT;

-- Look up the Case Number
SELECT TOP 1 @CaseNum = fs.HDCaseNum
FROM Erp.FSCallHd fs
WHERE fs.CallNum = @CallNum;

-- Check pack numbers (lightweight check)
SELECT
    STRING_AGG(CAST(CAST(ISNULL(fsu.Number02, 0) AS INT) AS NVARCHAR(20)), ', ')
        WITHIN GROUP (ORDER BY DATEADD(SECOND, fs.EntryTime, CAST(fs.EntryDate AS DATETIME))) AS AllPackNumbers
FROM Erp.FSCallhd fs
LEFT JOIN Erp.FSCallhd_UD fsu ON fsu.ForeignSysRowID = fs.SysRowID
WHERE fs.HDCaseNum = @CaseNum;
"""

# Simplified query without CallText and AllParts (for cases with no tracking)
SERVICE_CALL_QUERY_SIMPLE = """
DECLARE @CallNum INT = {CALL_NUMBER};
DECLARE @CaseNum INT;

-- Look up the Case Number
SELECT TOP 1 @CaseNum = fs.HDCaseNum
FROM Erp.FSCallHd fs
WHERE fs.CallNum = @CallNum;

-- Service Call Summary (without CallText and AllParts)
;WITH CallList AS (
    SELECT
        fs.Company,
        fs.HDCaseNum,
        fs.CallNum,
        CAST(ISNULL(fsu.Number02, 0) AS INT) AS PackNumber,
        CASE 
            WHEN fsu.FSLBIN_c IS NULL OR LTRIM(RTRIM(fsu.FSLBIN_c)) = '' 
                 THEN 'NoBin' 
            ELSE fsu.FSLBIN_c 
        END AS FSLBin,
        fs.EntryDate AS ServiceCallDate,
        fs.EntryTime AS ServiceCallTime,
        DATEADD(SECOND, fs.EntryTime, CAST(fs.EntryDate AS DATETIME)) AS ServiceCallSortKey
    FROM Erp.FSCallhd fs
    LEFT JOIN Erp.FSCallhd_UD fsu ON fsu.ForeignSysRowID = fs.SysRowID
    WHERE fs.HDCaseNum = @CaseNum
)
, PackTracking AS (
    SELECT
        cl.CallNum,
        (
            SELECT STRING_AGG(t.TrackingNumber, '; ')
            FROM (
                SELECT DISTINCT NULLIF(LTRIM(RTRIM(msh.TrackingNumber)), '') AS TrackingNumber
                FROM Erp.MscShpHd msh
                WHERE msh.Company = cl.Company
                  AND CAST(msh.PackNum AS NVARCHAR(20)) = CAST(cl.PackNumber AS NVARCHAR(20))
            ) t
            WHERE t.TrackingNumber IS NOT NULL
        ) AS TrackingAgg
    FROM CallList cl
)
, ServiceCallSummary AS (
    SELECT
        STRING_AGG(CAST(cl.CallNum AS NVARCHAR(20)), ', ')
            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllCallNums,
        STRING_AGG(CAST(cl.PackNumber AS NVARCHAR(20)), ', ')
            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllPackNumbers,
        STRING_AGG(cl.FSLBin, ', ')
            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllBins,
        STRING_AGG(
            CONVERT(NVARCHAR(30), cl.ServiceCallSortKey, 120),
            ', '
        ) WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllCallDateTimes,
        STRING_AGG(
            CASE
                WHEN cl.PackNumber = 0 THEN 'NP'
                WHEN pt.TrackingAgg IS NULL OR pt.TrackingAgg = '' THEN 'AT'
                ELSE pt.TrackingAgg
            END,
            ', '
        ) WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllTrackingStatuses
    FROM CallList cl
    LEFT JOIN PackTracking pt ON pt.CallNum = cl.CallNum
)
SELECT
    @CaseNum AS CaseNum,
    NULL AS CallText,
    scs.AllCallNums,
    scs.AllPackNumbers,
    scs.AllBins,
    scs.AllCallDateTimes,
    scs.AllTrackingStatuses,
    NULL AS AllParts
FROM ServiceCallSummary scs;
"""

# Full service call query template (with CallText and AllParts)
SERVICE_CALL_QUERY_FULL = """
DECLARE @CallNum INT = {CALL_NUMBER};

DECLARE @CaseNum INT;

---------------------------------------------------------

-- 0) Look up the Case Number associated with this Service Call

---------------------------------------------------------

SELECT TOP 1 

    @CaseNum = fs.HDCaseNum

FROM Erp.FSCallHd fs

WHERE fs.CallNum = @CallNum;

---------------------------------------------------------

-- 1) CRMCall Call Log Extraction (all logs for that Case)

---------------------------------------------------------

;WITH CallLogs AS (

    SELECT 

        c.CallHDCaseNum AS CaseNum,

        c.CallSeqNum,

        c.OrigDate,

        c.CallText

    FROM Erp.CRMCall c

    WHERE c.CallHDCaseNum = @CaseNum

),

CallTextCollapsed AS (

    SELECT

        @CaseNum AS CaseNum,

        STUFF((SELECT CHAR(10) + '--- Call ' + CAST(cl.CallSeqNum AS varchar(10)) 

                      + ' ---' + CHAR(10) + cl.CallText

               FROM CallLogs cl

               ORDER BY cl.CallSeqNum

               FOR XML PATH(''), TYPE

        ).value('.', 'varchar(max)'), 1, 1, '') AS AllCallText

)

---------------------------------------------------------

-- 2) SERVICE CALL SUMMARY (all calls under that Case)

---------------------------------------------------------

, CallList AS (

    SELECT

        fs.Company,

        fs.HDCaseNum,

        fs.CallNum,

        CAST(ISNULL(fsu.Number02, 0) AS INT) AS PackNumber,

        CASE 

            WHEN fsu.FSLBIN_c IS NULL OR LTRIM(RTRIM(fsu.FSLBIN_c)) = '' 

                 THEN 'NoBin' 

            ELSE fsu.FSLBIN_c 

        END AS FSLBin,

        fs.EntryDate AS ServiceCallDate,

        fs.EntryTime AS ServiceCallTime,

        DATEADD(SECOND, fs.EntryTime, CAST(fs.EntryDate AS DATETIME)) AS ServiceCallSortKey

    FROM Erp.FSCallhd fs

    LEFT JOIN Erp.FSCallhd_UD fsu

        ON fsu.ForeignSysRowID = fs.SysRowID

    WHERE fs.HDCaseNum = @CaseNum

)

, PartsCTE AS (

    SELECT

        fs.CallNum,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character01))), '') AS PartNum1,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character02))), '') AS PartNum2,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character03))), '') AS PartNum3,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character04))), '') AS PartNum4,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character05))), '') AS PartNum5,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character06))), '') AS PartNum6,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character07))), '') AS PartNum7,

        NULLIF(UPPER(LTRIM(RTRIM(fsu.Character08))), '') AS PartNum8

    FROM Erp.FSCallhd fs

    JOIN Erp.FSCallhd_UD fsu

        ON fs.SysRowID = fsu.ForeignSysRowID

    WHERE fs.CallNum IN (SELECT CallNum FROM CallList)

)

, ExpandedParts AS (

    SELECT 

        p.CallNum, 

        v.PartNumber

    FROM PartsCTE p

    CROSS APPLY (

        VALUES (PartNum1),(PartNum2),(PartNum3),(PartNum4),

               (PartNum5),(PartNum6),(PartNum7),(PartNum8)

    ) AS v(PartNumber)

    WHERE v.PartNumber IS NOT NULL

)

, PartDescriptions AS (

    SELECT

        ep.CallNum,

        '(' + STRING_AGG(

                p.PartNum + ' - ' + ISNULL(p.PartDescription,''), 

                ' || '

            ) + ')' AS PartsText

    FROM ExpandedParts ep

    LEFT JOIN Erp.Part p 

        ON p.PartNum = ep.PartNumber

    GROUP BY ep.CallNum

)

, PackTracking AS (

    SELECT

        cl.CallNum,

        (

            SELECT STRING_AGG(t.TrackingNumber, '; ')

            FROM (

                SELECT DISTINCT NULLIF(LTRIM(RTRIM(msh.TrackingNumber)), '') AS TrackingNumber

                FROM Erp.MscShpHd msh

                WHERE msh.Company = cl.Company

                  AND CAST(msh.PackNum AS NVARCHAR(20)) =

                      CAST(cl.PackNumber AS NVARCHAR(20))

            ) t

            WHERE t.TrackingNumber IS NOT NULL

        ) AS TrackingAgg

    FROM CallList cl

)

, ServiceCallSummary AS (

    SELECT

        STRING_AGG(CAST(cl.CallNum AS NVARCHAR(20)), ', ')

            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllCallNums,

        STRING_AGG(CAST(cl.PackNumber AS NVARCHAR(20)), ', ')

            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllPackNumbers,

        STRING_AGG(cl.FSLBin, ', ')

            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllBins,

        STRING_AGG(

            CONVERT(NVARCHAR(30), cl.ServiceCallSortKey, 120),

            ', '

        ) WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllCallDateTimes,

        STRING_AGG(

            CASE

                WHEN cl.PackNumber = 0 THEN 'NP'

                WHEN pt.TrackingAgg IS NULL OR pt.TrackingAgg = '' THEN 'AT'

                ELSE pt.TrackingAgg

            END,

            ', '

        ) WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllTrackingStatuses,

        STRING_AGG(pd.PartsText, ', ')

            WITHIN GROUP (ORDER BY cl.ServiceCallSortKey) AS AllParts

    FROM CallList cl

    LEFT JOIN PackTracking pt ON pt.CallNum = cl.CallNum

    LEFT JOIN PartDescriptions pd ON pd.CallNum = cl.CallNum

)

---------------------------------------------------------

-- 3) FINAL OUTPUT (ONE ROW)

---------------------------------------------------------

SELECT

    @CaseNum AS CaseNum,

    ctc.AllCallText AS CallText,

    scs.AllCallNums,

    scs.AllPackNumbers,

    scs.AllBins,

    scs.AllCallDateTimes,

    scs.AllTrackingStatuses,

    scs.AllParts

FROM CallTextCollapsed ctc

LEFT JOIN ServiceCallSummary scs ON 1 = 1;
"""

# Use full query by default (backward compatibility)
SERVICE_CALL_QUERY = SERVICE_CALL_QUERY_FULL


class TrackingService:
    """Service for querying tracking database and updating tracking match columns."""
    
    def __init__(self):
        """Initialize tracking service with database connection."""
        self.connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build SQL Server connection string for tracking database."""
        parts = []
        if TRACKING_DB_DRIVER:
            parts.append(f"DRIVER={{{TRACKING_DB_DRIVER}}}")
        if TRACKING_DB_SERVER:
            # Remove tcp: prefix if present (pyodbc handles this automatically)
            server = TRACKING_DB_SERVER.replace("tcp:", "") if TRACKING_DB_SERVER.startswith("tcp:") else TRACKING_DB_SERVER
            parts.append(f"SERVER={server}")
        if TRACKING_DB_DATABASE:
            parts.append(f"DATABASE={TRACKING_DB_DATABASE}")
        if TRACKING_DB_USERNAME:
            parts.append(f"UID={TRACKING_DB_USERNAME}")
        if TRACKING_DB_PASSWORD:
            parts.append(f"PWD={TRACKING_DB_PASSWORD}")
        if TRACKING_DB_TRUST_SERVER_CERT:
            parts.append("TrustServerCertificate=yes")
        
        return ";".join(parts)
    
    def _get_connection(self):
        """Get a database connection to the tracking database."""
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to tracking database: {str(e)}")
            raise
    
    def _extract_ups_order_numbers(self, call_text: str) -> str:
        """
        Extract UPS order numbers from CallText.
        UPS order numbers are 9-digit numbers starting with 11 or 12.
        Returns comma-separated list of unique numbers.
        """
        if not call_text:
            return ""
        
        # Pattern: 9-digit numbers starting with 11 or 12
        pattern = r'\b(1[12]\d{7})\b'
        
        # Find all matches
        matches = re.findall(pattern, str(call_text))
        
        # Remove duplicates while preserving order, then join with commas
        unique_matches = []
        seen = set()
        for match in matches:
            if match not in seen:
                unique_matches.append(match)
                seen.add(match)
        
        return ', '.join(unique_matches)
    
    def query_tracking_info(self, vendor_call_number: str) -> Optional[Tuple[str, List[str], Optional[str]]]:
        """
        Query tracking database using vendor call number.
        
        Args:
            vendor_call_number: The vendor call number to query (may contain extra text like "1438056 / CT-16936")
            
        Returns:
            Tuple of (query_tracking_number, parts_list, None) or None if query fails
            - query_tracking_number: The value to write to querytrackingnumber column:
              * Actual tracking number if available
              * "No Tracking" if latest pack number is 0
              * "Pack Created No Tracking yet" if latest pack number is not 0 AND status is "AT"
            - parts_list: List of parts extracted from AllParts
            - None: Third element is always None (kept for backward compatibility)
        """
        if not vendor_call_number:
            return None
        
        # Extract 7-digit vendor call number using regex (vendor call numbers are always 7 digits)
        # Examples: "1438056 / CT-16936" -> "1438056", "FB-07534 / 1438042" -> "1438042"
        seven_digit_pattern = r'\b\d{7}\b'
        matches = re.findall(seven_digit_pattern, str(vendor_call_number))
        
        if not matches:
            logger.warning(f"No 7-digit vendor call number found in: {vendor_call_number}")
            return None
        
        # Use the first 7-digit number found (should typically be only one)
        extracted_number = matches[0]
        if len(matches) > 1:
            logger.debug(f"Multiple 7-digit numbers found in '{vendor_call_number}', using first: {extracted_number}")
        
        # Validate that extracted number is a valid integer
        try:
            call_num = int(extracted_number)
        except (ValueError, TypeError):
            logger.warning(f"Invalid vendor call number format (extracted '{extracted_number}' from '{vendor_call_number}'): {vendor_call_number}")
            return None
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # First, do a lightweight check to see if there are any pack numbers > 0
            # This helps us decide whether to fetch CallText and AllParts (expensive operations)
            check_query = SERVICE_CALL_QUERY_CHECK_PACKS.replace('{CALL_NUMBER}', str(call_num))
            logger.debug(f"Checking pack numbers for vendor call {vendor_call_number} (extracted: {call_num})")
            cursor.execute(check_query)
            
            # Navigate to result set
            while cursor.description is None:
                if not cursor.nextset():
                    break
            
            pack_check_result = None
            if cursor.description:
                pack_rows = cursor.fetchall()
                if pack_rows:
                    pack_check_result = dict(zip([col[0] for col in cursor.description], pack_rows[0]))
            
            # Determine if we need CallText and AllParts
            # If all pack numbers are 0, we know it will be "No Tracking" - skip expensive operations
            has_tracking = False
            if pack_check_result and pack_check_result.get('AllPackNumbers'):
                pack_numbers_str = pack_check_result.get('AllPackNumbers', '')
                # Check if any pack number is > 0
                pack_numbers = [item.strip() for item in str(pack_numbers_str).split(',') if item.strip()]
                for pack_num in pack_numbers:
                    try:
                        if int(pack_num) > 0:
                            has_tracking = True
                            break
                    except (ValueError, TypeError):
                        continue
            
            # Choose query based on whether tracking exists
            if has_tracking:
                query = SERVICE_CALL_QUERY_FULL.replace('{CALL_NUMBER}', str(call_num))
                logger.info(f"Querying tracking for vendor call {vendor_call_number} (extracted: {call_num}) - using FULL query (tracking found)")
            else:
                query = SERVICE_CALL_QUERY_SIMPLE.replace('{CALL_NUMBER}', str(call_num))
                logger.info(f"Querying tracking for vendor call {vendor_call_number} (extracted: {call_num}) - using SIMPLE query (no tracking, skipping CallText/AllParts)")
            
            cursor.execute(query)
            
            # For multi-statement queries, we may need to skip to the last result set
            # Try to get the result set with data
            while cursor.description is None:
                if not cursor.nextset():
                    break
            
            # Check if we have a result set
            if cursor.description is None:
                logger.warning(f"No result set returned for vendor call number: {vendor_call_number} (extracted: {call_num})")
                return None
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            logger.debug(f"Query returned columns: {columns}")
            
            # Fetch results
            rows = cursor.fetchall()
            logger.debug(f"Query returned {len(rows)} row(s) for vendor call number {vendor_call_number}")
            
            if not rows:
                logger.warning(f"No results found for vendor call number: {vendor_call_number} (extracted: {call_num})")
                return None
            
            # Convert to dictionary
            row_dict = dict(zip(columns, rows[0]))
            all_parts_value = row_dict.get('AllParts') or ''
            all_parts_preview = all_parts_value[:100] if all_parts_value else ''
            logger.debug(f"Query result for {vendor_call_number}: AllPackNumbers={row_dict.get('AllPackNumbers')}, "
                        f"AllTrackingStatuses={row_dict.get('AllTrackingStatuses')}, AllParts={all_parts_preview}...")
            
            # Extract UPS order numbers from CallText if present (only if CallText was fetched)
            call_text = row_dict.get('CallText', '')
            if call_text:
                ups_order_numbers = self._extract_ups_order_numbers(call_text)
                row_dict['UPSOrderNumbers'] = ups_order_numbers
                logger.debug(f"Extracted UPS order numbers: {ups_order_numbers}")
            else:
                # If CallText is NULL (from simple query), set empty UPSOrderNumbers
                row_dict['UPSOrderNumbers'] = ''
            
            # Determine tracking number
            tracking_number = determine_tracking_number(row_dict)
            logger.debug(f"Determined tracking number: {tracking_number}")
            
            # Extract latest parts
            all_parts = row_dict.get('AllParts', '')
            parts_list = extract_latest_parts(all_parts)
            logger.debug(f"Extracted {len(parts_list)} parts: {parts_list}")
            
            # Determine querytrackingnumber value based on latest pack number
            # Logic:
            # - If latest pack number is 0 (or list ends with 0), set to "No Tracking"
            # - If latest pack number is not 0 AND status is "AT", set to "Pack Created No Tracking yet"
            # - Otherwise, use the actual tracking number
            all_pack_numbers = row_dict.get('AllPackNumbers', '')
            all_tracking_statuses = row_dict.get('AllTrackingStatuses', '')
            
            query_tracking_number = tracking_number  # Default to actual tracking number
            
            if all_pack_numbers and all_tracking_statuses:
                # Split comma-separated values and get the last one from each
                pack_numbers = [item.strip() for item in str(all_pack_numbers).split(',') if item.strip()]
                tracking_statuses = [item.strip() for item in str(all_tracking_statuses).split(',') if item.strip()]
                
                if pack_numbers and tracking_statuses:
                    last_pack = pack_numbers[-1]
                    last_status = tracking_statuses[-1]
                    
                    try:
                        last_pack_int = int(last_pack)
                        # If latest pack number is 0, set to "No Tracking"
                        if last_pack_int == 0:
                            query_tracking_number = "No Tracking"
                        # If latest pack number is not 0 AND status is "AT", set to "Pack Created No Tracking yet"
                        elif last_pack_int != 0 and last_status.upper() == 'AT':
                            query_tracking_number = "Pack Created No Tracking yet"
                        # Otherwise, use the actual tracking number (already set above)
                        # Note: If last_status contains dash-separated tracking numbers (e.g., "414152235843-414152235854"),
                        # the tracking_number from determine_tracking_number will be the first one, but we want to preserve
                        # the full dash-separated value in query_tracking_number for tracking status checks
                        elif last_pack_int != 0:
                            # Check if last_status contains dash-separated tracking numbers
                            extracted_nums = extract_tracking_numbers_from_value(last_status)
                            if len(extracted_nums) > 1:
                                # Preserve the dash-separated format for query_tracking_number
                                query_tracking_number = '-'.join(extracted_nums)
                                logger.debug(f"Preserving dash-separated tracking numbers: {query_tracking_number}")
                    except (ValueError, TypeError):
                        # If it's not a valid integer, use the actual tracking number
                        pass
            
            # Extract all tracking numbers for logging (may be dash-separated in query_tracking_number)
            all_tracking_nums = extract_tracking_numbers_from_value(query_tracking_number)
            tracking_nums_str = ', '.join(all_tracking_nums) if all_tracking_nums else query_tracking_number
            
            # Log the final result at INFO level
            logger.info(f"Tracking query result for {vendor_call_number}: tracking={tracking_number}, "
                       f"query_tracking={query_tracking_number}, tracking_numbers=[{tracking_nums_str}], "
                       f"parts={len(parts_list)}, AllPackNumbers={all_pack_numbers}, AllTrackingStatuses={all_tracking_statuses}")
            
            return (query_tracking_number, parts_list, None)  # No longer return parts_tracking_value
            
        except Exception as e:
            logger.error(f"Error querying tracking database for vendor call {vendor_call_number} (extracted: {call_num}): {str(e)}", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()
    
    def query_tracking_info_batch(self, service_call_tuples: List[Tuple[str, str]]) -> Dict[str, Optional[Tuple[str, List[str], Optional[str]]]]:
        """
        Query tracking database for multiple vendor call numbers in parallel.
        
        Args:
            service_call_tuples: List of (service_call_id, vendor_call_number) tuples
                - service_call_id: Used to map results back to the correct database row
                - vendor_call_number: The 7-digit number used for the Epicor query
        
        Returns:
            Dictionary mapping service_call_id to (query_tracking_number, parts_list, None) or None
            - query_tracking_number: The value to write to querytrackingnumber column (actual number, "No Tracking", or "Pack Created No Tracking yet")
            - parts_list: List of parts extracted from AllParts
            - None: Third element is always None (kept for backward compatibility)
        """
        if not service_call_tuples:
            return {}
        
        results = {}
        max_workers = TRACKING_MAX_WORKERS
        
        logger.info(f"Starting batch query for {len(service_call_tuples)} vendor call numbers (max_workers={max_workers})")
        
        def query_single(item: Tuple[str, str]) -> Tuple[str, Optional[Tuple[str, List[str], Optional[str]]]]:
            """Query a single vendor call number and return result with service_call_id."""
            service_call_id, vendor_call_number = item
            try:
                result = self.query_tracking_info(vendor_call_number)
                return (service_call_id, result)
            except Exception as e:
                logger.error(f"Error in batch query for service call {service_call_id} (vendor {vendor_call_number}): {str(e)}")
                return (service_call_id, None)
        
        # Execute queries in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_item = {
                executor.submit(query_single, item): item 
                for item in service_call_tuples
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_item):
                completed += 1
                service_call_id, result = future.result()
                results[service_call_id] = result
                
                # Log progress every 10 completions or on last
                if completed % 10 == 0 or completed == len(service_call_tuples):
                    logger.debug(f"Batch query progress: {completed}/{len(service_call_tuples)} completed")
        
        successful = sum(1 for r in results.values() if r is not None)
        logger.info(f"Batch query complete: {successful}/{len(service_call_tuples)} successful")
        
        return results
    
    def check_tracking_match(self, tracking_number: str, des_note: Optional[str], 
                           part_note: Optional[str]) -> bool:
        """
        Check if tracking number appears in DesNote or PartNote columns.
        
        Args:
            tracking_number: The tracking number to search for
            des_note: DesNote column value
            part_note: PartNote column value
            
        Returns:
            True if tracking number is found, False otherwise
        """
        if not tracking_number or tracking_number == 'not available yet':
            return False
        
        # Search for tracking number in DesNote and PartNote only (case-insensitive)
        search_text = tracking_number.lower()
        
        columns_to_search = [
            des_note or '',
            part_note or ''
        ]
        
        for column_text in columns_to_search:
            if column_text and search_text in column_text.lower():
                return True
        
        return False
    
    def update_tracking_columns(self, cursor, service_call_id: str, tracking_number: str, 
                               parts_list: List[str], tracking_match: bool, tracking_status: Optional[str] = None,
                               parts_tracking_value: Optional[str] = None):
        """
        Update tracking columns in Burroughs_Open_Calls table.
        
        Args:
            cursor: Database cursor (pymssql)
            service_call_id: Service call ID to update
            tracking_number: Tracking number to write to querytrackingnumber (may be actual number, "No Tracking", or "Pack Created No Tracking yet")
            parts_list: List of parts to write (will be pipe-separated)
            tracking_match: Boolean indicating if match was found
            tracking_status: Optional tracking status from FedEx/UPS API
            parts_tracking_value: Deprecated - no longer used (kept for backward compatibility)
        """
        # Convert parts list to pipe-separated string
        parts_str = ' || '.join(parts_list) if parts_list else ''
        
        # Convert boolean to SQL bit value (0 or 1)
        match_value = 1 if tracking_match else 0
        
        # If tracking_status not provided and tracking_number contains actual tracking numbers (not status messages),
        # try to get status from FedEx/UPS APIs for ALL tracking numbers
        if tracking_status is None and tracking_number and tracking_number not in ['not available yet', 'No Tracking', 'Pack Created No Tracking yet']:
            # Extract all tracking numbers from the tracking_number value (may be dash-separated)
            all_tracking_numbers = extract_tracking_numbers_from_value(tracking_number)
            
            if all_tracking_numbers:
                statuses = []
                for tn in all_tracking_numbers:
                    # Try FedEx first
                    if is_fedex_tracking_number(tn):
                        try:
                            fedex_status = get_fedex_tracking_status(tn)
                            if fedex_status:
                                statuses.append(f"FedEx {tn}: {fedex_status}")
                                logger.debug(f"Retrieved FedEx tracking status for {tn}: {fedex_status}")
                            else:
                                statuses.append(f"FedEx {tn}: Status unavailable")
                                logger.debug(f"Could not retrieve FedEx tracking status for {tn}")
                        except Exception as e:
                            statuses.append(f"FedEx {tn}: Error - {str(e)}")
                            logger.warning(f"Error getting FedEx tracking status for {tn}: {str(e)}")
                    # Try UPS if not FedEx
                    elif is_ups_tracking_number(tn):
                        try:
                            ups_status = get_ups_tracking_status(tn)
                            if ups_status:
                                statuses.append(f"UPS {tn}: {ups_status}")
                                logger.debug(f"Retrieved UPS tracking status for {tn}: {ups_status}")
                            else:
                                statuses.append(f"UPS {tn}: Status unavailable")
                                logger.debug(f"Could not retrieve UPS tracking status for {tn}")
                        except Exception as e:
                            statuses.append(f"UPS {tn}: Error - {str(e)}")
                            logger.warning(f"Error getting UPS tracking status for {tn}: {str(e)}")
                
                # Combine all statuses with semicolon separator
                if statuses:
                    tracking_status = '; '.join(statuses)
                    logger.debug(f"Combined tracking status: {tracking_status}")
        
        # Update query - only update querytrackingnumber (no longer update parts_tracking)
        update_sql = f"""
        UPDATE {SOURCE_TABLE}
        SET querytrackingnumber = %s,
            queryparts = %s,
            trackingmatch = %s,
            tracking_status = %s
        WHERE \"Service_Call_ID\" = %s;
        """
        update_params = (tracking_number, parts_str, match_value, tracking_status, service_call_id)
        
        # Log what we're writing (especially if NULL/empty values)
        log_values = {
            'querytrackingnumber': tracking_number or 'NULL',
            'queryparts': parts_str or 'NULL',
            'trackingmatch': match_value,
            'tracking_status': tracking_status or 'NULL'
        }
        logger.info(f"Updating tracking columns for service call {service_call_id}: "
                   f"querytrackingnumber={log_values['querytrackingnumber']}, "
                   f"queryparts={len(parts_str)} chars, trackingmatch={match_value}, "
                   f"tracking_status={log_values['tracking_status']}")
        
        try:
            cursor.execute(update_sql, update_params)
        except Exception as e:
            error_msg = str(e)
            # Check if it's a truncation error
            if "would be truncated" in error_msg or "2628" in error_msg:
                logger.error(f"Error updating tracking columns for service call {service_call_id}: Column size too small")
                logger.error(f"Parts string length: {len(parts_str)} characters")
                logger.error(f"Parts string preview: {parts_str[:200]}...")
                logger.warning("The queryparts column may need to be altered to NVARCHAR(MAX). This should be fixed automatically on next startup.")
                # Try to truncate and save what we can (last resort)
                if len(parts_str) > 1000:
                    logger.warning(f"Truncating parts string from {len(parts_str)} to 1000 characters")
                    parts_str = parts_str[:1000] + "... [truncated]"
                    try:
                        cursor.execute(update_sql, (tracking_number, parts_str, match_value, tracking_status, service_call_id))
                        logger.info(f"Successfully updated service call {service_call_id} with truncated parts string")
                        return
                    except Exception as e2:
                        logger.error(f"Failed to update even with truncated string: {str(e2)}")
            logger.error(f"Error updating tracking columns for service call {service_call_id}: {error_msg}")
            raise

