"""
Tracking service for querying tracking database and updating tracking match columns.
"""
import pyodbc
import re
import logging
from typing import Optional, Tuple, List
from app.config import (
    TRACKING_DB_DRIVER, TRACKING_DB_SERVER, TRACKING_DB_DATABASE,
    TRACKING_DB_USERNAME, TRACKING_DB_PASSWORD, TRACKING_DB_TRUST_SERVER_CERT,
    SOURCE_TABLE
)
from app.utils.tracking_parser import determine_tracking_number, extract_latest_parts
from app.services.fedex_tracker import is_fedex_tracking_number, get_fedex_tracking_status
from app.services.ups_tracker import is_ups_tracking_number, get_ups_tracking_status

logger = logging.getLogger(__name__)

# Service call query template (embedded from Tracking_Client/queries/service_call_query.sql)
SERVICE_CALL_QUERY = """
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
            parts.append(f"SERVER={TRACKING_DB_SERVER}")
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
    
    def query_tracking_info(self, vendor_call_number: str) -> Optional[Tuple[str, List[str]]]:
        """
        Query tracking database using vendor call number.
        
        Args:
            vendor_call_number: The vendor call number to query
            
        Returns:
            Tuple of (tracking_number, parts_list) or None if query fails
        """
        if not vendor_call_number:
            return None
        
        # Validate that vendor_call_number is a valid integer
        try:
            call_num = int(vendor_call_number)
        except (ValueError, TypeError):
            logger.warning(f"Invalid vendor call number format: {vendor_call_number}")
            return None
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Replace placeholder in query
            query = SERVICE_CALL_QUERY.replace('{CALL_NUMBER}', str(call_num))
            
            cursor.execute(query)
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Fetch results
            rows = cursor.fetchall()
            
            if not rows:
                logger.info(f"No results found for vendor call number: {vendor_call_number}")
                return None
            
            # Convert to dictionary
            row_dict = dict(zip(columns, rows[0]))
            
            # Extract UPS order numbers from CallText if present
            call_text = row_dict.get('CallText', '')
            if call_text:
                ups_order_numbers = self._extract_ups_order_numbers(call_text)
                row_dict['UPSOrderNumbers'] = ups_order_numbers
            
            # Determine tracking number
            tracking_number = determine_tracking_number(row_dict)
            
            # Extract latest parts
            all_parts = row_dict.get('AllParts', '')
            parts_list = extract_latest_parts(all_parts)
            
            return (tracking_number, parts_list)
            
        except Exception as e:
            logger.error(f"Error querying tracking database for vendor call {vendor_call_number}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def check_tracking_match(self, tracking_number: str, des_note: Optional[str], 
                           part_note: Optional[str], parts_tracking: Optional[str]) -> bool:
        """
        Check if tracking number appears in any of the three columns.
        
        Args:
            tracking_number: The tracking number to search for
            des_note: DesNote column value
            part_note: PartNote column value
            parts_tracking: parts_tracking column value
            
        Returns:
            True if tracking number is found, False otherwise
        """
        if not tracking_number or tracking_number == 'not available yet':
            return False
        
        # Search for tracking number in each column (case-insensitive)
        search_text = tracking_number.lower()
        
        columns_to_search = [
            des_note or '',
            part_note or '',
            parts_tracking or ''
        ]
        
        for column_text in columns_to_search:
            if column_text and search_text in column_text.lower():
                return True
        
        return False
    
    def update_tracking_columns(self, cursor, service_call_id: str, tracking_number: str, 
                               parts_list: List[str], tracking_match: bool):
        """
        Update tracking columns in Burroughs_Open_Calls table.
        
        Args:
            cursor: Database cursor (pymssql)
            service_call_id: Service call ID to update
            tracking_number: Tracking number to write
            parts_list: List of parts to write (will be pipe-separated)
            tracking_match: Boolean indicating if match was found
        """
        # Convert parts list to pipe-separated string
        parts_str = ' || '.join(parts_list) if parts_list else ''
        
        # Convert boolean to SQL bit value (0 or 1)
        match_value = 1 if tracking_match else 0
        
        # Check if tracking number is FedEx or UPS and get status
        tracking_status = None
        if tracking_number and tracking_number != 'not available yet':
            # Try FedEx first
            if is_fedex_tracking_number(tracking_number):
                try:
                    tracking_status = get_fedex_tracking_status(tracking_number)
                    if tracking_status:
                        logger.info(f"Retrieved FedEx tracking status for {tracking_number}: {tracking_status}")
                    else:
                        logger.warning(f"Could not retrieve FedEx tracking status for {tracking_number}")
                except Exception as e:
                    logger.error(f"Error getting FedEx tracking status for {tracking_number}: {str(e)}")
                    # Continue without status if FedEx API fails
            # Try UPS if not FedEx
            elif is_ups_tracking_number(tracking_number):
                try:
                    tracking_status = get_ups_tracking_status(tracking_number)
                    if tracking_status:
                        logger.info(f"Retrieved UPS tracking status for {tracking_number}: {tracking_status}")
                    else:
                        logger.warning(f"Could not retrieve UPS tracking status for {tracking_number}")
                except Exception as e:
                    logger.error(f"Error getting UPS tracking status for {tracking_number}: {str(e)}")
                    # Continue without status if UPS API fails
        
        # Update query
        update_sql = f"""
        UPDATE {SOURCE_TABLE}
        SET querytrackingnumber = %s,
            queryparts = %s,
            trackingmatch = %s,
            tracking_status = %s
        WHERE \"Service_Call_ID\" = %s;
        """
        
        try:
            cursor.execute(update_sql, (tracking_number, parts_str, match_value, tracking_status, service_call_id))
        except Exception as e:
            logger.error(f"Error updating tracking columns for service call {service_call_id}: {str(e)}")
            raise

