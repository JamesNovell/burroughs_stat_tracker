"""Database connection and table creation management."""
import pymssql
import logging
from app.config import (
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_HOURLY_TABLE, RECYCLERS_DAILY_TABLE,
    SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_HOURLY_TABLE, SMART_SAFES_DAILY_TABLE,
    SOURCE_TABLE
)

logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a database connection."""
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        raise ValueError("Database credentials must be set in config.json.")
    
    logger.debug(f"Connecting to database: {DB_NAME} at {DB_HOST}")
    conn = pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    conn.autocommit(True)
    logger.debug("Database connection established successfully")
    return conn


def create_stat_table(cursor, table_name):
    """Create a statistics table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        stat_table_schema = f"""
        CREATE TABLE {table_name} (
            StatID INT IDENTITY(1,1) PRIMARY KEY,
            Timestamp DATETIME,
            BatchID BIGINT,
            TotalOpenCalls INT,
            CallsClosedSinceLastBatch INT,
            SameDayClosures INT,
            CallsWithMultipleAppointments INT,
            AverageAppointmentNumber FLOAT,
            StatusSummary NVARCHAR(MAX),
            SameDayCloseRate FLOAT,
            AvgAppointmentsPerCompletedCall FLOAT,
            FirstTimeFixRate FLOAT,
            FourteenDayReopenRate FLOAT,
            TotalFollowUpAppointments INT,
            TotalAppointments INT,
            RepeatDispatchRate FLOAT
        );
        """
        cursor.execute(stat_table_schema)
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")
        # Migration: Add RDR columns if they don't exist
        try:
            # Check if TotalFollowUpAppointments column exists
            check_follow_up = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalFollowUpAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalFollowUpAppointments INT DEFAULT 0;
                PRINT 'Added TotalFollowUpAppointments column to {table_name}';
            END
            """
            cursor.execute(check_follow_up)
            
            # Check if TotalAppointments column exists
            check_total_appt = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalAppointments INT DEFAULT 0;
                PRINT 'Added TotalAppointments column to {table_name}';
            END
            """
            cursor.execute(check_total_appt)
            
            # Check if RepeatDispatchRate column exists
            check_rdr = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'RepeatDispatchRate')
            BEGIN
                ALTER TABLE {table_name} ADD RepeatDispatchRate FLOAT;
                PRINT 'Added RepeatDispatchRate column to {table_name}';
            END
            """
            cursor.execute(check_rdr)
        except Exception as e:
            # Ignore errors if columns already exist
            pass


def create_history_table(cursor, table_name):
    """Create a history table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        history_table_schema = f"""
        CREATE TABLE {table_name} (
            Service_Call_ID VARCHAR(255) NOT NULL,
            ClosedTimestamp DATETIME NOT NULL,
            OpenDateTime DATETIME,
            Equipment_ID VARCHAR(255),
            VendorCallNumber VARCHAR(255),
            PRIMARY KEY (Service_Call_ID, ClosedTimestamp)
        );
        """
        cursor.execute(history_table_schema)
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")
        # Migration: Add RDR columns if they don't exist
        try:
            # Check if TotalFollowUpAppointments column exists
            check_follow_up = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalFollowUpAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalFollowUpAppointments INT DEFAULT 0;
                PRINT 'Added TotalFollowUpAppointments column to {table_name}';
            END
            """
            cursor.execute(check_follow_up)
            
            # Check if TotalAppointments column exists
            check_total_appt = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalAppointments INT DEFAULT 0;
                PRINT 'Added TotalAppointments column to {table_name}';
            END
            """
            cursor.execute(check_total_appt)
            
            # Check if RepeatDispatchRate column exists
            check_rdr = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'RepeatDispatchRate')
            BEGIN
                ALTER TABLE {table_name} ADD RepeatDispatchRate FLOAT;
                PRINT 'Added RepeatDispatchRate column to {table_name}';
            END
            """
            cursor.execute(check_rdr)
        except Exception as e:
            # Ignore errors if columns already exist
            pass
    
    # Add new columns if table exists but columns don't (for existing installations)
    try:
        # Check if Equipment_ID column exists
        check_equipment = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'Equipment_ID')
        BEGIN
            ALTER TABLE {table_name} ADD Equipment_ID VARCHAR(255);
            PRINT 'Added Equipment_ID column to {table_name}';
        END
        """
        cursor.execute(check_equipment)
        
        # Check if VendorCallNumber column exists
        check_vendor = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'VendorCallNumber')
        BEGIN
            ALTER TABLE {table_name} ADD VendorCallNumber VARCHAR(255);
            PRINT 'Added VendorCallNumber column to {table_name}';
        END
        """
        cursor.execute(check_vendor)
    except Exception as e:
        # Ignore errors if table doesn't exist or columns already exist
        pass


def create_hourly_stat_table(cursor, table_name):
    """Create an hourly statistics table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        # Escape table name for constraint/index names
        safe_name = table_name.replace('.', '_').replace(' ', '_')
        
        hourly_table_schema = f"""
        CREATE TABLE {table_name} (
            HourlyStatID INT IDENTITY(1,1) PRIMARY KEY,
            Date DATE NOT NULL,
            Hour INT NOT NULL,
            PeriodMinute INT NOT NULL DEFAULT 0,
            PeriodStart DATETIME NOT NULL,
            PeriodEnd DATETIME NOT NULL,
            Timestamp DATETIME NOT NULL,
            
            -- Counts (for aggregation)
            TotalOpenCalls INT DEFAULT 0,
            TotalClosedCalls INT DEFAULT 0,
            TotalSameDayClosures INT DEFAULT 0,
            TotalCallsWithMultiAppt INT DEFAULT 0,
            TotalNewCalls INT DEFAULT 0,
            TotalReopenedCalls INT DEFAULT 0,
            TotalNotServicedYet INT DEFAULT 0,
            
            -- Sums (for averaging)
            SumAppointments INT DEFAULT 0,
            SumCompletedAppointments INT DEFAULT 0,
            
            -- Pre-calculated rates (for reference)
            AverageAppointmentNumber FLOAT,
            SameDayCloseRate FLOAT,
            FirstTimeFixRate FLOAT,
            AvgAppointmentsPerCompletedCall FLOAT,
            
            -- First-Time Fix Rate running totals (accumulated throughout the day)
            TotalFirstTimeFixes INT DEFAULT 0,
            TotalClosedCallsForFTF INT DEFAULT 0,
            FirstTimeFixRate_RunningTotal FLOAT,
            
            -- RDR metrics
            TotalFollowUpAppointments INT DEFAULT 0,
            TotalAppointments INT DEFAULT 0,
            RepeatDispatchRate FLOAT,
            
            -- Metadata
            BatchCount INT DEFAULT 0,
            BatchMissing BIT DEFAULT 0,
            CreatedAt DATETIME DEFAULT GETDATE(),
            
            -- Unique constraint on date/hour/period combination
            CONSTRAINT UQ_{safe_name}_Date_Hour_Period UNIQUE (Date, Hour, PeriodMinute)
        );
        """
        cursor.execute(hourly_table_schema)
        
        # Create indexes separately
        try:
            index1_sql = f"CREATE INDEX IX_{safe_name}_Date_Hour_Period ON {table_name}(Date, Hour, PeriodMinute);"
            cursor.execute(index1_sql)
            
            index2_sql = f"CREATE INDEX IX_{safe_name}_Period ON {table_name}(PeriodStart, PeriodEnd);"
            cursor.execute(index2_sql)
        except Exception as e:
            # Indexes might already exist, ignore error
            logger.debug(f"Index creation skipped (may already exist): {e}")
        
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")
        # Migration: Add/remove columns if they don't exist
        try:
            # Check if PeriodMinute column exists
            check_period_minute = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'PeriodMinute')
            BEGIN
                ALTER TABLE {table_name} ADD PeriodMinute INT NOT NULL DEFAULT 0;
                PRINT 'Added PeriodMinute column to {table_name}';
            END
            """
            cursor.execute(check_period_minute)
            
            # Check if BatchMissing column exists
            check_batch_missing = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'BatchMissing')
            BEGIN
                ALTER TABLE {table_name} ADD BatchMissing BIT DEFAULT 0;
                PRINT 'Added BatchMissing column to {table_name}';
            END
            """
            cursor.execute(check_batch_missing)
            
            # Check if TotalFollowUpAppointments column exists
            check_follow_up = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalFollowUpAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalFollowUpAppointments INT DEFAULT 0;
                PRINT 'Added TotalFollowUpAppointments column to {table_name}';
            END
            """
            cursor.execute(check_follow_up)
            
            # Check if TotalAppointments column exists
            check_total_appt = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalAppointments INT DEFAULT 0;
                PRINT 'Added TotalAppointments column to {table_name}';
            END
            """
            cursor.execute(check_total_appt)
            
            # Check if RepeatDispatchRate column exists
            check_rdr = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'RepeatDispatchRate')
            BEGIN
                ALTER TABLE {table_name} ADD RepeatDispatchRate FLOAT;
                PRINT 'Added RepeatDispatchRate column to {table_name}';
            END
            """
            cursor.execute(check_rdr)
            
            # Check if TotalNotServicedYet column exists (calls with Appointment = 1)
            check_not_serviced = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalNotServicedYet')
            BEGIN
                ALTER TABLE {table_name} ADD TotalNotServicedYet INT DEFAULT 0;
                PRINT 'Added TotalNotServicedYet column to {table_name}';
            END
            """
            cursor.execute(check_not_serviced)
            
            # Remove FourteenDayReopenRate if it exists
            check_reopen = f"""
            IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'FourteenDayReopenRate')
            BEGIN
                ALTER TABLE {table_name} DROP COLUMN FourteenDayReopenRate;
                PRINT 'Removed FourteenDayReopenRate column from {table_name}';
            END
            """
            cursor.execute(check_reopen)
            
            # Check if First-Time Fix Rate running total columns exist
            check_ftf_total = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalFirstTimeFixes')
            BEGIN
                ALTER TABLE {table_name} ADD TotalFirstTimeFixes INT DEFAULT 0;
                PRINT 'Added TotalFirstTimeFixes column to {table_name}';
            END
            """
            cursor.execute(check_ftf_total)
            
            check_ftf_closed = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalClosedCallsForFTF')
            BEGIN
                ALTER TABLE {table_name} ADD TotalClosedCallsForFTF INT DEFAULT 0;
                PRINT 'Added TotalClosedCallsForFTF column to {table_name}';
            END
            """
            cursor.execute(check_ftf_closed)
            
            check_ftf_running = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'FirstTimeFixRate_RunningTotal')
            BEGIN
                ALTER TABLE {table_name} ADD FirstTimeFixRate_RunningTotal FLOAT;
                PRINT 'Added FirstTimeFixRate_RunningTotal column to {table_name}';
            END
            """
            cursor.execute(check_ftf_running)
        except Exception as e:
            # Ignore errors if columns already exist
            pass


def create_daily_summary_table(cursor, table_name):
    """Create a daily summary table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        daily_table_schema = f"""
        CREATE TABLE {table_name} (
            SummaryID INT IDENTITY(1,1) PRIMARY KEY,
            Date DATE,
            Timestamp DATETIME,
            AvgApptNum_OpenAtEndOfDay FLOAT,
            AvgApptNum_ClosedToday FLOAT,
            TotalOpenAtEndOfDay INT,
            TotalClosedEOD INT,
            TotalSameDayClosures INT DEFAULT 0,
            TotalCallsWithMultiAppt INT DEFAULT 0,
            TotalNotServicedYet INT DEFAULT 0,
            FirstTimeFixRate_RunningTotal FLOAT,
            RepeatDispatchRate FLOAT,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        """
        cursor.execute(daily_table_schema)
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")
        # Migration: Add RDR columns if they don't exist
        try:
            # Check if TotalFollowUpAppointments column exists
            check_follow_up = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalFollowUpAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalFollowUpAppointments INT DEFAULT 0;
                PRINT 'Added TotalFollowUpAppointments column to {table_name}';
            END
            """
            cursor.execute(check_follow_up)
            
            # Check if TotalAppointments column exists
            check_total_appt = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalAppointments')
            BEGIN
                ALTER TABLE {table_name} ADD TotalAppointments INT DEFAULT 0;
                PRINT 'Added TotalAppointments column to {table_name}';
            END
            """
            cursor.execute(check_total_appt)
            
            # Check if RepeatDispatchRate column exists
            check_rdr = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'RepeatDispatchRate')
            BEGIN
                ALTER TABLE {table_name} ADD RepeatDispatchRate FLOAT;
                PRINT 'Added RepeatDispatchRate column to {table_name}';
            END
            """
            cursor.execute(check_rdr)
        except Exception as e:
            # Ignore errors if columns already exist
            pass
        # Migration: Handle existing tables
        try:
            # Check if SummaryID exists (new primary key)
            pk_constraint_name = f"PK_{table_name.replace('.', '_').replace(' ', '_')}"
            # Escape table name for use in dynamic SQL (escape single quotes for SQL string)
            table_name_escaped = table_name.replace("'", "''")
            # Build the dynamic SQL string - need to escape single quotes properly
            # Final SQL should be: DECLARE @sql NVARCHAR(MAX) = 'ALTER TABLE [table] DROP CONSTRAINT ' + @pk_name;
            drop_sql_template = f"ALTER TABLE [{table_name_escaped}] DROP CONSTRAINT "
            # Escape single quotes in the template for use inside SQL string literal
            drop_sql_template_escaped = drop_sql_template.replace("'", "''")
            check_summary_id = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'SummaryID')
            BEGIN
                -- Add SummaryID column
                ALTER TABLE {table_name} ADD SummaryID INT IDENTITY(1,1);
                -- Make it the primary key (drop old PK first if Date was PK)
                IF EXISTS (SELECT * FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('{table_name}') AND type = 'PK')
                BEGIN
                    DECLARE @pk_name NVARCHAR(255);
                    SELECT @pk_name = name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('{table_name}') AND type = 'PK';
                    DECLARE @sql NVARCHAR(MAX) = '{drop_sql_template_escaped}' + @pk_name;
                    EXEC sp_executesql @sql;
                END
                ALTER TABLE {table_name} ADD CONSTRAINT {pk_constraint_name} PRIMARY KEY (SummaryID);
                PRINT 'Added SummaryID as primary key to {table_name}';
            END
            """
            cursor.execute(check_summary_id)
            
            # Check if Timestamp column exists
            check_timestamp = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'Timestamp')
            BEGIN
                ALTER TABLE {table_name} ADD Timestamp DATETIME;
                PRINT 'Added Timestamp column to {table_name}';
            END
            """
            cursor.execute(check_timestamp)
            
            # Check if TotalClosedToday exists and rename to TotalClosedEOD
            check_old_col = f"""
            IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalClosedToday')
            BEGIN
                EXEC sp_rename '{table_name}.TotalClosedToday', 'TotalClosedEOD', 'COLUMN';
                PRINT 'Renamed TotalClosedToday to TotalClosedEOD';
            END
            """
            cursor.execute(check_old_col)
            
            # Remove CallsClosedSinceLastBatch if it exists (no longer needed)
            check_remove_col = f"""
            IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'CallsClosedSinceLastBatch')
            BEGIN
                ALTER TABLE {table_name} DROP COLUMN CallsClosedSinceLastBatch;
                PRINT 'Removed CallsClosedSinceLastBatch column';
            END
            """
            cursor.execute(check_remove_col)
            
            # Check if RepeatDispatchRate column exists
            check_rdr = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'RepeatDispatchRate')
            BEGIN
                ALTER TABLE {table_name} ADD RepeatDispatchRate FLOAT;
                PRINT 'Added RepeatDispatchRate column to {table_name}';
            END
            """
            cursor.execute(check_rdr)
            
            # Check if TotalSameDayClosures column exists
            check_same_day = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalSameDayClosures')
            BEGIN
                ALTER TABLE {table_name} ADD TotalSameDayClosures INT DEFAULT 0;
                PRINT 'Added TotalSameDayClosures column to {table_name}';
            END
            """
            cursor.execute(check_same_day)
            
            # Check if TotalCallsWithMultiAppt column exists
            check_multi_appt = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalCallsWithMultiAppt')
            BEGIN
                ALTER TABLE {table_name} ADD TotalCallsWithMultiAppt INT DEFAULT 0;
                PRINT 'Added TotalCallsWithMultiAppt column to {table_name}';
            END
            """
            cursor.execute(check_multi_appt)
            
            # Check if TotalNotServicedYet column exists
            check_not_serviced = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalNotServicedYet')
            BEGIN
                ALTER TABLE {table_name} ADD TotalNotServicedYet INT DEFAULT 0;
                PRINT 'Added TotalNotServicedYet column to {table_name}';
            END
            """
            cursor.execute(check_not_serviced)
            
            # Remove TotalActiveToday if it exists
            check_active = f"""
            IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'TotalActiveToday')
            BEGIN
                ALTER TABLE {table_name} DROP COLUMN TotalActiveToday;
                PRINT 'Removed TotalActiveToday column from {table_name}';
            END
            """
            cursor.execute(check_active)
            
            # Check if FirstTimeFixRate_RunningTotal column exists
            check_ftf_running = f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}') AND name = 'FirstTimeFixRate_RunningTotal')
            BEGIN
                ALTER TABLE {table_name} ADD FirstTimeFixRate_RunningTotal FLOAT;
                PRINT 'Added FirstTimeFixRate_RunningTotal column to {table_name}';
            END
            """
            cursor.execute(check_ftf_running)
        except Exception as e:
            # Ignore errors if columns don't exist or already renamed
            pass


def ensure_tracking_columns_exist(cursor):
    """Ensure tracking columns exist in Burroughs_Open_Calls table."""
    try:
        # Check if querytrackingnumber column exists
        check_tracking_num = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{SOURCE_TABLE}') AND name = 'querytrackingnumber')
        BEGIN
            ALTER TABLE {SOURCE_TABLE} ADD querytrackingnumber NVARCHAR(255);
            PRINT 'Added querytrackingnumber column to {SOURCE_TABLE}';
        END
        """
        cursor.execute(check_tracking_num)
        
        # Check if queryparts column exists
        check_parts = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{SOURCE_TABLE}') AND name = 'queryparts')
        BEGIN
            ALTER TABLE {SOURCE_TABLE} ADD queryparts NVARCHAR(MAX);
            PRINT 'Added queryparts column to {SOURCE_TABLE}';
        END
        ELSE
        BEGIN
            -- Check if column is too small and alter it if needed
            DECLARE @max_length INT;
            SELECT @max_length = max_length 
            FROM sys.columns 
            WHERE object_id = OBJECT_ID('{SOURCE_TABLE}') AND name = 'queryparts';
            
            -- If column exists but is not MAX (which is -1), alter it to MAX
            IF @max_length != -1
            BEGIN
                ALTER TABLE {SOURCE_TABLE} ALTER COLUMN queryparts NVARCHAR(MAX);
                PRINT 'Altered queryparts column to NVARCHAR(MAX) in {SOURCE_TABLE}';
            END
        END
        """
        cursor.execute(check_parts)
        
        # Check if trackingmatch column exists
        check_match = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{SOURCE_TABLE}') AND name = 'trackingmatch')
        BEGIN
            ALTER TABLE {SOURCE_TABLE} ADD trackingmatch BIT DEFAULT 0;
            PRINT 'Added trackingmatch column to {SOURCE_TABLE}';
        END
        """
        cursor.execute(check_match)
        
        # Check if tracking_status column exists
        check_status = f"""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{SOURCE_TABLE}') AND name = 'tracking_status')
        BEGIN
            ALTER TABLE {SOURCE_TABLE} ADD tracking_status NVARCHAR(500);
            PRINT 'Added tracking_status column to {SOURCE_TABLE}';
        END
        """
        cursor.execute(check_status)
    except Exception as e:
        # Log error but don't fail - columns may already exist or table may not exist yet
        logger.warning(f"Could not ensure tracking columns exist: {e}")


def create_weekly_summary_table(cursor, table_name):
    """Create a weekly summary table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        weekly_table_schema = f"""
        CREATE TABLE {table_name} (
            WeeklyStatID INT IDENTITY(1,1) PRIMARY KEY,
            WeekStartDate DATE NOT NULL,
            WeekEndDate DATE NOT NULL,
            Year INT NOT NULL,
            WeekNumber INT NOT NULL,
            Timestamp DATETIME NOT NULL,
            
            -- Aggregated metrics from daily summaries
            AvgApptNum_OpenAtEndOfWeek FLOAT,
            AvgApptNum_ClosedThisWeek FLOAT,
            TotalOpenAtEndOfWeek INT DEFAULT 0,
            TotalClosedThisWeek INT DEFAULT 0,
            TotalSameDayClosures INT DEFAULT 0,
            TotalCallsWithMultiAppt INT DEFAULT 0,
            TotalNotServicedYet INT DEFAULT 0,
            FirstTimeFixRate_RunningTotal FLOAT,
            RepeatDispatchRate FLOAT,
            
            -- Metadata
            DayCount INT DEFAULT 0,
            CreatedAt DATETIME DEFAULT GETDATE(),
            
            -- Unique constraint on week
            CONSTRAINT UQ_{table_name.replace('.', '_').replace(' ', '_')}_Week UNIQUE (Year, WeekNumber)
        );
        """
        cursor.execute(weekly_table_schema)
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")


def create_monthly_summary_table(cursor, table_name):
    """Create a monthly summary table if it doesn't exist."""
    # Check if table exists
    check_table = f"SELECT COUNT(*) as table_count FROM sys.tables WHERE name = '{table_name}';"
    cursor.execute(check_table)
    table_exists = cursor.fetchone()['table_count'] > 0
    
    if not table_exists:
        monthly_table_schema = f"""
        CREATE TABLE {table_name} (
            MonthlyStatID INT IDENTITY(1,1) PRIMARY KEY,
            Year INT NOT NULL,
            Month INT NOT NULL,
            MonthStartDate DATE NOT NULL,
            MonthEndDate DATE NOT NULL,
            Timestamp DATETIME NOT NULL,
            
            -- Aggregated metrics from weekly summaries
            AvgApptNum_OpenAtEndOfMonth FLOAT,
            AvgApptNum_ClosedThisMonth FLOAT,
            TotalOpenAtEndOfMonth INT DEFAULT 0,
            TotalClosedThisMonth INT DEFAULT 0,
            TotalSameDayClosures INT DEFAULT 0,
            TotalCallsWithMultiAppt INT DEFAULT 0,
            TotalNotServicedYet INT DEFAULT 0,
            FirstTimeFixRate_RunningTotal FLOAT,
            RepeatDispatchRate FLOAT,
            
            -- Metadata
            WeekCount INT DEFAULT 0,
            CreatedAt DATETIME DEFAULT GETDATE(),
            
            -- Unique constraint on year/month
            CONSTRAINT UQ_{table_name.replace('.', '_').replace(' ', '_')}_Month UNIQUE (Year, Month)
        );
        """
        cursor.execute(monthly_table_schema)
        logger.info(f"Created table: {table_name}")
    else:
        logger.debug(f"Table already exists: {table_name}")


def create_tables_if_not_exist(cursor):
    """Ensures the necessary statistics and history tables exist for both equipment types."""
    from app.config import (
        RECYCLERS_WEEKLY_TABLE, RECYCLERS_MONTHLY_TABLE,
        SMART_SAFES_WEEKLY_TABLE, SMART_SAFES_MONTHLY_TABLE
    )
    
    logger.info("=" * 80)
    logger.info("Checking and creating tables")
    logger.info("=" * 80)
    
    # Create tables for recyclers
    logger.info("[Recyclers Tables]")
    create_stat_table(cursor, RECYCLERS_STAT_TABLE)
    create_history_table(cursor, RECYCLERS_HISTORY_TABLE)
    create_hourly_stat_table(cursor, RECYCLERS_HOURLY_TABLE)
    create_daily_summary_table(cursor, RECYCLERS_DAILY_TABLE)
    create_weekly_summary_table(cursor, RECYCLERS_WEEKLY_TABLE)
    create_monthly_summary_table(cursor, RECYCLERS_MONTHLY_TABLE)
    
    # Create tables for smart safes
    logger.info("[Smart Safes Tables]")
    create_stat_table(cursor, SMART_SAFES_STAT_TABLE)
    create_history_table(cursor, SMART_SAFES_HISTORY_TABLE)
    create_hourly_stat_table(cursor, SMART_SAFES_HOURLY_TABLE)
    create_daily_summary_table(cursor, SMART_SAFES_DAILY_TABLE)
    create_weekly_summary_table(cursor, SMART_SAFES_WEEKLY_TABLE)
    create_monthly_summary_table(cursor, SMART_SAFES_MONTHLY_TABLE)
    
    # Ensure tracking columns exist in source table
    logger.info("[Source Table - Tracking Columns]")
    ensure_tracking_columns_exist(cursor)
    
    logger.info("Table check complete")

