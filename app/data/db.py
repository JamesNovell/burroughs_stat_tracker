"""Database connection and table creation management."""
import pymssql
from app.config import (
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    RECYCLERS_STAT_TABLE, RECYCLERS_HISTORY_TABLE, RECYCLERS_DAILY_TABLE,
    SMART_SAFES_STAT_TABLE, SMART_SAFES_HISTORY_TABLE, SMART_SAFES_DAILY_TABLE
)


def get_db_connection():
    """Create and return a database connection."""
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        raise ValueError("Database credentials must be set in config.json.")
    
    conn = pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    conn.autocommit(True)
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
            FourteenDayReopenRate FLOAT
        );
        """
        cursor.execute(stat_table_schema)
        print(f"Created table: {table_name}")
    else:
        print(f"Table already exists: {table_name}")


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
        print(f"Created table: {table_name}")
    else:
        print(f"Table already exists: {table_name}")
    
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
            TotalActiveToday INT,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        """
        cursor.execute(daily_table_schema)
        print(f"Created table: {table_name}")
    else:
        print(f"Table already exists: {table_name}")
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
        except Exception as e:
            # Ignore errors if columns don't exist or already renamed
            pass


def create_tables_if_not_exist(cursor):
    """Ensures the necessary statistics and history tables exist for both equipment types."""
    print("\n=== Checking and creating tables ===")
    # Create tables for recyclers
    print(f"\n[Recyclers Tables]")
    create_stat_table(cursor, RECYCLERS_STAT_TABLE)
    create_history_table(cursor, RECYCLERS_HISTORY_TABLE)
    create_daily_summary_table(cursor, RECYCLERS_DAILY_TABLE)
    
    # Create tables for smart safes
    print(f"\n[Smart Safes Tables]")
    create_stat_table(cursor, SMART_SAFES_STAT_TABLE)
    create_history_table(cursor, SMART_SAFES_HISTORY_TABLE)
    create_daily_summary_table(cursor, SMART_SAFES_DAILY_TABLE)
    print("=== Table check complete ===\n")

