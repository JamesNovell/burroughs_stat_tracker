"""Database connectivity health checks."""
import logging
import pymssql
import pyodbc
from typing import Tuple, Optional
from app.config import (
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    TRACKING_DB_DRIVER, TRACKING_DB_SERVER, TRACKING_DB_DATABASE,
    TRACKING_DB_USERNAME, TRACKING_DB_PASSWORD, TRACKING_DB_TRUST_SERVER_CERT
)

logger = logging.getLogger(__name__)


def check_main_database() -> Tuple[bool, Optional[str]]:
    """
    Check connectivity to the main database (pymssql).
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        logger.debug(f"Testing main database connection: {DB_NAME} at {DB_HOST}")
        conn = pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        
        # Simple query to verify connectivity
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        
        if result and result[0] == 1:
            logger.info(f"✓ Main database connection successful: {DB_NAME} at {DB_HOST}")
            cursor.close()
            conn.close()
            return (True, None)
        else:
            error_msg = "Main database query returned unexpected result"
            logger.error(f"✗ {error_msg}")
            cursor.close()
            conn.close()
            return (False, error_msg)
            
    except pymssql.Error as e:
        error_msg = f"Main database connection failed: {str(e)}"
        logger.error(f"✗ {error_msg}")
        return (False, error_msg)
    except Exception as e:
        error_msg = f"Unexpected error checking main database: {str(e)}"
        logger.error(f"✗ {error_msg}")
        return (False, error_msg)


def check_tracking_database() -> Tuple[bool, Optional[str]]:
    """
    Check connectivity to the tracking database (pyodbc).
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    if not all([TRACKING_DB_SERVER, TRACKING_DB_DATABASE, TRACKING_DB_USERNAME, TRACKING_DB_PASSWORD]):
        logger.warning("⚠ Tracking database configuration incomplete - skipping connectivity check")
        return (False, "Tracking database configuration incomplete")
    
    try:
        # Build connection string
        connection_string_parts = []
        if TRACKING_DB_DRIVER:
            connection_string_parts.append(f"DRIVER={{{TRACKING_DB_DRIVER}}}")
        if TRACKING_DB_SERVER:
            # Remove tcp: prefix if present (pyodbc handles this automatically)
            server = TRACKING_DB_SERVER.replace("tcp:", "") if TRACKING_DB_SERVER.startswith("tcp:") else TRACKING_DB_SERVER
            connection_string_parts.append(f"SERVER={server}")
        if TRACKING_DB_DATABASE:
            connection_string_parts.append(f"DATABASE={TRACKING_DB_DATABASE}")
        if TRACKING_DB_USERNAME:
            connection_string_parts.append(f"UID={TRACKING_DB_USERNAME}")
        if TRACKING_DB_PASSWORD:
            connection_string_parts.append(f"PWD={TRACKING_DB_PASSWORD}")
        if TRACKING_DB_TRUST_SERVER_CERT:
            connection_string_parts.append("TrustServerCertificate=yes")
        
        connection_string = ";".join(connection_string_parts)
        
        logger.debug(f"Testing tracking database connection: {TRACKING_DB_DATABASE} at {TRACKING_DB_SERVER}")
        conn = pyodbc.connect(connection_string, timeout=10)
        cursor = conn.cursor()
        
        # Simple query to verify connectivity
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        
        if result and result[0] == 1:
            logger.info(f"✓ Tracking database connection successful: {TRACKING_DB_DATABASE} at {TRACKING_DB_SERVER}")
            cursor.close()
            conn.close()
            return (True, None)
        else:
            error_msg = "Tracking database query returned unexpected result"
            logger.error(f"✗ {error_msg}")
            cursor.close()
            conn.close()
            return (False, error_msg)
            
    except pyodbc.Error as e:
        error_msg = f"Tracking database connection failed: {str(e)}"
        logger.error(f"✗ {error_msg}")
        return (False, error_msg)
    except Exception as e:
        error_msg = f"Unexpected error checking tracking database: {str(e)}"
        logger.error(f"✗ {error_msg}")
        return (False, error_msg)


def check_all_databases() -> bool:
    """
    Check connectivity to both databases.
    
    Returns:
        True if both databases are reachable, False otherwise
    """
    logger.info("=" * 80)
    logger.info("Database Connectivity Health Check")
    logger.info("=" * 80)
    
    main_db_ok, main_error = check_main_database()
    tracking_db_ok, tracking_error = check_tracking_database()
    
    logger.info("=" * 80)
    if main_db_ok and tracking_db_ok:
        logger.info("✓ All databases are reachable")
        logger.info("=" * 80)
        return True
    else:
        logger.warning("⚠ Some databases are not reachable:")
        if not main_db_ok:
            logger.warning(f"  - Main database: {main_error}")
        if not tracking_db_ok:
            logger.warning(f"  - Tracking database: {tracking_error}")
        logger.info("=" * 80)
        logger.warning("Application will continue, but some features may not work correctly")
        return False

