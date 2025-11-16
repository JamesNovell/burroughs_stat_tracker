"""Configuration management for the stat tracker."""
import os
import json


def load_config():
    """Load configuration from config.json file."""
    # Get the project root directory (parent of app/)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(project_root, "config.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_db_fields = ["host", "user", "password"]
    db_config = config.get("database", {})
    
    for field in required_db_fields:
        if not db_config.get(field):
            raise ValueError(f"Missing required database configuration: {field}")
    
    return config


# Load configuration
try:
    CONFIG = load_config()
    DB_HOST = CONFIG["database"]["host"]
    DB_USER = CONFIG["database"]["user"]
    DB_PASSWORD = CONFIG["database"]["password"]
    DB_NAME = CONFIG["database"].get("name", "master")
    SOURCE_TABLE = CONFIG["tables"].get("source_table", "dbo.Burroughs_Open_Calls")
    RECYCLERS_STAT_TABLE = CONFIG["tables"]["recyclers"].get("stat_table", "Burroughs_Recyclers_Stat")
    RECYCLERS_HISTORY_TABLE = CONFIG["tables"]["recyclers"].get("history_table", "Burroughs_Recyclers_Closed_Call_History")
    RECYCLERS_HOURLY_TABLE = CONFIG["tables"]["recyclers"].get("hourly_table", "Burroughs_Recyclers_Hourly_Stat")
    RECYCLERS_DAILY_TABLE = CONFIG["tables"]["recyclers"].get("daily_table", "Burroughs_Recyclers_Daily_Summary")
    SMART_SAFES_STAT_TABLE = CONFIG["tables"]["smart_safes"].get("stat_table", "Burroughs_Smart_Safes_Stat")
    SMART_SAFES_HISTORY_TABLE = CONFIG["tables"]["smart_safes"].get("history_table", "Burroughs_Smart_Safes_Closed_Call_History")
    SMART_SAFES_HOURLY_TABLE = CONFIG["tables"]["smart_safes"].get("hourly_table", "Burroughs_Smart_Safes_Hourly_Stat")
    SMART_SAFES_DAILY_TABLE = CONFIG["tables"]["smart_safes"].get("daily_table", "Burroughs_Smart_Safes_Daily_Summary")
    
    # Polling configuration
    POLL_INTERVAL_MINUTES = CONFIG.get("polling", {}).get("interval_minutes", 5)
    
    # Daily summary configuration
    EOD_HOUR = CONFIG.get("daily_summary", {}).get("eod_hour", 23)
    EOD_MINUTE = CONFIG.get("daily_summary", {}).get("eod_minute", 59)
    
    # Tracking database configuration
    TRACKING_DB_CONFIG = CONFIG.get("tracking_database", {})
    TRACKING_DB_DRIVER = TRACKING_DB_CONFIG.get("driver", "ODBC Driver 18 for SQL Server")
    TRACKING_DB_SERVER = TRACKING_DB_CONFIG.get("server", "")
    TRACKING_DB_DATABASE = TRACKING_DB_CONFIG.get("database", "")
    TRACKING_DB_USERNAME = TRACKING_DB_CONFIG.get("username", "")
    TRACKING_DB_PASSWORD = TRACKING_DB_CONFIG.get("password", "")
    TRACKING_DB_TRUST_SERVER_CERT = TRACKING_DB_CONFIG.get("trust_server_certificate", True)
    
    # Aggregation configuration
    AGGREGATION_CONFIG = CONFIG.get("aggregation", {})
    HOURLY_AGGREGATION_ENABLED = AGGREGATION_CONFIG.get("hourly", {}).get("enabled", True)
    HOURLY_TRIGGER_ON_BOUNDARY = AGGREGATION_CONFIG.get("hourly", {}).get("trigger_on_hour_boundary", True)
    HOURLY_VALIDATION_ENABLED = AGGREGATION_CONFIG.get("hourly", {}).get("validation_enabled", True)
    DAILY_AGGREGATION_ENABLED = AGGREGATION_CONFIG.get("daily", {}).get("enabled", True)
    DAILY_AGGREGATE_FROM = AGGREGATION_CONFIG.get("daily", {}).get("aggregate_from", "hourly")
    
    # FedEx API configuration
    FEDEX_API_CONFIG = CONFIG.get("fedex_api", {})
    FEDEX_API_KEY = FEDEX_API_CONFIG.get("api_key", "")
    FEDEX_API_SECRET = FEDEX_API_CONFIG.get("api_secret", "")
    FEDEX_USE_PRODUCTION = FEDEX_API_CONFIG.get("use_production", True)
    
    # UPS API configuration
    UPS_API_CONFIG = CONFIG.get("ups_api", {})
    UPS_CLIENT_ID = UPS_API_CONFIG.get("client_id", "")
    UPS_CLIENT_SECRET = UPS_API_CONFIG.get("client_secret", "")
except (FileNotFoundError, ValueError, KeyError) as e:
    print(f"Configuration error: {e}")
    print("Please ensure config.json exists and contains all required fields.")
    raise

