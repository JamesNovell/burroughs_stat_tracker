"""Configuration management for the stat tracker."""
import os
import json


def load_config():
    """Load configuration from config.json file."""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    
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
    RECYCLERS_DAILY_TABLE = CONFIG["tables"]["recyclers"].get("daily_table", "Burroughs_Recyclers_Daily_Summary")
    SMART_SAFES_STAT_TABLE = CONFIG["tables"]["smart_safes"].get("stat_table", "Burroughs_Smart_Safes_Stat")
    SMART_SAFES_HISTORY_TABLE = CONFIG["tables"]["smart_safes"].get("history_table", "Burroughs_Smart_Safes_Closed_Call_History")
    SMART_SAFES_DAILY_TABLE = CONFIG["tables"]["smart_safes"].get("daily_table", "Burroughs_Smart_Safes_Daily_Summary")
    
    # Polling configuration
    POLL_INTERVAL_MINUTES = CONFIG.get("polling", {}).get("interval_minutes", 5)
    
    # Daily summary configuration
    EOD_HOUR = CONFIG.get("daily_summary", {}).get("eod_hour", 23)
    EOD_MINUTE = CONFIG.get("daily_summary", {}).get("eod_minute", 59)
except (FileNotFoundError, ValueError, KeyError) as e:
    print(f"Configuration error: {e}")
    print("Please ensure config.json exists and contains all required fields.")
    raise

