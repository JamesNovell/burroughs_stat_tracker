"""Main entry point for the Burroughs statistics tracker."""
import logging
import sys
from app.utils.logging_config import setup_logging
from app.utils.db_health_check import check_all_databases
from app.controllers import poll_for_batches

# Configure logging
setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting Burroughs Statistics Tracker")
    logger.info("=" * 80)
    
    # Check database connectivity before starting
    databases_ok = check_all_databases()
    
    if not databases_ok:
        logger.warning("Proceeding with startup despite database connectivity issues...")
        logger.warning("Some features may not work correctly until databases are reachable")
    
    # Run in polling mode by default (uses POLL_INTERVAL_MINUTES from config.json)
    poll_for_batches()

