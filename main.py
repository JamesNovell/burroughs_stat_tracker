"""Main entry point for the Burroughs statistics tracker."""
from app.controllers import poll_for_batches


if __name__ == "__main__":
    # Run in polling mode by default (uses POLL_INTERVAL_MINUTES from config.json)
    poll_for_batches()

