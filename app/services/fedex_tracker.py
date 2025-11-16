"""
FedEx tracking service for querying FedEx API and retrieving package tracking status.
"""
import requests
import json
import re
import logging
from typing import Optional
from app.config import FEDEX_API_KEY, FEDEX_API_SECRET, FEDEX_USE_PRODUCTION

logger = logging.getLogger(__name__)

# API endpoints
TOKEN_URL_PRODUCTION = "https://apis.fedex.com/oauth/token"
TRACK_URL_PRODUCTION = "https://apis.fedex.com/track/v1/trackingnumbers"
TOKEN_URL_SANDBOX = "https://apis-sandbox.fedex.com/oauth/token"
TRACK_URL_SANDBOX = "https://apis-sandbox.fedex.com/track/v1/trackingnumbers"


def is_fedex_tracking_number(tracking_number: str) -> bool:
    """
    Check if a tracking number appears to be a FedEx tracking number.
    
    FedEx tracking numbers can be:
    - 12 digits (Ground)
    - 15 digits (Express)
    - Alphanumeric (some formats)
    
    Args:
        tracking_number: The tracking number to check
        
    Returns:
        True if it appears to be a FedEx tracking number, False otherwise
    """
    if not tracking_number or tracking_number == 'not available yet':
        return False
    
    # Remove any whitespace
    tracking_number = tracking_number.strip()
    
    # Check for 12 or 15 digit numbers (common FedEx formats)
    if re.match(r'^\d{12}$', tracking_number) or re.match(r'^\d{15}$', tracking_number):
        return True
    
    # Check for alphanumeric patterns (some FedEx formats)
    # FedEx Express tracking numbers often start with specific patterns
    if re.match(r'^[0-9]{4}[0-9]{4}[0-9]{4}$', tracking_number.replace('-', '').replace(' ', '')):
        return True
    
    # Additional patterns for FedEx tracking numbers
    # Some formats include letters
    if re.match(r'^[0-9A-Z]{12,15}$', tracking_number.replace('-', '').replace(' ', '')):
        return True
    
    return False


def get_access_token() -> Optional[str]:
    """
    Get OAuth access token from FedEx API.
    
    Returns:
        Access token string or None if authentication fails
    """
    token_url = TOKEN_URL_PRODUCTION if FEDEX_USE_PRODUCTION else TOKEN_URL_SANDBOX
    
    payload = f'grant_type=client_credentials&client_id={FEDEX_API_KEY}&client_secret={FEDEX_API_SECRET}'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(token_url, data=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting FedEx access token: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Status Code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing FedEx token response: {e}")
        return None


def track_package(tracking_number: str, access_token: str) -> Optional[dict]:
    """
    Track a package using FedEx API.
    
    Args:
        tracking_number: The tracking number to track
        access_token: OAuth access token
        
    Returns:
        Tracking data dictionary or None if tracking fails
    """
    track_url = TRACK_URL_PRODUCTION if FEDEX_USE_PRODUCTION else TRACK_URL_SANDBOX
    
    headers = {
        'Content-Type': 'application/json',
        'X-locale': 'en_US',
        'Authorization': f'Bearer {access_token}'
    }
    
    payload = {
        "includeDetailedScans": True,
        "trackingInfo": [
            {
                "trackingNumberInfo": {
                    "trackingNumber": tracking_number
                }
            }
        ]
    }
    
    try:
        response = requests.post(track_url, data=json.dumps(payload), headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error tracking FedEx package {tracking_number}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Status Code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing FedEx tracking response: {e}")
        return None


def extract_tracking_status(tracking_data: dict) -> Optional[str]:
    """
    Extract the latest tracking status from FedEx API response.
    
    Args:
        tracking_data: The JSON response from FedEx API
        
    Returns:
        Status description string or None if not available
    """
    if not tracking_data or 'output' not in tracking_data:
        return None
    
    output = tracking_data['output']
    
    # Check for alerts/errors
    if 'alerts' in output:
        alerts = output['alerts']
        if alerts:
            # Return the first alert message
            if isinstance(alerts, list) and len(alerts) > 0:
                return alerts[0].get('message', 'Alert received')
            elif isinstance(alerts, dict):
                return alerts.get('message', 'Alert received')
        return None
    
    # Extract status from tracking results
    if 'completeTrackResults' in output and output['completeTrackResults']:
        for result in output['completeTrackResults']:
            if 'trackResults' in result:
                for track in result['trackResults']:
                    # Get latest status
                    if 'latestStatusDetail' in track:
                        status = track['latestStatusDetail']
                        description = status.get('description', '')
                        code = status.get('code', '')
                        
                        # Combine description and code if available
                        if description:
                            if code:
                                return f"{description} ({code})"
                            return description
                    
                    # Fallback to scan events if no latest status
                    if 'scanEvents' in track and track['scanEvents']:
                        latest_event = track['scanEvents'][0]  # Most recent event
                        event_desc = latest_event.get('eventDescription', '')
                        if event_desc:
                            return event_desc
    
    return None


def get_fedex_tracking_status(tracking_number: str) -> Optional[str]:
    """
    Get tracking status for a FedEx tracking number.
    
    Args:
        tracking_number: The FedEx tracking number
        
    Returns:
        Status description string or None if tracking fails
    """
    if not is_fedex_tracking_number(tracking_number):
        return None
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        logger.warning(f"Failed to authenticate with FedEx API for tracking {tracking_number}")
        return None
    
    # Track the package
    tracking_data = track_package(tracking_number, access_token)
    if not tracking_data:
        logger.warning(f"Failed to retrieve tracking data for {tracking_number}")
        return None
    
    # Extract status
    status = extract_tracking_status(tracking_data)
    return status

