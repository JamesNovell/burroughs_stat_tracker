"""
UPS tracking service for querying UPS API and retrieving package tracking status.
"""
import requests
import json
import re
import logging
from typing import Optional
from app.config import UPS_CLIENT_ID, UPS_CLIENT_SECRET

logger = logging.getLogger(__name__)

# API endpoints
TOKEN_URL = "https://onlinetools.ups.com/security/v1/oauth/token"
TRACK_URL = "https://onlinetools.ups.com/api/track/v1/details"


def is_ups_tracking_number(tracking_number: str) -> bool:
    """
    Check if a tracking number appears to be a UPS tracking number.
    
    UPS tracking numbers are:
    - 18 characters total
    - Start with "1Z" followed by 16 alphanumeric characters
    - Format: 1Z[16 alphanumeric]
    
    Args:
        tracking_number: The tracking number to check
        
    Returns:
        True if it appears to be a UPS tracking number, False otherwise
    """
    if not tracking_number or tracking_number == 'not available yet':
        return False
    
    # Remove any whitespace and convert to uppercase
    tracking_number = tracking_number.strip().upper().replace(' ', '').replace('-', '')
    
    # UPS pattern: 1Z followed by exactly 16 alphanumeric characters
    ups_pattern = r'\b1Z[0-9A-Z]{16}\b'
    return bool(re.match(ups_pattern, tracking_number))


def get_access_token() -> Optional[str]:
    """
    Get OAuth access token from UPS API.
    
    Returns:
        Access token string or None if authentication fails
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-merchant-id": "string"
    }
    
    data = {
        "grant_type": "client_credentials"
    }
    
    try:
        response = requests.post(
            TOKEN_URL,
            headers=headers,
            data=data,
            auth=(UPS_CLIENT_ID, UPS_CLIENT_SECRET),
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting UPS access token: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Status Code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing UPS token response: {e}")
        return None


def track_package(tracking_number: str, access_token: str) -> Optional[dict]:
    """
    Track a package using UPS API.
    
    Args:
        tracking_number: The tracking number to track
        access_token: OAuth access token
        
    Returns:
        Tracking data dictionary or None if tracking fails
    """
    url = f"{TRACK_URL}/{tracking_number}"
    
    query = {
        "locale": "en_US",
        "returnSignature": "false",
        "returnMilestones": "false",
        "returnPOD": "false"
    }
    
    headers = {
        "transId": "string",
        "transactionSrc": "PackageTracker",
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, params=query, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error tracking UPS package {tracking_number}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Status Code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing UPS tracking response: {e}")
        return None


def extract_tracking_status(tracking_data: dict) -> Optional[str]:
    """
    Extract the latest tracking status from UPS API response.
    
    Args:
        tracking_data: The JSON response from UPS API
        
    Returns:
        Status description string or None if not available
    """
    if not tracking_data:
        return None
    
    # Check for errors in response
    if 'errors' in tracking_data and tracking_data['errors']:
        error = tracking_data['errors'][0]
        error_message = error.get('message', 'Unknown error')
        error_code = error.get('code', '')
        if error_code:
            return f"Error: {error_message} ({error_code})"
        return f"Error: {error_message}"
    
    # Extract status from tracking results
    if 'trackResponse' in tracking_data:
        track_response = tracking_data['trackResponse']
        
        # Check for shipment
        if 'shipment' in track_response and track_response['shipment']:
            shipment = track_response['shipment'][0]
            
            # Get package
            if 'package' in shipment and shipment['package']:
                package = shipment['package'][0]
                
                # Get activity (latest status)
                if 'activity' in package and package['activity']:
                    latest_activity = package['activity'][0]  # Most recent activity
                    
                    # Get status description
                    if 'status' in latest_activity:
                        status = latest_activity['status']
                        description = status.get('description', '')
                        code = status.get('code', '')
                        
                        if description:
                            if code:
                                return f"{description} ({code})"
                            return description
                    
                    # Fallback to activity description
                    if 'description' in latest_activity:
                        return latest_activity['description']
                
                # Check for delivery information
                if 'deliveryDate' in package:
                    delivery_date = package['deliveryDate']
                    if delivery_date:
                        return f"Delivered on {delivery_date}"
                
                # Check for current status
                if 'currentStatus' in package:
                    current_status = package['currentStatus']
                    if 'description' in current_status:
                        return current_status['description']
    
    return None


def get_ups_tracking_status(tracking_number: str) -> Optional[str]:
    """
    Get tracking status for a UPS tracking number.
    
    Args:
        tracking_number: The UPS tracking number
        
    Returns:
        Status description string or None if tracking fails
    """
    if not is_ups_tracking_number(tracking_number):
        return None
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        logger.warning(f"Failed to authenticate with UPS API for tracking {tracking_number}")
        return None
    
    # Track the package
    tracking_data = track_package(tracking_number, access_token)
    if not tracking_data:
        logger.warning(f"Failed to retrieve tracking data for {tracking_number}")
        return None
    
    # Extract status
    status = extract_tracking_status(tracking_data)
    return status

