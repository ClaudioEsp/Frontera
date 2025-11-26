import os
import logging
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logger = logging.getLogger("fetch_dispatches_by_dates")
logging.basicConfig(level=logging.INFO)

# API URL and token from the environment
BASE_URL = "https://paris.dispatchtrack.com/api/external/v1/dispatches"
X_AUTH_TOKEN = os.getenv("DISPATCHTRACK_TOKEN")

if not X_AUTH_TOKEN:
    raise ValueError("X_AUTH_TOKEN is not set in the environment")

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

# MongoDB client setup
client = MongoClient(MONGO_URI)
db = client[DISPATCHTRACK_DB]
dispatches_col = db[DISPATCHES_COLLECTION]

# Fetch dispatches by date range with pagination
def fetch_dispatches_by_dates(start_date: str, end_date: str):
    """
    Fetch dispatches for a specific date range.
    This will handle pagination and continue fetching until no more results are returned.
    Start and end dates should be in the format YYYY-MM-DD.
    """
    page = 1
    while True:
        url = f"{BASE_URL}?s={start_date}&e={end_date}&page={page}"
        
        headers = {
            "X-AUTH-TOKEN": X_AUTH_TOKEN
        }

        # Make the API request
        logger.info(f"Fetching dispatches for dates between {start_date} and {end_date}, page={page}...")
        
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            response_dispatches = data.get('response', [])
            
            if not response_dispatches:
                logger.info(f"No more dispatches found for dates {start_date} to {end_date}, page={page}.")
                break

            logger.info(f"Successfully fetched {len(response_dispatches)} dispatches for dates {start_date} to {end_date}, page={page}.")

            # Save each dispatch immediately after fetching it
            for dispatch in response_dispatches:
                save_dispatch_to_mongo(dispatch)  # Save to MongoDB as soon as the dispatch is fetched

            page += 1  # Move to the next page
        else:
            logger.error(f"Failed to fetch dispatches. Status code: {response.status_code}, {response.text}")
            break


# Save dispatch to MongoDB
def save_dispatch_to_mongo(dispatch: dict):
    """
    Save or update the dispatch in MongoDB.
    """
    dispatch_id = dispatch.get("identifier")

    # Check if the dispatch already exists in the database
    existing_dispatch = dispatches_col.find_one({"identifier": dispatch_id})
    
    if existing_dispatch:
        logger.info(f"Dispatch {dispatch_id} already exists in the database. Skipping update.")
        return  # Skip this dispatch if it already exists

    # Create the document to save (add the dispatch_raw data and other relevant fields)
    dispatch_doc = {
        "dispatch_id": dispatch.get("dispatch_id"),
        "identifier": dispatch.get("identifier"),
        "status": dispatch.get("status"),
        "contact_name": dispatch.get("contact_name"),
        "contact_address": dispatch.get("contact_address"),
        "contact_phone": dispatch.get("contact_phone"),
        "contact_email": dispatch.get("contact_email"),
        "latitude": dispatch.get("latitude"),
        "longitude": dispatch.get("longitude"),
        "route_id": dispatch.get("route_id"),
        "substatus": dispatch.get("substatus"),
        "substatus_code": dispatch.get("substatus_code"),
        "tags": dispatch.get("tags"),
        "is_trunk": dispatch.get("is_trunk"),
        "is_pickup": dispatch.get("is_pickup"),
        "delivered_in_client": dispatch.get("delivered_in_client"),
        "arrived_at": dispatch.get("arrived_at"),
        "estimated_at": dispatch.get("estimated_at"),
        "min_delivery_time": dispatch.get("min_delivery_time"),
        "max_delivery_time": dispatch.get("max_delivery_time"),
        "beecode": dispatch.get("beecode"),
        "locked": dispatch.get("locked"),
        "end_type": dispatch.get("end_type"),
        "number_of_retries": dispatch.get("number_of_retries"),
        "min_age_required": dispatch.get("min_age_required"),
        "address_reference": dispatch.get("address_reference"),
        "pickup_address_reference": dispatch.get("pickup_address_reference"),
        "to_be_payed": dispatch.get("to_be_payed"),
        "external_pincode": dispatch.get("external_pincode"),
        "items": dispatch.get("items"),
        "last_refreshed_at": dispatch.get("last_refreshed_at")
    }

    # Insert the dispatch into MongoDB (this will add new ones or update existing ones)
    dispatches_col.update_one(
        {"identifier": dispatch_id},  # Search by dispatch identifier
        {"$set": dispatch_doc},       # Set the dispatch fields
        upsert=True                    # If not found, insert a new document
    )

    logger.info(f"Dispatch {dispatch_id} saved/updated in MongoDB.")


# Example usage: fetching dispatches for a specific date range
if __name__ == "__main__":
    start_date = "2025-11-26"
    end_date = "2025-11-27"
    
    fetch_dispatches_by_dates(start_date, end_date)
