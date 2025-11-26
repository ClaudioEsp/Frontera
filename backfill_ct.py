import os
import logging
from typing import Any, Dict, Optional

from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logger = logging.getLogger("job.get_ct")
logging.basicConfig(level=logging.INFO)

# MongoDB connection and configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

CT_DATABASE = os.getenv("CT_DATABASE", "ct_db")
CT_COLLECTION = os.getenv("CT_COLLECTION", "ct_collection")

# Extract CODCOMU tag value from tags field
def _extract_codcomu_value(disp_doc: Dict[str, Any]) -> Optional[str]:
    """
    Extract CODCOMU tag value from tags:

      Find tag where tag.name == "CODCOMU" (case-insensitive)
      Return tag.value as string.
    """
    tags = disp_doc.get("tags")  # Direct access to tags field

    if not isinstance(tags, list):
        return None

    for tag in tags:
        name = tag.get("name") or tag.get("Name")
        if not name:
            continue

        # Normalize name
        if str(name).strip().upper() == "CODCOMU":
            value = tag.get("value") or tag.get("Value")
            if value is None:
                return None
            return str(value).strip()

    return None

# Main function to update CT values for dispatches
def run() -> None:
    """
    Match CT for all dispatches missing `ct`:

       - tags[*].name == "CODCOMU"
           → extract tag.value = external_id
       - external_id matches CT_DB["Id Externo"]
       - dispatch.ct = CT_DB["CT CORRESPONDE"]
       
    Updates all dispatches in the collection with the missing CT values.
    """
    logger.info(
        "Starting get_ct: dispatch_db=%s, ct_db=%s.%s",
        DISPATCHTRACK_DB,
        CT_DATABASE,
        CT_COLLECTION,
    )

    client = MongoClient(MONGO_URI)

    disp_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    ct_col = client[CT_DATABASE][CT_COLLECTION]

    # Dispatches missing CT
    cursor = disp_col.find(
        {
            "$or": [
                {"ct": None},
                {"ct": {"$exists": False}},
            ],
        }
    )

    total = 0
    updated = 0
    no_codcomu = 0
    not_found = 0

    for disp in cursor:
        total += 1

        external_id = _extract_codcomu_value(disp)
        if not external_id:
            no_codcomu += 1
            continue

        # Look up in CT DB
        mapping = ct_col.find_one({"Id Externo": external_id})
        if not mapping:
            not_found += 1
            continue

        ct_value = mapping.get("CT CORRESPONDE")
        if not ct_value:
            not_found += 1
            continue

        ct_str = str(ct_value).strip()

        # Update the dispatch with the corresponding CT value
        disp_col.update_one(
            {"_id": disp["_id"]},
            {
                "$set": {
                    "ct": ct_str,
                    "ct_match_codcomu": external_id,
                }
            },
        )

        updated += 1

    client.close()

    logger.info(
        "get_ct complete. Processed=%d, updated=%d, no_CODCOMU=%d, not_found_in_CT=%d",
        total,
        updated,
        no_codcomu,
        not_found,
    )

# Run the function
if __name__ == "__main__":
    run()
