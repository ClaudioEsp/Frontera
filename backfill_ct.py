import os
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timedelta, timezone  # <-- NUEVO

from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logger = logging.getLogger("job.get_ct")
logging.basicConfig(level=logging.INFO)

# MongoDB connection and configuration
MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("MONGO_DB_NAME", "FRONTERA")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")
CT_COLLECTION = os.getenv("CT_COLLECTION", "CTS")

# Ventana de horas para considerar "recientes"
SYNC_WINDOW_HOURS = int(os.getenv("SYNC_WINDOW_HOURS", "6"))  # p.ej. últimas 6 horas


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
    Match CT for recent dispatches missing `ct`:

       - Only dispatches:
           * ct is None or does not exist
           * AND sync_timestamp within the last SYNC_WINDOW_HOURS
       
       - tags[*].name == "CODCOMU"
           → extract tag.value = external_id
       - external_id matches CT_DB["Id Externo"]
       - dispatch.ct = CT_DB["CT CORRESPONDE"]
    """
    logger.info(
        "Starting get_ct (recent only): dispatch_db=%s, ct_db=%s.%s, window=%d hours",
        DATABASE,
        DATABASE,
        CT_COLLECTION,
        SYNC_WINDOW_HOURS,
    )

    client = MongoClient(MONGO_URI)

    disp_col = client[DATABASE][DISPATCHES_COLLECTION]
    ct_col = client[DATABASE][CT_COLLECTION]

    # Umbral de tiempo para considerar "reciente"
    now_utc = datetime.now(timezone.utc)
    threshold_dt = now_utc - timedelta(hours=SYNC_WINDOW_HOURS)
    threshold_iso = threshold_dt.isoformat()

    # Dispatches missing CT AND with recent sync_timestamp
    cursor = disp_col.find(
        {
            "$or": [
                {"ct": None},
                {"ct": {"$exists": False}},
            ],
            "sync_timestamp": {"$gte": threshold_iso},
        }
    ).batch_size(100)  # Batch size for better performance

    total = 0
    updated = 0
    no_codcomu = 0
    not_found = 0
    batch_counter = 0

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

        # Track progress every 100 batches for logging purposes
        batch_counter += 1
        if batch_counter >= 100:
            logger.info("Processed %d dispatches so far...", total)
            batch_counter = 0

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
