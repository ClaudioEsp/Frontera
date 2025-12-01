import os
import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from the .env file located one folder above the current directory
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

logger = logging.getLogger("job.backfill_compromise_date_from_tags")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "FRONTERA")
MONGO_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")

# Ventana de horas para considerar "recientes"
SYNC_WINDOW_HOURS = int(os.getenv("SYNC_WINDOW_HOURS", "6"))  # p.ej. últimas 6 horas

# Batch size for processing
BATCH_SIZE = 100

def extract_fecsoldes(tags: List[dict]) -> Optional[str]:
    """
    Given the tags array, return FECSOLDES value (YYYYMMDD)
    or None if not found.
    """
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if isinstance(tag, dict) and tag.get("name") == "FECSOLDES":
            return tag.get("value")

    return None


def normalize_compromise_date(raw: str) -> Optional[str]:
    """
    Convert YYYYMMDD → YYYY-MM-DD.
    Return None if format invalid.
    """
    if not raw:
        return None

    s = str(raw).strip()
    if len(s) != 8 or not s.isdigit():
        return None

    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def process_batch(docs, col):
    """
    Process a batch of documents to update their compromise_date.
    The 'col' parameter is the MongoDB collection used to perform updates.
    """
    updated = 0
    for doc in docs:
        _id = doc.get("_id")
        tags = doc.get("tags", [])

        raw_fecsoldes = extract_fecsoldes(tags)
        compromise_date = normalize_compromise_date(raw_fecsoldes)

        if raw_fecsoldes is None:
            logger.info("%s: FECSOLDES not found → skipped", _id)
            continue

        if compromise_date is None:
            logger.info(
                "%s: FECSOLDES invalid format '%s' → skipped",
                _id,
                raw_fecsoldes,
            )
            continue

        # Update document in MongoDB
        col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "compromise_date_raw": raw_fecsoldes,
                    "compromise_date": compromise_date,
                }
            },
        )

        logger.info(
            "Updated %s: FECSOLDES=%s → %s (sync_timestamp=%s)",
            _id,
            raw_fecsoldes,
            compromise_date,
            doc.get("sync_timestamp"),
        )
        updated += 1

    return updated


def run() -> int:
    """
    Backfill compromise_date ONLY for recent dispatches in batches.
    """
    logger.info(
        "Starting job: backfill_compromise_date_from_tags "
        "on %s.%s (window: last %d hours)",
        MONGO_DB_NAME,
        MONGO_COLLECTION,
        SYNC_WINDOW_HOURS,
    )

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # Umbral de tiempo para considerar "reciente"
    now_utc = datetime.now(timezone.utc)
    threshold_dt = now_utc - timedelta(hours=SYNC_WINDOW_HOURS)
    threshold_iso = threshold_dt.isoformat()

    # Query for documents without 'compromise_date' and within the sync window
    query = {
        "compromise_date": {"$exists": False},
        "sync_timestamp": {"$gte": threshold_iso},
    }

    # Initializing count and updated counters
    count = 0
    updated = 0

    # Batch processing loop
    while True:
        # Get a batch of documents
        docs = list(col.find(query, {"_id": 1, "tags": 1, "sync_timestamp": 1}).batch_size(BATCH_SIZE))

        if not docs:
            break  # Exit loop if no more documents to process

        count += len(docs)
        updated += process_batch(docs, col)

    mongo.close()

    logger.info(
        "Finished backfill_compromise_date_from_tags. "
        "Scanned: %d docs — Updated: %d docs.",
        count,
        updated,
    )
    return updated


if __name__ == "__main__":
    run()
