import os
import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone  # <-- NUEVO

from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from the .env file located one folder above the current directory
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

logger = logging.getLogger("job.backfill_tipo_orden_from_tags")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "FRONTERA")
MONGO_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")

# Ventana de horas para considerar "recientes"
SYNC_WINDOW_HOURS = int(os.getenv("SYNC_WINDOW_HOURS", "6"))  # por defecto, últimas 6 horas


def extract_tipo_orden(tags: List[dict]) -> Optional[str]:
    """
    Given the tags array inside dispatch document, return TIPO_ORDEN value
    or None if not found.
    """
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if isinstance(tag, dict) and tag.get("name") == "TIPO_ORDEN":
            return tag.get("value")

    return None


def run() -> int:
    """
    Backfill tipo_orden ONLY for recent dispatches:

      - Docs where:
          * tipo_orden does NOT exist
          * AND sync_timestamp is within the last SYNC_WINDOW_HOURS
      - Reads tag 'TIPO_ORDEN' from tags
      - Sets tipo_orden if found
    """
    logger.info(
        "Starting job: backfill_tipo_orden_from_tags (recent only) "
        "on %s.%s (window: last %d hours)",
        MONGO_DB_NAME,
        MONGO_COLLECTION,
        SYNC_WINDOW_HOURS,
    )

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # Umbral para considerar "reciente"
    now_utc = datetime.now(timezone.utc)
    threshold_dt = now_utc - timedelta(hours=SYNC_WINDOW_HOURS)
    threshold_iso = threshold_dt.isoformat()

    # Only docs lacking tipo_orden AND with recent sync_timestamp
    docs = col.find(
        {
            "tipo_orden": {"$exists": False},
            "sync_timestamp": {"$gte": threshold_iso},
        },
        {
            "_id": 1,
            "tags": 1,
            "sync_timestamp": 1,
        },
    )

    count = 0
    updated = 0

    for doc in docs:
        count += 1

        _id = doc.get("_id")
        tags = doc.get("tags", [])

        tipo_orden_value = extract_tipo_orden(tags)

        if tipo_orden_value is None:
            logger.info("%s: TIPO_ORDEN not found → skipped", _id)
            continue

        col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "tipo_orden": tipo_orden_value,
                }
            },
        )

        logger.info(
            "Updated %s: TIPO_ORDEN=%s (sync_timestamp=%s)",
            _id,
            tipo_orden_value,
            doc.get("sync_timestamp"),
        )
        updated += 1

    mongo.close()

    logger.info(
        "Finished backfill_tipo_orden_from_tags (recent only). "
        "Scanned: %d docs — Updated: %d docs.",
        count,
        updated,
    )
    return updated


if __name__ == "__main__":
    run()
