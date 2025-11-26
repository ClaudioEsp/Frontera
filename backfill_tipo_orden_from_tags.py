import os
import logging
from typing import List, Optional

from pymongo import MongoClient

logger = logging.getLogger("job.backfill_tipo_orden_from_tags")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "dispatches")


def extract_tipo_orden(tags: List[dict]) -> Optional[str]:
    """
    Given the tags array inside dispatch_raw, return TIPO_ORDEN value
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
    Backfill tipo_orden for all dispatches:

      - Reads tag 'TIPO_ORDEN' from tags
      - Sets tipo_orden if valid
      - Only touches docs where tipo_orden does NOT exist.
    """
    logger.info(
        "Starting job: backfill_tipo_orden_from_tags "
        "on %s.%s",
        MONGO_DB_NAME,
        MONGO_COLLECTION,
    )

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # Only docs lacking tipo_orden
    docs = col.find(
        {
            "tipo_orden": {"$exists": False},
        },
        {
            "_id": 1,
            "tags": 1,
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
            "Updated %s: TIPO_ORDEN=%s",
            _id,
            tipo_orden_value,
        )
        updated += 1

    mongo.close()

    logger.info(
        "Finished backfill_tipo_orden_from_tags. "
        "Scanned: %d docs — Updated: %d docs.",
        count,
        updated,
    )
    return updated


if __name__ == "__main__":
    run()
