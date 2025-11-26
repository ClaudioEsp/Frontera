# orchestrator/jobs/backfill_promise_date_from_tags.py

import os
import logging
from typing import List, Optional

from pymongo import MongoClient

logger = logging.getLogger("job.backfill_promise_date_from_tags")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "dispatches")


def extract_fecsoldes(tags: List[dict]) -> Optional[str]:
    """
    Given the tags array inside dispatch_raw, return FECSOLDES value (YYYYMMDD)
    or None if not found.
    """
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if isinstance(tag, dict) and tag.get("name") == "FECSOLDES":
            return tag.get("value")

    return None


def normalize_promise_date(raw: str) -> Optional[str]:
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


def run(route_key: str) -> int:
    """
    Backfill promise_date for dispatches of a specific route (route_key):

      - Reads tag 'FECSOLDES' from dispatch_raw.tags
      - If valid (YYYYMMDD), sets:
          promise_date_raw = FECSOLDES
          promise_date     = YYYY-MM-DD
      - Only touches docs where:
          route_key == route_key AND promise_date does NOT exist.
    """
    logger.info(
        "Starting job: backfill_promise_date_from_tags "
        "for route_key=%s on %s.%s",
        route_key,
        MONGO_DB_NAME,
        MONGO_COLLECTION,
    )

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # Only docs for this route_key lacking promise_date
    docs = col.find(
        {
            "route_key": route_key,
            "promise_date": {"$exists": False},
        },
        {
            "_id": 1,
            "dispatch_raw.tags": 1,
        },
    )

    count = 0
    updated = 0

    for doc in docs:
        count += 1

        _id = doc.get("_id")
        tags = doc.get("dispatch_raw", {}).get("tags", [])

        raw_fecsoldes = extract_fecsoldes(tags)
        promise_date = normalize_promise_date(raw_fecsoldes)

        if raw_fecsoldes is None:
            logger.info("%s: FECSOLDES not found → skipped", _id)
            continue

        if promise_date is None:
            logger.info(
                "%s: FECSOLDES invalid format '%s' → skipped",
                _id,
                raw_fecsoldes,
            )
            continue

        col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "promise_date_raw": raw_fecsoldes,
                    "promise_date": promise_date,
                }
            },
        )

        logger.info(
            "Updated %s: FECSOLDES=%s → %s",
            _id,
            raw_fecsoldes,
            promise_date,
        )
        updated += 1

    mongo.close()

    logger.info(
        "Finished backfill_promise_date_from_tags for route_key=%s. "
        "Scanned: %d docs — Updated: %d docs.",
        route_key,
        count,
        updated,
    )
    return updated


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill promise_date from FECSOLDES tag for dispatches of a route."
    )
    parser.add_argument(
        "--route-key",
        required=True,
        help="Route identifier (route_key) whose dispatches should be updated.",
    )
    args = parser.parse_args()

    run(args.route_key)
