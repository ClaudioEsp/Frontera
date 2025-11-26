# orchestrator/jobs/get_ct.py

import os
import logging
from typing import Any, Dict, Optional

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_ct")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

CT_DATABASE = os.getenv("CT_DATABASE", "ct_db")
CT_COLLECTION = os.getenv("CT_COLLECTION", "ct_collection")


def _extract_codcomu_value(disp_doc: Dict[str, Any]) -> Optional[str]:
    """
    Extract CODCOMU tag value from dispatch_raw.tags:

      Find tag where tag.name == "CODCOMU" (case-insensitive)
      Return tag.value as string.

    The list order DOES NOT matter.
    """
    raw = disp_doc.get("dispatch_raw") or {}
    tags = raw.get("tags") or raw.get("Tags")

    if not isinstance(tags, list):
        return None

    for tag in tags:
        name = tag.get("name") or tag.get("Name")
        if not name:
            continue

        # normalize name
        if str(name).strip().upper() == "CODCOMU":
            value = tag.get("value") or tag.get("Value")
            if value is None:
                return None
            return str(value).strip()

    return None


def run(route_key: str) -> None:
    """
    Match CT for all dispatches of a given route (route_key):

       dispatch_raw.tags[*].name == "CODCOMU"
           → extract tag.value = external_id

       external_id matches CT_DB["Id Externo"]

       dispatch.ct = CT_DB["CT CORRESPONDE"]

    Only processes dispatches where:
      - route_key == route_key
      - ct is None or does not exist.
    """
    logger.info(
        "Starting get_ct for route_key=%s: dispatch_db=%s, ct_db=%s.%s",
        route_key,
        DISPATCHTRACK_DB,
        CT_DATABASE,
        CT_COLLECTION,
    )

    client = MongoClient(MONGO_URI)

    disp_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    ct_col = client[CT_DATABASE][CT_COLLECTION]

    # Dispatches of this route still missing CT
    cursor = disp_col.find(
        {
            "route_key": route_key,
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
        "get_ct complete for route_key=%s. "
        "Processed=%d, updated=%d, no_CODCOMU=%d, not_found_in_CT=%d",
        route_key,
        total,
        updated,
        no_codcomu,
        not_found,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Match CT for dispatches of a single route."
    )
    parser.add_argument(
        "--route-key",
        required=True,
        help="Route identifier (route_key) whose dispatches should be updated.",
    )
    args = parser.parse_args()

    run(args.route_key)
