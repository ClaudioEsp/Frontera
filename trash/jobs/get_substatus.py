# orchestrator/jobs/get_substatus.py

import os
import logging
import math
from typing import Any, Dict, Optional, Set

from pymongo import MongoClient
from dotenv import load_dotenv

# Load env (.env at project root)
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_substatus")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

SUB_STATUS_DATABASE = os.getenv("SUB_STATUS_DATABASE", "substatus_db")
SUB_STATUS_COLLECTION = os.getenv("SUB_STATUS_COLLECTION", "substatus_collection")


def _is_bad_number(value: Any) -> bool:
    """
    Returns True if value is NaN or infinite (float('nan'), inf, -inf).
    """
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    return False


def _normalize_code(code: Any) -> Optional[str]:
    """
    Normalize substatus_code to a canonical string.

    Returns:
        - normalized string, or
        - None if the code is invalid/empty and should be treated as "no code".
    """
    if code is None:
        return None

    if _is_bad_number(code):
        return None

    s = str(code).strip()
    if s == "" or s.lower() == "nan":
        return None

    return s


def _code_variants(code: Any) -> Optional[Set[Any]]:
    """
    Build possible representations for the lookup in substatus collection.

    Example:
      "1"   -> {"1", 1}
      1     -> {"1", 1}
      "001" -> {"001", "1", 1}
      "abc" -> {"abc"}
    """
    norm = _normalize_code(code)
    if norm is None:
        return None

    variants: Set[Any] = set()

    # canonical string
    variants.add(norm)

    # numeric version (if all digits)
    if norm.isdigit():
        variants.add(int(norm))

    return variants


def _lookup_substatus(sub_col, code: Any) -> Optional[Dict[str, Any]]:
    """
    Look up mapping where "Código Sub" equals dispatch.substatus_code,
    using only the code (no other fields).
    """
    variants = _code_variants(code)
    if not variants:
        return None

    mapping = sub_col.find_one(
        {"Código Sub": {"$in": list(variants)}},
        projection={
            "_id": False,
            "Código Sub": True,
            "Estado Beetrack": True,
            "Estado Guía": True,
            "Cierre": True,
        },
    )
    return mapping


def run(route_key: str) -> None:
    """
    For each dispatch of a given route (route_key):

    - If substatus_code is null/empty/invalid:
         set estado_beetrack = null,
             estado_guia     = null,
             cierre          = null

    - If substatus_code is valid:
         look for SUB_STATUS_COLLECTION["Código Sub"] == substatus_code
         and set:
             estado_beetrack = "Estado Beetrack"
             estado_guia     = "Estado Guía"
             cierre          = "Cierre"

    Only substatus_code is used to decide what to write.
    """

    logger.info(
        "Starting get_substatus (pure mapping from substatus_code) "
        "for route_key=%s  dispatch_db=%s, substatus_db=%s.%s",
        route_key,
        DISPATCHTRACK_DB,
        SUB_STATUS_DATABASE,
        SUB_STATUS_COLLECTION,
    )

    client = MongoClient(MONGO_URI)
    disp_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    sub_col = client[SUB_STATUS_DATABASE][SUB_STATUS_COLLECTION]

    # Only dispatches for this route_key
    cursor = disp_col.find({"route_key": route_key})

    total = 0
    null_or_invalid_code = 0
    mapped = 0
    unmatched = 0

    for disp in cursor:
        total += 1
        code = disp.get("substatus_code", None)
        norm = _normalize_code(code)

        # Case 1: no usable code -> force estados to null
        if norm is None:
            update_fields = {
                "estado_beetrack": None,
                "estado_guia": None,
                "cierre": None,
            }
            disp_col.update_one({"_id": disp["_id"]}, {"$set": update_fields})
            null_or_invalid_code += 1
            continue

        # Case 2: valid code -> lookup in substatus collection
        mapping = _lookup_substatus(sub_col, code)

        if mapping:
            update_fields = {
                "estado_beetrack": mapping.get("Estado Beetrack"),
                "estado_guia": mapping.get("Estado Guía"),
                "cierre": mapping.get("Cierre"),
            }
            mapped += 1
        else:
            # If there is a code but no mapping, we still want to avoid old wrong values
            update_fields = {
                "estado_beetrack": None,
                "estado_guia": None,
                "cierre": None,
            }
            unmatched += 1
            if unmatched <= 5:
                logger.warning(
                    "No substatus mapping for substatus_code=%r (dispatch_id=%s, route_key=%s)",
                    code,
                    disp.get("_id"),
                    route_key,
                )

        disp_col.update_one({"_id": disp["_id"]}, {"$set": update_fields})

    client.close()

    logger.info(
        "get_substatus finished for route_key=%s. "
        "Processed=%d  Mapped=%d  Null_or_invalid_code=%d  Unmatched_with_code=%d",
        route_key,
        total,
        mapped,
        null_or_invalid_code,
        unmatched,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Map substatus_code -> estados/cierre for dispatches of a route."
    )
    parser.add_argument(
        "--route-key",
        required=True,
        help="Route identifier (route_key) whose dispatches should be updated.",
    )
    args = parser.parse_args()

    run(args.route_key)
