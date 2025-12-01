import os
import logging
import math
from typing import Any, Dict, Optional, Set
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_substatus")

MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("DATABASE", "FRONTERA")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")
SUB_STATUS_COLLECTION = os.getenv("SUB_STATUS_COLLECTION", "SUB_STATUS")

# Ventana de horas para considerar "recientes"
SYNC_WINDOW_HOURS = int(os.getenv("SYNC_WINDOW_HOURS", "6"))  # e.g. last 6 hours


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


def _extract_codcomu_value(disp_doc: Dict[str, Any]) -> Optional[str]:
    """
    Extract CODCOMU tag value directly from the tags field.

    The list order DOES NOT matter.
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


def run() -> None:
    """
    Process ONLY recent dispatches (sync_timestamp within last SYNC_WINDOW_HOURS),
    in batches so that cursors never live too long.
    """
    logger.info(
        "Starting get_substatus (pure mapping from substatus_code) for recent dispatches "
        "dispatch_db=%s, substatus_db=%s.%s, window=%d hours",
        DATABASE,
        DATABASE,
        SUB_STATUS_COLLECTION,
        SYNC_WINDOW_HOURS,
    )

    client = MongoClient(MONGO_URI)
    disp_col = client[DATABASE][DISPATCHES_COLLECTION]
    sub_col = client[DATABASE][SUB_STATUS_COLLECTION]

    # Umbral de tiempo para considerar "reciente"
    now_utc = datetime.now(timezone.utc)
    threshold_dt = now_utc - timedelta(hours=SYNC_WINDOW_HOURS)
    threshold_iso = threshold_dt.isoformat()

    BATCH_SIZE = 1000  # adjust if needed
    last_id = None

    # Flags for tracking
    total = 0
    null_or_invalid_code = 0
    mapped = 0
    unmatched = 0

    try:
        while True:
            # Base query: only recent ones
            query: Dict[str, Any] = {
                "sync_timestamp": {"$gte": threshold_iso},
            }
            # Move forward by _id to avoid long cursors
            if last_id is not None:
                query["_id"] = {"$gt": last_id}

            cursor = (
                disp_col.find(query)
                .sort("_id", 1)
                .limit(BATCH_SIZE)
            )

            docs = list(cursor)
            if not docs:
                break  # no more recent documents

            for disp in docs:
                last_id = disp["_id"]
                total += 1

                code = disp.get("substatus_code", None)
                norm = _normalize_code(code)

                # Case 1: no usable code -> force states to null
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
                    update_fields = {
                        "estado_beetrack": None,
                        "estado_guia": None,
                        "cierre": None,
                    }
                    unmatched += 1
                    if unmatched <= 5:
                        logger.warning(
                            "No substatus mapping for substatus_code=%r (dispatch_id=%s)",
                            code,
                            disp.get("_id"),
                        )

                disp_col.update_one({"_id": disp["_id"]}, {"$set": update_fields})

            logger.info(
                "Processed so far (recent only): %d documents (last_id=%s)",
                total,
                last_id,
            )

    finally:
        client.close()

    logger.info(
        "get_substatus finished (recent only). "
        "Processed=%d  Mapped=%d  Null_or_invalid_code=%d  Unmatched_with_code=%d",
        total,
        mapped,
        null_or_invalid_code,
        unmatched,
    )


if __name__ == "__main__":
    run()
