# orchestrator/jobs/close_route_if_all_dispatches_closed.py

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.close_route_if_all_dispatches_closed")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
ROUTES_COLLECTION = os.getenv("ROUTES_COLLECTION", "routes")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")


def _get_route_payload(route_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prefer full_raw (from /routes/:id) and fall back to minified_raw
    if for some reason full_raw is not present.
    """
    return route_doc.get("full_raw") or route_doc.get("minified_raw") or {}


def _extract_dispatch_ids(route_payload: Dict[str, Any]) -> List[str]:
    """
    Extract the list of dispatch identifiers from the route payload.

    The payload is expected to have a top-level 'dispatches' list with
    each dispatch containing an 'identifier' field.
    """
    dispatches = route_payload.get("dispatches")
    if not isinstance(dispatches, list):
        logger.warning(
            "Route payload does not contain 'dispatches' as a list. Keys=%s",
            list(route_payload.keys()),
        )
        return []

    ids: List[str] = []
    for disp in dispatches:
        ident = disp.get("identifier")
        if ident is None:
            continue
        ids.append(str(ident))

    return ids


def run(route_key: str) -> None:
    """
    For a given route (identified by route_key), check in the dispatches DB
    if *all* of its dispatches have cierre == true.

    If yes:
      - Mark the route as closed in routes collection:
          is_closed = True
          closed_at = now (UTC)

    If any dispatch is:
      - missing in the dispatches collection, or
      - has cierre != True,
      the route remains open (is_closed not set to True).
    """
    logger.info("Starting close-route check for route_key=%s", route_key)

    client = MongoClient(MONGO_URI)
    db = client[DISPATCHTRACK_DB]
    routes_col = db[ROUTES_COLLECTION]
    disp_col = db[DISPATCHES_COLLECTION]

    route_doc = routes_col.find_one({"route_key": route_key})
    if not route_doc:
        logger.warning("No route document found for route_key=%s", route_key)
        client.close()
        return

    # If it's already closed, nothing to do
    if route_doc.get("is_closed") is True:
        logger.info("Route %s is already closed. Skipping.", route_key)
        client.close()
        return

    route_payload = _get_route_payload(route_doc)
    dispatch_ids = _extract_dispatch_ids(route_payload)

    if not dispatch_ids:
        # No dispatches -> we can consider the route closed immediately.
        logger.info(
            "Route %s has no dispatches in payload. Marking as closed.",
            route_key,
        )
        now_utc = datetime.now(timezone.utc)
        routes_col.update_one(
            {"_id": route_doc["_id"]},
            {"$set": {"is_closed": True, "closed_at": now_utc}},
        )
        client.close()
        return

    # Fetch all dispatch docs for those identifiers
    cursor = disp_col.find(
        {"dispatch_key": {"$in": dispatch_ids}},
        projection={"dispatch_key": 1, "cierre": 1},
    )

    found: Dict[str, bool] = {}  # dispatch_key -> cierre_is_true
    for doc in cursor:
        dk = str(doc.get("dispatch_key"))
        cierre_is_true = (doc.get("cierre") is True)
        found[dk] = cierre_is_true

    all_closed = True
    missing_or_open: List[str] = []

    for dk in dispatch_ids:
        if dk not in found:
            all_closed = False
            missing_or_open.append(f"{dk} (missing)")
        elif not found[dk]:
            all_closed = False
            missing_or_open.append(f"{dk} (cierre!=true)")

    if all_closed:
        now_utc = datetime.now(timezone.utc)
        routes_col.update_one(
            {"_id": route_doc["_id"]},
            {"$set": {"is_closed": True, "closed_at": now_utc}},
        )
        logger.info(
            "Route %s: all %d dispatches have cierre==true. Marked as closed.",
            route_key,
            len(dispatch_ids),
        )
    else:
        logger.info(
            "Route %s is still open. Dispatches missing or not closed: %s",
            route_key,
            ", ".join(missing_or_open),
        )

    client.close()
    logger.info("Finished close-route check for route_key=%s", route_key)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Check if all dispatches of a route are closed and mark route closed."
    )
    parser.add_argument("--route-key", required=True, help="Route identifier (route_key)")
    args = parser.parse_args()

    run(args.route_key)
