# orchestrator/jobs/print_routes_with_incomplete_close.py

import os
import logging
from datetime import datetime

from pymongo import MongoClient
from dotenv import load_dotenv

# Load env (.env at project root)
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.print_routes_with_incomplete_close")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")


def run() -> None:
    """
    - Find all dispatches where cierre != true (including cierre missing/null/false)
    - Collect distinct route_key
    - Collect distinct route_dispatch_date
    - Print both sets
    """

    client = MongoClient(MONGO_URI)
    disp_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]

    cursor = disp_col.find(
        {"cierre": {"$ne": True}},
        projection={"route_key": 1, "route_dispatch_date": 1},
    )

    route_keys = set()
    dispatch_dates = set()
    total_docs = 0

    for doc in cursor:
        total_docs += 1

        rk = doc.get("route_key")
        if rk is not None:
            route_keys.add(rk)

        d = doc.get("route_dispatch_date")
        if d is not None:
            dispatch_dates.add(d)

    client.close()

    logger.info(
        "Found %d dispatches with cierre != true, %d distinct route_key, %d distinct route_dispatch_date",
        total_docs,
        len(route_keys),
        len(dispatch_dates),
    )

    print("=== Distinct route_key with cierre != true ===")
    for rk in sorted(route_keys):
        print(rk)

    print("\n=== Distinct route_dispatch_date with cierre != true ===")
    # route_dispatch_date is probably a datetime; sort safely via str()
    for d in sorted(dispatch_dates, key=lambda x: str(x)):
        if isinstance(d, datetime):
            print(d.isoformat())
        else:
            print(d)


if __name__ == "__main__":
    run()
