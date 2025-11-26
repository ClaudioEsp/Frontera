# jobs/get_routes.py

import os
import logging
from datetime import datetime, timezone
from typing import Optional
from pymongo import MongoClient, UpdateOne

from .dispatchtrack_client import fetch_routes_page, DispatchTrackAPIError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_routes")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "dispatchtrack"
ROUTES_COLLECTION = "routes"


def _extract_route_key(route: dict) -> Optional[str]:
    """
    Try to extract a unique identifier for the route.

    We only accept real values, not None. Prefer number/route_number,
    then fall back to id.
    """
    for field in ("number", "route_number", "id"):
        value = route.get(field)
        if value is not None:
            return str(value)

    return None


def _upsert_routes_for_page(
    routes_col,
    date_str: str,
    page: int,
) -> int:
    """
    Fetch and upsert routes for a single (date, page).
    Returns the number of routes upserted/updated.

    New behavior:
      - On INSERT, routes start as is_closed=False so the right-block
        can process them and later decide when to mark them closed.
      - On UPDATE, we do NOT touch is_closed (so closed routes stay closed).
    """
    try:
        routes_page = fetch_routes_page(date_str, page)
    except DispatchTrackAPIError:
        raise

    if not routes_page:
        logger.info("No routes returned for date=%s page=%s", date_str, page)
        return 0

    logger.info(
        "Fetched %d routes for date=%s page=%s",
        len(routes_page),
        date_str,
        page,
    )

    bulk_ops = []
    total_routes = 0
    now_utc = datetime.now(timezone.utc)

    for route in routes_page:
        key = _extract_route_key(route)
        if not key:
            logger.warning("Skipping route without key: %s", route)
            continue

        # Save the date AND the page where this route was found
        meta = {
            "date": date_str,
            "page": page,
            "minified_raw": route,
            "last_refreshed_at": now_utc,
        }

        # IMPORTANT:
        # - We always update date/page/minified_raw/last_refreshed_at.
        # - We only set created_at, has_full_details, is_closed on first insert.
        update = {
            "$set": meta,
            "$setOnInsert": {
                "created_at": now_utc,
                "has_full_details": False,  # get_details_from_route will fill full_raw
                "is_closed": False,         # route starts "open" until proven closed
            },
        }

        bulk_ops.append(
            UpdateOne(
                {"route_key": key},
                update,
                upsert=True,
            )
        )
        total_routes += 1

    if bulk_ops:
        routes_col.bulk_write(bulk_ops, ordered=False)

    return total_routes


def run(date_str: str, specific_page: int | None = None) -> None:
    """
    Fetch routes for the given date from DispatchTrack and upsert into Mongo.

    Modes:
      - If specific_page is None:
          Iterate page=1..N until API returns empty.
      - If specific_page is an int:
          Only fetch that single page.

    Each route stores:
      - date: YYYY-MM-DD
      - page: the page number of the routes listing
      - minified_raw
      - last_refreshed_at
      - created_at (on first insert)
      - has_full_details (False on first insert)
      - is_closed (False on first insert; later the right block will flip it)
    """
    logger.info(
        "Starting get_routes for date=%s%s",
        date_str,
        f" (page={specific_page})" if specific_page is not None else "",
    )

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    routes_col = db[ROUTES_COLLECTION]

    total_routes = 0

    if specific_page is not None:
        # Only this page
        total_routes += _upsert_routes_for_page(routes_col, date_str, specific_page)
    else:
        # Full scan: page 1..N
        page = 1
        while True:
            count = _upsert_routes_for_page(routes_col, date_str, page)
            if count == 0:
                logger.info("No more routes at page=%s", page)
                break
            total_routes += count
            page += 1

    client.close()
    logger.info(
        "Finished get_routes for %s. Upserted/updated ~%d routes.",
        date_str,
        total_routes,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch routes for a day.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    parser.add_argument(
        "--page",
        type=int,
        default=None,
        help="If provided, only fetch this page for the given date.",
    )
    args = parser.parse_args()

    run(args.date, specific_page=args.page)
