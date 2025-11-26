# orchestrator/jobs/run_actualization.py

import os
import sys
import logging
import subprocess
from datetime import datetime, date, timezone, timedelta
from typing import Set, Dict, Any

from pymongo import MongoClient
from dotenv import load_dotenv

# Load env (.env at project root)
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.run_actualization")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
ROUTES_COLLECTION = os.getenv("ROUTES_COLLECTION", "routes")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")
META_COLLECTION = os.getenv("META_COLLECTION", "meta")  # meta/info collection


def _run_job(
    module: str,
    date_str: str | None = None,
    extra_args: list[str] | None = None,
) -> None:
    """
    Run a single job as:
      python -m <module> [--date YYYY-MM-DD] [extra args...]
    """
    cmd = [sys.executable, "-m", module]
    if date_str is not None:
        cmd.extend(["--date", date_str])
    if extra_args:
        cmd.extend(extra_args)

    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd)

    if result.returncode != 0:
        raise SystemExit(
            f"Job {module} failed with exit code {result.returncode}. "
            "Aborting actualization."
        )
    logger.info("Finished: %s", module)


def _extract_date_str(value) -> str | None:
    """
    Normalize any date-like value to 'YYYY-MM-DD' string.
    Accepts datetime, date, or string-like.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    # assume string like 'YYYY-MM-DD...' and take first 10 chars
    s = str(value)
    if len(s) >= 10:
        return s[:10]
    return None


def _get_dates_and_pages_for_open_routes() -> Dict[str, Dict[str, Any]]:
    """
    Returns a mapping of dates and pages for routes that are still open:

      {
        "YYYY-MM-DD": {
          "pages": set_of_int,
          "has_missing_page": bool,
        },
        ...
      }

    Logic:
      - Look at routes with is_closed != True (or missing).
      - Use route['date'] as date.
      - Use route['page'] (if present) to know which pages we need to re-fetch.
      - If any open route for a date has no page stored, we mark that date as
        has_missing_page=True so we later do a full scan for that date.
    """
    client = MongoClient(MONGO_URI)
    routes_col = client[DISPATCHTRACK_DB][ROUTES_COLLECTION]

    cursor = routes_col.find(
        {"is_closed": {"$ne": True}},
        projection={"date": 1, "page": 1},
    )

    result: Dict[str, Dict[str, Any]] = {}

    for doc in cursor:
        d = _extract_date_str(doc.get("date"))
        if not d:
            continue

        info = result.setdefault(d, {"pages": set(), "has_missing_page": False})

        page = doc.get("page")
        if page is None:
            # For this date we don't know the page for at least one open route,
            # so we must later fetch ALL pages for that date.
            info["has_missing_page"] = True
        else:
            try:
                info["pages"].add(int(page))
            except (TypeError, ValueError):
                info["has_missing_page"] = True

    client.close()
    return result


def _save_meta(dates_processed: Set[str]) -> None:
    """
    Save an entry with timestamp + dates processed.
    """
    client = MongoClient(MONGO_URI)
    meta_col = client[DISPATCHTRACK_DB][META_COLLECTION]

    now_utc = datetime.now(timezone.utc)

    meta_col.update_one(
        {"_id": "dispatches_actualization"},
        {
            "$set": {
                "last_run_utc": now_utc,
                "dates_processed": sorted(dates_processed),
            }
        },
        upsert=True,
    )

    client.close()
    logger.info(
        "Saved meta: last_run_utc=%s, dates_processed=%s",
        now_utc.isoformat(),
        sorted(dates_processed),
    )


def run(delta_time_update: int = 0, delta_day: int = 0) -> None:
    """
    Actualization process.

    Args:
      delta_time_update (int):
          Minimum number of hours that must have passed since the route's
          last_processed_at to consider it in this run.
          - If 0, do not filter by time (process all open routes).
          - last_processed_at is set at the end of the per-route pipeline.

      delta_day (int):
          Logical days back from *today* to run the process.
          Example:
            - today (real) = 2025-11-26
            - delta_day = 1  -> logical_today = 2025-11-25
          We will:
            - Treat logical_today as "today" (always fetch its routes).
            - Filter open routes by dates <= logical_today.
          This does NOT affect delta_time_update: time checks use real now().
    """

    logger.info(
        "Starting actualization process with delta_time_update=%d hours, delta_day=%d days",
        delta_time_update,
        delta_day,
    )

    now_utc = datetime.now(timezone.utc)
    logical_today = date.today() - timedelta(days=delta_day)
    logical_today_str = logical_today.isoformat()

    logger.info("Logical today resolved to %s (real today=%s)",
                logical_today_str, date.today().isoformat())

    # 1) Get dates/pages for open routes
    date_page_info = _get_dates_and_pages_for_open_routes()

    # Filter by logical_today: only process routes with date <= logical_today
    filtered_date_page_info: Dict[str, Dict[str, Any]] = {}
    for d, info in date_page_info.items():
        if d <= logical_today_str:
            filtered_date_page_info[d] = info

    date_page_info = filtered_date_page_info

    # 2) Ensure logical_today is always processed (we want daily new routes for that day)
    if logical_today_str not in date_page_info:
        date_page_info[logical_today_str] = {"pages": set(), "has_missing_page": True}

    dates_to_process = set(date_page_info.keys())

    logger.info(
        "Dates to process (with page info): %s",
        {
            d: {
                "pages": sorted(info["pages"]),
                "has_missing_page": info["has_missing_page"],
            }
            for d, info in date_page_info.items()
        },
    )

    for d in sorted(dates_to_process):
        info = date_page_info[d]
        pages = sorted(info["pages"])
        has_missing_page = info["has_missing_page"]

        logger.info(
            "Processing date %s (pages=%s, has_missing_page=%s)",
            d,
            pages,
            has_missing_page,
        )

        # 3.1) get_routes (page-aware)
        if has_missing_page or not pages:
            # Full scan of all pages for this date
            _run_job("orchestrator.jobs.get_routes", date_str=d)
        else:
            # Only re-fetch pages where there are open routes
            for page in pages:
                _run_job(
                    "orchestrator.jobs.get_routes",
                    date_str=d,
                    extra_args=["--page", str(page)],
                )

        # 3.2) Refresh full details for all open routes of this date
        _run_job("orchestrator.jobs.get_details_from_route", date_str=d)

        # 3.3) For each open route of this date, run the per-route pipeline
        client = MongoClient(MONGO_URI)
        routes_col = client[DISPATCHTRACK_DB][ROUTES_COLLECTION]

        # Base criteria: open routes of this date
        criteria: Dict[str, Any] = {
            "date": d,
            "is_closed": {"$ne": True},
        }

        # If delta_time_update > 0, only include routes not processed recently
        if delta_time_update > 0:
            threshold = now_utc - timedelta(hours=delta_time_update)
            criteria["$or"] = [
                {"last_processed_at": {"$lte": threshold}},
                {"last_processed_at": {"$exists": False}},
            ]

        open_routes_cursor = routes_col.find(
            criteria,
            projection={"route_key": 1},
        )

        route_docs = [doc for doc in open_routes_cursor if doc.get("route_key")]
        route_keys = [doc["route_key"] for doc in route_docs]

        logger.info(
            "Date %s has %d open routes to process after delta_time_update filter.",
            d,
            len(route_keys),
        )

        for route_key in route_keys:
            logger.info("Processing route_key=%s for date=%s", route_key, d)

            # 2) Extract/refresh dispatches for this route
            _run_job(
                "orchestrator.jobs.get_dispatches",
                extra_args=["--route-key", str(route_key)],
            )

            # 3) backfill tipo_orden from tags (for this route)
            _run_job(
                "orchestrator.jobs.backfill_tipo_orden_from_tags",
                extra_args=["--route-key", str(route_key)],
            )

            # 4) backfill promise_date from tags (for this route)
            _run_job(
                "orchestrator.jobs.backfill_promise_date_from_tags",
                extra_args=["--route-key", str(route_key)],
            )

            # 5) get CT (for this route)
            _run_job(
                "orchestrator.jobs.get_ct",
                extra_args=["--route-key", str(route_key)],
            )

            # 6) get substatus (for this route)
            _run_job(
                "orchestrator.jobs.get_substatus",
                extra_args=["--route-key", str(route_key)],
            )

            # 7) close route if all its dispatches are closed
            _run_job(
                "orchestrator.jobs.close_route_if_all_dispatches_closed",
                extra_args=["--route-key", str(route_key)],
            )

            # Update last_processed_at for this route
            routes_col.update_one(
                {"route_key": route_key},
                {"$set": {"last_processed_at": now_utc}},
            )

        client.close()

    # 4) Save meta info
    _save_meta(set(dates_to_process))

    logger.info("Actualization process completed successfully.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run dispatches/routes actualization pipeline."
    )
    parser.add_argument(
        "--delta-time-update",
        type=int,
        default=0,
        help="Minimum hours since last_processed_at to re-process a route (0 = no limit).",
    )
    parser.add_argument(
        "--delta-day",
        type=int,
        default=0,
        help="Logical days back from today to run the process (0 = today).",
    )
    args = parser.parse_args()

    run(delta_time_update=args.delta_time_update, delta_day=args.delta_day)
