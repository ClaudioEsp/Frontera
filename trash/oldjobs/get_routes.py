# jobs/get_routes.py

import os
import time  # you can remove later if not used elsewhere
import datetime as dt
import pandas as pd
from pathlib import Path
import hashlib

from pymongo import MongoClient
from orchestrator.jobs.dispatchtrack_client import DispatchTrackClient

# ---------- File / reports path ----------
PROJECT_DIR = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_DIR.parent

DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Mongo configuration ----------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_ROUTES_COLLECTION_NAME = os.getenv("MONGO_ROUTES_COLLECTION_NAME", "routes")


def _get_routes_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_ROUTES_COLLECTION_NAME]

    # 1 doc per route
    collection.create_index("id", unique=True)
    # index by date if you want to query by day
    collection.create_index("dispatch_date_param")
    return collection


def _normalize_date(date):
    if date is None:
        return dt.date.today().strftime("%Y-%m-%d")
    if isinstance(date, dt.date):
        return date.strftime("%Y-%m-%d")
    return str(date)


def _extract_dispatch_ids_from_route(route_obj: dict):
    stops = route_obj.get("stops") or route_obj.get("dispatches") or []

    dispatch_ids = []
    for stop in stops:
        if not isinstance(stop, dict):
            continue

        dispatch_id = (
            stop.get("id")
            or stop.get("order_id")
            or stop.get("shipment_id")
            or stop.get("identifier")
        )

        if dispatch_id:
            dispatch_ids.append(str(dispatch_id))

    return dispatch_ids


def _build_dispatch_status_fingerprint_from_route_obj(route_obj: dict) -> str:
    stops = route_obj.get("stops") or route_obj.get("dispatches") or []

    entries = []
    for stop in stops:
        if not isinstance(stop, dict):
            continue

        dispatch_id = (
            stop.get("id")
            or stop.get("order_id")
            or stop.get("shipment_id")
            or stop.get("identifier")
        )
        if not dispatch_id:
            continue

        status_id = (
            stop.get("status_id")
            or (stop.get("status") or {}).get("id")
        )
        substatus_code = (
            stop.get("substatus_code")
            or (stop.get("substatus") or {}).get("code")
        )

        entries.append(f"{dispatch_id}|{status_id}|{substatus_code}")

    entries.sort()
    payload = "||".join(entries)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------
# JOB 1: Fetch MINIFIED routes and save minimal route info to Mongo
# --------------------------------------------------------------------
def run(date=None, days_back: int = 0, minified=True):
    """
    Fetch routes for a given date, save CSV snapshot,
    and upsert minimal info into MongoDB (one document per route).
    """
    if date is None:
        target_date = dt.date.today() - dt.timedelta(days=days_back)
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        date_str = _normalize_date(date)

    client = DispatchTrackClient()
    print(
        f"[get_routes] Fetching routes from DispatchTrack for date {date_str} "
        f"(minified={minified}, days_back={days_back})..."
    )

    routes_json = client.get_routes(date=date_str, minified=minified)

    routes = (
        routes_json.get("response", {}).get("routes", [])
        if isinstance(routes_json, dict)
        else []
    )

    if not routes:
        print(f"[get_routes] No routes returned from API for {date_str}.")
        return

    # ---------- CSV snapshot ----------
    df = pd.json_normalize(routes, sep="__")
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = DATA_DIR / f"routes_{date_str}_{ts}.csv"
    df.to_csv(file_path, index=False)
    print(f"[get_routes] Saved CSV with {len(df)} routes to {file_path}")

    # ---------- Mongo upsert (minimal per-route info) ----------
    collection = _get_routes_collection()

    new_count = 0
    updated_count = 0

    for route in routes:
        route_id = route.get("id")
        if route_id is None:
            continue

        # list of dispatch identifiers for this route (what Job 2 will use)
        dispatch_ids = _extract_dispatch_ids_from_route(route)
        dispatch_status_hash = _build_dispatch_status_fingerprint_from_route_obj(route)
        now = dt.datetime.utcnow()

        # 👇 NEW: extract truck identifier from the route
        truck_obj = route.get("truck") or {}
        truck_identifier = (
            truck_obj.get("identifier")
            or truck_obj.get("code")
            or route.get("truck_identifier")
            or route.get("truck_id")
        )

        update_doc = {
            "$set": {
                "id": route_id,
                "dispatch_date_param": date_str,
                "last_seen_at": now,
                "dispatch_identifiers": dispatch_ids,
                "dispatches_count": len(dispatch_ids),
                "minified_dispatch_status_hash": dispatch_status_hash,
                # 👇 Save truck info for Job 2 (get_dispatches)
                "truck": truck_obj,
                "truck_identifier": truck_identifier,
            }
        }

        result = collection.update_one(
            {"id": route_id},
            update_doc,
            upsert=True,
        )

        if result.upserted_id is not None:
            new_count += 1
        elif result.modified_count > 0:
            updated_count += 1

    print(
        f"[get_routes] Upserted {len(routes)} routes for {date_str}. "
        f"New: {new_count}, Updated: {updated_count}"
    )
