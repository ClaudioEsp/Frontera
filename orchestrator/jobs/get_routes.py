import os
import datetime as dt
import pandas as pd
from pathlib import Path

from pymongo import MongoClient
from dispatchtrack_client import DispatchTrackClient

# ---------- File / reports path (../data/raw, data is OUTSIDE the project) ----------
# PROJECT_DIR = .../PROYECTO_REPORTES
PROJECT_DIR = Path(__file__).resolve().parents[2]

# BASE_DIR = parent folder that contains both PROYECTO_REPORTES and data
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
    collection.create_index("id", unique=True)  # route id
    return collection


def _normalize_date(date):
    """
    Accepts:
      - None -> use today's date
      - datetime.date -> format
      - 'YYYY-MM-DD' string -> just return it
    """
    if date is None:
        return dt.date.today().strftime("%Y-%m-%d")
    if isinstance(date, dt.date):
        return date.strftime("%Y-%m-%d")
    # assume it's already a string 'YYYY-MM-DD'
    return str(date)


def run(date=None, minified=True):
    """
    Fetch routes for a given date, save CSV snapshot,
    and upsert into MongoDB.

    - If date is None -> uses today's date.
    - You can call run("2025-01-22") to fetch another day.
    """
    date_str = _normalize_date(date)

    client = DispatchTrackClient()
    print(f"[get_routes] Fetching routes from DispatchTrack for date {date_str} (minified={minified})...")

    routes_json = client.get_routes(date=date_str, minified=minified)

    # structure: {"status": "...", "response": {"routes": [...]}}
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

    # ---------- Mongo upsert ----------
    collection = _get_routes_collection()
    now = dt.datetime.utcnow()

    new_count = 0
    updated_count = 0

    for route in routes:
        route_id = route.get("id")
        if route_id is None:
            continue

        route["last_seen_at"] = now
        route["dispatch_date_param"] = date_str  # the date you queried with

        result = collection.update_one(
            {"id": route_id},         # match by route id
            {"$set": route},          # update full route doc
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
