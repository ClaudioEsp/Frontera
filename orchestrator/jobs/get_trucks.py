import os
import datetime as dt
import pandas as pd
from pathlib import Path

from dispatchtrack_client import DispatchTrackClient

# Go up to PROYECTO_REPORTES and create data/raw there
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def run():
    """
    Fetch vehicles from DispatchTrack and store them as a timestamped CSV file.
    """
    client = DispatchTrackClient()
    print("[sync_vehicles] Fetching vehicles from DispatchTrack...")

    vehicles_json = client.get_vehicles()
    df = pd.DataFrame(vehicles_json)

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = DATA_DIR / f"vehicles_{ts}.csv"

    df.to_csv(file_path, index=False)
    print(f"[sync_vehicles] Saved {len(df)} vehicles to {file_path}")
