# jobs/dispatchtrack_client.py

import os
import logging
from typing import Any, Dict, List, Optional, Union

from pathlib import Path

from dotenv import load_dotenv

# --- load ../.env ---
BASE_DIR = Path(__file__).resolve().parent      # jobs/
ENV_PATH = BASE_DIR.parent.parent.parent / ".env"            # ../.env

load_dotenv(dotenv_path=ENV_PATH)
# ---------------------

import requests




logger = logging.getLogger(__name__)

DISPATCHTRACK_API_BASE_URL = os.getenv(
    "DISPATCHTRACK_API_BASE_URL",
    "https://paris.dispatchtrack.com/api/external/v1",
)

DISPATCHTRACK_API_TOKEN = os.getenv("DISPATCHTRACK_TOKEN")

class DispatchTrackAPIError(Exception):
    """Custom exception for DispatchTrack API errors."""
    pass


def _auth_headers() -> Dict[str, str]:
    if not DISPATCHTRACK_API_TOKEN:
        raise DispatchTrackAPIError("DISPATCHTRACK_TOKEN is not set in env.")
    return {
        "X-AUTH-TOKEN": f"{DISPATCHTRACK_API_TOKEN}",
    }


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Low-level GET wrapper that:
    - prefixes base URL
    - adds auth headers
    - raises DispatchTrackAPIError on non-2xx
    """
    url = f"{DISPATCHTRACK_API_BASE_URL}{path}"
    headers = _auth_headers()

    logger.debug("Requesting %s with params=%s", url, params)
    resp = requests.get(url, headers=headers, params=params, timeout=60)

    if not resp.ok:
        raise DispatchTrackAPIError(
            f"GET {url} failed: {resp.status_code} {resp.text}"
        )

    try:
        return resp.json()
    except Exception as exc:
        raise DispatchTrackAPIError(
            f"Failed to parse JSON from {url}: {exc}"
        ) from exc


# ---------- High level helpers ----------

def fetch_routes_page(date_str: str, page: int) -> List[Dict[str, Any]]:
    """
    Fetch a single page of routes.

    Endpoint:
      GET /routes?date=YYYY-MM-DD&minified=true&page=N

    Returns:
      A list of route dicts (empty list means no more pages).
    """
    params = {
        "date": date_str,
        "minified": "true",
        "page": page,
    }
    data = _get("/routes", params=params)

    # Unwrap common envelope shapes
    # 1) Plain list
    if isinstance(data, list):
        return data

    # 2) Dict with "routes"
    if isinstance(data, dict):
        # Some APIs do: { "routes": [...] }
        if "routes" in data and isinstance(data["routes"], list):
            return data["routes"]

        # Your case: { "status": ..., "response": ... }
        if "response" in data:
            inner = data["response"]

            # Could be: { "routes": [...] }
            if isinstance(inner, dict) and "routes" in inner and isinstance(inner["routes"], list):
                return inner["routes"]

            # Or directly a list
            if isinstance(inner, list):
                return inner

    # If we reach here, we don't know how to interpret it
    raise DispatchTrackAPIError(
        f"Unexpected payload for routes page: type={type(data)} "
        f"keys={getattr(data, 'keys', lambda: [])()}"
    )



def fetch_route_details(route_number: Union[str, int]) -> Dict[str, Any]:
    """
    Fetch full details of a single route.

    Endpoint:
      GET /routes/:route_number
    """
    path = f"/routes/{route_number}"
    return _get(path)
