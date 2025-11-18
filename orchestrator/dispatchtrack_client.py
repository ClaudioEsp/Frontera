import os
import requests
from dotenv import load_dotenv

load_dotenv()

DISPATCHTRACK_BASE_URL = os.getenv("DISPATCHTRACK_BASE_URL")
DISPATCHTRACK_TOKEN = os.getenv("DISPATCHTRACK_TOKEN")  # put your token in .env



class DispatchTrackClient:
    def __init__(self, base_url: str = DISPATCHTRACK_BASE_URL, token: str = DISPATCHTRACK_TOKEN):
        if base_url is None:
            raise ValueError("DISPATCHTRACK_BASE_URL is not set and no base_url was provided")
        if token is None:
            raise ValueError("DISPATCHTRACK_TOKEN not set")

        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            # DispatchTrack expects this header, not "Authorization: Bearer"
            "X-AUTH-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def get_vehicles(self, **params):
        """
        Calls https://paris.dispatchtrack.com/api/external/v1/trucks
        """
        url = f"{self.base_url}/trucks"
        resp = self.session.get(url, params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def get_routes(self, date: str, **params):
        """
        Calls /routes with a required date parameter (YYYY-MM-DD).
        Example: GET /routes?date=2025-01-22
        """
        url = f"{self.base_url}/routes"
        params = {**params, "date": date}

        resp = self.session.get(url, params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()