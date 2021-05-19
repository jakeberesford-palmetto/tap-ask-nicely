import json
import requests
from datetime import datetime
from singer.utils import strptime_to_utc


class AskNicelyClient:
    def __init__(self, base_url: str, api_key: str):
        self._base_url = base_url
        self._api_key = api_key
        self._client = requests.Session()

    def fetch_responses(
        self, page: int, page_size: int, start_time_utc: str, end_time_utc: str
    ) -> dict:
        start_time_unix = int(strptime_to_utc(start_time_utc).timestamp())
        end_time_unix = int(strptime_to_utc(end_time_utc).timestamp())
        url = f"{self._base_url}/api/v1/responses/asc/{page_size}/{page}/{start_time_unix}/json/{end_time_unix}"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()

    def fetch_contact(self, contact_id: int) -> dict:
        url = f"{self._base_url}/api/v1/contact/get/{contact_id}/id"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()
