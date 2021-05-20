import requests
from datetime import datetime
from singer.utils import strptime_to_utc


class AskNicelyClient:
    def __init__(self, config):
        self._base_url = f"https://{config['subdomain']}.asknice.ly/api/v1"
        self._api_key = config["api_key"]
        self._client = requests.Session()

    def fetch_unsubscribed(self):
        url = f"{self._base_url}/contacts/unsubscribed"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()

    def fetch_responses(
        self, page: int, page_size: int, start_time_utc: str, end_time_utc: str
    ) -> dict:
        start_time_unix = int(strptime_to_utc(start_time_utc).timestamp())
        end_time_unix = int(strptime_to_utc(end_time_utc).timestamp())
        url = f"{self._base_url}/responses/asc/{page_size}/{page}/{start_time_unix}/json/{end_time_unix}"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()

    def fetch_contact(self, contact_id: int) -> dict:
        url = f"{self._base_url}/contact/get/{contact_id}/id"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()
