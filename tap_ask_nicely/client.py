import requests
from datetime import datetime
from singer.utils import strptime_to_utc



class AskNicelyClient:
    def __init__(self, config):
        self._BASE_URL = f"https://{config['subdomain']}.asknice.ly/api/v1"
        self._api_key = config["api_key"]
        self._client = requests.Session()

    def fetch_unsubscribed(self):
        url = f"{self._BASE_URL}/contacts/unsubscribed?X-apikey={self._api_key}"
        return self._client.get(url).json()


    def fetch_responses(
        self, page: int, page_size: int, start_time_utc: str, end_time_utc: str
    ) -> dict:
        start_time_unix = int(strptime_to_utc(start_time_utc).timestamp())
        end_time_unix = int(strptime_to_utc(end_time_utc).timestamp())
        url = f"{self._base_url}/api/v1/responses/asc/{page_size}/{page}/{start_time_unix}/json/{end_time_unix}"
        params = {"X-apikey": self._api_key}
        return self._client.get(url, params=params).json()
