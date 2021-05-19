import requests

# Build the Data Resource Service Here as a class with each endpoint as a function.
# Do not iterate over paginated endpoints in this file.  Below are just samples


class AskNicelyClient:
    def __init__(self, config):
        self._BASE_URL = f"https://{config['subdomain']}.asknice.ly/api/v1"
        self._client = requests.Session()
        self._api_key = config["api_key"]

    def fetch_unsubscribed(self):
        url = f"{self._BASE_URL}/contacts/unsubscribed?X-apikey={self._api_key}"
        return self._client.get(url).json()
